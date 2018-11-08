{-# LANGUAGE BangPatterns, DataKinds, DeriveAnyClass, DerivingStrategies,
             ExistentialQuantification, LambdaCase, PatternSynonyms,
             RankNTypes, RecursiveDo, ScopedTypeVariables, StandaloneDeriving,
             TypeApplications, TypeOperators, UnicodeSyntax #-}

-- TODO race condition: kill thread misses its mark

module Lib
  ( Supervisor
  , supervisorThreadId
  , supervisor
  , transient
  , transient_
  , permanent
  , permanent_
  , Worker
  , waitWorker
  , waitWorkerSTM
  , cancelWorker
  , SupervisorVanished(..)
  , MaxRestartIntensityReached(..)
  ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Function
import Data.HashMap.Strict (HashMap)
import Data.IORef
import Data.Maybe
import Data.Void
import GHC.Clock (getMonotonicTime)
import Numeric.Natural

import qualified Data.HashMap.Strict as HashMap
import qualified SlaveThread

-- | A handle to a supervisor thread.
data Supervisor
  = Supervisor
      !ThreadId
      !(MVar SupervisorStatus)

-- The status of the supervisor. It's alive until it's not. Attempting to use
-- a dead supervisor will throw a SupervisorVanished exception.
data SupervisorStatus
  = SupervisorDead
  | SupervisorAlive
      (  WorkerType
      -> ((∀ a. IO a -> IO a) -> IO ())
      -> IO (IO ThreadId)
      )

-- | A 'SupervisorVanished' exception is thrown if you attempt to create a new
-- child of a dead supervisor.
--
-- A supervisor can die in two ways:
--
-- * You kill it with 'killThread', in which case it kills all of its children,
--   runs their finalizers, then ends gracefully.
--
-- * Its maximum restart intensity threshold is met, in which case it kills all
--   of its children, runs their finalizers, then throws a
--   'MaxRestartIntensityReached' exception.
data SupervisorVanished
  = SupervisorVanished !ThreadId
  deriving stock (Show)
  deriving anyclass (Exception)

-- | A 'MaxRestartIntensityReached' exception is thrown by a supervisor if more
-- than /intensity/ restarts occur within /period/ seconds.
data MaxRestartIntensityReached
  = MaxRestartIntensityReached !ThreadId
  deriving stock (Show)
  deriving anyclass (Exception)

data Worker a
  = Worker !(IO ThreadId) !(STM a)

waitWorker :: Worker a -> IO a
waitWorker =
  atomically . waitWorkerSTM

waitWorkerSTM :: Worker a -> STM a
waitWorkerSTM (Worker _ result) =
  result

-- TODO race condition-y!!
cancelWorker :: Worker a -> IO ()
cancelWorker (Worker thread _) =
  killThread =<< thread

data WorkerType
  = Transient
  | Permanent

-- A supervisor messge.
data Message
  -- A request to run a new worker.
  --
  -- Contains the worker type, action to run, and MVar to put an IORef
  -- containing the worker's ThreadId into.
  --
  -- This allows the enqueing thread to block until we've spawned the worker.
  --
  -- It's an IO ThreadId, not a ThreadId, because the ThreadId will change if
  -- the worker fails and is restarted.
  = RunWorker
      !WorkerType
      !((∀ a. IO a -> IO a) -> IO ())
      !(MVar (IO ThreadId))

  -- A worker thread died with some exception.
  --
  -- If it's ThreadKilled, ok. Somebody explicitly tried to kill it, and we
  -- honor that request, even if it was a Permant worker.
  --
  -- Otherwise, restart it (and update its associated IORef ThreadId).
  | WorkerThreadDied
      ThreadId
      !SomeException

  -- A worker thread ended naturally.
  --
  -- If it was a Permanent worker, whoops, it wasn't meant to end. Restart it.
  | WorkerThreadEnded
      ThreadId

data RunningWorkerInfo
  = RunningWorkerInfo
      !WorkerType
      !((∀ a. IO a -> IO a) -> IO ())
      !(IORef ThreadId)

-- | Get a supervisor's thread id.
supervisorThreadId :: Supervisor -> ThreadId
supervisorThreadId (Supervisor thread _) =
  thread

-- | Fork a supervisor thread with the given /intensity/ and /period/.
--
-- If more than /intensity/ restarts occur within /period/ seconds, all workers
-- are killed, finalized, and the supervisor throws
-- 'MaxRestartIntensityReached'.
--
-- The purpose of this mechanism is to prevent an infinite loop of crashes and
-- restarts in case of a misconfiguration or other unrecoverable error.
--
-- The smallest acceptable /intensity/ is 1, and /period/ must be positive
-- (though exceedingly small values offer effectively no protection).
supervisor :: (Natural, Double) -> IO Supervisor
supervisor (intensity, period) = do
  mailbox :: TQueue Message <-
    newTQueueIO

  statusVar :: MVar SupervisorStatus <-
    let
      enqueue :: WorkerType -> ((∀ a. IO a -> IO a) -> IO ()) -> IO (IO ThreadId)
      enqueue ty action = do
        threadVar <- newEmptyMVar
        atomically (writeTQueue mailbox (RunWorker ty action threadVar))
        takeMVar threadVar
    in
      newMVar (SupervisorAlive enqueue)

  let
    -- Fork a new worker. It's assumed (required) that this function is called
    -- in the MaskedUninterruptible state, and that the action provided has
    -- asynchronous exceptions unmasked.
    --
    -- When the worker ends, naturally or by exception, a corresponding message
    -- is written to the supervisor's mailbox. So even though we forked this
    -- thread with "slave-thread" semantics, we won't ever be sniped by its
    -- exceptions.
    forkWorker :: IO () -> IO ThreadId
    forkWorker action = do
      rec
        thread <-
          -- FIXME unmask timing
          SlaveThread.fork $
            catch
              (do
                action
                atomically (writeTQueue mailbox (WorkerThreadEnded thread)))
              (atomically . writeTQueue mailbox . WorkerThreadDied thread)
      pure thread

  thread :: ThreadId <-
    -- FIXME explain `finally` vs. forkFinally
    SlaveThread.fork . (`finally` swapMVar statusVar SupervisorDead) $
      -- FIXME unmask timing
      uninterruptibleMask $ \unmask -> do
        myThread <- myThreadId

        let
          -- The main supervisor loop. Handle mailbox messages one by one, with
          -- asynchronous exceptions masked.
          loop :: [Double] -> HashMap ThreadId RunningWorkerInfo -> IO ()
          loop restarts children =
            unmask (atomically (readTQueue mailbox)) >>= \case
              RunWorker ty action threadVar -> do
                thread <- forkWorker (action unmask)
                threadRef <- newIORef thread
                putMVar threadVar (readIORef threadRef)
                let !children' =
                      HashMap.insert
                        thread
                        (RunningWorkerInfo ty action threadRef)
                        children
                loop restarts children'

              WorkerThreadDied thread ex ->
                case fromException ex of
                  Just ThreadKilled ->
                    loop restarts (HashMap.delete thread children)

                  _ -> do
                    now <- getMonotonicTime
                    let restarts' = takeWhile (>= (now - period)) (now : restarts)
                    when (fromIntegral (length restarts') == intensity)
                      (throwIO (MaxRestartIntensityReached myThread))

                    let RunningWorkerInfo ty action threadRef = child thread
                    thread' <- forkWorker (action unmask)
                    writeIORef threadRef thread'
                    let !children' =
                          children
                            & HashMap.delete thread
                            & HashMap.insert thread' (RunningWorkerInfo ty action threadRef)
                    loop restarts' children'

              WorkerThreadEnded thread ->
                case child thread of
                  RunningWorkerInfo Permanent action threadRef -> do
                    now <- getMonotonicTime
                    let restarts' = takeWhile (>= (now - period)) (now : restarts)
                    when (fromIntegral (length restarts') == intensity)
                      (throwIO (MaxRestartIntensityReached myThread))

                    thread' <- forkWorker (action unmask)
                    writeIORef threadRef thread'

                    let !children' =
                          children
                            & HashMap.delete thread
                            & HashMap.insert thread' (RunningWorkerInfo Permanent action threadRef)
                    loop restarts' children'

                  RunningWorkerInfo Transient _ _ ->
                    loop restarts (HashMap.delete thread children)

            where
              child :: ThreadId -> RunningWorkerInfo
              child thread =
                fromJust (HashMap.lookup thread children)

        loop [] mempty

  pure (Supervisor thread statusVar)

-- | Fork a /transient/ worker thread.
transient :: Supervisor -> IO a -> IO (Worker a)
transient (Supervisor thread statusVar) action =
  readMVar statusVar >>= \case
    SupervisorDead ->
      throwIO (SupervisorVanished thread)
    SupervisorAlive enqueue -> do
      resultVar <- newEmptyTMVarIO
      threadRef <-
        enqueue Transient $ \unmask -> do
          result <- unmask action
          atomically (putTMVar resultVar result)
      pure (Worker threadRef (readTMVar resultVar))

-- | Fork a /transient/ worker thread.
transient_ :: Supervisor -> IO () -> IO ()
transient_ sup action =
  void (transient sup action)

-- | Fork a /permanent/ worker thread.
permanent :: Supervisor -> IO () -> IO (Worker Void)
permanent (Supervisor thread statusVar) action =
  readMVar statusVar >>= \case
    SupervisorDead ->
      throwIO (SupervisorVanished thread)
    SupervisorAlive enqueue -> do
      threadRef <- enqueue Permanent ($ action)
      pure (Worker threadRef retry)

-- | Fork a /permanent/ worker thread.
permanent_ :: Supervisor -> IO () -> IO ()
permanent_ sup action =
  void (permanent sup action)
