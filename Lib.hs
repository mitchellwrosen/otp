{-# LANGUAGE BangPatterns, DataKinds, DeriveAnyClass, DerivingStrategies,
             ExistentialQuantification, LambdaCase, PatternSynonyms,
             RankNTypes, RecursiveDo, ScopedTypeVariables, StandaloneDeriving,
             TypeApplications, TypeOperators, UnicodeSyntax #-}

-- TODO CancelThread newtype, use this instead of KillThread, do not expose
-- thread ids

module Lib
  ( Supervisor
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
import Data.HashMap.Strict (HashMap)
import Data.IORef
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
      -> IO WorkerId
      )
      (WorkerId -> IO ())

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
  = Worker !(IO ()) !(STM a)

type WorkerId
  = Int

freshWorkerId :: IO WorkerId
freshWorkerId = undefined

waitWorker :: Worker a -> IO a
waitWorker =
  atomically . waitWorkerSTM

waitWorkerSTM :: Worker a -> STM a
waitWorkerSTM (Worker _ result) =
  result

cancelWorker :: Worker a -> IO ()
cancelWorker (Worker cancel _) =
  cancel

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
      !(MVar WorkerId)

  | CancelWorker
      !WorkerId

  -- A worker thread died with some exception.
  --
  -- If it's ThreadKilled, ok. Somebody explicitly tried to kill it, and we
  -- honor that request, even if it was a Permant worker.
  --
  -- Otherwise, restart it (and update its associated IORef ThreadId).
  | WorkerThreadDied
      !WorkerId
      !SomeException

  -- A worker thread ended naturally.
  --
  -- If it was a Permanent worker, whoops, it wasn't meant to end. Restart it.
  | WorkerThreadEnded
      !WorkerId

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
      enqueue :: WorkerType -> ((∀ a. IO a -> IO a) -> IO ()) -> IO WorkerId
      enqueue workerTy action = do
        workerIdVar <- newEmptyMVar
        atomically (writeTQueue mailbox (RunWorker workerTy action workerIdVar))
        takeMVar workerIdVar

      cancel :: WorkerId -> IO ()
      cancel workerId =
        atomically (writeTQueue mailbox (CancelWorker workerId))
    in
      newMVar (SupervisorAlive enqueue cancel)

  let
    -- Fork a new worker. It's assumed (required) that this function is called
    -- in the MaskedUninterruptible state, and that the action provided has
    -- asynchronous exceptions unmasked.
    --
    -- When the worker ends, naturally or by exception, a corresponding message
    -- is written to the supervisor's mailbox. So even though we forked this
    -- thread with "slave-thread" semantics, we won't ever be sniped by its
    -- exceptions.
    forkWorker :: WorkerId -> IO () -> IO ThreadId
    forkWorker workerId action = do
      -- FIXME unmask timing
      SlaveThread.fork $
        catch
          (do
            action
            atomically (writeTQueue mailbox (WorkerThreadEnded workerId)))
          (atomically . writeTQueue mailbox . WorkerThreadDied workerId)

  thread :: ThreadId <-
    -- FIXME explain `finally` vs. forkFinally
    SlaveThread.fork . (`finally` swapMVar statusVar SupervisorDead) $
      -- FIXME unmask timing
      uninterruptibleMask $ \unmask -> do
        myThread <- myThreadId

        let
          -- The main supervisor loop. Handle mailbox messages one by one, with
          -- asynchronous exceptions masked.
          loop :: [Double] -> HashMap WorkerId RunningWorkerInfo -> IO ()
          loop restarts !children =
            unmask (atomically (readTQueue mailbox)) >>= \case
              RunWorker workerTy action workerIdVar -> do
                workerId <- freshWorkerId
                thread <- forkWorker workerId (action unmask)
                putMVar workerIdVar workerId
                threadRef <- newIORef thread
                loop
                  restarts
                  (HashMap.insert
                    workerId
                    (RunningWorkerInfo workerTy action threadRef)
                    children)

              CancelWorker workerId ->
                case lookupRunningWorkerInfo workerId of
                  Nothing ->
                    pure ()

                  Just (RunningWorkerInfo _ _ threadRef) -> do
                    killThread =<< readIORef threadRef
                    loop restarts (HashMap.delete workerId children)

              WorkerThreadDied workerId ex ->
                case fromException ex of
                  Just ThreadKilled ->
                    loop restarts (HashMap.delete workerId children)

                  _ -> do
                    now <- getMonotonicTime
                    let restarts' = takeWhile (>= (now - period)) (now : restarts)
                    when (fromIntegral (length restarts') == intensity)
                      (throwIO (MaxRestartIntensityReached myThread))
                    case lookupRunningWorkerInfo workerId of
                      Nothing ->
                        pure ()

                      Just (RunningWorkerInfo _ action threadRef) -> do
                        threadId <- forkWorker workerId (action unmask)
                        writeIORef threadRef threadId
                        loop restarts' children

              WorkerThreadEnded workerId ->
                case lookupRunningWorkerInfo workerId of
                  Nothing ->
                    pure ()

                  Just (RunningWorkerInfo workerTy action threadRef) ->
                    case workerTy of
                      Permanent -> do
                        now <- getMonotonicTime
                        let restarts' = takeWhile (>= (now - period)) (now : restarts)
                        when (fromIntegral (length restarts') == intensity)
                          (throwIO (MaxRestartIntensityReached myThread))
                        threadId <- forkWorker workerId (action unmask)
                        writeIORef threadRef threadId
                        loop restarts' children

                      Transient ->
                        loop restarts (HashMap.delete workerId children)

            where
              lookupRunningWorkerInfo :: WorkerId -> Maybe RunningWorkerInfo
              lookupRunningWorkerInfo workerId =
                HashMap.lookup workerId children

        loop [] mempty

  pure (Supervisor thread statusVar)

-- | Fork a /transient/ worker thread.
transient :: Supervisor -> IO a -> IO (Worker a)
transient (Supervisor thread statusVar) action =
  readMVar statusVar >>= \case
    SupervisorDead ->
      throwIO (SupervisorVanished thread)

    SupervisorAlive enqueue cancel -> do
      resultVar <- newEmptyTMVarIO
      workerId <-
        enqueue Transient $ \unmask -> do
          result <- unmask action
          atomically (putTMVar resultVar result)
      pure (Worker (cancel workerId) (readTMVar resultVar))

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

    SupervisorAlive enqueue cancel -> do
      workerId <- enqueue Permanent ($ action)
      pure (Worker (cancel workerId) retry)

-- | Fork a /permanent/ worker thread.
permanent_ :: Supervisor -> IO () -> IO ()
permanent_ sup action =
  void (permanent sup action)
