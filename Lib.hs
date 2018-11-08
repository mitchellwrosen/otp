{-# LANGUAGE DataKinds, DeriveAnyClass, DerivingStrategies,
             ExistentialQuantification, LambdaCase, OverloadedLabels,
             PatternSynonyms, RankNTypes, RecursiveDo, ScopedTypeVariables,
             StandaloneDeriving, TypeApplications, TypeOperators,
             ViewPatterns, UnicodeSyntax #-}

module Lib
  ( Supervisor
  , supervisorThreadId
  , supervisor
  , transient
  , permanent
  ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Data.Function
import Data.HashMap.Strict (HashMap)
import Data.IORef
import Data.Maybe
import Named
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
  | SupervisorAlive (WorkerType -> IO () -> IO (IO ThreadId))

-- | A 'SupervisorVanished' exception is thrown if you attempt to create a new
-- child of a dead supervisor.
--
-- A supervisor can die in three ways:
--
-- * You kill it with 'killThread', in which case it kills all of its children,
--   runs their finalizers, then ends gracefully.
--
-- * Its maximum restart intensity threshold is met, in which case it kills all
--   of its children, runs their finalizers, then throws a '
data SupervisorVanished
  = SupervisorVanished !ThreadId
  deriving stock (Show)
  deriving anyclass (Exception)

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
      !(IO ())
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
      !(IO ())
      !(IORef ThreadId)

-- | Get a supervisor's thread id.
supervisorThreadId :: Supervisor -> ThreadId
supervisorThreadId (Supervisor thread _) =
  thread

-- | Fork a supervisor thread with the given /intensity/ and /period/.
--
-- If any worker is restarted more than /intensity/ times within /period/
-- seconds, all workers are killed, finalized, and the supervisor
supervisor
  :: "intensity" :! Natural
  -> "period" :! Double
  -> IO Supervisor
supervisor (arg #intensity -> intensity) (arg #period -> period) = do
  mailbox :: TQueue Message <-
    newTQueueIO

  statusVar :: MVar SupervisorStatus <-
    (newMVar . SupervisorAlive) $ \ty action -> do
      threadVar <- newEmptyMVar
      atomically (writeTQueue mailbox (RunWorker ty action threadVar))
      takeMVar threadVar

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
          -- FIXME
          SlaveThread.fork $
            catch
              (do
                action
                atomically (writeTQueue mailbox (WorkerThreadEnded thread)))
              (atomically . writeTQueue mailbox . WorkerThreadDied thread)
      pure thread

  thread :: ThreadId <-
    -- FIXME
    SlaveThread.fork $ (`finally` swapMVar statusVar SupervisorDead) $
      uninterruptibleMask $ \unmask -> do
        let
          -- The main supervisor loop. Handle mailbox messages one by one, with
          -- asynchronous exceptions masked.
          loop :: HashMap ThreadId RunningWorkerInfo -> IO ()
          loop children =
            unmask (atomically (readTQueue mailbox)) >>= \case
              RunWorker ty action threadVar -> do
                thread <- forkWorker (unmask action)
                threadRef <- newIORef thread
                putMVar threadVar (readIORef threadRef)
                loop $
                  HashMap.insert
                    thread
                    (RunningWorkerInfo ty action threadRef)
                    children

              WorkerThreadDied thread ex ->
                case fromException ex of
                  Just ThreadKilled ->
                    loop (HashMap.delete thread children)

                  _ -> do
                    let RunningWorkerInfo ty action threadRef = child thread
                    thread' <- forkWorker (unmask action)
                    writeIORef threadRef thread'
                    loop $
                      children
                        & HashMap.delete thread
                        & HashMap.insert thread' (RunningWorkerInfo ty action threadRef)

              WorkerThreadEnded thread ->
                case child thread of
                  RunningWorkerInfo Permanent action threadRef -> do
                    thread' <- forkWorker (unmask action)
                    writeIORef threadRef thread'
                    loop $
                      children
                        & HashMap.delete thread
                        & HashMap.insert thread' (RunningWorkerInfo Permanent action threadRef)

                  RunningWorkerInfo Transient _ _ ->
                    loop (HashMap.delete thread children)

            where
              child :: ThreadId -> RunningWorkerInfo
              child thread =
                fromJust (HashMap.lookup thread children)

        loop mempty

  pure (Supervisor thread statusVar)

-- | Fork a /transient/ worker thread.
transient :: Supervisor -> IO () -> IO (IO ThreadId)
transient =
  spawn Transient

-- | Fork a /permanent/ worker thread.
permanent :: Supervisor -> IO () -> IO (IO ThreadId)
permanent =
  spawn Permanent

spawn :: WorkerType -> Supervisor -> IO () -> IO (IO ThreadId)
spawn ty (Supervisor thread statusVar) action =
  readMVar statusVar >>= \case
    SupervisorDead ->
      throwIO (SupervisorVanished thread)

    SupervisorAlive enqueue ->
      enqueue ty action
