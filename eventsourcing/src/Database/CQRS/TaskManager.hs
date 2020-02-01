{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Database.CQRS.TaskManager
  ( TaskManager
  , Task(..)
  , runTaskManager
  ) where

import Control.Concurrent (threadDelay)
import Control.Monad (forever, when)
import Control.Monad.Trans
import Data.Hashable (Hashable)
import Data.List (find, sortOn)
import Data.Maybe (isNothing, mapMaybe)
import Pipes ((>->))

import qualified Control.Monad.Except       as Exc
import qualified Control.Monad.State.Strict as St
import qualified Data.Time                  as T
import qualified Pipes

import qualified Database.CQRS                          as CQRS
import qualified Database.CQRS.ReadModel.AggregateStore as CQRS.AS

-- | A task as accumulated by a 'TaskManager'.
data Task action = Task
  { action :: action
  , from   :: Maybe T.UTCTime -- ^ Time from which the task can be run.
  }

-- | Projection aggregating a list of tasks from a stream of events together
-- with some state.
type TaskManager event action st =
  CQRS.Aggregator event ([Task action], st)

-- | Repeatedly loop through the streams and run the tasks.
--
-- If it finds no work, it waits 5 minutes before the next run or less if it
-- knows of an earlier planned task.
runTaskManager
  :: forall streamFamily action st m.
     ( Exc.MonadError CQRS.Error m
     , CQRS.Stream m (CQRS.StreamType streamFamily)
     , CQRS.StreamFamily m streamFamily
     , Hashable (CQRS.StreamIdentifier streamFamily)
     , MonadIO m
     , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , Ord (CQRS.StreamIdentifier streamFamily)
     , Show (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     )
  => streamFamily
  -> (CQRS.StreamIdentifier streamFamily -> st)
     -- ^ Initial state of the task manager for any given stream.
  -> TaskManager
      (CQRS.EventWithContext' (CQRS.StreamType streamFamily))
      action st
  -> (action -> m ())
  -> m ()
runTaskManager streamFamily mkInitState taskManager runAction = do
  as <- CQRS.AS.makeAggregateStore
    streamFamily taskManager (([],) . mkInitState) 0.1 1000

  forever $ do
    -- Next run in 5 minutes (unless changed.)
    initialNextRun <- T.addUTCTime (5 * 60) <$> liftIO T.getCurrentTime

    mNextRun <-
      flip St.execStateT (Just initialNextRun) . Pipes.runEffect $ do
        Pipes.hoist lift (CQRS.latestEventIdentifiers streamFamily)
        >-> streamProcessor as

    case mNextRun of
      Nothing -> pure () -- Do not wait.
      Just nextRun -> liftIO $ do
        now <- T.getCurrentTime
        when (now < nextRun) $
          threadDelay . round $ T.diffUTCTime nextRun now

  where
    streamProcessor
      :: CQRS.AS.AggregateStore streamFamily ([Task action], st)
      -> Pipes.Consumer
          ( CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          )
          (St.StateT (Maybe T.UTCTime) m) ()
    streamProcessor as = do
      -- We don't actually care about the latest event identifier as it might
      -- have changed. We used the aggregate store to fetch the latest version.
      (streamId, _) <- Pipes.await

      lift . lift $ runSomeImmediateTasks as streamId 5
      mNextTaskTime <- lift . lift $ runPlannedTasks as streamId
      tasks <- lift . lift $ fst . CQRS.AS.aggregate <$> CQRS.query as streamId

      -- Do not wait after this run if there are some immediate tasks left.
      when (any (isNothing . from) tasks) $
        St.modify' $ const Nothing

      -- Wait less time if we know that there is an earlier upcoming task.
      case mNextTaskTime of
        Just t -> St.modify' $ fmap (min t)
        Nothing -> pure ()

    -- Run n immediate tasks.
    -- It refetches the tasks after each run since executing one task can change
    -- the list of tasks itself.
    runSomeImmediateTasks
      :: CQRS.AS.AggregateStore streamFamily ([Task action], st)
      -> CQRS.StreamIdentifier streamFamily
      -> Int
      -> m ()
    runSomeImmediateTasks as streamId n
      | n > 0 = do
          tasks <- fst . CQRS.AS.aggregate <$> CQRS.query as streamId

          case find (isNothing . from) tasks of
            Nothing -> pure ()
            Just task -> do
              runAction . action $ task
              runSomeImmediateTasks as streamId (n-1)

      | otherwise = pure ()

    -- Run planned tasks and return the time of the earliest task planned in
    -- the future if any.
    runPlannedTasks
      :: CQRS.AS.AggregateStore streamFamily ([Task action], st)
      -> CQRS.StreamIdentifier streamFamily
      -> m (Maybe T.UTCTime)
    runPlannedTasks as streamId = do
      tasks <- fst . CQRS.AS.aggregate <$> CQRS.query as streamId
      now <- liftIO T.getCurrentTime

      case sortOn fst . mapMaybe (\Task{..} -> (, action) <$> from) $ tasks of
        (t, action) : _
          | now < t -> do
              runAction action
              runPlannedTasks as streamId
          | otherwise -> pure $ Just t
        _ -> pure Nothing