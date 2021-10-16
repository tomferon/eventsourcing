{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQRS.Projection
  ( Aggregator
  , runAggregator
  , Projection
  , runProjection
  , TrackedState(..)
  , TrackingTable(..)
  , InMemoryTrackingTable(..)
  , createInMemoryTrackingTable
  , executeInMemoryActions
  ) where

import Control.Monad
import Control.Monad.Trans (MonadIO(..), lift)
import Data.Hashable (Hashable)
import Data.Tuple (swap)
import Pipes ((>->))

import qualified Data.HashMap.Strict        as HM
import qualified Control.Concurrent.STM     as STM
import qualified Control.Monad.Except       as Exc
import qualified Control.Monad.State.Strict as St
import qualified Pipes

import Database.CQRS.Error
import Database.CQRS.Stream
import Database.CQRS.StreamFamily

-- | Function aggregating a state in memory.
type Aggregator event agg =
  event -> St.State agg ()

-- | Projection returning actions that can be batched and executed.
--
-- This can be used to batch changes to tables in a database for example.
type Projection event st action =
  event -> St.State st [action]

-- | Run an 'Aggregator' on events from a stream starting with a given state and
-- return the new aggregate state, the identifier of the last event processed if
-- any and how many of them were processed.
runAggregator
  :: forall m stream aggregate.
     ( Exc.MonadError Error m
     , Show (EventIdentifier stream)
     , Stream m stream
     )
  => Aggregator (EventWithContext' stream) aggregate
  -> stream
  -> StreamBounds' stream
  -> aggregate
  -> m (aggregate, Maybe (EventIdentifier stream), Int)
runAggregator aggregator stream bounds initState = do
  flip St.execStateT (initState, Nothing, 0) . Pipes.runEffect $
    Pipes.hoist lift (streamEvents stream bounds)
      >-> flatten
      >-> aggregatorPipe

  where
    aggregatorPipe
      :: Pipes.Consumer
          (Either (EventIdentifier stream, String) (EventWithContext' stream))
          (St.StateT (aggregate, Maybe (EventIdentifier stream), Int) m) ()
    aggregatorPipe = forever $ do
      ewc <- Pipes.await >>= \case
        Left (eventId, err) ->
          Exc.throwError $ EventDecodingError (show eventId) err
        Right e -> pure e

      St.modify' $ \(aggregate, _, eventCount) ->
        let aggregate' = St.execState (aggregator  ewc) aggregate
        in (aggregate', Just (identifier ewc), eventCount + 1)

flatten :: Monad m => Pipes.Pipe [a] a m ()
flatten = forever $ Pipes.await >>= Pipes.each

runProjection
  :: forall streamFamily action trackingTable m st.
     ( Exc.MonadError Error m
     , Hashable (StreamIdentifier streamFamily)
     , Ord (EventIdentifier (StreamType streamFamily))
     , Ord (StreamIdentifier streamFamily)
     , Stream m (StreamType streamFamily)
     , StreamFamily m streamFamily
     , TrackingTable m trackingTable
        (StreamIdentifier streamFamily)
        (EventIdentifier (StreamType streamFamily))
        st
     )
  => streamFamily
  -> (StreamIdentifier streamFamily -> st)
     -- ^ Initialise state when no events have been processed yet.
  -> Projection
      (EventWithContext' (StreamType streamFamily))
      st action
  -> trackingTable
  -> (trackingTable
      -> ( st
         , [action]
         , StreamIdentifier streamFamily
         , EventIdentifier (StreamType streamFamily)
         )
      -> m ())
  -- ^ Commit the custom actions. See 'executeSqlActions' for 'SqlAction's.
  -- This consumer is expected to update the tracking table accordingly.
  -> m ()
runProjection streamFamily initState projection trackingTable
              executeActions = do
    newEvents <- allNewEvents streamFamily
    Pipes.runEffect $ do
      latestEventIdentifiers streamFamily >-> catchUp
      newEvents
        >-> groupByStream
        >-> bisectFailingBatches id
              (\batch -> Pipes.runEffect $
                Pipes.for (projectionPipe batch)
                  (lift . executeActions trackingTable))

  where
    catchUp
      :: Pipes.Consumer
          ( StreamIdentifier streamFamily
          , EventIdentifier (StreamType streamFamily)
          ) m ()
    catchUp = forever $ do
      (streamId, eventId) <- Pipes.await
      stream <- lift $ getStream streamFamily streamId
      state <- lift $ getTrackedState trackingTable streamId

      lift . Pipes.runEffect $ case state of
        NeverRan -> catchUp' streamId stream mempty
        SuccessAt lastSuccesfulEventId _
          | lastSuccesfulEventId < eventId ->
              catchUp' streamId stream (afterEvent lastSuccesfulEventId)
          | otherwise -> pure ()
        -- We are catching up, so maybe the executable was restarted and this
        -- stream won't fail this time.
        FailureAt (Just (lastSuccessfulEventId, _)) _ _ ->
          catchUp' streamId stream (afterEvent lastSuccessfulEventId)
        FailureAt Nothing _ _ -> catchUp' streamId stream mempty

    catchUp'
      :: StreamIdentifier streamFamily
      -> StreamType streamFamily
      -> StreamBounds' (StreamType streamFamily)
      -> Pipes.Effect m ()
    catchUp' streamId stream bounds =
      streamEvents stream bounds
        >-> bisectFailingBatches (streamId,)
              (\batch -> Pipes.runEffect $
                Pipes.for (projectionPipe batch)
                  (lift . executeActions trackingTable))

    groupByStream
      :: Pipes.Pipe
          [ ( StreamIdentifier streamFamily
            , Either
                (EventIdentifier (StreamType streamFamily), String)
                (EventWithContext' (StreamType streamFamily))
            ) ]
          ( StreamIdentifier streamFamily
          , [ Either
                (EventIdentifier (StreamType streamFamily), String)
                (EventWithContext' (StreamType streamFamily)) ]
          )
          m ()
    groupByStream = forever $ do
      events <- Pipes.await
      let eventsByStream =
            HM.toList . HM.fromListWith (flip (++)) . map (fmap pure) $ events
      Pipes.each eventsByStream

    -- It must be used with 'Pipes.for' to ensure the execution continues after
    -- the call to 'Pipes.yield'.
    projectionPipe
      :: ( StreamIdentifier streamFamily
         , [ Either
              (EventIdentifier (StreamType streamFamily), String)
              (EventWithContext' (StreamType streamFamily)) ] )
      -> Pipes.Producer
          ( st
          , [action]
          , StreamIdentifier streamFamily
          , EventIdentifier (StreamType streamFamily)
          ) m ()
    projectionPipe (streamId, eEvents) = do
      let filterEventsAfter eventId = filter $ \case
            Left (eventId', _) -> eventId' > eventId
            Right EventWithContext{ identifier = eventId' } ->
              eventId' > eventId

      state <- lift $ getTrackedState trackingTable streamId
      case state of
        NeverRan ->
          coreProjectionPipe streamId (initState streamId) eEvents
        SuccessAt lastSuccesfulEventId st ->
          coreProjectionPipe streamId st
            . filterEventsAfter lastSuccesfulEventId
            $ eEvents
        -- If this batch has events lower than the identifier for which the
        -- projection has failed, it means this is part of the catch-up phase.
        -- It happens after a restart, so the code might have changed and we
        -- should retry.
        FailureAt mSuccess eventId _ -> do
          let check = \case
                Left (eventId', _) -> eventId' <= eventId
                Right EventWithContext{..} -> identifier <= eventId
          when (any check eEvents) $
            case mSuccess of
              Nothing ->
                coreProjectionPipe streamId (initState streamId) eEvents
              Just (lastSuccesfulEventId, st) ->
                coreProjectionPipe streamId st
                  . filterEventsAfter lastSuccesfulEventId
                  $ eEvents

    coreProjectionPipe
      :: StreamIdentifier streamFamily
      -> st
      -> [ Either
            (EventIdentifier (StreamType streamFamily), String)
            (EventWithContext' (StreamType streamFamily)) ]
      -> Pipes.Producer
          ( st
          , [action]
          , StreamIdentifier streamFamily
          , EventIdentifier (StreamType streamFamily)
          ) m ()
    coreProjectionPipe streamId st eEvents = do
      -- "Healthy" events up until the first error if any. We want to process
      -- the events before throwing the error so that chunking as no effect on
      -- semantics.
      let (events, mFirstError) = stopOnLeft eEvents
          (st', actions) =
            fmap mconcat
              . swap
              . flip St.runState st
              . mapM projection
              $ events

      -- Event if actions is an empty list, we still want to yield an empty list
      -- such that the tracking table will get updated, possibly with an updated
      -- internal state.
      unless (null events) $ do
        -- There is a last event, otherwise @null events@ would be True.
        let latestEventId = identifier . last $ events
        Pipes.yield (st', actions, streamId, latestEventId)

      case mFirstError of
        Nothing -> pure ()
        Just (eventId, err) -> do
          lift $ upsertError trackingTable streamId eventId err
          pure ()

    bisectFailingBatches
      :: (a
          -> ( StreamIdentifier streamFamily
             , [ Either
                  (EventIdentifier (StreamType streamFamily), String)
                  (EventWithContext' (StreamType streamFamily)) ]))
      -> (( StreamIdentifier streamFamily
          , [ Either
                (EventIdentifier (StreamType streamFamily), String)
                (EventWithContext' (StreamType streamFamily)) ])
          -> m ())
      -> Pipes.Consumer a m ()
    bisectFailingBatches split f = forever $ do
        input <- Pipes.await
        let (streamId, batch) = split input
        void . lift $ go streamId batch

      where
        go
          :: StreamIdentifier streamFamily
          -> [ Either
                (EventIdentifier (StreamType streamFamily), String)
                (EventWithContext' (StreamType streamFamily)) ]
          -> m Bool
        go streamId batch =
          Exc.catchError
            (f (streamId, batch) >> pure True)
            (\err -> case (err, batch) of
              (ProjectionError _, _ : _ : _) -> do
                let len = length batch
                    (batch1, batch2) =
                      splitAt (floor @Double (fromIntegral len / 2)) batch
                succeeded <- go streamId batch1
                if succeeded
                  then go streamId batch2
                  else pure False

              (ProjectionError err', [Left (eventId, _)]) -> do
                upsertError trackingTable streamId eventId err'
                pure False

              (ProjectionError err', [Right EventWithContext{..}]) -> do
                upsertError trackingTable streamId identifier err'
                pure False

              _ -> Exc.throwError err)

-- | Return all the 'Right' elements before the first 'Left' and the value of
-- the first 'Left'.
stopOnLeft :: [Either a b] -> ([b], Maybe a)
stopOnLeft = go id
  where
    go :: ([b] -> [b]) -> [Either a b] -> ([b], Maybe a)
    go f = \case
      [] -> (f [], Nothing)
      Left err : _ -> (f [], Just err)
      Right x : xs -> go (f . (x:)) xs

data TrackedState identifier st
  = NeverRan
  | SuccessAt identifier st
  | FailureAt (Maybe (identifier, st)) identifier String
    -- ^ Last succeeded at, failed at.
  deriving (Eq, Show)

class
    TrackingTable m table streamId eventId st
    | table -> streamId, table -> eventId, table -> st where
  getTrackedState :: table -> streamId -> m (TrackedState eventId st)
  upsertError     :: table -> streamId -> eventId -> String -> m ()

newtype InMemoryTrackingTable streamId eventId st
  = InMemoryTrackingTable
      (STM.TVar (HM.HashMap
                  streamId (Maybe (eventId, st), Maybe (eventId, String))))

instance
    (Hashable streamId, MonadIO m, Ord streamId)
    => TrackingTable m (InMemoryTrackingTable streamId eventId st)
        streamId eventId st where

  getTrackedState (InMemoryTrackingTable tvar) streamId =
    liftIO . STM.atomically $ do
      hm <- STM.readTVar tvar
      pure $ case HM.lookup streamId hm of
        Nothing -> NeverRan
        Just (Nothing, Nothing) -> NeverRan -- This actually can't happen.
        Just (successPair, Just (eventId, err)) ->
          FailureAt successPair eventId err
        Just (Just (eventId, st), Nothing) -> SuccessAt eventId st

  upsertError (InMemoryTrackingTable tvar) streamId eventId err =
    liftIO . STM.atomically . STM.modifyTVar tvar $
      HM.alter
        (\case
          Nothing -> Just (Nothing, Just (eventId, err))
          Just (mSuccess, _) -> Just (mSuccess, Just (eventId, err)))
        streamId

createInMemoryTrackingTable
  :: MonadIO m => m (InMemoryTrackingTable streamId eventId st)
createInMemoryTrackingTable =
  liftIO . fmap InMemoryTrackingTable . STM.newTVarIO $ HM.empty

executeInMemoryActions
  :: ( Exc.MonadError Error m
     , Hashable streamId
     , MonadIO m
     , Ord streamId
     )
  => STM.TVar projected
  -> (projected -> action -> Either Error projected)
     -- ^ For tabular data actions, use `applyTabularDataAction`.
  -> InMemoryTrackingTable streamId eventId st
  -> (st, [action], streamId, eventId)
  -> m ()
executeInMemoryActions
    tvar applyAction (InMemoryTrackingTable trackingTVar)
    (st, actions, streamId, eventId) =

  Exc.liftEither <=< liftIO . STM.atomically $ do
    projected <- STM.readTVar tvar

    case foldM applyAction projected actions of
      Left err -> pure $ Left err

      Right projected' -> do
        STM.writeTVar tvar projected'
        STM.modifyTVar' trackingTVar $
          HM.insert streamId (Just (eventId, st), Nothing)
        pure $ Right ()
