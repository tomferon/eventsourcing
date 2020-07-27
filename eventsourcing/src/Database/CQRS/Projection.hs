{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQRS.Projection
  ( Projection
  , Aggregator
  , EffectfulProjection
  , runAggregator
  , runProjection
  , TrackedState(..)
  , TrackingTable(..)
  , InMemoryTrackingTable(..)
  , createInMemoryTrackingTable
  , executeInMemoryActions
  ) where

import Control.Monad (unless, forever, void)
import Control.Monad.Trans (MonadIO(..), lift)
import Data.Hashable (Hashable)
import Data.List
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

-- | A projection is simply a function consuming events and producing results
-- in an environment @f@.
type Projection f event a =
  event -> f a

-- | Projection aggregating a state in memory.
type Aggregator event agg =
  event -> St.State agg ()

-- | Projection returning actions that can be batched and executed.
--
-- This can be used to batch changes to tables in a database for example.
type EffectfulProjection event st action =
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
  -> EffectfulProjection
      (EventWithContext' (StreamType streamFamily))
      st action
  -> trackingTable
  -> (trackingTable
      -> Pipes.Consumer
          ( st
          , [action]
          , StreamIdentifier streamFamily
          , EventIdentifier (StreamType streamFamily)
          ) m ())
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
        >-> familyProjectionPipe
        >-> executeActions trackingTable

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
        NeverRan -> catchUp' streamId stream mempty (initState streamId)
        SuccessAt lastSuccesfulEventId st
          | lastSuccesfulEventId < eventId ->
              catchUp' streamId stream (afterEvent lastSuccesfulEventId) st
          | otherwise -> pure ()
        -- We are catching up, so maybe the executable was restarted and this
        -- stream won't fail this time.
        FailureAt (Just (lastSuccessfulEventId, st)) _ ->
          catchUp' streamId stream (afterEvent lastSuccessfulEventId) st
        FailureAt Nothing _ ->
          catchUp' streamId stream mempty (initState streamId)

    catchUp'
      :: StreamIdentifier streamFamily
      -> StreamType streamFamily
      -> StreamBounds' (StreamType streamFamily)
      -> st
      -> Pipes.Effect m ()
    catchUp' streamId stream bounds st =
      streamEvents stream bounds
        >-> streamProjectionPipe streamId st
        >-> executeActions trackingTable

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
            HM.toList . HM.fromListWith (++) . map (fmap pure) $ events
      Pipes.each eventsByStream

    familyProjectionPipe
      :: Pipes.Pipe
          ( StreamIdentifier streamFamily
          , [ Either
                (EventIdentifier (StreamType streamFamily), String)
                (EventWithContext' (StreamType streamFamily)) ]
          )
          ( st
          , [action]
          , StreamIdentifier streamFamily
          , EventIdentifier (StreamType streamFamily)
          ) m ()
    familyProjectionPipe = forever $ do
      (streamId, eEvents) <- Pipes.await

      state <- lift $ getTrackedState trackingTable streamId

      let filterEventsAfter eventId = filter $ \case
            Left (eventId', _) -> eventId' > eventId
            Right EventWithContext{ identifier = eventId' } ->
              eventId' > eventId

      case state of
        NeverRan ->
          void $ coreProjectionPipe streamId (initState streamId) eEvents
        SuccessAt lastSuccesfulEventId st ->
          void
            . coreProjectionPipe streamId st
            . filterEventsAfter lastSuccesfulEventId
            $ eEvents
        -- This is used after catching up. If it's still marked as failed, all
        -- hope is lost.
        FailureAt _ _ -> pure ()

    streamProjectionPipe
      :: StreamIdentifier streamFamily
      -> st
      -> Pipes.Pipe
          [ Either
              (EventIdentifier (StreamType streamFamily), String)
              (EventWithContext' (StreamType streamFamily)) ]
          ( st
          , [action]
          , StreamIdentifier streamFamily
          , EventIdentifier (StreamType streamFamily)
          ) m ()
    streamProjectionPipe streamId st = do
      eEvents <- Pipes.await
      mSt' <- coreProjectionPipe streamId st eEvents
      case mSt' of
        Just st' -> streamProjectionPipe streamId st'
        Nothing -> pure ()

    coreProjectionPipe
      :: StreamIdentifier streamFamily
      -> st
      -> [ Either
            (EventIdentifier (StreamType streamFamily), String)
            (EventWithContext' (StreamType streamFamily)) ]
      -> Pipes.Pipe
          a -- It's not supposed to consume any data.
          ( st
          , [action]
          , StreamIdentifier streamFamily
          , EventIdentifier (StreamType streamFamily)
          ) m (Maybe st) -- Nothing in case of failure.
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

      unless (null actions) $ do
        -- There is a last event, otherwise actions would be empty.
        let latestEventId = identifier . last $ events
        Pipes.yield (st', actions, streamId, latestEventId)

      case mFirstError of
        Nothing -> pure Nothing
        Just (eventId, err) -> do
          lift $ upsertError trackingTable streamId eventId err
          pure $ Just st'

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
  | FailureAt (Maybe (identifier, st)) identifier
    -- ^ Last succeeded at, failed at.

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
        Just (successPair, Just (eventId, _)) ->
          FailureAt successPair eventId
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
  :: ( Hashable streamId
     , MonadIO m
     , Ord streamId
     )
  => STM.TVar projected
  -> (projected -> action -> projected)
  -> InMemoryTrackingTable streamId eventId st
  -> Pipes.Consumer (st, [action], streamId, eventId) m ()
executeInMemoryActions tvar applyAction (InMemoryTrackingTable trackingTVar) =
  forever $ do
    (st, actions, streamId, eventId) <- Pipes.await

    liftIO . STM.atomically $ do
      STM.modifyTVar' tvar $ \projected ->
        foldl' applyAction projected actions
      STM.modifyTVar' trackingTVar $
        HM.insert streamId (Just (eventId, st), Nothing)
