{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

module Database.CQRS.ReadModel.TopUp
  ( TopUpReadModel(..)
  , Response(..)
  , projectionToAggregator
  , trackedStateToTopUpUnderlyingReadModel
  ) where

import Data.Functor ((<&>))
import Data.List    (foldl')
import Data.Maybe   (fromMaybe)

import qualified Control.Monad.Except       as Exc
import qualified Control.Monad.State.Strict as St

import qualified Database.CQRS as CQRS

-- | Read model that fetches the projected state corresponding to a stream
-- identifier from an underlying read model, fetches the events that haven't
-- been processed by the projection yet and applies them in memory.
data TopUpReadModel streamFamily model aggregate = TopUpReadModel
  { streamFamily :: streamFamily
  , readModel    :: model
  , aggregator
      :: CQRS.Aggregator
          (CQRS.EventWithContext' (CQRS.StreamType streamFamily))
          aggregate
  , initState :: CQRS.StreamIdentifier streamFamily -> aggregate
  }

data Response eventId aggregate = Response
  { lastEventId :: Maybe eventId
  , aggregate   :: aggregate
  , eventCount  :: Int -- ^ Number of new events added on top.
  }

instance
    ( Exc.MonadError CQRS.Error m
    , CQRS.ReadModel m model
    , CQRS.ReadModelQuery model ~ CQRS.StreamIdentifier streamFamily
    , CQRS.ReadModelResponse model ~
        Either CQRS.Error
          ( CQRS.StreamBounds (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
          , Maybe aggregate
          )
    , CQRS.Stream m (CQRS.StreamType streamFamily)
    , CQRS.StreamFamily m streamFamily
    , Monad m
    , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
    , Show (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
    ) => CQRS.ReadModel m (TopUpReadModel streamFamily model aggregate) where

  type ReadModelQuery (TopUpReadModel streamFamily model aggregate) =
    CQRS.StreamIdentifier streamFamily

  type ReadModelResponse (TopUpReadModel streamFamily model aggregate) =
    Response (CQRS.EventIdentifier (CQRS.StreamType streamFamily)) aggregate

  query = topUpReadModelQuery

topUpReadModelQuery
  :: ( Exc.MonadError CQRS.Error m
     , CQRS.ReadModel m model
     , CQRS.ReadModelQuery model ~ CQRS.StreamIdentifier streamFamily
     , CQRS.ReadModelResponse model ~
        Either CQRS.Error
          ( CQRS.StreamBounds (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
          , Maybe aggregate
          )
     , CQRS.Stream m (CQRS.StreamType streamFamily)
     , CQRS.StreamFamily m streamFamily
     , Monad m
     , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , Show (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     )
  => TopUpReadModel streamFamily model aggregate
  -> CQRS.StreamIdentifier streamFamily
  -> m (Response (CQRS.EventIdentifier (CQRS.StreamType streamFamily)) aggregate)
topUpReadModelQuery TopUpReadModel{..} streamId = do
  (bounds, mAggregate) <- Exc.liftEither =<< CQRS.query readModel streamId
  let aggregate = fromMaybe (initState streamId) mAggregate
  stream <- CQRS.getStream streamFamily streamId
  (aggregate', lastEventId, eventCount) <-
    CQRS.runAggregator aggregator stream bounds aggregate
  pure Response { aggregate = aggregate', .. }

-- | Transform a read model like @TrackedSQLQuery@ in @eventsourcing-postgresql@
-- which return a 'TrackedState' into one suitable as the underlying read model
-- of a 'TopUpReadModel'.
trackedStateToTopUpUnderlyingReadModel
  :: ( CQRS.ReadModelResponse model ~ CQRS.TrackedState eventId a
     , Ord eventId
     )
  => model
  -> CQRS.ReadModelF model
      (Either CQRS.Error (CQRS.StreamBounds eventId, Maybe a))
trackedStateToTopUpUnderlyingReadModel model =
  CQRS.toReadModelF model <&> \case
    CQRS.NeverRan            -> Right (mempty, Nothing)
    CQRS.SuccessAt eventId x -> Right (CQRS.afterEvent eventId, Just x)
    CQRS.FailureAt _ _ err   -> Left $ CQRS.ProjectionError err

-- | Turn a projection into an aggregator so it can be used to create a top-up
-- read model on top of the result of that projection.
--
-- For example, there might be a projection processing events and producing
-- actions that modify some projected state for the stream in a database. Given
-- a function to apply these actions in memory, we can create an aggregator
-- which can then be used to start from the projected state from the database
-- and apply the actions of the events that have been created since the last run
-- of the projection.
projectionToAggregator
  :: CQRS.Projection event st action
  -> (aggregate -> action -> aggregate)
  -> CQRS.Aggregator event (st, aggregate)
projectionToAggregator projection applyAction event = do
  St.modify' $ \(st, aggregate) ->
    let (actions, st') = St.runState (projection event) st
    in
    (st', foldl' applyAction aggregate actions)
