{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.CQRS.ReadModel.AggregateStore
  ( AggregateStore
  ) where

import Control.Monad.Trans (MonadIO(..))
import Data.Hashable (Hashable)

import qualified Control.Concurrent.STM as STM
import qualified Data.HashPSQ           as HashPSQ
import qualified Data.Time              as T

import qualified Database.CQRS as CQRS

data AggregateStore streamFamily aggregate = AggregateStore
  { streamFamily     :: streamFamily
  , aggregator       :: CQRS.Aggregator
                          (CQRS.EventWithContext'
                            (CQRS.StreamType streamFamily))
                          aggregate
  , initialAggregate :: CQRS.StreamIdentifier streamFamily -> aggregate
  , cache            :: Cache
                          (CQRS.StreamIdentifier streamFamily)
                          (CQRS.EventIdentifier
                            (CQRS.StreamType streamFamily))
                          aggregate
  , lagTolerance     :: T.NominalDiffTime
  }

instance
    ( CQRS.StreamFamily m streamFamily
    , CQRS.Stream m (CQRS.StreamType streamFamily)
    , Hashable (CQRS.StreamIdentifier streamFamily)
    , MonadIO m
    , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
    , Ord (CQRS.StreamIdentifier streamFamily)
    )
    => CQRS.ReadModel m (AggregateStore streamFamily aggregate) where

  type ReadModelQuery (AggregateStore streamFamily aggregate) =
    CQRS.StreamIdentifier streamFamily

  type ReadModelResponse (AggregateStore streamFamily aggregate) = aggregate

  query = aggregateStoreQuery

aggregateStoreQuery
  :: forall m streamFamily aggregate.
     ( CQRS.StreamFamily m streamFamily
     , CQRS.Stream m (CQRS.StreamType streamFamily)
     , Hashable (CQRS.StreamIdentifier streamFamily)
     , MonadIO m
     , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , Ord (CQRS.StreamIdentifier streamFamily)
     )
  => AggregateStore streamFamily aggregate
  -> CQRS.StreamIdentifier streamFamily
  -> m aggregate
aggregateStoreQuery AggregateStore{..} streamId = do
    hpsq <- liftIO . STM.atomically . STM.readTVar . cachedValues $ cache
    now  <- liftIO T.getCurrentTime

    case HashPSQ.lookup streamId hpsq of
      Just (lastUpToDateTime, (aggregate, lastEventId)) -> do
        if now <= T.addUTCTime lagTolerance lastUpToDateTime
          then pure aggregate
          else getAggregate now (Just (aggregate, lastEventId))
      Nothing -> getAggregate now Nothing

  where
    getAggregate
      :: T.UTCTime
      -> Maybe (aggregate, CQRS.EventIdentifier (CQRS.StreamType streamFamily))
      -> m aggregate
    getAggregate now mPrevious = do
      let (initAggregate, bounds) = case mPrevious of
            Just (aggregate, eventId) ->
              (aggregate, CQRS.afterEvent eventId)
            Nothing -> (initialAggregate streamId, mempty)

      stream <- CQRS.getStream streamFamily streamId
      (aggregate, mEventId) <-
        CQRS.runAggregator aggregator stream bounds initAggregate

      liftIO $ case (mEventId, mPrevious) of
        (Nothing, Nothing) -> pure ()
        (Nothing, Just (_, lastEventId)) ->
          addValueToCache cache streamId aggregate now lastEventId
        (Just lastEventId, _) ->
          addValueToCache cache streamId aggregate now lastEventId

      pure aggregate

data Cache streamId eventId aggregate = Cache
  { cachedValues
      :: STM.TVar (HashPSQ.HashPSQ streamId T.UTCTime (aggregate, eventId))
  , size    :: STM.TVar Int
  , maxSize :: Int
  }

addValueToCache
  :: ( Hashable streamId
     , Ord eventId
     , Ord streamId
     )
  => Cache streamId eventId aggregate
  -> streamId
  -> aggregate
  -> T.UTCTime
  -> eventId
  -> IO ()
addValueToCache Cache{..} streamId aggregate now eventId =
  STM.atomically $ do
    hpsq <- STM.readTVar cachedValues
    currentSize <- STM.readTVar size

    let (newSize, hpsq') = (\f -> HashPSQ.alter f streamId hpsq) $ \case
          Nothing -> (currentSize + 1, Just (now, (aggregate, eventId)))
          Just current@(_, (_, currentEventId))
            | currentEventId > eventId -> (currentSize, Just current)
            | otherwise -> (currentSize, Just (now, (aggregate, eventId)))

        (newSize', hpsq'')
          | newSize > maxSize = (newSize - 1, HashPSQ.deleteMin hpsq')
          | otherwise = (newSize, hpsq')

    STM.writeTVar size newSize'
    STM.writeTVar cachedValues hpsq''