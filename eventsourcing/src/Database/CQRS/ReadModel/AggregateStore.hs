{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.CQRS.ReadModel.AggregateStore
  ( AggregateStore
  , makeAggregateStore
  , Response(..)
  ) where

import Control.Monad.Trans (MonadIO(..))
import Data.Hashable (Hashable)

import qualified Control.Monad.Except   as Exc
import qualified Control.Concurrent.STM as STM
import qualified Data.HashPSQ           as HashPSQ
import qualified Data.Time              as T

import qualified Database.CQRS as CQRS

data Response eventId aggregate = Response
  { lastEventId     :: Maybe eventId
  , aggregate       :: aggregate
  , eventCount      :: Int -- ^ Number of events processed in this fetch.
  , totalEventCount :: Int -- ^ Total number of events making this aggregate.
  }

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

makeAggregateStore
  :: MonadIO m
  => streamFamily
  -> CQRS.Aggregator
      (CQRS.EventWithContext' (CQRS.StreamType streamFamily))
      aggregate
  -> (CQRS.StreamIdentifier streamFamily -> aggregate)
  -> T.NominalDiffTime -- ^ Lag tolerance.
  -> Int -- ^ Maximum number of elements in the cache.
  -> m (AggregateStore streamFamily aggregate)
makeAggregateStore streamFamily aggregator initialAggregate lagTolerance
                   maxSize = do
  cache <- liftIO . STM.atomically $ do
    cachedValues <- STM.newTVar HashPSQ.empty
    size <- STM.newTVar 0
    pure Cache{..}
  pure AggregateStore{..}

instance
    ( CQRS.StreamFamily m streamFamily
    , CQRS.Stream m (CQRS.StreamType streamFamily)
    , Exc.MonadError CQRS.Error m
    , Hashable (CQRS.StreamIdentifier streamFamily)
    , MonadIO m
    , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
    , Ord (CQRS.StreamIdentifier streamFamily)
    , Show (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
    )
    => CQRS.ReadModel m (AggregateStore streamFamily aggregate) where

  type ReadModelQuery (AggregateStore streamFamily aggregate) =
    CQRS.StreamIdentifier streamFamily

  type ReadModelResponse (AggregateStore streamFamily aggregate) =
    Response (CQRS.EventIdentifier (CQRS.StreamType streamFamily)) aggregate

  query = aggregateStoreQuery

aggregateStoreQuery
  :: forall m streamFamily aggregate.
     ( CQRS.StreamFamily m streamFamily
     , CQRS.Stream m (CQRS.StreamType streamFamily)
     , Exc.MonadError CQRS.Error m
     , Hashable (CQRS.StreamIdentifier streamFamily)
     , MonadIO m
     , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , Ord (CQRS.StreamIdentifier streamFamily)
     , Show (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     )
  => AggregateStore streamFamily aggregate
  -> CQRS.StreamIdentifier streamFamily
  -> m (Response
        (CQRS.EventIdentifier (CQRS.StreamType streamFamily)) aggregate)
aggregateStoreQuery AggregateStore{..} streamId = do
    hpsq <- liftIO . STM.atomically . STM.readTVar . cachedValues $ cache
    now  <- liftIO T.getCurrentTime

    case HashPSQ.lookup streamId hpsq of
      Just (lastUpToDateTime, item@CacheItem{..}) -> do
        if now < T.addUTCTime lagTolerance lastUpToDateTime
          then
            pure Response
              { lastEventId = Just cachedLastEventId
              , aggregate = cachedAggregate
              , eventCount = 0
              , totalEventCount = cachedEventCount
              }
          else getAggregate now (Just item)
      Nothing -> getAggregate now Nothing

  where
    getAggregate
      :: T.UTCTime
      -> Maybe (CacheItem
                (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
                aggregate)
      -> m (Response
            (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
            aggregate)
    getAggregate now mPrevious = do
      let (initAggregate, bounds, eventCount) = case mPrevious of
            Just CacheItem{..} ->
              ( cachedAggregate
              , CQRS.afterEvent cachedLastEventId
              , cachedEventCount
              )
            Nothing -> (initialAggregate streamId, mempty, 0)

      stream <- CQRS.getStream streamFamily streamId
      (aggregate, mEventId, processedEventCount) <-
        CQRS.runAggregator aggregator stream bounds initAggregate

      let totalEventCount = eventCount + processedEventCount
          mkCacheItem lastEventId = CacheItem
            { cachedAggregate   = aggregate
            , cachedLastEventId = lastEventId
            , cachedEventCount  = totalEventCount
            }

      lastEventId <- liftIO $ case (mEventId, mPrevious) of
        (Nothing, Nothing) -> pure Nothing
        (Nothing, Just CacheItem{..}) -> do
          addValueToCache cache streamId now (mkCacheItem cachedLastEventId)
          pure $ Just cachedLastEventId
        (Just lastEventId, _) -> do
          addValueToCache cache streamId now (mkCacheItem lastEventId)
          pure $ Just lastEventId

      pure Response
        { lastEventId
        , aggregate
        , eventCount = processedEventCount
        , totalEventCount
        }

data CacheItem eventId aggregate = CacheItem
  { cachedAggregate   :: aggregate
  , cachedLastEventId :: eventId
  , cachedEventCount  :: Int
  }

data Cache streamId eventId aggregate = Cache
  { cachedValues
      :: STM.TVar (HashPSQ.HashPSQ
                    streamId T.UTCTime (CacheItem eventId aggregate))
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
  -> T.UTCTime
  -> CacheItem eventId aggregate
  -> IO ()
addValueToCache Cache{..} streamId now item =
  STM.atomically $ do
    hpsq <- STM.readTVar cachedValues
    currentSize <- STM.readTVar size

    let (newSize, hpsq') = (\f -> HashPSQ.alter f streamId hpsq) $ \case
          Nothing -> (currentSize + 1, Just (now, item))
          Just current@(_, currentItem)
            | cachedLastEventId currentItem > cachedLastEventId item ->
                (currentSize, Just current)
            | otherwise -> (currentSize, Just (now, item))

        (newSize', hpsq'')
          | newSize > maxSize = (newSize - 1, HashPSQ.deleteMin hpsq')
          | otherwise = (newSize, hpsq')

    STM.writeTVar size newSize'
    STM.writeTVar cachedValues hpsq''
