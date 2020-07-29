{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.TabularDataTest
  ( tests
  ) where

import Control.DeepSeq (NFData)
import Control.Concurrent (ThreadId, forkIO, killThread, threadDelay)
import Control.Monad
import Control.Monad.Trans (MonadIO(..))
import Data.List (nub)
import GHC.Generics (Generic)
import Hedgehog
import Test.Tasty
import Test.Tasty.Hedgehog

import qualified Control.Concurrent.STM     as STM
import qualified Control.Monad.Except       as Exc
import qualified Control.Monad.State.Strict as St
import qualified Data.HashMap.Strict        as HM
import qualified Hedgehog.Gen               as Gen
import qualified Hedgehog.Range             as Range

import Database.CQRS.TabularData (pattern (:~), empty)
import Helpers (interleaveSeqs)

import qualified Database.CQRS             as CQRS
import qualified Database.CQRS.InMemory    as CQRS.InMem
import qualified Database.CQRS.TabularData as CQRS.Tab

tests :: TestTree
tests = testGroup "Database.CQRS.TabularData"
  [ testProperty "Projection using tabular data behaves like aggregator" $
      property tabularDataVsAggregator
  , testProperty "Projection can be restarted" $ property restartingProjection
  ]

tabularDataVsAggregator :: PropertyT IO ()
tabularDataVsAggregator = do
  streamIds <- forAll $
    nub <$> Gen.list (Range.linear 1 10) (Gen.int (Range.linear 1 1000))

  events <- forAll $
    foldM interleaveSeqs [] <=< forM streamIds $ \streamId ->
      Gen.list (Range.linear 1 1000) ((streamId,) <$> testEventGen)

  n <- forAll $ Gen.int (Range.linear 0 (length events))
  let (beforeEvents, afterEvents) = splitAt n events

  streamFamily <- liftIO CQRS.InMem.emptyStreamFamily

  trackingTable <- CQRS.createInMemoryTrackingTable
  table <- liftIO $ STM.newTVarIO []

  liftIO . forM_ beforeEvents $ \(streamId, event) -> do
    stream <- CQRS.getStream streamFamily streamId
    void . Exc.runExceptT $ CQRS.writeEvent stream event

  threadId <- liftIO . forkIO . void .  Exc.runExceptT $ do
    CQRS.runProjection streamFamily id goodProjection trackingTable $
      CQRS.executeInMemoryActions table CQRS.Tab.applyTabularDataAction

  liftIO . forM_ afterEvents $ \(streamId, event) -> do
    stream <- CQRS.getStream streamFamily streamId
    void . Exc.runExceptT $ CQRS.writeEvent stream event

  forM_ streamIds $
    checkConsistency True streamFamily trackingTable table threadId

  liftIO . killThread $ threadId

checkConsistency
  :: Bool
  -> CQRS.InMem.StreamFamily Int () TestEvent
  -> CQRS.InMemoryTrackingTable Int Integer Int
  -> STM.TVar (CQRS.Tab.Table ProjectedCols)
  -> ThreadId
  -> Int
  -> PropertyT IO ()
checkConsistency
    shouldRetry streamFamily trackingTable table threadId streamId = do
  projected <- liftIO $ STM.readTVarIO table

  stream <- CQRS.getStream streamFamily streamId
  eExpected <- Exc.runExceptT $
    CQRS.runAggregator aggregator stream mempty (streamId, NoRow)
  let mResult = HM.lookup (streamId :~ empty) projected

  trackedState <- liftIO $ CQRS.getTrackedState trackingTable streamId
  annotateShow trackedState

  case (mResult, eExpected) of
    (Nothing, Right ((_, NoRow), _, _)) -> pure ()
    ( Just (counter :~ CQRS.Tab.Nil),
      Right ((_, SuccessState counter'), _, _) )
      | counter == counter' -> pure ()
    ( Just (counter :~ CQRS.Tab.Nil),
      Right ((_, FailedState counter'), _, _) )
      | counter == counter' -> pure ()
    _ | shouldRetry -> do
          liftIO . threadDelay $ 10 * 1000 -- Wait 10ms for the projection.
          checkConsistency
            False streamFamily trackingTable table threadId streamId
      | otherwise -> do
          liftIO . killThread $ threadId
          annotateShow mResult
          annotateShow eExpected
          failure

restartingProjection :: PropertyT IO ()
restartingProjection = do
  events <- forAll $ Gen.list (Range.linear 1 100) testEventGen
  n <- forAll $ Gen.int (Range.linear 0 (length events))
  let (beforeEvents, afterEvents) = splitAt n events

  streamFamily <- liftIO CQRS.InMem.emptyStreamFamily

  trackingTable <- CQRS.createInMemoryTrackingTable
  -- Set a dummy row in the table to make the bad projection crash.
  table <- liftIO $ STM.newTVarIO [(0 :~ empty, 0 :~ empty)]

  liftIO . forM_ beforeEvents $ \event -> do
    stream <- CQRS.getStream streamFamily 1
    void . Exc.runExceptT $ CQRS.writeEvent stream event

  threadId <- liftIO . forkIO . void . Exc.runExceptT $
    CQRS.runProjection streamFamily id badProjection trackingTable $
      CQRS.executeInMemoryActions table CQRS.Tab.applyTabularDataAction

  liftIO . forM_ afterEvents $ \event -> do
    stream <- CQRS.getStream streamFamily 1
    void . Exc.runExceptT $ CQRS.writeEvent stream event

  liftIO . killThread $ threadId
  threadId' <- liftIO . forkIO . void . Exc.runExceptT $
    CQRS.runProjection streamFamily id goodProjection trackingTable $
      CQRS.executeInMemoryActions table CQRS.Tab.applyTabularDataAction

  checkConsistency True streamFamily trackingTable table threadId' 1

  liftIO . killThread $ threadId'

data TestEvent
  = CreatedEvent
  | IncrementedEvent
  | ResetEvent
  | DeletedEvent
  deriving (Bounded, Enum, Eq, Generic, NFData, Show)

testEventGen :: Gen TestEvent
testEventGen = Gen.enumBounded

type ProjectedCols =
  'CQRS.Tab.WithUniqueKey
    '[ '("stream_id", Int)]
    '[ '("counter", Int)]

goodProjection
  :: CQRS.Projection
      (CQRS.EventWithContext Integer () TestEvent)
      Int
      (CQRS.Tab.TabularDataAction ProjectedCols)
goodProjection CQRS.EventWithContext{..} = do
  streamId <- St.get

  pure $ case event of
    CreatedEvent ->
      [ CQRS.Tab.Insert $ streamId :~ 0 :~ empty
      ]

    IncrementedEvent ->
      [ CQRS.Tab.Update
          (CQRS.Tab.ffield @ProjectedCols @"counter" (CQRS.Tab.plus 1))
          (CQRS.Tab.field @ProjectedCols @"stream_id" streamId)
      ]

    ResetEvent ->
      [ CQRS.Tab.Upsert $ streamId :~ 0 :~ empty
      ]

    DeletedEvent ->
      [ CQRS.Tab.Delete $
          CQRS.Tab.ffield @ProjectedCols @"stream_id" (CQRS.Tab.equal streamId)
      ]

badProjection
  :: CQRS.Projection
      (CQRS.EventWithContext Integer () TestEvent)
      Int
      (CQRS.Tab.TabularDataAction ProjectedCols)
badProjection CQRS.EventWithContext{..} = do
  streamId <- St.get

  pure $ case event of
    CreatedEvent ->
      [ CQRS.Tab.Insert $ streamId :~ 0 :~ empty
      ]

    IncrementedEvent ->
      [ CQRS.Tab.Insert $ 0 :~ 0 :~ empty -- Make it fail.
      ]

    ResetEvent ->
      [ CQRS.Tab.Upsert $ streamId :~ 0 :~ empty
      ]

    DeletedEvent ->
      [ CQRS.Tab.Delete $
          CQRS.Tab.ffield @ProjectedCols @"stream_id" (CQRS.Tab.equal streamId)
      ]

data ExpectedResult
  = NoRow
  | FailedState Int
  | SuccessState Int
  deriving Show

aggregator
  :: CQRS.Aggregator
      (CQRS.EventWithContext Integer () TestEvent)
      (Int, ExpectedResult)
aggregator CQRS.EventWithContext{..} =
  St.modify' $ \(streamId, current) ->
    (streamId,) $ case (event, current) of
      (_, FailedState _) -> current
      (CreatedEvent, NoRow) -> SuccessState 0
      (CreatedEvent, SuccessState c) -> FailedState c
      (IncrementedEvent, NoRow) -> NoRow
      (IncrementedEvent, SuccessState c) -> SuccessState (c + 1)
      (ResetEvent, _) -> SuccessState 0
      (DeletedEvent, _) -> NoRow
