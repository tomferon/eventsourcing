{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.TransformerTest
  ( tests
  ) where

import Control.DeepSeq (NFData)
import Control.Monad (forM_, unless, void, when)
import Control.Monad.Trans (liftIO)
import Data.Maybe (maybeToList)
import GHC.Generics (Generic)
import Hedgehog hiding (collect)
import Test.Tasty
import Test.Tasty.Hedgehog

import qualified Control.Monad.Except as Exc
import qualified Hedgehog.Gen         as Gen
import qualified Hedgehog.Range       as Range

import Helpers (collect)

import qualified Database.CQRS          as CQRS
import qualified Database.CQRS.InMemory as CQRS.InMem

tests :: TestTree
tests = testGroup "Transformer"
  [ testProperty "Events are dropped and merged accordingly" $
      property eventsDroppedAndMerged
  ]

data TestEvent
  = CreatedEvent String
  | RenamedEvent String
  | SuffixAddedEvent String -- We want to get rid of this one.
  | DeletedEvent
  deriving (Eq, Show, Generic)
  deriving anyclass (NFData)

testEventGen :: Gen TestEvent
testEventGen = do
  name <- Gen.string (Range.linear 0 10) Gen.alphaNum
  Gen.element
    [ RenamedEvent name
    , SuffixAddedEvent name
    ]

testEventSequenceGen :: Gen [TestEvent]
testEventSequenceGen = do
  events <- Gen.list (Range.linear 1 1000) testEventGen
  name <- Gen.string (Range.linear 0 10) Gen.alphaNum
  deletedEvent <- Gen.element [Nothing, Just DeletedEvent]
  pure $ CreatedEvent name : events ++ maybeToList deletedEvent

isCreatedEvent :: CQRS.EventWithContext Integer () TestEvent -> Bool
isCreatedEvent = \case
  CQRS.EventWithContext _ _ (CreatedEvent _) -> True
  _ -> False

isRenamedEvent :: CQRS.EventWithContext Integer () TestEvent -> Bool
isRenamedEvent = \case
  CQRS.EventWithContext _ _ (RenamedEvent _) -> True
  _ -> False

isDeletedEvent :: CQRS.EventWithContext Integer () TestEvent -> Bool
isDeletedEvent = \case
  CQRS.EventWithContext _ _ DeletedEvent -> True
  _ -> False

transformer
  :: CQRS.Transformer
      (Either (Integer, String) (CQRS.EventWithContext Integer () TestEvent))
      Integer (CQRS.EventWithContext Integer () TestEvent)
transformer = \case
  Left (i, err) -> CQRS.failTransformer i err
  Right ewc@(CQRS.EventWithContext _ () event) ->
    case event of
      CreatedEvent _ -> CQRS.pushEvent ewc

      RenamedEvent name -> do
        changed <- CQRS.mergeEvents $ \events ->
          case break isCreatedEvent events of
            (levs, CQRS.EventWithContext eventId _ (CreatedEvent _) : revs) ->
              let ewc' = CQRS.EventWithContext eventId () (CreatedEvent name)
              in (True, levs ++ ewc' : revs)
            _ -> (False, events)
        unless changed $ CQRS.pushEvent ewc

      SuffixAddedEvent suffix -> do
        changed <- CQRS.mergeEvents $ \events ->
          case break (\e -> isCreatedEvent e || isRenamedEvent e) events of
            (levs, CQRS.EventWithContext eventId _ (CreatedEvent n) : revs) ->
              let ewc' =
                    CQRS.EventWithContext eventId ()
                      (CreatedEvent (n ++ suffix))
              in (True, levs ++ ewc' : revs)
            (levs, CQRS.EventWithContext eventId _ (RenamedEvent n) : revs) ->
              let ewc' =
                    CQRS.EventWithContext eventId ()
                      (RenamedEvent (n ++ suffix))
              in (True, levs ++ ewc' : revs)
            _ -> (False, events)
        unless changed $ CQRS.pushEvent ewc

      DeletedEvent ->
        CQRS.mergeEvents $ \events ->
          case break isCreatedEvent events of
            (_, CQRS.EventWithContext _ _ (CreatedEvent _) : _) ->
              ((), [])
            _ -> ((), [ewc])

eventsDroppedAndMerged :: PropertyT IO ()
eventsDroppedAndMerged = do
  inputEvents <- forAll $ do
    seq1 <- map (1,) <$> testEventSequenceGen
    seq2 <- map (2,) <$> testEventSequenceGen
    interleaveSeqs seq1 seq2

  inputStreamFamily <- liftIO CQRS.InMem.emptyStreamFamily
  let transformedStreamFamily =
        CQRS.transformStreamFamily @IO @Int
          transformer pure pure pure pure inputStreamFamily

  liftIO . forM_ inputEvents $ \(streamId, event) -> do
    stream <- CQRS.getStream inputStreamFamily streamId
    void . Exc.runExceptT $ CQRS.writeEvent stream event

  stream1 <- liftIO $ CQRS.getStream transformedStreamFamily 1
  stream2 <- liftIO $ CQRS.getStream transformedStreamFamily 2

  outputEvents1 <- liftIO . collect $ CQRS.streamEvents stream1 mempty
  outputEvents2 <- liftIO . collect $ CQRS.streamEvents stream2 mempty

  forM_ [outputEvents1, outputEvents2] $ \outputEvents -> do
    forM_ outputEvents $ \batch -> do
      when (any (either (const False) isCreatedEvent) batch) $
        length batch === 1
      when (any (either (const False) isDeletedEvent) batch) $
        length batch === 1

interleaveSeqs :: [a] -> [a] -> Gen [a]
interleaveSeqs = go id
  where
    go :: ([a] -> [a]) -> [a] -> [a] -> Gen [a]
    go acc [] [] = pure $ acc []
    go acc [] ys = pure $ acc ys
    go acc xs [] = pure $ acc xs
    go acc (x:xs) (y:ys) =
      Gen.choice
        [ go (acc . (x:)) xs (y:ys)
        , go (acc . (y:)) (x:xs) ys
        ]
