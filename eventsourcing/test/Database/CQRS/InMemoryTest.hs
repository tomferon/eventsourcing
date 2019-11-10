{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.InMemoryTest
  ( tests
  ) where

import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar
import Control.Monad (forever, replicateM_, zipWithM_)
import Control.Monad.Trans (liftIO)
import Data.Foldable (for_, traverse_)
import Data.List (nub)
import Data.Traversable (for)
import Hedgehog hiding (collect)
import Pipes ((<-<))
import System.Mem (performGC)
import Test.Tasty
import Test.Tasty.Hedgehog

import qualified Hedgehog.Gen   as Gen
import qualified Hedgehog.Range as Range
import qualified Pipes
import qualified Pipes.Prelude  as Pipes

import Helpers (collect)

import qualified Database.CQRS          as CQRS
import qualified Database.CQRS.InMemory as CQRS.InMem

tests :: TestTree
tests = testGroup "In memory"
  [ testProperty "All events are written exactly once" $
      property writtenExactlyOnce
  , testProperty "Writing an event sends a notification" $
      property writingSendsNotification
  , testProperty
      "GC'ing notification stream removes it from the stream family" $
      withTests 1 $ property gcRemovesListener
  ]

writtenExactlyOnce :: PropertyT IO ()
writtenExactlyOnce = do
  streamIds <- forAll $
    nub <$> Gen.list (Range.linear 1 100) (Gen.int (Range.linear 1 1000))
  chunks <- for streamIds $ \streamId -> do
    events <- forAll $
      Gen.list (Range.linear 1 100) (Gen.int (Range.linear 1 100))
    mvar   <- liftIO newEmptyMVar
    pure (streamId, events, mvar)

  streamFamily <- CQRS.InMem.emptyStreamFamily @()

  mvars <- liftIO $ for chunks $ \(streamId, events, mvar) -> do
    _ <- forkIO $ do
      stream <- CQRS.getStream streamFamily streamId
      traverse_ (CQRS.writeEvent stream) events
      putMVar mvar ()
    pure mvar

  liftIO $ traverse_ takeMVar mvars

  lastEventIdentifiers <-
    collect $ CQRS.latestEventIdentifiers streamFamily

  length chunks === length lastEventIdentifiers

  for_ lastEventIdentifiers $ \(streamId, eventId) -> do
    stream <- CQRS.getStream streamFamily streamId
    events <- collect $ CQRS.streamEvents stream mempty
    annotateShow events
    length events === fromInteger eventId

writingSendsNotification :: PropertyT IO ()
writingSendsNotification = do
  streamFamily <- CQRS.InMem.emptyStreamFamily @()
  notifStream <- CQRS.allNewEvents streamFamily

  events <- forAll $ Gen.list (Range.linear 1 1000) $
    (,)
      <$> Gen.integral (Range.linear 1 10)
      <*> Gen.double (Range.linearFrac 100 10000)

  _ <- liftIO . forkIO $
    for_ events $ \(streamId, event) -> do
      stream <- CQRS.getStream streamFamily (streamId :: Int)
      CQRS.writeEvent stream (event :: Double)

  notifications <- collect $
    Pipes.take (length events)
      <-< forever (Pipes.each =<< Pipes.await)
      <-< notifStream

  (\f -> zipWithM_ f events notifications) $
    \(streamId, event) (streamId', ewc) -> do
      let Right CQRS.EventWithContext{ CQRS.event = event' } = ewc
      streamId === streamId'
      event    === event'

gcRemovesListener :: PropertyT IO ()
gcRemovesListener = do
  streamFamily <- CQRS.InMem.emptyStreamFamily @()
  stream <- CQRS.getStream streamFamily ()

  notifStreamMVar <- liftIO newEmptyMVar
  liftIO $ putMVar notifStreamMVar . Just =<< CQRS.allNewEvents streamFamily

  -- Fill the queue.
  replicateM_ 100 $ CQRS.writeEvent stream ()

  do
    -- Trying to write a new event should fail.
    mvar <- liftIO newEmptyMVar
    threadId <- liftIO . forkIO $ do
      _ <- CQRS.writeEvent stream () -- This should block.
      putMVar mvar () -- This should not happen.
    liftIO $ threadDelay 1000

    failed <- liftIO $ isEmptyMVar mvar
    liftIO $ killThread threadId
    failed === True

  liftIO $ modifyMVar_ notifStreamMVar $ \_ -> pure Nothing
  liftIO performGC

  do
    mvar <- liftIO newEmptyMVar
    threadId <- liftIO . forkIO $ do
      _ <- CQRS.writeEvent stream () -- This should not block.
      putMVar mvar () -- This should happen.
    liftIO $ threadDelay 1000

    failed <- liftIO $ isEmptyMVar mvar
    liftIO $ killThread threadId
    failed === False