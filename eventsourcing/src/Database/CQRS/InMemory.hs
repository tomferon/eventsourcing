{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}

module Database.CQRS.InMemory
  ( Stream
  , emptyStream
  , StreamFamily
  , emptyStreamFamily
  ) where

import Prelude hiding (last, length)

import Control.Exception (evaluate)
import Control.Monad (forM, when)
import Control.Monad.Trans (MonadIO(..))
import Data.Foldable (foldrM)
import System.Mem.Weak (addFinalizer)

import qualified Control.Concurrent.STM as STM
import qualified Control.DeepSeq        as Seq
import qualified Data.Hashable          as Hash
import qualified Data.HashMap.Strict    as HM
import qualified Pipes

import qualified Database.CQRS as CQRS

-- | In-memory event stream.
--
-- It's using STM internally so it's safe to have different thread writing
-- events to the stream concurrently. It's main purpose is for tests but
-- it could be used for production code as well.
data Stream event = Stream
  { events :: InnerStream event
  , last   :: STM.TVar (STM.TMVar (InnerStream event))
  , length :: STM.TVar Integer
  , notify :: CQRS.EventWithContext event Integer () -> STM.STM ()
  }

data InnerStream a
  = Cons a (InnerStream a)
  | Future (STM.TMVar (InnerStream a))

-- | Initialise an empty event stream.
emptyStream :: MonadIO m => m (Stream event)
emptyStream =
  liftIO . STM.atomically $ emptyStreamSTM

emptyStreamSTM :: STM.STM (Stream event)
emptyStreamSTM = do
  tmvar  <- STM.newEmptyTMVar
  last   <- STM.newTVar tmvar
  length <- STM.newTVar 0
  let events = Future tmvar
      notify = const $ pure ()
  pure Stream{..}

instance (MonadIO m, Seq.NFData event)
  => CQRS.Stream m (Stream event) where
  type EventType       (Stream event) = event
  type EventIdentifier (Stream event) = Integer -- starting at 1
  type EventMetadata   (Stream event) = ()

  writeEvent   = streamWriteEvent
  streamEvents = streamStreamEvent

streamWriteEvent
  :: (MonadIO m, Seq.NFData event)
  => Stream event -> event -> m Integer
streamWriteEvent Stream{..} event =
  liftIO $ do
    evaluate $ Seq.rnf event -- Force exceptions now and in this thread.
    STM.atomically $ do
      lastVar <- STM.readTVar last
      newVar  <- STM.newEmptyTMVar
      let innerStream = Cons event (Future newVar)
      STM.putTMVar lastVar innerStream
      STM.writeTVar last newVar
      identifier <- STM.stateTVar length $ \l ->
        let l' = l + 1 in l' `seq` (l', l')
      notify $ CQRS.EventWithContext event identifier ()
      pure identifier

streamStreamEvent
  :: MonadIO m
  => Stream event
  -> CQRS.StreamBounds (Stream event)
  -> Pipes.Producer (CQRS.EventWithContext' (Stream event)) m ()
streamStreamEvent stream bounds = do
    let innerStream = events stream
    go 1 innerStream
  where
    go
      :: MonadIO m
      => Integer
      -> InnerStream event
      -> Pipes.Producer (CQRS.EventWithContext event Integer ()) m ()
    go n = \case
      Cons event innerStream -> do
        when (inBounds n) $
          Pipes.yield $ CQRS.EventWithContext event n ()
        go (n+1) innerStream
      Future var -> do
        mInnerStream <- liftIO . STM.atomically . STM.tryReadTMVar $ var
        case mInnerStream of
          Nothing -> pure ()
          Just innerStream -> go n innerStream

    inBounds :: Integer -> Bool
    inBounds n =
      maybe True (n >) (CQRS._afterEvent bounds)
      && maybe True (n <=) (CQRS._untilEvent bounds)

data StreamFamily identifier event = StreamFamily
  { hashMap :: STM.TVar (HM.HashMap identifier (Stream event))
  , queues
      :: STM.TVar
           [ ( STM.TBQueue (identifier, CQRS.EventWithContext' (Stream event))
             , STM.TVar Bool -- Is this queue still alive?
             )
           ]
  }

emptyStreamFamily :: MonadIO m => m (StreamFamily identifier event)
emptyStreamFamily =
  liftIO . STM.atomically $ do
    hashMap <- STM.newTVar HM.empty
    queues  <- STM.newTVar []
    pure StreamFamily{..}

instance (Eq identifier, Hash.Hashable identifier, MonadIO m)
  => CQRS.StreamFamily m (StreamFamily identifier event) where
  type StreamType (StreamFamily identifier event) = Stream event
  type StreamIdentifier (StreamFamily identifier event) = identifier

  getStream              = streamFamilyGetStream
  allNewEvents           = streamFamilyAllNewEvents
  latestEventIdentifiers = streamFamilyLatestEventIdentifiers

streamFamilyNotify
  :: StreamFamily identifier event
  -> identifier
  -> CQRS.EventWithContext' (Stream event) 
  -> STM.STM ()
streamFamilyNotify StreamFamily{..} identifier event = do
  qs <- STM.readTVar queues
  qs' <- (\f -> foldrM f [] qs) $ \pair@(q, aliveVar) acc -> do
    alive <- STM.readTVar aliveVar
    if alive
      then do
        STM.writeTBQueue q (identifier, event)
        pure $ pair : acc
      else
        pure acc
  STM.writeTVar queues qs'

streamFamilyGetStream
  :: (Eq identifier, Hash.Hashable identifier, MonadIO m)
  => StreamFamily identifier event -> identifier -> m (Stream event)
streamFamilyGetStream sf@StreamFamily{..} identifier =
  liftIO . STM.atomically $ do
    hm <- STM.readTVar hashMap
    case HM.lookup identifier hm of
      Nothing -> do
        stream <- emptyStreamSTM
        let stream' = stream { notify = streamFamilyNotify sf identifier }
            hm' = HM.insert identifier stream' hm
        STM.writeTVar hashMap hm'
        pure stream'
      Just stream -> pure stream

streamFamilyAllNewEvents
  :: MonadIO m
  => StreamFamily identifier event
  -> Pipes.Producer (identifier, CQRS.EventWithContext' (Stream event)) m ()
streamFamilyAllNewEvents =
    go ()
  where
    go
      :: MonadIO m
      => () -- When this is garbage-collected, the queue is deregistered.
      -> StreamFamily identifier event
      -> Pipes.Producer
          (identifier, CQRS.EventWithContext' (Stream event))
          m ()
    go key StreamFamily{..} = do
      queue <- liftIO $ do
        q     <- STM.newTBQueueIO 100
        alive <- STM.newTVarIO True
        addFinalizer key $
          STM.atomically . STM.writeTVar alive $ False
        STM.atomically $
          STM.modifyTVar' queues ((q, alive) :)
        pure q
      consume key queue

    consume :: MonadIO m => () -> STM.TBQueue a -> Pipes.Producer a m ()
    consume key queue = do
      x <- liftIO . STM.atomically $ STM.readTBQueue queue
      Pipes.yield x
      consume key queue

streamFamilyLatestEventIdentifiers
  :: MonadIO m
  => StreamFamily identifier event
  -> Pipes.Producer (identifier, Integer) m ()
streamFamilyLatestEventIdentifiers StreamFamily{..} = do
  ids <- liftIO . STM.atomically $ do
    hm <- STM.readTVar hashMap
    let pairs = HM.toList hm
    forM pairs $ \(streamID, stream) -> do
      len <- STM.readTVar $ length stream
      let lastEventID = len - 1
      pure (streamID, lastEventID)

  Pipes.each ids
