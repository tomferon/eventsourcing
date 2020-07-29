{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Database.CQRS.InMemory
  ( Stream
  , emptyStream
  , StreamFamily
  , emptyStreamFamily
  ) where

import Prelude hiding (last, length)

import Control.Exception   (evaluate)
import Control.Monad       (forever, forM, when)
import Control.Monad.Trans (MonadIO(..))
import Data.Foldable       (foldrM)
import System.Mem.Weak     (Weak, deRefWeak, mkWeakPtr)

import qualified Control.Concurrent.STM as STM
import qualified Control.DeepSeq        as Seq
import qualified Control.Monad.Except   as Exc
import qualified Data.Hashable          as Hash
import qualified Data.HashMap.Strict    as HM
import qualified GHC.Conc
import qualified Pipes

import qualified Database.CQRS as CQRS

-- | In-memory event stream.
--
-- It's using STM internally so it's safe to have different thread writing
-- events to the stream concurrently. Its main purpose is for tests but it could
-- be used for production code as well.
data Stream metadata event = Stream
  { events :: InnerStream (event, metadata)
  , last   :: STM.TVar (STM.TMVar (InnerStream (event, metadata)))
  , length :: STM.TVar Integer
  , notify :: CQRS.EventWithContext Integer metadata event -> STM.STM ()
  }

data InnerStream a
  = Cons a (InnerStream a)
  | Future (STM.TMVar (InnerStream a))

-- | Initialise an empty event stream.
emptyStream :: MonadIO m => m (Stream metadata event)
emptyStream =
  liftIO . STM.atomically $ emptyStreamSTM

emptyStreamSTM :: STM.STM (Stream metadata event)
emptyStreamSTM = do
  tmvar  <- STM.newEmptyTMVar
  last   <- STM.newTVar tmvar
  length <- STM.newTVar 0
  let events = Future tmvar
      notify = const $ pure ()
  pure Stream{..}

instance MonadIO m => CQRS.Stream m (Stream metadata event) where
  type EventType       (Stream metadata event) = event
  type EventIdentifier (Stream metadata event) = Integer -- starting at 1
  type EventMetadata   (Stream metadata event) = metadata

  streamEvents = streamStreamEvent

instance
    ( Exc.MonadError CQRS.Error m
    , MonadIO m
    , Seq.NFData event
    , Seq.NFData metadata
    )
    => CQRS.WritableStream m (Stream metadata event) where
  writeEventWithMetadata = streamWriteEventWithMetadata

streamWriteEventWithMetadata
  :: ( CQRS.EventIdentifier (Stream metadata event) ~ Integer
     , Exc.MonadError CQRS.Error m
     , MonadIO m
     , Seq.NFData event
     , Seq.NFData metadata
     )
  => Stream metadata event
  -> event
  -> metadata
  -> CQRS.ConsistencyCheck Integer
  -> m Integer
streamWriteEventWithMetadata Stream{..} event metadata cc =
  (Exc.liftEither =<<) . liftIO $ do
    -- Force exceptions now and in this thread.
    evaluate $ Seq.rnf (event, metadata)
    STM.atomically $ do
      mErr <- case cc of
        CQRS.CheckNoEvents -> do
          len <- STM.readTVar length
          pure $ if len == 0 then Nothing else Just "stream not empty"
        CQRS.CheckLastEvent identifier -> do
          len <- STM.readTVar length
          -- The identifier of the last event is the stream length.
          pure $ if len == identifier
            then Nothing
            else Just "last event identifier doesn't match"
        CQRS.NoConsistencyCheck -> pure Nothing

      case mErr of
        Just err -> pure . Left . CQRS.ConsistencyCheckError $ err
        Nothing -> do
          lastVar <- STM.readTVar last
          newVar  <- STM.newEmptyTMVar
          let innerStream = Cons (event, metadata) (Future newVar)
          STM.putTMVar lastVar innerStream
          STM.writeTVar last newVar
          identifier <- STM.stateTVar length $ \l ->
            let l' = l + 1 in l' `seq` (l', l')
          notify $ CQRS.EventWithContext identifier metadata event
          pure $ Right identifier

streamStreamEvent
  :: MonadIO m
  => Stream metadata event
  -> CQRS.StreamBounds' (Stream metadata event)
  -> Pipes.Producer
      [ Either
          (Integer, String) (CQRS.EventWithContext' (Stream metadata event))
      ] m ()
streamStreamEvent stream bounds = do
    let innerStream = events stream
    go 1 innerStream
  where
    go
      :: MonadIO m
      => Integer
      -> InnerStream (event, metadata)
      -> Pipes.Producer
          [ Either
              (Integer, String) (CQRS.EventWithContext Integer metadata event)
          ] m ()
    go n = \case
      Cons (event, metadata) innerStream -> do
        when (inBounds n) $
          Pipes.yield [Right (CQRS.EventWithContext n metadata event)]
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

-- | A family of in-memory streams.
--
-- There are two things to be aware of when using this type:
--
-- 'getStream' adds a new empty stream to the family regardless of whether it's
-- used or not. If an attacker can make your application call 'getStream' with
-- arbitrary stream identifiers, it can lead to a Denial-of-Service attack.
--
-- Slow consumers of event notifications (from 'allNewEvents') could become an
-- issue if events are written at a higher pace that they can keep up with. In
-- order to avoid the queue of notifications to grow bigger and bigger, it's
-- capped at 100 (hard-coded for now.) This means that writing a new event can
-- be blocked by a full consumer's queue.
data StreamFamily identifier metadata event = StreamFamily
  { hashMap :: STM.TVar (HM.HashMap identifier (Stream metadata event))
  , queues
      :: STM.TVar [Weak (STM.TBQueue
                          ( identifier
                          , CQRS.EventWithContext' (Stream metadata event)
                          ))]
  }

-- | Initialise an empty stream family.
emptyStreamFamily
  -- metadata first as it's the most likely to be passed explicitly.
  :: forall metadata identifier event m. MonadIO m
  => m (StreamFamily identifier metadata event)
emptyStreamFamily =
  liftIO . STM.atomically $ do
    hashMap <- STM.newTVar HM.empty
    queues  <- STM.newTVar []
    pure StreamFamily{..}

instance
    (Eq identifier, Hash.Hashable identifier, MonadIO m)
    => CQRS.StreamFamily m (StreamFamily identifier metadata event) where
  type StreamType (StreamFamily identifier metadata event) =
    Stream metadata event
  type StreamIdentifier (StreamFamily identifier metadata event) =
    identifier

  getStream              = streamFamilyGetStream
  allNewEvents           = streamFamilyAllNewEvents
  latestEventIdentifiers = streamFamilyLatestEventIdentifiers

streamFamilyNotify
  :: StreamFamily identifier metadata event
  -> identifier
  -> CQRS.EventWithContext' (Stream metadata event)
  -> STM.STM ()
streamFamilyNotify StreamFamily{..} identifier event = do
  qs <- STM.readTVar queues
  qs' <- (\f -> foldrM f [] qs) $ \queueWeak acc -> do
    -- This call to unsafeIOToSTM should be safe in this context.
    mQueue <- GHC.Conc.unsafeIOToSTM $ deRefWeak queueWeak
    case mQueue of
      Just queue -> do
        STM.writeTBQueue queue (identifier, event)
        pure $ queueWeak : acc
      Nothing -> pure acc
  STM.writeTVar queues qs'

streamFamilyGetStream
  :: (Eq identifier, Hash.Hashable identifier, MonadIO m)
  => StreamFamily identifier metadata event
  -> identifier
  -> m (Stream metadata event)
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
  :: forall identifier metadata event m a. MonadIO m
  => StreamFamily identifier metadata event
  -> m (Pipes.Producer
        [ ( identifier
          , Either
              (Integer, String)
              (CQRS.EventWithContext' (Stream metadata event))
          ) ]
        m a)
streamFamilyAllNewEvents StreamFamily{..} = do
    queue <- initialise
    pure $ producer queue

  where
    initialise
      :: m (STM.TBQueue
             (identifier, CQRS.EventWithContext' (Stream metadata event)))
    initialise = liftIO $ do
      queue <- STM.newTBQueueIO 100
      queueWeak <- mkWeakPtr queue Nothing
      STM.atomically $ STM.modifyTVar' queues (queueWeak :)
      pure queue

    producer
      :: STM.TBQueue
          (identifier, CQRS.EventWithContext' (Stream metadata event))
      -> Pipes.Producer
          [ ( identifier
            , Either
                (Integer, String)
                (CQRS.EventWithContext' (Stream metadata event))
            ) ]
          m a
    producer queue = forever $ do
      xs <- liftIO . STM.atomically $
        (:) <$> STM.readTBQueue queue <*> STM.flushTBQueue queue
      Pipes.yield $ map (fmap Right) xs

streamFamilyLatestEventIdentifiers
  :: MonadIO m
  => StreamFamily identifier metadata event
  -> Pipes.Producer (identifier, Integer) m ()
streamFamilyLatestEventIdentifiers StreamFamily{..} = do
  ids <- liftIO . STM.atomically $ do
    hm <- STM.readTVar hashMap
    let pairs = HM.toList hm
    forM pairs $ \(streamId, stream) -> do
      lastEventId <- STM.readTVar $ length stream
      pure (streamId, lastEventId)

  Pipes.each ids
