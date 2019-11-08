{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}

module Database.CQRS.Transformer
  ( -- * Transformed stream
    Transformer
  , TransformedStream
  , transformStream

    -- * Transformed stream family
  , TransformedStreamFamily
  , transformStreamFamily

    -- * Transform monad
  , Transform
  , pushEvent
  , mergeEvents
  , flushEvents
  ) where

import Control.Monad (forM_)
import Control.Monad.Trans (lift)
import Data.Hashable (Hashable)

import qualified Data.HashMap.Strict as HM
import qualified Control.Monad.Free  as Free
import qualified Control.Monad.State as St
import qualified Pipes
import qualified Pipes.Prelude       as Pipes

import qualified Database.CQRS as CQRS

type Transformer inputEvent event =
  inputEvent -> Transform event ()

data TransformedStream m identifier metadata event =
  forall stream. CQRS.Stream m stream => TransformedStream
    { transformer
        :: Transformer
            (Either
              (CQRS.EventIdentifier stream, String)
              (CQRS.EventWithContext' stream))
            (CQRS.EventWithContext identifier metadata event)
    , inputStream :: stream
    , reverseIdentifier :: identifier -> m (CQRS.EventIdentifier stream)
    }

transformStream
  :: CQRS.Stream m stream
  => Transformer
      (Either
        (CQRS.EventIdentifier stream, String)
        (CQRS.EventWithContext' stream))
      (CQRS.EventWithContext identifier metadata event)
  -> (identifier -> m (CQRS.EventIdentifier stream))
  -> stream
  -> TransformedStream m identifier metadata event
transformStream transformer reverseIdentifier inputStream =
  TransformedStream{..}

instance
    Monad m
    => CQRS.Stream m (TransformedStream m identifier metadata event) where
  type EventType (TransformedStream m identifier metadata event) = event
  type EventIdentifier (TransformedStream m identifier metadata event) =
    identifier
  type EventMetadata (TransformedStream m identifier metadata event) = metadata

  streamEvents = transformedStreamStreamEvents

transformedStreamStreamEvents
  :: Monad m
  => TransformedStream m identifier metadata event
  -> CQRS.StreamBounds identifier
  -> Pipes.Producer
      [ Either
          (identifier, String)
          (CQRS.EventWithContext identifier metadata event)
      ] m ()
transformedStreamStreamEvents TransformedStream{..} bounds = do
  inputBounds <- lift $ traverse reverseIdentifier bounds
  let inputs = Pipes.hoist lift $ CQRS.streamEvents inputStream inputBounds

  finalEvents <-
    (\f -> Pipes.foldM f (pure []) pure inputs) $ \waitingEvents batch -> do
      let (batches, waitingEvents') =
            runTransform waitingEvents . mapM_ transformer $ batch
      Pipes.each $ map (map Right) batches

      -- If there are too many waiting events, we flush them anyway to avoid
      -- using too much memmory.
      case drop 100 waitingEvents' of
        [] -> pure waitingEvents'
        _ -> do
          Pipes.yield $ map Right waitingEvents'
          pure []

  Pipes.yield $ map Right finalEvents

data TransformedStreamFamily m streamId eventId metadata event =
  forall streamFamily.
    ( Hashable (CQRS.StreamIdentifier streamFamily)
    , Ord (CQRS.StreamIdentifier streamFamily)
    , CQRS.Stream m (CQRS.StreamType streamFamily)
    , CQRS.StreamFamily m streamFamily
    )
  => TransformedStreamFamily
    { inputStreamFamily :: streamFamily
    , streamTransformer
        :: Transformer
            (Either
              (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
              (CQRS.EventWithContext' (CQRS.StreamType streamFamily)))
            (CQRS.EventWithContext eventId metadata event)
    , makeStreamIdentifier
        :: CQRS.StreamIdentifier streamFamily -> m streamId
    , reverseStreamIdentifier
        :: streamId -> m (CQRS.StreamIdentifier streamFamily)
    , makeEventIdentifier
        :: CQRS.EventIdentifier (CQRS.StreamType streamFamily) -> m eventId
    , reverseEventIdentifier
        :: eventId -> m (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
    }

transformStreamFamily
  :: ( Hashable (CQRS.StreamIdentifier streamFamily)
     , Ord (CQRS.StreamIdentifier streamFamily)
     , CQRS.Stream m (CQRS.StreamType streamFamily)
     , CQRS.StreamFamily m streamFamily
     )
  => Transformer
      (Either
        (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
        (CQRS.EventWithContext' (CQRS.StreamType streamFamily)))
      (CQRS.EventWithContext eventId metadata event)
  -> (CQRS.StreamIdentifier streamFamily -> m streamId)
  -> (streamId -> m (CQRS.StreamIdentifier streamFamily))
  -> (CQRS.EventIdentifier (CQRS.StreamType streamFamily) -> m eventId)
  -> (eventId -> m (CQRS.EventIdentifier (CQRS.StreamType streamFamily)))
  -> streamFamily
  -> TransformedStreamFamily m streamId eventId metadata event
transformStreamFamily streamTransformer
                      makeStreamIdentifier reverseStreamIdentifier
                      makeEventIdentifier reverseEventIdentifier
                      inputStreamFamily =
  TransformedStreamFamily{..}

instance
    Monad m
    => CQRS.StreamFamily m
        (TransformedStreamFamily m streamId eventId metadata event) where

  type StreamType (TransformedStreamFamily m streamId eventId metadata event) =
    TransformedStream m eventId metadata event

  type StreamIdentifier
        (TransformedStreamFamily m streamId eventId metadata event) = streamId

  getStream = transformedStreamFamilyGetStream
  allNewEvents = transformedStreamFamilyAllNewEvents
  latestEventIdentifiers = transformedStreamFamilyLatestEventIdentifiers

transformedStreamFamilyGetStream
  :: Monad m
  => TransformedStreamFamily m streamId eventId metadata event
  -> streamId
  -> m (TransformedStream m eventId metadata event)
transformedStreamFamilyGetStream TransformedStreamFamily{..} streamId = do
  inputStreamId <- reverseStreamIdentifier streamId
  inputStream <- CQRS.getStream inputStreamFamily inputStreamId
  pure $ transformStream streamTransformer reverseEventIdentifier inputStream

transformedStreamFamilyAllNewEvents
  :: Monad m
  => TransformedStreamFamily m streamId eventId metadata event
  -> m (Pipes.Producer
          [ ( streamId
            , Either
                (eventId, String)
                (CQRS.EventWithContext eventId metadata event)
            ) ]
          m a)
transformedStreamFamilyAllNewEvents TransformedStreamFamily{..} = do
  inputNewEvents <- CQRS.allNewEvents inputStreamFamily
  pure $ Pipes.for inputNewEvents $ \batch -> do
    let eventsByStream =
          HM.toList . HM.fromListWith (++) . map (fmap pure) $ batch
    forM_ eventsByStream $ \(inputStreamId, events) -> do
      streamId <- lift $ makeStreamIdentifier inputStreamId
      let (batches, waitingEvents) =
            runTransform [] . mapM_ streamTransformer $ events
          adorn = map ((streamId,) . Right)
      Pipes.each $ map adorn (batches ++ [waitingEvents])

transformedStreamFamilyLatestEventIdentifiers
  :: Monad m
  => TransformedStreamFamily m streamId eventId metadata event
  -> Pipes.Producer (streamId, eventId) m ()
transformedStreamFamilyLatestEventIdentifiers TransformedStreamFamily{..} = do
  let inputIds = CQRS.latestEventIdentifiers inputStreamFamily
  Pipes.for inputIds $ \(inputStreamId, inputEventId) -> do
    streamId <- lift $ makeStreamIdentifier inputStreamId
    eventId  <- lift $ makeEventIdentifier  inputEventId
    Pipes.yield (streamId, eventId)

data TransformF event a
  = PushEvent a event
  | MergeEvents ([event] -> (a, [event]))
  | FlushEvents a
  deriving Functor

-- | Monad in which you can push, merge and flush events.
type Transform event = Free.Free (TransformF event)

-- | Run the transformation starting with some waiting events and returning
-- batches of events to flush and a new list of waiting events to be fed to the
-- next call.
runTransform :: [event] -> Transform event () -> ([[event]], [event])
runTransform lastWaitingEvents =
    flip St.execState ([], lastWaitingEvents) . Free.foldFree interpreter
  where
    interpreter :: TransformF event a -> St.State ([[event]], [event]) a
    interpreter = \case
      PushEvent x event -> do
        St.modify $ \(batches, waitingEvents) ->
          (batches, waitingEvents ++ [event])
        pure x
      MergeEvents f ->
        St.state $ \(batches, waitingEvents) ->
          let (x, waitingEvents') = f waitingEvents
              st = (batches, waitingEvents')
          in (x, st)
      FlushEvents x -> do
        St.modify $ \(batches, waitingEvents) ->
          (batches ++ [waitingEvents], [])
        pure x

-- | Push a new event at the end of the queue.
pushEvent :: event -> Transform event ()
pushEvent = Free.liftF . PushEvent ()

-- | Apply a function to the queue of event returning a value and a new queue,
-- sets the queue to the new one and return the value.
--
-- The intent is to allow a new event to be merged in a previous one if possible
-- to make the new event stream more compact.
mergeEvents :: ([event] -> (a, [event])) -> Transform event a
mergeEvents = Free.liftF . MergeEvents

-- | Flush the queue so it can be processed downstream, e.g. sent to a message
-- broker.
--
-- Flushing may also occur automatically.
flushEvents :: Transform event ()
flushEvents = Free.liftF $ FlushEvents ()