{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Events can be written to a stream and can be streamed from it.

module Database.CQRS.Stream
  ( Stream(..)
  , WritableStream(..)
  , EventWithContext(..)
  , EventWithContext'
  , MonadMetadata(..)
  , writeEvent
  , StreamBounds(..)
  , StreamBounds'
  , afterEvent
  , untilEvent
  ) where

import GHC.Generics

import qualified Pipes

class Stream f stream where
  -- | Type of the events contained in that stream.
  type EventType stream :: *

  -- | Type of unique identifiers for events in the stream.
  --
  -- There must be a total order on identifiers so they can be sorted.
  type EventIdentifier stream :: *

  -- | Depending on the store, this structure can contain the creation date, a
  -- correlation ID, etc.
  type EventMetadata stream :: *

  -- | Stream all the events within some bounds.
  --
  -- Events must be streamed from lowest to greatest identifier.
  streamEvents
    :: stream
    -> StreamBounds' stream
    -> Pipes.Producer (EventWithContext' stream) f ()

class Stream f stream => WritableStream f stream where
  -- | Append the event to the stream and return the identifier.
  --
  -- The identifier must be greater than the previous events' identifiers.
  writeEventWithMetadata
    :: stream
    -> EventType stream
    -> EventMetadata stream
    -> f (EventIdentifier stream)

-- | Once added to the stream, an event is adorned with an identifier and some
-- metadata.
data EventWithContext identifier metadata event = EventWithContext
  { identifier :: identifier
  , metadata   :: metadata
  , event      :: event
  } deriving (Eq, Show, Generic)

type EventWithContext' stream
  = EventWithContext
      (EventIdentifier stream)
      (EventMetadata stream)
      (EventType stream)

-- | The event metadata come from the current "environment".
class MonadMetadata metadata m where
  getMetadata :: m metadata

instance Monad m => MonadMetadata () m where
  getMetadata = pure ()

-- | Get the metadata from the environment, append the event to the store and
-- return the identifier.
writeEvent
  :: (Monad m, MonadMetadata (EventMetadata stream) m, WritableStream m stream)
  => stream
  -> EventType stream
  -> m (EventIdentifier stream)
writeEvent stream ev = do
  md <- getMetadata
  writeEventWithMetadata stream ev md

-- | Lower/upper bounds of an event stream.
--
-- The 'Semigroup' instance returns bounds for the intersection of the two
-- streams.
data StreamBounds identifier = StreamBounds
  { _afterEvent :: Maybe identifier -- ^ Exclusive.
  , _untilEvent :: Maybe identifier -- ^ Inclusive.
  }

type StreamBounds' stream = StreamBounds (EventIdentifier stream)

instance
    forall identifier. Ord identifier
    => Semigroup (StreamBounds identifier) where
  sb1 <> sb2 =
    StreamBounds
      { _afterEvent = combine _afterEvent max
      , _untilEvent = combine _untilEvent min
      }
    where
      combine
        :: (StreamBounds identifier -> Maybe b) -> (b -> b -> b) -> Maybe b
      combine proj merge =
        case (proj sb1, proj sb2) of
          (mx, Nothing) -> mx
          (Nothing, my) -> my
          (Just x, Just y) -> Just $ merge x y

instance Ord identifier => Monoid (StreamBounds identifier) where
  mempty = StreamBounds Nothing Nothing

-- | After the event with the given identifier, excluding it.
afterEvent :: Ord identifier => identifier -> StreamBounds identifier
afterEvent i = mempty { _afterEvent = Just i }

-- | Until the event with the given identifier, including it.
untilEvent :: Ord identifier => identifier -> StreamBounds identifier
untilEvent i = mempty { _untilEvent = Just i }
