{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Events can be written to a stream and can be streamed from it.

module Database.CQRS.Stream
  ( Stream(..)
  , EventWithContext(..)
  , EventWithContext'
  , StreamBounds(..)
  , afterEvent
  , untilEvent
  ) where

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

  -- | Append the event to the stream and return the identifier.
  --
  -- The identifier must be greater than the previous events' identifiers.
  writeEvent :: stream -> EventType stream -> f (EventIdentifier stream)

  -- | Stream all the events within some bounds.
  --
  -- Events must be streamed from lowest to greatest identifier.
  streamEvents
    :: stream
    -> StreamBounds stream
    -> Pipes.Producer (EventWithContext' stream) f ()

-- | Once added to the stream, an event is adorned with an identifier and some
-- metadata.
data EventWithContext event identifier metadata
  = EventWithContext
      { event      :: event
      , identifier :: identifier
      , metadata   :: metadata
      }

type EventWithContext' stream
  = EventWithContext
      (EventType stream)
      (EventIdentifier stream)
      (EventMetadata stream)

-- | Lower/upper bounds of an event stream.
--
-- The 'Semigroup' instance returns bounds for the intersection of the two
-- streams.
data StreamBounds stream = StreamBounds
  { _afterEvent :: Maybe (EventIdentifier stream) -- ^ Exclusive.
  , _untilEvent :: Maybe (EventIdentifier stream) -- ^ Inclusive.
  }

instance
    forall stream. Ord (EventIdentifier stream)
    => Semigroup (StreamBounds stream) where
  sb1 <> sb2 =
    StreamBounds
      { _afterEvent = combine _afterEvent max
      , _untilEvent = combine _untilEvent min
      }
    where
      combine :: (StreamBounds stream -> Maybe b) -> (b -> b -> b) -> Maybe b
      combine proj merge =
        case (proj sb1, proj sb2) of
          (mx, Nothing) -> mx
          (Nothing, my) -> my
          (Just x, Just y) -> Just $ merge x y

instance Ord (EventIdentifier stream) => Monoid (StreamBounds stream) where
  mempty = StreamBounds Nothing Nothing

-- | After the event with the given identifier, excluding it.
afterEvent
  :: Ord (EventIdentifier stream)
  => EventIdentifier stream -> StreamBounds stream
afterEvent i = mempty { _afterEvent = Just i }

-- | Until the event with the given identifier, including it.
untilEvent
  :: Ord (EventIdentifier stream)
  => EventIdentifier stream -> StreamBounds stream
untilEvent i = mempty { _untilEvent = Just i }
