{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies          #-}

module Database.CQRS.StreamFamily
  ( StreamFamily(..)
  ) where

import qualified Pipes

import Database.CQRS.Stream

-- | A stream family is a collection of streams of the same type.
--
-- For example, a table in a relational database can contain events for all
-- aggregates of a certain type. Each aggregate has its own stream of events but
-- they are all stored in the same table. That table is a stream family indexed
-- by aggregate ID.
class StreamFamily f fam where
  -- | Type of the streams contained in this stream family.
  type StreamType fam :: *

  -- | Identifier for a specific stream, e.g. an aggregate ID.
  type StreamIdentifier fam :: *

  -- | Get the stream corresponding to a given identifier.
  getStream :: fam -> StreamIdentifier fam -> f (StreamType fam)

  -- | Initialise and return a producer of newly-created events from *all*
  -- streams in arbitrary batches. If an event can't be decoded, the decoding
  -- error is returned instead.
  --
  -- Events should appear in the correct order within a given stream but not
  -- necessarily in-between them, i.e. two events belonging to different streams
  -- won't necessarily be ordered according to the chronological history.
  --
  -- It is okay for events to be sent more than one time as long as the order
  -- is respected within each stream if it makes the implementation easier and
  -- prevents the loss of some events.
  --
  -- How events are batched together is up to the implementation as long as the
  -- order is respected.
  --
  -- It is okay for batches to be empty to signal that there are currently no
  -- new notifications. This is important for migrations, so they know they have
  -- processed all events.
  allNewEvents
    :: fam
    -> f (Pipes.Producer
          [ ( StreamIdentifier fam
            , Either
                (EventIdentifier (StreamType fam), String)
                (EventWithContext' (StreamType fam))
            ) ]
          f ())

  -- | Stream the identifier of the latest events for each stream in the family.
  --
  -- It is a snapshot of the last event identifiers at the time the producer is
  -- called. It is meant to be used by projections to catch up to the latest
  -- state.
  latestEventIdentifiers
    :: fam
    -> Pipes.Producer
        (StreamIdentifier fam, EventIdentifier (StreamType fam))
        f ()
