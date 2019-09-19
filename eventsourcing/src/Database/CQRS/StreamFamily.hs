{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

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

  -- | Stream newly-created events from *all* streams.
  --
  -- Events should appear in the correct order within a given stream but not
  -- necessarily in-between them, i.e. two events belonging to different streams
  -- won't necessarily be ordered according to the chronological history.
  allNewEvents
    :: fam
    -> Pipes.Producer
        (StreamIdentifier fam, (EventWithContext' (StreamType fam)))
        f ()

  -- | Stream the identifier of the latest events for each stream in the family.
  latestEventIdentifiers
    :: fam
    -> Pipes.Producer
        (StreamIdentifier fam, EventIdentifier (StreamType fam))
        f ()
