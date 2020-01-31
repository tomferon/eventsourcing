{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module provides the interface between an event stream and the event
-- type of the stream. An 'Event' is something that can be read from an event
-- stream. It can also be a 'WritableEvent' which, unsurprisingly, means it
-- can be added to the stream. Having two different typeclasses allows us to
-- prevent a subset of events to be written as in the following example.
--
-- @
--  data Current
--  data Deprecated
--
--  data MyEvent a where
--    EventA :: MyEvent Current
--    EventB :: MyEvent Current
--    EventC :: MyEvent Deprecated
--
--  instance Event (MyEvent e) where
--    type DecodingFormat = Aeson.Value
--    decodeEvent = Aeson.eitherDecode
--
--  instance WritableEvent (MyEvent Current) where
--    type EncodingFormat = Aeson.Value
--    encodeEvent = Aeson.encode
--
--  instance TypeError (Text "Cannot write deprecated event")
--      => WritableEvent (MyEvent Deprecated)
-- @

module Database.CQRS.Event
  ( Event(..)
  , WritableEvent(..)
  ) where

-- | Event that can read from some event stream with compatible decoding format
-- and fed to a projection or aggregated in some way.
class Event e where
  -- | Format in which the event is encoded, e.g. 'Data.Aeson.Value'.
  type EncodingFormat e :: *
  decodeEvent :: EncodingFormat e -> Either String e

-- | Event that can be written to an event stream. This is separate from 'Event'
-- to make it possible to restrict the events that can be written with a GADT.
class Event e => WritableEvent e where
  -- | Format from which the event can be decoded, e.g. 'Data.Aeson.Value'.
  encodeEvent :: e -> EncodingFormat e