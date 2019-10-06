module Database.CQRS.PostgreSQL
  ( -- * Stream
    Stream
  , makeStream
  , makeStream'

    -- * Stream family
  , StreamFamily(..)
  ) where

import Database.CQRS.PostgreSQL.Stream
import Database.CQRS.PostgreSQL.StreamFamily
