module Database.CQRS
  ( -- * Events
    Event(..)
  , WritableEvent(..)

    -- * Projections
  , Projection
  ) where

import Database.CQRS.Event
import Database.CQRS.Projection
