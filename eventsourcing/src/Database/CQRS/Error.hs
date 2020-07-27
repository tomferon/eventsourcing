{-# LANGUAGE DeriveGeneric #-}

module Database.CQRS.Error
  ( Error(..)
  ) where

import GHC.Generics

data Error
  = EventWriteError String
  | EventDecodingError String String
  | EventRetrievalError String
  | NewEventsStreamingError String
  | ProjectionError String
  | MigrationError String
  | ConsistencyCheckError String
  | TrackingTableError String
  deriving (Eq, Show, Generic)
