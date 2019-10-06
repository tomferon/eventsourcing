{-# LANGUAGE DeriveGeneric #-}

module Database.CQRS.Error
  ( Error(..)
  ) where

import GHC.Generics

data Error
  = EventWriteError String
  | EventDecodingError String
  | EventRetrievalError String
  | NewEventsStreamingError String
  deriving (Eq, Show, Generic)
