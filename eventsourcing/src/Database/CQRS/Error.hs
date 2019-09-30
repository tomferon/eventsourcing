{-# LANGUAGE DeriveGeneric #-}

module Database.CQRS.Error
  ( Error(..)
  ) where

import GHC.Generics

data Error
  = EventWriteError String
  | EventDecodingError String
  deriving (Eq, Show, Generic)
