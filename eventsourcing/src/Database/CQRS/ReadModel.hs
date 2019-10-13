{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

module Database.CQRS.ReadModel
  ( ReadModel(..)
  ) where

class ReadModel f model where
  type ReadModelQuery    model :: *
  type ReadModelResponse model :: *

  query :: model -> ReadModelQuery model -> f (ReadModelResponse model)