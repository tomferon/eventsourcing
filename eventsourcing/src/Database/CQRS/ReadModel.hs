{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TypeFamilies          #-}

module Database.CQRS.ReadModel
  ( ReadModel(..)
  , ReadModelF(..)
  , toReadModelF
  ) where

class ReadModel f model where
  type ReadModelQuery    model :: *
  type ReadModelResponse model :: *

  query :: model -> ReadModelQuery model -> f (ReadModelResponse model)

-- | Turn a read model into a functor.
data ReadModelF model a = ReadModelF
  { readModel :: model
  , transform :: ReadModelResponse model -> a
  }

instance Functor (ReadModelF model) where
  fmap f ReadModelF{..} = ReadModelF { transform = f . transform, .. }

instance (Functor f, ReadModel f model) => ReadModel f (ReadModelF model a) where
  type ReadModelQuery (ReadModelF model a) = ReadModelQuery model
  type ReadModelResponse (ReadModelF model a) = a
  query = readModelFQuery

readModelFQuery
  :: (Functor f, ReadModel f model)
  => ReadModelF model a -> ReadModelQuery model -> f a
readModelFQuery ReadModelF{..} = fmap transform . query readModel

toReadModelF :: model -> ReadModelF model (ReadModelResponse model)
toReadModelF readModel = ReadModelF { transform = id, .. }
