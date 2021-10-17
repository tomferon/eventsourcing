{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE QuantifiedConstraints     #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE UndecidableInstances      #-}

module Database.CQRS.ReadModel
  ( ReadModel(..)
  , AllF
  , ReadModelP(..)
  , toReadModelP
  , toReadModelP'
  , bindReadModelP
  ) where

import Control.Monad   ((>=>))
import Data.Kind       (Constraint, Type)
import Data.Profunctor

class ReadModel f model where
  type ReadModelQuery    model :: *
  type ReadModelResponse model :: *

  query :: model -> ReadModelQuery model -> f (ReadModelResponse model)

type family AllF
    (cs :: [(Type -> Type) -> Constraint])
    (f :: Type -> Type) :: Constraint where
  AllF '[] f = ()
  AllF (c ': cs) f = (c f, AllF cs f)

-- | Turn a read model into a profunctor.
data ReadModelP cs a b =
  forall model. (forall f. AllF cs f => ReadModel f model) => ReadModelP
    { readModel         :: model
    , transformQuery    :: a -> ReadModelQuery model
    , transformResponse
        :: forall f. (Monad f, AllF cs f)
        => (ReadModelQuery model, ReadModelResponse model) -> f b
    }

instance Functor (ReadModelP cs a) where
  fmap f ReadModelP{..} =
    ReadModelP { transformResponse = fmap f . transformResponse, .. }

instance Profunctor (ReadModelP cs) where
  lmap f ReadModelP{..} = ReadModelP { transformQuery = transformQuery . f, .. }
  rmap = fmap

instance (Monad f, AllF cs f) => ReadModel f (ReadModelP cs a b) where
  type ReadModelQuery (ReadModelP cs a b) = a
  type ReadModelResponse (ReadModelP cs a b) = b
  query = readModelFQuery

readModelFQuery :: (Monad f, AllF cs f) => ReadModelP cs a b -> a -> f b
readModelFQuery ReadModelP{..} q = do
  let q' = transformQuery q
  r <- query readModel q'
  transformResponse (q', r)

bindReadModelP
  :: (forall m. (Monad m, AllF cs m) => b -> m c)
  -> ReadModelP cs a b
  -> ReadModelP cs a c
bindReadModelP f ReadModelP{..} =
  ReadModelP { transformResponse = transformResponse >=> f, .. }

toReadModelP'
  :: (forall f. AllF cs f => ReadModel f model)
  => model
  -> ReadModelP cs
      (ReadModelQuery model)
      (ReadModelQuery model, ReadModelResponse model)
toReadModelP' readModel =
  ReadModelP
    { transformQuery = id
    , transformResponse = pure
    , ..
    }

toReadModelP
  :: (forall f. AllF cs f => ReadModel f model)
  => model
  -> ReadModelP cs (ReadModelQuery model) (ReadModelResponse model)
toReadModelP = fmap snd . toReadModelP'
