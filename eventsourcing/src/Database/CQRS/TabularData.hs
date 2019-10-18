{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | This module provides a backend-agnostic abstraction on top on tabular data
-- allowing projections to decouple projections from their storage.
--
-- A 'Table' is a list of 'Tuple's. 'TabularDataAction's can be performed on a
-- table (it can be a 'Table' but also a SQL relation.)
--
-- @
-- type User f = Tuple f ['("user_id", Int), '("email", String), ("admin", Bool)]
--
-- incompleteUser :: User Last
-- incompleteUser = field @"admin" True <> field @"email" "admin@example.com"
--
-- userConditions :: User Condition
-- userConditions =
--   field @"admin" (Equal True) <> field @"user_id" (LowerThan 100)
--
-- userStrings :: [(String, String)]
-- userStrings = toList @Show (maybe "NULL" show . getLast) incompleteUser
-- -- [("user_id", "NULL"), ("email", "\"admin@example.com\""), ("admin", "True")]
-- @

module Database.CQRS.TabularData
  ( TabularDataAction(..)
  , applyTabularDataAction
  , Condition(..)
  , Tuple(..)
  , Table
  , field
  , ffield
  , toList
  ) where

import Data.Functor.Compose as Comp
import Data.Kind (Constraint, Type)
import Data.Monoid (Last(..))
import Data.Proxy (Proxy(..))
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)

import qualified Control.Monad.Identity as Id

data Condition a where
  Equal              :: Eq a => a -> Condition a
  NotEqual           :: Eq a =>  a -> Condition a
  LowerThan          :: Ord a => a -> Condition a
  LowerThanOrEqual   :: Ord a => a -> Condition a
  GreaterThan        :: Ord a => a -> Condition a
  GreaterThanOrEqual :: Ord a => a -> Condition a

-- | Action on tabular data with an index.
--
-- Its purpose is to be used by an 'EffectfulProjection' to create persisting
-- backend-agnostic projections.
data TabularDataAction cols
  = Insert (Tuple Id.Identity cols)
  | Update (Tuple Last cols) (Tuple (Comp.Compose [] Condition) cols)
  | Delete (Tuple (Comp.Compose [] Condition) cols)

applyTabularDataAction :: Table cols -> TabularDataAction cols -> Table cols
applyTabularDataAction tbl = \case
    Insert tuple -> tuple : tbl
    Update values conditions ->
      map (\tuple ->
            if tuple `match` conditions
              then update values tuple
              else tuple) tbl
    Delete conditions -> filter (`match` conditions) tbl

  where
    update
      :: Tuple Last cols
      -> Tuple Id.Identity cols
      -> Tuple Id.Identity cols
    update = curry $ \case
      (Nil, Nil) -> Nil
      (Cons (Last (Just x)) xs, Cons _ ys) -> Cons (pure x) (update xs ys)
      (Cons (Last Nothing) xs, Cons y ys) -> Cons y (update xs ys)

    match
      :: Tuple Id.Identity cols
      -> Tuple (Comp.Compose [] Condition) cols
      -> Bool
    match = curry $ \case
      (Nil, Nil) -> True
      (Cons (Id.Identity x) xs, Cons (Comp.Compose conds) ys) ->
        all (matchCond x) conds && xs `match` ys

    matchCond :: a -> Condition a -> Bool
    matchCond x = \case
      Equal y -> x == y
      NotEqual y -> x /= y
      LowerThan y -> x < y
      LowerThanOrEqual y -> x <= y
      GreaterThan y -> x > y
      GreaterThanOrEqual y -> x >= y

class Field f (sym :: Symbol) a (cols :: [(Symbol, Type)]) | cols sym -> a where
  cfield :: proxy sym -> f a -> Tuple f cols

instance Monoid (Tuple f cols) => Field f sym a ('(sym, a) : cols) where
  cfield _ x = Cons x mempty

instance
    {-# OVERLAPPABLE #-}
    (Monoid (f b), Field f sym a cols)
    => Field f sym a ('(sym', b) : cols) where
  cfield proxy x = Cons mempty (cfield proxy x)

type Table cols = [Tuple Id.Identity cols]

-- | A named tuple representing a row in the table.
data Tuple :: (Type -> Type) -> [(Symbol, Type)] -> Type where
  Nil  :: Tuple f '[]
  Cons :: f a -> Tuple f cols -> Tuple f ('(sym, a) ': cols)

-- | Create a tuple with the given field set to the given value.
--
-- It is meant to be used together with @TypeApplications@, e.g.
-- @
-- field @"field_name" value
-- @
field
  :: forall sym f a cols.
     (Applicative f, Field f sym a cols)
  => a -> Tuple f cols
field value = cfield (Proxy :: Proxy sym) (pure value)

-- | Create a tuple with the given field set to the given "wrapped" value.
--
-- It is more flexible than 'field' but less convenient to use if the goal is to
-- simply wrap the value inside the 'Applicative'.
ffield
  :: forall sym f a cols. (Field f sym a cols)
  => f a -> Tuple f cols
ffield fvalue = cfield (Proxy :: Proxy sym) fvalue

instance Eq (Tuple f '[]) where
  Nil == Nil = True

instance (Eq (f a), Eq (Tuple f cols)) => Eq (Tuple f ('(sym, a) ': cols)) where
  Cons x xs == Cons y ys = x == y && xs == ys

instance Semigroup (Tuple f '[]) where
  Nil <> Nil = Nil

instance
    (Semigroup (f a), Semigroup (Tuple f cols))
    => Semigroup (Tuple f ('(sym, a) ': cols)) where
  Cons x xs <> Cons y ys = Cons (x <> y) (xs <> ys)

instance Monoid (Tuple f '[]) where
  mempty = Nil

instance
    (Monoid (f a), Monoid (Tuple f xs))
    => Monoid (Tuple f ('(sym, a) ': xs)) where
  mempty = Cons mempty mempty

type family All (cs :: Type -> Constraint) (tuple :: Type) :: Constraint where
  All _ (Tuple f '[]) = ()
  All cs (Tuple f ('(sym, a) ': cols)) =
    (cs a, KnownSymbol sym, All cs (Tuple f cols))

-- | Transform a tuple into a list of pairs given a function to transform the
-- field values.
--
-- @cs@ is some constraint that the values need to satisfy. For example,
-- @
-- toList @Show (maybe "NULL" show . getLast)
--   :: Tuple Last cols -> [(String, String)]
-- @
toList
  :: forall cs f cols b.
     All cs (Tuple f cols)
  => (forall a. cs a => f a -> b) -> Tuple f cols -> [(String, b)]
toList f = \case
    Nil -> []
    pair@(Cons _ _) -> go Proxy pair
  where
    go
      :: (KnownSymbol sym, cs a, All cs (Tuple f cols'))
      => Proxy sym -> Tuple f ('(sym, a) ': cols') -> [(String, b)]
    go proxy (Cons x xs) =
      (symbolVal proxy, f x) : toList @cs f xs