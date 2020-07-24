{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Database.CQRS.TabularData.Internal where

import Data.Hashable (Hashable(..))
import Data.Kind (Constraint, Type)
import Data.Proxy (Proxy(..))
import Data.Monoid (Last(..))
import GHC.TypeLits (symbolVal, KnownSymbol, Symbol)

import qualified Control.Monad.Identity as Id

data Condition a where
  Equal              :: Eq a => a -> Condition a
  NotEqual           :: Eq a =>  a -> Condition a
  LowerThan          :: Ord a => a -> Condition a
  LowerThanOrEqual   :: Ord a => a -> Condition a
  GreaterThan        :: Ord a => a -> Condition a
  GreaterThanOrEqual :: Ord a => a -> Condition a

deriving instance Show a => Show (Condition a)
deriving instance Eq (Condition a)

newtype Conditions a
  = Conditions { getConditions :: [Condition a] }
  deriving newtype (Eq, Show)

instance Semigroup (Conditions a) where
  Conditions cs <> Conditions cs' = Conditions $ cs <> cs'

instance Monoid (Conditions a) where
  mempty = Conditions []

-- | Kind of types that describe columns of a table.
--
-- This is not intended as a type. It's promoted to a kind.
--
-- Use 'Flat' for simple tables and 'WithUniqueKey' if you want to be able to
-- do upserts and/or store tuples in memory with a hash map instead of a list.
data Columns where
  Flat :: [(Symbol, Type)] -> Columns
  WithUniqueKey :: [(Symbol, Type)] -> [(Symbol, Type)] -> Columns

type family Flatten (cols :: k) :: [(Symbol, Type)] where
  Flatten ('WithUniqueKey (col ': keyCols) cols) =
    col ': Flatten ('WithUniqueKey keyCols cols)
  Flatten ('WithUniqueKey '[] cols) = cols
  Flatten ('Flat cols) = cols

-- | A named tuple representing a row in the table.
type Tuple f cols = FlatTuple f (Flatten cols)

data FlatTuple :: (Type -> Type) -> [(Symbol, Type)] -> Type where
  Nil  :: FlatTuple f '[]
  Cons :: f a -> FlatTuple f cols -> FlatTuple f ('(sym, a) ': cols)

(~:)
  :: Applicative f => a -> FlatTuple f cols -> FlatTuple f ('(sym, a) ': cols)
(~:) x xs = Cons (pure x) xs

infixr 5 ~:

empty :: FlatTuple f '[]
empty = Nil

instance Eq (FlatTuple f '[]) where
  Nil == Nil = True

instance
    (Eq (f a), Eq (FlatTuple f cols))
    => Eq (FlatTuple f ('(sym, a) ': cols)) where
  Cons x xs == Cons y ys = x == y && xs == ys

instance Ord (FlatTuple f '[]) where
  compare Nil Nil = EQ

instance
    (Ord (f a), Ord (FlatTuple f cols))
    => Ord (FlatTuple f ('(sym, a) ': cols)) where
  compare (Cons x xs) (Cons y ys) =
    case compare x y of
      EQ -> compare xs ys
      res -> res

instance Hashable (FlatTuple f '[]) where
  hashWithSalt salt Nil = salt

instance
    (Hashable (f a), Hashable (FlatTuple f cols))
    => Hashable (FlatTuple f ('(sym, a) ': cols)) where
  hashWithSalt salt (Cons x xs) =
    hashWithSalt (hashWithSalt salt x) xs

instance
    ( AllColumns Show cols, forall a. Show a => Show (f a) )
    => Show (FlatTuple f cols) where
  show = show . toList @Show (\name value -> show (name, value))

instance Semigroup (FlatTuple f '[]) where
  Nil <> Nil = Nil

instance
    (Semigroup (f a), Semigroup (FlatTuple f cols))
    => Semigroup (FlatTuple f ('(sym, a) ': cols)) where
  Cons x xs <> Cons y ys = Cons (x <> y) (xs <> ys)

instance Monoid (FlatTuple f '[]) where
  mempty = Nil

instance
    (Monoid (f a), Monoid (FlatTuple f xs))
    => Monoid (FlatTuple f ('(sym, a) ': xs)) where
  mempty = Cons mempty mempty

class Field f (sym :: Symbol) a (cols :: [(Symbol, Type)]) | cols sym -> a where
  cfield :: proxy sym -> f a -> FlatTuple f cols

instance Monoid (FlatTuple f cols) => Field f sym a ('(sym, a) : cols) where
  cfield _ x = Cons x mempty

instance
    {-# OVERLAPPABLE #-}
    (Monoid (f b), Field f sym a cols)
    => Field f sym a ('(sym', b) : cols) where
  cfield proxy x = Cons mempty (cfield proxy x)

-- | Create a tuple with the given field set to the given value.
--
-- It is meant to be used together with @TypeApplications@, e.g.
-- @
-- field @"field_name" value
-- @
field
  :: forall cols sym f a.
     (Applicative f, Field f sym a (Flatten cols))
  => a -> Tuple f cols
field value = cfield (Proxy :: Proxy sym) (pure value)

-- | Create a tuple with the given field set to the given "wrapped" value.
--
-- It is more flexible than 'field' but less convenient to use if the goal is to
-- simply wrap the value inside the 'Applicative'. In particular, it can be used
-- with 'Conditions' such as
-- @
-- ffield @"email" (equal "someone@example.com")
-- @
ffield
  :: forall cols sym f a. Field f sym a (Flatten cols)
  => f a -> Tuple f cols
ffield fvalue = cfield (Proxy :: Proxy sym) fvalue

class MergeSplitTuple keyCols cols where
  mergeTuple
    :: (FlatTuple f keyCols, FlatTuple f cols)
    -> Tuple f ('WithUniqueKey keyCols cols)

  splitTuple
    :: Tuple f ('WithUniqueKey keyCols cols)
    -> (FlatTuple f keyCols, FlatTuple f cols)

instance MergeSplitTuple '[] cols where
  mergeTuple (Nil, tuple) = tuple
  splitTuple tuple = (Nil, tuple)

instance
    MergeSplitTuple keyCols cols
    => MergeSplitTuple (a ': keyCols) cols where
  mergeTuple (Cons x xs, tuple') = Cons x (mergeTuple (xs, tuple'))
  splitTuple (Cons x xs) =
    let (tuple, tuple') = splitTuple xs in (Cons x tuple, tuple')

type family AllColumns
    (cs :: Type -> Constraint) (cols :: [(Symbol, Type)]) :: Constraint where
  AllColumns _ '[] = ()
  AllColumns cs ('(sym, a) ': cols) =
    (cs a, KnownSymbol sym, AllColumns cs cols)

-- | Transform a tuple into a list of pairs given a function to transform the
-- field values.
--
-- @cs@ is some constraint that the values need to satisfy. For example,
-- @
-- toList @Show (\name value -> (name, maybe "NULL" show (getLast value)))
--   :: Tuple Last cols -> [(String, String)]
-- @
toList
  :: forall cs cols f b. AllColumns cs cols
  => (forall a. cs a => String -> f a -> b) -> FlatTuple f cols -> [b]
toList f = \case
    Nil -> []
    pair@(Cons _ _) -> go Proxy pair
  where
    go
      :: (KnownSymbol sym, cs a, AllColumns cs cols')
      => Proxy sym -> FlatTuple f ('(sym, a) ': cols') -> [b]
    go proxy (Cons x xs) =
      f (symbolVal proxy) x : toList @cs f xs

-- | Used to optimise operations on the in-memory storage. When we want to
-- update or delete rows based on some conditions that would match one row
-- matching its key, it's more efficient to use 'HM.alter' instead of traversing
-- the hash map.
class GetKeyAndConditions keyCols cols where
  getKeyAndConditions
    :: Tuple Conditions ('WithUniqueKey keyCols cols)
    -> Maybe (FlatTuple Id.Identity keyCols, FlatTuple Conditions cols)

instance GetKeyAndConditions '[] cols where
  getKeyAndConditions conds = Just (Nil, conds)

instance
    GetKeyAndConditions keyCols cols
    => GetKeyAndConditions ('(sym, a) ': keyCols) cols where
  getKeyAndConditions (Cons cond conds) =
    case getConditions cond of
      [Equal x] -> do
        (tuple, otherConditions) <- getKeyAndConditions conds
        pure (x ~: tuple, otherConditions)
      _ -> Nothing

matches
  :: FlatTuple Id.Identity cols
  -> FlatTuple Conditions cols
  -> Bool
matches = curry $ \case
  (Nil, Nil) -> True
  (Cons (Id.Identity x) xs, Cons (Conditions conds) ys) ->
    all (matchesCond x) conds && xs `matches` ys

matchesCond :: a -> Condition a -> Bool
matchesCond x = \case
  Equal y -> x == y
  NotEqual y -> x /= y
  LowerThan y -> x < y
  LowerThanOrEqual y -> x <= y
  GreaterThan y -> x > y
  GreaterThanOrEqual y -> x >= y

update
  :: FlatTuple Last cols
  -> FlatTuple Id.Identity cols
  -> FlatTuple Id.Identity cols
update = curry $ \case
  (Nil, Nil) -> Nil
  (Cons (Last (Just x)) xs, Cons _ ys) -> Cons (pure x) (update xs ys)
  (Cons (Last Nothing) xs, Cons y ys) -> Cons y (update xs ys)
