{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | This module provides a backend-agnostic abstraction on top on tabular data
-- allowing projections to be decoupled from their storage.
--
-- A 'Table' is a list (if using 'Flat') or a hash map
-- (if using 'WithUniqueKeys') of 'Tuple's.

-- 'TabularDataAction's can be performed on a 'Table' in memory or a table in a
-- database with the help of an adaptor translating them in commands.
-- For example, `eventsourcing-postgresql` has a function
-- `fromTabularDataActions`.
--
-- @
-- type UserCols =
--   'WithUniqueKey
--     '[ '("user_id", Int)]
--        -- Key columns. (The backtick and the space are important for it to be
--        -- parsed correctly since the list only has one element.)
--     ['("email", String), '("admin", Bool)] -- Other columns.
--
-- type User f = Tuple f UserCols
--
-- completeUser :: User Identity
-- completeUser = 3 ~: "admin@example.com" ~: True ~: empty
--
-- incompleteUser :: User Last
-- incompleteUser =
--   field @UserCols @"admin" True
--   <> field @UserCols @"email" "admin@example.com"
--
-- userConditions :: User Conditions
-- userConditions =
--   ffield @UserCols @"admin" (equal True)
--   <> ffield @UserCols @"user_id" (lowerThan 100)
--
-- userStrings :: [(String, String)]
-- userStrings = toList @Show (maybe "NULL" show . getLast) incompleteUser
-- -- [("user_id", "NULL"), ("email", "\"admin@example.com\""), ("admin", "True")]
-- @

module Database.CQRS.TabularData
  ( TabularDataAction(..)
  , Condition(..)
  , Conditions(..)
  , equal
  , notEqual
  , lowerThan
  , lowerThanOrEqual
  , greaterThan
  , greaterThanOrEqual
  , Update(..)
  , set
  , plus
  , minus
  , Columns(..)
  , Tuple
  , Flatten
  , FlatTuple(..)
  , pattern (:~)
  , empty
  , Table
  , field
  , ffield
  , MergeSplitTuple(..)
  , applyTabularDataAction
  , GetKeyAndConditions
  , AllColumns
  , toList
  ) where

import Control.Monad (foldM)
import Data.Hashable (Hashable(..))
import Data.Kind (Type)
import Data.List (foldl')

import qualified Control.Monad.Except       as Exc
import qualified Control.Monad.Identity     as Id
import qualified Control.Monad.State.Strict as St
import qualified Data.HashMap.Strict        as HM

import Database.CQRS.TabularData.Internal

import qualified Database.CQRS as CQRS

equal :: Eq a => a -> Conditions a
equal = Conditions . pure . Equal

notEqual :: Eq a => a -> Conditions a
notEqual = Conditions . pure . NotEqual

lowerThan :: Ord a => a -> Conditions a
lowerThan = Conditions . pure . LowerThan

lowerThanOrEqual :: Ord a => a -> Conditions a
lowerThanOrEqual = Conditions . pure . LowerThanOrEqual

greaterThan :: Ord a => a -> Conditions a
greaterThan = Conditions . pure . GreaterThan

greaterThanOrEqual :: Ord a => a -> Conditions a
greaterThanOrEqual = Conditions . pure . GreaterThanOrEqual

set :: a -> Update a
set = Set

plus :: Num a => a -> Update a
plus = Plus

minus :: Num a => a -> Update a
minus = Minus

-- | Action on tabular data with an index.
--
-- Its purpose is to be used by an 'EffectfulProjection' to create persisting
-- backend-agnostic projections.
data TabularDataAction (cols :: Columns) where
  Insert :: Tuple Id.Identity cols -> TabularDataAction cols
  Update :: Tuple Update cols -> Tuple Conditions cols -> TabularDataAction cols
  Upsert
    :: Tuple Id.Identity ('WithUniqueKey keyCols cols)
    -> TabularDataAction ('WithUniqueKey keyCols cols)
    -- ^ Insert a new row or update the row with the same key if it exists.
  Delete :: Tuple Conditions cols -> TabularDataAction cols

deriving instance
  AllColumns Show (Flatten cols) => Show (TabularDataAction cols)

-- | In-memory table that supports 'TabularDataAction'.
-- See 'applyTabularDataAction'.
type family Table (cols :: Columns) :: Type where
  Table ('WithUniqueKey keyCols cols) =
    HM.HashMap (FlatTuple Id.Identity keyCols) (FlatTuple Id.Identity cols)
  Table ('Flat cols) = [FlatTuple Id.Identity cols]

class ApplyTabularDataAction f (cols :: Columns) where
  -- | Apply some 'TabularDataAction' on an in-memory table and return a new
  -- table.
  applyTabularDataAction
    :: Table cols -> TabularDataAction cols -> f (Table cols)

instance Applicative f => ApplyTabularDataAction f ('Flat cols) where
  applyTabularDataAction tbl = pure . \case
    Insert tuple -> tuple : tbl
    Update updates conditions ->
      map (\tuple ->
            if tuple `matches` conditions
              then update updates tuple
              else tuple) tbl
    Delete conditions -> filter (`matches` conditions) tbl

instance
    ( AllColumns Show keyCols
    , Exc.MonadError CQRS.Error m
    , GetKeyAndConditions keyCols cols
    , Hashable (FlatTuple Id.Identity keyCols)
    , Ord (FlatTuple Id.Identity keyCols)
    , MergeSplitTuple keyCols cols
    )
    => ApplyTabularDataAction m ('WithUniqueKey keyCols cols) where

  applyTabularDataAction tbl = \case
    Insert tuple -> do
      let (keyTuple, otherTuple) = splitTuple tuple
          op = \case
            Nothing -> pure $ Just otherTuple
            Just _ -> Exc.throwError . CQRS.ProjectionError $
              "duplicate key on insert: " ++ show keyTuple
      HM.alterF op keyTuple tbl

    Update updates conditions ->
      case getKeyAndConditions conditions of
        Just (keyTuple, otherConditions) ->
          case HM.lookup keyTuple tbl of
            Nothing -> pure tbl
            Just otherTuple
              | otherTuple `matches` otherConditions -> do
                  let (keyTuple', otherTuple') =
                        splitTuple . update updates . mergeTuple
                          $ (keyTuple, otherTuple)
                  if keyTuple == keyTuple'
                    then pure $ HM.insert keyTuple otherTuple' tbl
                    else
                      case HM.lookup keyTuple' tbl of
                        Nothing ->
                          pure . HM.delete keyTuple
                            . HM.insert keyTuple' otherTuple' $ tbl
                        Just _ ->
                          Exc.throwError . CQRS.ProjectionError $
                            "duplicate key on update: " ++ show keyTuple'
              | otherwise -> pure tbl

        -- It traverses the hash map collecting changing values if the key
        -- doesn't change but the row matches the condition. When the key
        -- changes, it keeps track of the key that has to be deleted and the new
        -- row that has to be inserted. After the traversal, it deletes all the
        -- rows in the first list and inserts all the rows from the second
        -- checking for duplicated keys.
        Nothing -> do
          let step keyTuple otherTuple = do
                let merged = mergeTuple (keyTuple, otherTuple)
                if merged `matches` conditions
                  then do
                    let (keyTuple', otherTuple') =
                          splitTuple $ update updates merged
                    if keyTuple == keyTuple'
                      then pure $ Just otherTuple'
                      else do
                        St.modify' $ \(tbd, tbi) ->
                          (keyTuple : tbd, (keyTuple', otherTuple') : tbi)
                        pure $ Just otherTuple
                  else pure $ Just otherTuple

              (toBeDeleted, toBeInserted) =
                St.execState (HM.traverseWithKey step tbl) ([], [])

              tbl' = foldl' (flip HM.delete) tbl toBeDeleted

          foldM
            (\t (keyTuple, otherTuple) ->
              case HM.lookup keyTuple t of
                Just _ -> Exc.throwError . CQRS.ProjectionError $
                  "duplicate key on update: " ++ show keyTuple
                Nothing -> pure . HM.insert keyTuple otherTuple $ t
            )
            tbl' toBeInserted

    Upsert tuple -> do
      let (keyTuple, otherTuple) = splitTuple tuple
      pure $ HM.insert keyTuple otherTuple tbl

    Delete conditions ->
      case getKeyAndConditions conditions of
        Just (keyTuple, otherConditions) -> do
          let op = \case
                Nothing -> Nothing
                Just otherTuple
                  | otherTuple `matches` otherConditions -> Nothing
                  | otherwise -> Just otherTuple
          pure . HM.alter op keyTuple $ tbl

        Nothing -> do
          let op keyTuple otherTuple
                | mergeTuple (keyTuple, otherTuple) `matches` conditions =
                    Nothing
                | otherwise = Just otherTuple
          pure . HM.mapMaybeWithKey op $ tbl
