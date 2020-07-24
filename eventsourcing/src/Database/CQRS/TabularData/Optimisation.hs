{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQRS.TabularData.Optimisation
  ( Optimiser
  , Optimisable(..)
  ) where

import qualified Control.Monad.Identity as Id

import Database.CQRS.TabularData
import Database.CQRS.TabularData.Internal

-- TODO: Consecutive updates with the same conditions should be merged.
-- TODO: Inserts that will be updated later on should insert the final version.
--       => Reordering of the actions to put the insert at the end.

type Optimiser cols = [TabularDataAction cols] -> [TabularDataAction cols]

class Optimisable (cols :: Columns) where
  optimiseActions :: Optimiser cols
  optimiseActions = optimiseInsertBeforeDelete

  optimiseInsertBeforeDelete :: Optimiser cols

instance Optimisable ('Flat cols) where
  optimiseInsertBeforeDelete :: Optimiser ('Flat cols)
  optimiseInsertBeforeDelete =
      foldr
        (\action actions ->
          case action of
            Insert tuple | isInsertBeforeDelete tuple actions -> actions
            _ -> action: actions)
        []

    where
      isInsertBeforeDelete
        :: Tuple Id.Identity ('Flat cols)
        -> [TabularDataAction ('Flat cols)]
        -> Bool
      isInsertBeforeDelete tuple = \case
        [] -> False
        Delete conds : actions ->
          tuple `matches` conds || isInsertBeforeDelete tuple actions
        Update updates conds : actions
          | tuple `matches` conds ->
              let tuple' = update updates tuple
              in isInsertBeforeDelete tuple' actions
          | otherwise -> isInsertBeforeDelete tuple actions
        Insert _ : actions -> isInsertBeforeDelete tuple actions

instance
    ( Eq (FlatTuple Id.Identity keyCols)
    , MergeSplitTuple keyCols cols
    )
    => Optimisable ('WithUniqueKey keyCols cols) where

  optimiseInsertBeforeDelete
    ::
      ( Eq (FlatTuple Id.Identity keyCols)
      , MergeSplitTuple keyCols cols
      )
    => Optimiser ('WithUniqueKey keyCols cols)
  optimiseInsertBeforeDelete =
      foldr
        (\action actions ->
          case action of
            Insert tuple | isInsertBeforeDelete tuple actions -> actions
            _ -> action: actions)
        []

    where
      isInsertBeforeDelete
        :: Tuple Id.Identity ('WithUniqueKey keyCols cols)
        -> [TabularDataAction ('WithUniqueKey keyCols cols)]
        -> Bool
      isInsertBeforeDelete tuple = \case
        [] -> False
        Delete conds : actions ->
          tuple `matches` conds || isInsertBeforeDelete tuple actions
        Upsert tuple' : actions -> isUpsertBeforeDelete tuple tuple' actions
        Update updates conds : actions
          | tuple `matches` conds ->
              let tuple' = update updates tuple
              in isInsertBeforeDelete tuple' actions
          | otherwise -> isInsertBeforeDelete tuple actions
        Insert _ : actions -> isInsertBeforeDelete tuple actions

      isUpsertBeforeDelete
        :: Tuple Id.Identity ('WithUniqueKey keyCols cols)
        -> Tuple Id.Identity ('WithUniqueKey keyCols cols)
        -> [TabularDataAction ('WithUniqueKey keyCols cols)]
        -> Bool
      isUpsertBeforeDelete tuple tuple' actions =
        let keyTuple, keyTuple' :: FlatTuple Id.Identity keyCols
            _otherTuple, _otherTuple' :: FlatTuple Id.Identity cols
            (keyTuple, _otherTuple) = splitTuple tuple
            (keyTuple', _otherTuple') = splitTuple tuple'
        in
        isInsertBeforeDelete
          (if keyTuple == keyTuple' then tuple' else tuple)
          actions
