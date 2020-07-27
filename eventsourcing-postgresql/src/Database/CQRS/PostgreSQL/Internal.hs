{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.PostgreSQL.Internal where

import Control.Exception

import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.ToRow     as PG.To

-- | Return all the 'Right' elements before the first 'Left' and the value of
-- the first 'Left'.
stopOnLeft :: [Either a b] -> ([b], Maybe a)
stopOnLeft = go id
  where
    go :: ([b] -> [b]) -> [Either a b] -> ([b], Maybe a)
    go f = \case
      [] -> (f [], Nothing)
      Left err : _ -> (f [], Just err)
      Right x : xs -> go (f . (x:)) xs

type SqlAction = (PG.Query, [PG.To.Action])

makeSqlAction :: PG.To.ToRow r => PG.Query -> r -> SqlAction
makeSqlAction query r = (query, PG.To.toRow r)

appendSqlActions :: [SqlAction] -> SqlAction
appendSqlActions = \case
    [] -> ("", [])
    action : actions -> foldl step action actions
  where
    step :: SqlAction -> SqlAction -> SqlAction
    step (q1,v1) (q2,v2) = (q1 <> ";" <> q2, v1 ++ v2)

handleError
  :: forall e e' a proxy. (Exception e, Show e)
  => proxy e -> (String -> e') -> Handler (Either e' a)
handleError _ f = Handler $ pure . Left . f . show @e

data SomeParams =  forall r. PG.To.ToRow r => SomeParams r

instance PG.To.ToRow SomeParams where
  toRow (SomeParams x) = PG.To.toRow x
