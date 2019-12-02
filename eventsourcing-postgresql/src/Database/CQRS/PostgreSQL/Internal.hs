{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.PostgreSQL.Internal where

import Control.Exception
import Control.Monad (void)

import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.ToRow     as PG.To
import qualified Database.PostgreSQL.Simple.Types     as PG

import qualified Database.CQRS as CQRS

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

data TrackedState identifier
  = NeverRan
  | SuccessAt identifier
  | FailureAt (Maybe identifier) identifier -- ^ Last succeeded at, failed at.

-- | Create tracking table if it doesn't exist already.
--
-- A tracking table is a table used to track the last events processed by a
-- projection for each stream in a stream family. It allows them to restart from
-- where they have left off.
createTrackingTable
  :: (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query -- ^ Table name
  -> PG.Query -- ^ Type of stream identifiers.
  -> PG.Query -- ^ Type of event identifiers.
  -> IO ()
createTrackingTable connectionPool trackingTable streamIdType eventIdType =
  connectionPool $ \conn -> do
    let query =
          "CREATE TABLE IF NOT EXISTS " <> trackingTable
          <> " ( stream_id " <> streamIdType <> " PRIMARY KEY"
          <> " , event_id " <> eventIdType
          <> " , failed_event_id " <> eventIdType
          <> " , failed_message varchar )"
    void $ PG.execute_ conn query

-- | Get the tracked state of a stream in a family from a tracking table.
getTrackedState
  :: ( PG.From.FromField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> IO (TrackedState (CQRS.EventIdentifier (CQRS.StreamType streamFamily)))
getTrackedState connectionPool trackingTable _ streamId =
  connectionPool $ \conn -> do
    let query =
          "SELECT event_id, failed_event_id FROM "
          <> trackingTable <> " WHERE stream_id = ?"
    rows <- PG.query conn query (PG.Only streamId)
    pure $ case rows of
      [(Just eventId, Nothing)] -> SuccessAt eventId
      [(mEventId, Just failedAt)] -> FailureAt mEventId failedAt
      _ -> NeverRan

-- | Return SQL query to upsert a row in a tracking table.
upsertTrackingTable
  :: ( PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => PG.Query -- ^ Name of tracking table.
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> CQRS.EventIdentifier (CQRS.StreamType streamFamily)
  -> Maybe String -- ^ The error message if it failed.
  -> SqlAction
upsertTrackingTable trackingTable _ streamId eventId mErr =
  let (updates, updateValues, insertValues) = case mErr of
        Nothing ->
          ( "event_id = ?, failed_event_id = NULL, failed_message = NULL"
          , [PG.To.toField eventId]
          , PG.To.toRow (streamId, eventId, PG.Null, PG.Null)
          )
        Just err ->
          ( "failed_event_id = ?, failed_message = ?"
          , PG.To.toRow (eventId, err)
          , PG.To.toRow (streamId, PG.Null, eventId, err)
          )
      query =
        "INSERT INTO " <> trackingTable
        <> " (stream_id, event_id, failed_event_id, failed_message)"
        <> " VALUES (?, ?, ?, ?) ON CONFLICT DO UPDATE SET "
        <> updates <> " WHERE stream_id = ?"
  in
  makeSqlAction query $
    insertValues ++ updateValues ++ [PG.To.toField streamId]