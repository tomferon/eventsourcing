{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.PostgreSQL.Internal where

import Control.Applicative ((<|>))
import Control.Exception
import Control.Monad ((<=<), void)
import Control.Monad.Trans
import Data.Proxy (Proxy(..))

import qualified Control.Monad.Except                 as Exc
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

data TrackedState identifier st
  = NeverRan
  | SuccessAt identifier st
  | FailureAt (Maybe (identifier, st)) identifier
    -- ^ Last succeeded at, failed at.

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
  -> PG.Query -- ^ Type of the state.
  -> IO ()
createTrackingTable
    connectionPool trackingTable streamIdType eventIdType stateType =
  connectionPool $ \conn -> do
    let query =
          "CREATE TABLE IF NOT EXISTS " <> trackingTable
          <> " ( stream_id " <> streamIdType <> " PRIMARY KEY"
          <> " , event_id " <> eventIdType
          <> " , failed_event_id " <> eventIdType
          <> " , failed_message varchar"
          <> " , state " <> stateType <> ")"
    void $ PG.execute_ conn query

-- | An alternative to 'Maybe st'.
--
-- If we use 'Maybe st' and 'st ~ Maybe a' (or something else where @NULL@ is a
-- valid state,) then 'getTrackedState''s @SELECT@ statement would return a
-- value of type 'Maybe (Maybe a)' which would be 'Nothing' instead of
-- 'Just Nothing' if the column is 'NULL.
--
-- This type works differently: it tries to use 'PG.From.fromField' on the field
-- and return 'NoState' if it didn't work AND it is @NULL@. Otherwise, it fails.
data OptionalState st = SomeState st | NoState

instance PG.From.FromField st => PG.From.FromField (OptionalState st) where
  fromField f mBS =
    case mBS of
      Nothing -> (SomeState <$> PG.From.fromField f mBS) <|> pure NoState
      Just _  -> SomeState <$> PG.From.fromField f mBS

fromOptionalState :: OptionalState a -> Maybe a
fromOptionalState = \case
  SomeState x -> Just x
  NoState     -> Nothing

-- | Get the tracked state of a stream in a family from a tracking table.
getTrackedState
  :: ( PG.From.FromField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.From.FromField st
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> IO (TrackedState (CQRS.EventIdentifier (CQRS.StreamType streamFamily)) st)
getTrackedState connectionPool trackingTable _ streamId =
  connectionPool $ \conn -> do
    let query =
          "SELECT event_id, failed_event_id, state FROM "
          <> trackingTable <> " WHERE stream_id = ?"
    rows <- PG.query conn query (PG.Only streamId)
    pure $ case rows of
      [(Just eventId, Nothing, SomeState state)] -> SuccessAt eventId state
      [(mEventId, Just failedAt, oState)] ->
        FailureAt ((,) <$> mEventId <*> fromOptionalState oState) failedAt
      _ -> NeverRan

-- | Return SQL query to upsert a row in a tracking table.
upsertTrackingTable
  :: ( PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     , PG.To.ToField st
     )
  => PG.Query -- ^ Name of tracking table.
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> CQRS.EventIdentifier (CQRS.StreamType streamFamily)
  -> Either String st -- ^ The new state or the error message if it failed.
  -> SqlAction
upsertTrackingTable trackingTable _ streamId eventId eState =
  let (updates, updateValues, insertValues) =
        case eState of
          Right state ->
            ( "event_id = ?, failed_event_id = NULL,\
              \ failed_message = NULL, state = ?"
            , [PG.To.toField eventId, PG.To.toField state]
            , PG.To.toRow (streamId, eventId, PG.Null, PG.Null, state)
            )
          Left err ->
            ( "failed_event_id = ?, failed_message = ?"
            , PG.To.toRow (eventId, err)
            , PG.To.toRow (streamId, PG.Null, eventId, err, PG.Null)
            )
      query =
        "INSERT INTO " <> trackingTable
        <> " (stream_id, event_id, failed_event_id, failed_message, state)"
        <> " VALUES (?, ?, ?, ?, ?) ON CONFLICT (stream_id) DO UPDATE SET "
        <> updates
  in
  makeSqlAction query $ insertValues ++ updateValues

-- | Update the tracking table for the given stream.
doUpsertTrackingTable
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     , PG.To.ToField st
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query -- ^ Name of tracking table.
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> CQRS.EventIdentifier (CQRS.StreamType streamFamily)
  -> Either String st -- ^ The new state or the error message if it failed.
  -> m ()
doUpsertTrackingTable
    connectionPool trackingTable streamFamily streamId eventId eState = do
  Exc.liftEither <=< liftIO . connectionPool $ \conn -> do
    let (uquery, uvalues) =
          upsertTrackingTable trackingTable streamFamily streamId eventId eState
    (const (Right ()) <$> PG.execute conn uquery uvalues)
      `catches`
        [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
        , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
        ]
