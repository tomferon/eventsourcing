{-# OPTIONS_GHC -fno-warn-orphans #-}

{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TypeApplications      #-}

module Database.CQRS.PostgreSQL.TrackingTable
  ( TrackingTable
  , createTrackingTable
  , upsertTrackingTable
  , doUpsertTrackingTable
  ) where

import Control.Monad       (void, (<=<))
import Control.Monad.Trans (MonadIO (..))

import qualified Control.Monad.Except                 as Exc
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.ToRow     as PG.To
import qualified Database.PostgreSQL.Simple.Types     as PG

import Database.CQRS.PostgreSQL.Internal
  (SqlAction, makeSqlAction)
import Database.CQRS.PostgreSQL.TrackingTable.Internal

import qualified Database.CQRS as CQRS

instance
    ( Exc.MonadError CQRS.Error m
    , MonadIO m
    , PG.From.FromField eventId
    , PG.From.FromField st
    , PG.From.FromField streamId
    , PG.To.ToField eventId
    , PG.To.ToField st
    , PG.To.ToField streamId
    )
    => CQRS.TrackingTable m (TrackingTable streamId eventId st)
        streamId eventId st where

  getTrackedState
    :: TrackingTable streamId eventId st
    -> streamId
    -> m (CQRS.TrackedState eventId st)
  getTrackedState TrackingTable{..} streamId =
    Exc.liftEither <=< liftIO . connectionPool $ \conn ->
      getTrackedStateWithConn conn relation streamId

  upsertError
    :: TrackingTable streamId eventId st
    -> streamId
    -> eventId
    -> String
    -> m ()
  upsertError trackingTable streamId eventId err =
    doUpsertTrackingTable trackingTable streamId eventId (Left err)

-- | Return SQL query to upsert a row in a tracking table.
upsertTrackingTable
  :: ( PG.To.ToField streamId
     , PG.To.ToField eventId
     , PG.To.ToField st
     )
  => TrackingTable streamId eventId st
  -> streamId
  -> eventId
  -> Either String st -- ^ The new state or the error message if it failed.
  -> SqlAction
upsertTrackingTable TrackingTable{..} streamId eventId eState =
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
        "INSERT INTO " <> relation
        <> " (stream_id, event_id, failed_event_id, failed_message, state)"
        <> " VALUES (?, ?, ?, ?, ?) ON CONFLICT (stream_id) DO UPDATE SET "
        <> updates
  in
  makeSqlAction query $ insertValues ++ updateValues

-- | Update the tracking table for the given stream.
doUpsertTrackingTable
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField eventId
     , PG.To.ToField streamId
     , PG.To.ToField st
     )
  => TrackingTable streamId eventId st
  -> streamId
  -> eventId
  -> Either String st -- ^ The new state or the error message if it failed.
  -> m ()
doUpsertTrackingTable tt@TrackingTable{..} streamId eventId eState = do
  handlePgErrors . connectionPool $ \conn -> do
    let (query, values) = upsertTrackingTable tt streamId eventId eState
    void $ PG.execute conn query values

-- | Create tracking table if it doesn't exist already.
--
-- A tracking table is a table used to track the last events processed by a
-- projection for each stream in a stream family. It allows them to restart from
-- where they have left off.
createTrackingTable
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query -- ^ Name of the tracking table.
  -> PG.Query -- ^ Type of stream identifiers.
  -> PG.Query -- ^ Type of event identifiers.
  -> PG.Query -- ^ Type of the state.
  -> m (TrackingTable streamId eventId st)
createTrackingTable
    connectionPool relation streamIdType eventIdType stateType = do

  handlePgErrors . connectionPool $ \conn -> do
    let query =
          "CREATE TABLE IF NOT EXISTS " <> relation
          <> " ( stream_id " <> streamIdType <> " PRIMARY KEY"
          <> " , event_id " <> eventIdType
          <> " , failed_event_id " <> eventIdType
          <> " , failed_message varchar"
          <> " , state " <> stateType <> ")"
    void $ PG.execute_ conn query

  pure TrackingTable{..}
