{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}

module Database.CQRS.PostgreSQL.SQLQuery
  ( SQLQuery(..)
  , TrackedSQLQuery(..)
  ) where

import Control.Monad       ((<=<))
import Control.Monad.Trans (MonadIO (..))

import qualified Control.Monad.Except                 as Exc
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.FromRow   as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.ToRow     as PG.To

import qualified Database.CQRS.PostgreSQL.TrackingTable.Internal as CQRS.TT

import qualified Database.CQRS as CQRS

-- | A wrapper around a SELECT query that instantiates 'ReadModel' so that it
-- can be used by the application layer without said layer to be aware of SQL.
-- The implementation can then be swapped for something else, e.g. for tests.
data SQLQuery req resp = SQLQuery
  { connectionPool :: forall a. (PG.Connection -> IO a) -> IO a
  , queryTemplate  :: PG.Query
  }

instance
    (MonadIO m, PG.From.FromRow resp, PG.To.ToRow req)
    => CQRS.ReadModel m (SQLQuery req resp) where
  type ReadModelQuery    (SQLQuery req resp) = req
  type ReadModelResponse (SQLQuery req resp) = [resp]

  query = sqlQueryQuery

sqlQueryQuery
  :: (MonadIO m, PG.From.FromRow resp, PG.To.ToRow req)
  => SQLQuery req resp -> req -> m [resp]
sqlQueryQuery SQLQuery{..} req =
  liftIO . connectionPool $ \conn -> do
    PG.query conn queryTemplate req

-- | SQL query that fetches the projected state corresponding to a specific
-- stream identifier together with the tracked state in a single transaction.
--
-- If the tracked state is 'CQRS.SuccessAt' but the query doesn't return any
-- row, a 'CQRS.FailureAt' is returned instead.
--
-- If the query returns more than 1 row, a 'CQRS.ProjectionError' is thrown.
data TrackedSQLQuery streamId eventId st resp = TrackedSQLQuery
  { trackingTable :: CQRS.TT.TrackingTable streamId eventId st
  , queryTemplate :: PG.Query
  }

instance
    ( Exc.MonadError CQRS.Error m
    , MonadIO m
    , PG.From.FromField eventId
    , PG.From.FromField st
    , PG.From.FromField streamId
    , PG.From.FromRow resp
    , PG.To.ToField eventId
    , PG.To.ToField st
    , PG.To.ToField streamId
    ) => CQRS.ReadModel m (TrackedSQLQuery streamId eventId st resp) where

  type ReadModelQuery (TrackedSQLQuery streamId eventId st resp) = streamId
  type ReadModelResponse (TrackedSQLQuery streamId eventId st resp) =
    CQRS.TrackedState eventId (st, resp)

  query = trackedSqlQueryQuery

trackedSqlQueryQuery
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.From.FromField eventId
     , PG.From.FromField st
     , PG.From.FromField streamId
     , PG.From.FromRow resp
     , PG.To.ToField eventId
     , PG.To.ToField st
     , PG.To.ToField streamId
     )
  => TrackedSQLQuery streamId eventId st resp
  -> streamId
  -> m (CQRS.TrackedState eventId (st, resp))
trackedSqlQueryQuery TrackedSQLQuery{..} streamId = do
  Exc.liftEither <=< liftIO . CQRS.TT.connectionPool trackingTable $ \conn -> do
    resps <- PG.query conn queryTemplate $ PG.Only streamId
    eTrackedState <- CQRS.TT.getTrackedStateWithConn conn (CQRS.TT.relation trackingTable) streamId
    pure $ do
      trackedState <- eTrackedState
      case (resps, trackedState) of
        ([], CQRS.NeverRan) -> pure CQRS.NeverRan
        (_ : _, CQRS.NeverRan) ->
          Left . CQRS.ProjectionError $
            "TrackedSQLQuery: got " ++ show (length resps)
            ++ " rows from query while tracked state is NeverRan"
        ([resp], CQRS.SuccessAt eventId st) ->
          pure $ CQRS.SuccessAt eventId (st, resp)
        ([], CQRS.SuccessAt eventId _) ->
          pure . CQRS.FailureAt Nothing eventId $
            "TrackedSQLQuery: tracked state is SuccessAt but query didn't return"
        ([], CQRS.FailureAt _ failureEventId err) ->
          pure $ CQRS.FailureAt Nothing failureEventId err
        ([resp], CQRS.FailureAt mEventIdSt failureEventId err) ->
          pure $ CQRS.FailureAt (fmap (,resp) <$> mEventIdSt) failureEventId err
        (_ : _ : _, _) -> Left . CQRS.ProjectionError $
          "Expected 0 or 1 result from read model but got " ++ show (length resps)
