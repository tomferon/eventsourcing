{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.PostgreSQL.Migration
  ( migrate
  ) where

import Control.Exception
import Control.Monad              ((<=<), unless)
import Control.Monad.Trans        (MonadIO(..), lift)
import Data.Hashable              (Hashable)
import Data.List                  (foldl', intersperse)
import Data.Proxy                 (Proxy(..))
import Database.PostgreSQL.Simple ((:.)(..))
import Pipes                      ((>->))

import qualified Control.Monad.Except                 as Exc
import qualified Control.Monad.State.Strict           as St
import qualified Data.HashMap.Strict                  as HM
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.Types     as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.FromRow   as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.ToRow     as PG.To
import qualified Pipes

import Database.CQRS.PostgreSQL.StreamFamily
import Database.CQRS.PostgreSQL.Internal

import qualified Database.CQRS as CQRS

-- | Migrate a stream family stored in a PostgreSQL database to the same
-- database. It is meant to run in parallel with the application using the
-- stream family without disturbing it.
--
-- An alternative use of this is to migrate a stream family to a new relation
-- without swapping the tables at the end. The old table stays in use by the
-- application and the new one can be read by an external system for instance.
--
-- If the new table already exists (and the initialisation query does not fail
-- in that case,) the migration will start over from the point it left off.
migrate
  :: forall streamId eventId metadata event transformedStreamFamily m.
     ( CQRS.WritableEvent
        (CQRS.EventType (CQRS.StreamType transformedStreamFamily))
     , CQRS.Stream m (CQRS.StreamType transformedStreamFamily)
     , CQRS.StreamFamily m transformedStreamFamily
     , Exc.MonadError CQRS.Error m
     , Hashable (CQRS.StreamIdentifier transformedStreamFamily)
     , MonadIO m
     , Ord (CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily))
     , Ord (CQRS.StreamIdentifier transformedStreamFamily)
     , PG.From.FromField (CQRS.StreamIdentifier transformedStreamFamily)
     , PG.From.FromField
        (CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily))
     , PG.From.FromRow
        (CQRS.EventMetadata (CQRS.StreamType transformedStreamFamily))
     , PG.From.FromField
        (CQRS.EncodingFormat
          (CQRS.EventType (CQRS.StreamType transformedStreamFamily)))
     , PG.To.ToField (CQRS.StreamIdentifier transformedStreamFamily)
     , PG.To.ToField
        (CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily))
     , PG.To.ToRow
        (CQRS.EventMetadata (CQRS.StreamType transformedStreamFamily))
     , PG.To.ToField
        (CQRS.EncodingFormat
          (CQRS.EventType (CQRS.StreamType transformedStreamFamily)))
     , Show
        (CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily))
     )
  => StreamFamily streamId eventId metadata event
  -> (StreamFamily streamId eventId metadata event -> transformedStreamFamily)
  -> PG.Query -- ^ Name of the new (temporary) relation.
  -> PG.Query -- ^ New stream identifier column name.
  -> PG.Query -- ^ New event identifier column name.
  -> [PG.Query] -- ^ New metadata column names.
  -> PG.Query -- ^ New event column name.
  -> (PG.Query -> PG.Query)
  -- ^ Initialisation query that creates the new relation. It is given the name
  -- of the current relation to migrate. It must be idempotent to be able to
  -- restart the migrator.
  -> (PG.Query -> PG.Query)
  -- ^ Query to lock the relation used by the application. It is given the name
  -- of the current relation. It must be idempotent to be able to restart the
  -- migrator.
  -> (PG.Query -> PG.Query)
  -- ^ Query to swap the relation used by the application. It is given the name
  -- of the current relation.
  -> m ()
migrate fam@StreamFamily { connectionPool, relation } transform
        tempRelation streamIdentifierColumn
        eventIdentifierColumn metadataColumns eventColumn
        initQuery lockQuery swapQuery = do

    let transformedStreamFamily = transform fam

    Exc.liftEither <=< liftIO . connectionPool $ \conn ->
      (const (Right ()) <$> PG.execute_ conn (initQuery relation))
      `catches` handlers

    newEvents <- CQRS.allNewEvents transformedStreamFamily

    flip St.evalStateT HM.empty $ do
      Pipes.runEffect . Pipes.for
        (Pipes.hoist lift (CQRS.latestEventIdentifiers tempStreamFamily))
        $ \(streamId, eventId) ->
            St.modify' $ HM.insert streamId eventId

      -- Phase 1: Process batches of events for all streams.
      -- FIXME: handle exceptions
      Pipes.runEffect $
        Pipes.hoist lift (CQRS.latestEventIdentifiers transformedStreamFamily)
        >-> migrateStream transformedStreamFamily

      -- Phase 2: Go through the new events that were created while phase 1 was
      -- still ongoing.
      processNewEvents newEvents

      -- Phase 3: Prevent writes to the original relation, go through
      -- notifications one last time and swap the relations.

      Exc.liftEither <=< liftIO . connectionPool $ \conn ->
        (const (Right ()) <$> PG.execute_ conn (lockQuery relation))
        `catches` handlers

      processNewEvents newEvents

      Exc.liftEither <=< liftIO . connectionPool $ \conn ->
        (const (Right ()) <$> PG.execute_ conn (swapQuery relation))
        `catches` handlers

  where
    migrateStream
      :: transformedStreamFamily
      -> Pipes.Consumer
          ( CQRS.StreamIdentifier transformedStreamFamily
          , CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily)
          )
          (St.StateT
            (HM.HashMap
              (CQRS.StreamIdentifier transformedStreamFamily)
              (CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily)))
            m)
          ()
    migrateStream transformedStreamFamily = do
      (streamId, eventId) <- Pipes.await
      stream <- lift . lift $ CQRS.getStream transformedStreamFamily streamId

      state <- St.get
      let bounds = case HM.lookup streamId state of
            Nothing -> CQRS.untilEvent eventId
            Just lastEventId ->
              CQRS.afterEvent lastEventId <> CQRS.untilEvent eventId

      lift . Pipes.runEffect . Pipes.for
        (Pipes.hoist lift (CQRS.streamEvents stream bounds))
        $ \batch -> do
            let (ewcs, mErr) = stopOnLeft batch
                params =
                  map (\CQRS.EventWithContext{..} ->
                        PG.Only streamId :. PG.Only identifier :. metadata
                        :. PG.Only (CQRS.encodeEvent event))
                      ewcs

            unless (null ewcs) $ do
              Exc.liftEither <=< liftIO . connectionPool $ \conn ->
                (const (Right ())
                  <$> PG.execute conn insertQuery (PG.Only (PG.Values [] params)))
                `catches` handlers

              let lastEventId = CQRS.identifier . last $ ewcs
              St.modify' $ HM.insert streamId lastEventId

            case mErr of
              Nothing -> pure ()
              Just (errEventId, err) ->
                Exc.throwError $ CQRS.EventDecodingError (show errEventId) err

    processNewEvents
      :: Pipes.Producer
          [ ( CQRS.StreamIdentifier transformedStreamFamily
            , Either
                ( CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily)
                , String )
                (CQRS.EventWithContext'
                  (CQRS.StreamType transformedStreamFamily))
            ) ]
          m ()
      -> St.StateT
          (HM.HashMap
            (CQRS.StreamIdentifier transformedStreamFamily)
            (CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily)))
          m ()
    processNewEvents newEvents =
      Pipes.runEffect . untilEmpty (Pipes.hoist lift newEvents) $ \batch -> do
        state <- St.get

        let (events, mErr) =
              stopOnLeft . map sequence $ batch
            params =
              map (\(streamId, CQRS.EventWithContext{..}) ->
                    PG.Only streamId :. PG.Only identifier :. metadata
                    :. PG.Only (CQRS.encodeEvent event))
              . filter (\(streamId, CQRS.EventWithContext{..}) ->
                        maybe True (identifier >) . HM.lookup streamId $ state)
              $ events

        unless (null events) $
          Exc.liftEither <=< liftIO . connectionPool $ \conn ->
            (const (Right ())
              <$> PG.execute conn insertQuery (PG.Only (PG.Values [] params)))
            `catches` handlers

        St.put
          . foldl'
              (\s (streamId, CQRS.EventWithContext{..}) ->
                HM.insert streamId identifier s)
              state
          $ events

        case mErr of
          Nothing -> pure ()
          Just (errEventId, err) ->
            Exc.throwError $ CQRS.EventDecodingError (show errEventId) err

    tempStreamFamily
      :: StreamFamily
          (CQRS.StreamIdentifier transformedStreamFamily)
          (CQRS.EventIdentifier (CQRS.StreamType transformedStreamFamily))
          (CQRS.EventMetadata (CQRS.StreamType transformedStreamFamily))
          (CQRS.EventType (CQRS.StreamType transformedStreamFamily))
    tempStreamFamily = StreamFamily
      { relation            = tempRelation
      , notificationChannel = "unused"
      , parseNotification   = const $ Left "unused"
      , ..
      }

    untilEmpty
      :: Monad n
      => Pipes.Producer [b] n ()
      -> ([b] -> Pipes.Effect n ())
      -> Pipes.Effect n ()
    untilEmpty pipe f =
      let pipe' = do
            xs <- Pipes.await
            if null xs
              then pure ()
              else lift . Pipes.runEffect . f $ xs
      in
      pipe >-> pipe'

    insertQuery :: PG.Query
    insertQuery =
      "INSERT INTO " <> tempRelation
      <> " (" <> streamIdentifierColumn
      <> ", " <> eventIdentifierColumn
      <> ", " <> mconcat (intersperse "," metadataColumns)
      <> ", " <> eventColumn
      <> ") VALUES ?"

    handlers :: [Handler (Either CQRS.Error ())]
    handlers =
      [ handleError (Proxy @PG.FormatError) CQRS.MigrationError
      , handleError (Proxy @PG.SqlError)    CQRS.MigrationError
      ]