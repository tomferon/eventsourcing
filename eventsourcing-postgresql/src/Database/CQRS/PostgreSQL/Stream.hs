{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.CQRS.PostgreSQL.Stream
  ( Stream
  , makeStream
  , makeStream'
  ) where

import Control.Exception          (Exception, Handler(..), catches)
import Control.Monad              (when)
import Control.Monad.Trans        (MonadIO(..))
import Data.Functor               ((<&>))
import Data.List                  (intersperse)
import Data.Maybe                 (catMaybes, listToMaybe)
import Data.Proxy                 (Proxy(..))
import Data.String                (fromString)
import Database.PostgreSQL.Simple ((:.)(..))

import qualified Control.Monad.Except                 as Exc
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.FromRow   as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.ToRow     as PG.To
import qualified Pipes

import Database.CQRS.PostgreSQL.Internal (SomeParams(..))

import qualified Database.CQRS as CQRS

-- | Stream of events stored in a PostgreSQL relation.
--
-- The job of sharding streams in different tables is left to the database. If
-- this is something you want to do, you can create a view and a trigger on
-- insert into that view.
data Stream identifier metadata event =
  forall r r'. (PG.To.ToRow r, PG.To.ToRow r') => Stream
    { connectionPool :: forall a. (PG.Connection -> IO a) -> IO a
    , selectQuery    :: (PG.Query, r)
      -- ^ Select all events in correct order.
    , insertQuery
        :: forall encEvent.
           (PG.To.ToField encEvent, PG.To.ToRow metadata)
        => encEvent -> metadata -> CQRS.ConsistencyCheck identifier
        -> (PG.Query, r')
      -- ^ Build a query to insert a new event. The constraints are there to
      -- postpone them to calls to 'writeEventWithMetadata'. If the user never
      -- writes new events, these constraints won't be required by the compiler.
    , identifierColumn :: PG.Query
    }

-- | Make a 'Stream' from basic information about the relation name and columns.
makeStream
  :: forall identifier metadata event.
     ( CQRS.WritableEvent event
     , PG.To.ToField (CQRS.EncodingFormat event)
     , PG.To.ToField identifier
     , PG.To.ToRow metadata
     )
  => (forall a. (PG.Connection -> IO a) -> IO a)
     -- ^ Connection pool as a function.
  -> PG.Query   -- ^ Relation name.
  -> PG.Query   -- ^ Identifier column name. If there are several, use a tuple.
  -> [PG.Query] -- ^ Column names for metadata.
  -> PG.Query   -- ^ Event column name.
  -> Stream identifier metadata event
makeStream connectionPool relation
           identifierColumn metadataColumns eventColumn =
    let selectQuery = (selectQuery', ())
    in Stream{..}

  where
    selectQuery' :: PG.Query
    selectQuery' =
      "SELECT "
      <> identifierColumn <> ", "
      <> metadataList <> ", "
      <> eventColumn
      <> " FROM "  <> relation
      <> " ORDER BY " <> identifierColumn <> " ASC"

    insertQuery
      :: (PG.To.ToField encEvent, PG.To.ToRow metadata)
      => encEvent -> metadata -> CQRS.ConsistencyCheck identifier
      -> (PG.Query, SomeParams)
    insertQuery encEvent metadata cc =
      let baseParams = metadata :. PG.Only encEvent
          (cond, params) = case cc of
            CQRS.NoConsistencyCheck -> ("", SomeParams baseParams)
            CQRS.CheckNoEvents ->
              ( " WHERE NOT EXISTS (SELECT 1 FROM " <> relation <> ")"
              , SomeParams baseParams
              )
            CQRS.CheckLastEvent identifier ->
              ( " WHERE NOT EXISTS (SELECT 1 FROM " <> relation <> " WHERE "
                <> identifierColumn <> " > ?)"
              , SomeParams (baseParams :. PG.Only identifier)
              )
          query =
            "INSERT INTO " <> relation <> "("
            <> metadataList <> ", " <> eventColumn
            <> ") SELECT " <> metadataMarks <> ", ?"
            <> cond
            <> " RETURNING " <> identifierColumn
      in
      (query, params)

    metadataList :: PG.Query
    metadataList =
      mconcat . intersperse "," $ metadataColumns

    metadataMarks :: PG.Query
    metadataMarks =
      mconcat . intersperse "," . map (const "?") $ metadataColumns

-- | Make a stream from queries.
--
-- This function is less safe than 'makeStream' and should only be used when
-- 'makeStream' is not flexible enough. It is also closer to the implementation
-- and more subject to changes.
makeStream'
  :: (PG.To.ToRow r, PG.To.ToRow r')
  => (forall a. (PG.Connection -> IO a) -> IO a)
     -- ^ Connection pool as a function.
  -> (PG.Query, r) -- ^ Select query.
  -> (forall encEvent. (PG.To.ToField encEvent, PG.To.ToRow metadata)
      => encEvent -> metadata -> CQRS.ConsistencyCheck identifier
      -> (PG.Query, r'))
     -- ^ Insert query builder.
  -> PG.Query -- ^ Identifier column (use tuple if several.)
  -> Stream identifier metadata event
makeStream' connectionPool selectQuery insertQuery identifierColumn =
  Stream{..}

instance
    ( CQRS.Event event
    , Exc.MonadError CQRS.Error m
    , MonadIO m
    , Ord identifier
    , PG.From.FromField identifier
    , PG.To.ToField identifier
    , PG.From.FromRow metadata
    , PG.From.FromField (CQRS.EncodingFormat event)
    ) => CQRS.Stream m (Stream identifier metadata event) where

  type EventType       (Stream identifier metadata event) = event
  type EventIdentifier (Stream identifier metadata event) = identifier
  type EventMetadata   (Stream identifier metadata event) = metadata

  streamEvents = streamStreamEvents

instance
    ( CQRS.WritableEvent event
    , Exc.MonadError CQRS.Error m
    , MonadIO m
    , Ord identifier
    , PG.From.FromField identifier
    , PG.To.ToField identifier
    , PG.From.FromField (CQRS.EncodingFormat event)
    , PG.To.ToField (CQRS.EncodingFormat event)
    , PG.From.FromRow metadata
    , PG.To.ToRow metadata
    ) => CQRS.WritableStream m (Stream identifier metadata event) where

  writeEventWithMetadata = streamWriteEventWithMetadata

streamWriteEventWithMetadata
  :: ( CQRS.WritableEvent event
     , Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.From.FromField identifier
     , PG.From.FromField (CQRS.EncodingFormat event)
     , PG.To.ToField (CQRS.EncodingFormat event)
     , PG.To.ToRow metadata
     )
  => Stream identifier metadata event
  -> event
  -> metadata
  -> CQRS.ConsistencyCheck identifier
  -> m identifier
streamWriteEventWithMetadata Stream{..} event metadata cc = do
  eIds <- liftIO . connectionPool $ \conn -> do
    let (req, params) = insertQuery (CQRS.encodeEvent event) metadata cc
    (Right <$> PG.query conn req params)
      `catches`
        [ handleError (Proxy @PG.FormatError) CQRS.EventWriteError
        , handleError (Proxy @PG.QueryError)  CQRS.EventWriteError
        , handleError (Proxy @PG.ResultError) CQRS.EventWriteError
        , handleError (Proxy @PG.SqlError)    CQRS.EventWriteError
        ]

  case eIds of
    Left err -> Exc.throwError err
    Right [PG.Only identifier] -> pure identifier
    Right [] -> Exc.throwError $ CQRS.ConsistencyCheckError "no events inserted"
    Right ids -> Exc.throwError $ CQRS.EventWriteError $
      show (length ids) ++ " events were inserted"

streamStreamEvents
  :: forall identifier metadata event m.
     ( CQRS.Event event
     , Exc.MonadError CQRS.Error m
     , MonadIO m
     , Ord identifier
     , PG.From.FromField identifier
     , PG.To.ToField identifier
     , PG.From.FromRow metadata
     , PG.From.FromField (CQRS.EncodingFormat event)
     )
  => Stream identifier metadata event
  -> CQRS.StreamBounds identifier
  -> Pipes.Producer
      [ Either
          (identifier, String) (CQRS.EventWithContext identifier metadata event)
      ] m ()
streamStreamEvents Stream{..} bounds =
    go Nothing

  where
    go
      :: Maybe identifier
      -> Pipes.Producer
          [ Either
              (identifier, String)
              (CQRS.EventWithContext identifier metadata event)
          ] m ()
    go lastFetchedIdentifier = do
      -- Fetch 'batchSize' parsed events from the DB.
      eRows <- liftIO . connectionPool $ \conn ->
        (Right <$> fetchBatch conn lastFetchedIdentifier)
          `catches`
            [ handleError (Proxy @PG.FormatError) CQRS.EventRetrievalError
            , handleError (Proxy @PG.QueryError)  CQRS.EventRetrievalError
            , handleError (Proxy @PG.ResultError) CQRS.EventRetrievalError
            , handleError (Proxy @PG.SqlError)    CQRS.EventRetrievalError
            ]

      rows <- either Exc.throwError pure eRows

      Pipes.yield $
        map
          (\(PG.Only identifier :. metadata :. PG.Only encEvent) ->
            case CQRS.decodeEvent encEvent of
              Left err -> Left (identifier, err)
              Right event ->
                Right $ CQRS.EventWithContext identifier metadata event
          ) rows

      let mLfi =
            fmap (\(PG.Only identifier :. _) -> identifier)
            . listToMaybe
            . reverse
            $ rows
      when (length rows == batchSize) $ go mLfi

    fetchBatch
      :: PG.Connection
      -> Maybe identifier
      -> IO [ PG.Only identifier
              :. metadata
              :. PG.Only (CQRS.EncodingFormat event) ]
    fetchBatch conn lastFetchedIdentifier = do
      let bounds' =
            bounds <> maybe mempty CQRS.afterEvent lastFetchedIdentifier
          conditions = catMaybes
            [ CQRS._afterEvent bounds' <&> \i ->
                (identifierColumn <> " > ?", i)
            , CQRS._untilEvent bounds' <&> \i ->
                (identifierColumn <> " <= ?", i)
            ]
          whereClause
            | null conditions = mempty
            | otherwise =
                (" WHERE " <>)
                . mconcat
                . intersperse " AND "
                . map fst
                $ conditions
          params = snd selectQuery :. map snd conditions
          query =
            "SELECT * FROM (" <> fst selectQuery <> ") AS _"
            <> whereClause
            <> " LIMIT "
            <> fromString (show batchSize)

      PG.query conn query params

    batchSize :: Int
    batchSize = 100

handleError
  :: forall e e' a proxy. (Exception e, Show e)
  => proxy e -> (String -> e') -> Handler (Either e' a)
handleError _ f = Handler $ pure . Left . f . show @e
