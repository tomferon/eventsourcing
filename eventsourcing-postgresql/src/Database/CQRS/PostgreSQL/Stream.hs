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
import Data.Foldable              (foldlM)
import Data.Functor               ((<&>))
import Data.List                  (intersperse)
import Data.Maybe                 (catMaybes)
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

import qualified Database.CQRS as CQRS

-- | Stream of events stored in a PostgreSQL relation.
--
-- The job of sharding streams in different tables is left to the database. If
-- this is something you want to do, you can create a view and a trigger on
-- insert into that view.
data Stream identifier metadata event =
  forall r r'. (PG.To.ToRow r, PG.To.ToRow r') => Stream
    { connectionPool   :: forall a. (PG.Connection -> IO a) -> IO a
    , selectQuery      :: (PG.Query, r)
      -- ^ Select all events in correct order.
    , insertQuery      :: (PG.Query, r')
    , identifierColumn :: PG.Query
    }

-- | Make a 'Stream' from basic information about the relation name and columns.
makeStream
  :: (forall a. (PG.Connection -> IO a) -> IO a)
     -- ^ Connection pool as a function.
  -> PG.Query   -- ^ Relation name.
  -> PG.Query   -- ^ Identifier column name. If there are several, use a tuple.
  -> [PG.Query] -- ^ Column names for metadata.
  -> PG.Query   -- ^ Event column name.
  -> Stream identifier metadata event
makeStream connectionPool relation
           identifierColumn metadataColumns eventColumn =
    let selectQuery = (selectQuery', ())
        insertQuery = (insertQuery', ())
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

    insertQuery' :: PG.Query
    insertQuery' =
      "INSERT INTO " <> relation <> "("
      <> identifierColumn <> ", " <> metadataList <> ", " <> eventColumn
      <> ") VALUES (?, "
      <> metadataMarks
      <> ", ?) RETURNING "
      <> identifierColumn

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
  -> (PG.Query, r') -- ^ Insert query template (with question marks.)
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
  -> m identifier
streamWriteEventWithMetadata Stream{..} event metadata = do
  eIds <- liftIO . connectionPool $ \conn -> do
    let params = metadata :. PG.Only (CQRS.encodeEvent event)
    (Right <$> PG.query conn (fst insertQuery) (snd insertQuery :. params))
      `catches`
        [ handleError (Proxy @PG.FormatError) CQRS.EventWriteError
        , handleError (Proxy @PG.QueryError)  CQRS.EventWriteError
        , handleError (Proxy @PG.ResultError) CQRS.EventWriteError
        , handleError (Proxy @PG.SqlError)    CQRS.EventWriteError
        ]

  case eIds of
    Left err -> Exc.throwError err
    Right [PG.Only identifier] -> pure identifier
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
  -> Pipes.Producer (CQRS.EventWithContext identifier metadata event) m ()
streamStreamEvents Stream{..} bounds =
    go Nothing

  where
    go
      :: Maybe identifier
      -> Pipes.Producer (CQRS.EventWithContext identifier metadata event) m ()
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

      -- Yield events accumulating the last event identifier.
      mLfi <- (\f -> foldlM f Nothing rows) $
        \_ (PG.Only identifier :. metadata :. PG.Only encEvent) ->
          case CQRS.decodeEvent encEvent of
            Left err -> Exc.throwError $ CQRS.EventDecodingError err
            Right event -> do
              Pipes.yield $ CQRS.EventWithContext identifier metadata event
              pure $ Just identifier

      -- Loop unless we're done.
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
                ("WHERE " <>)
                . mconcat
                . intersperse " AND "
                . map fst
                $ conditions
          params = snd selectQuery :. map snd conditions
          query =
            "SELECT * FROM (" <> fst selectQuery <> ") "
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
