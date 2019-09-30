{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.CQRS.PostgreSQL.Stream
  ( Stream
  ) where

import Control.Monad                    (when)
import Control.Monad.Trans              (MonadIO(..))
import Data.Foldable                    (foldlM)
import Data.Functor                     ((<&>))
import Data.List                        (intersperse)
import Data.Maybe                       (catMaybes)
import Database.PostgreSQL.Simple       ((:.)(..))
import Database.PostgreSQL.Simple.SqlQQ (sql)

import qualified Control.Monad.Except                 as Exc
import qualified Data.Pool                            as Pool
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.FromRow   as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.ToRow     as PG.To
import qualified Pipes

import qualified Database.CQRS as CQRS

data Stream identifier metadata event = Stream
  { connectionPool   :: Pool.Pool PG.Connection
  , selectQuery      :: PG.Query -- Select all events in correct order.
  , insertQuery      :: PG.Query
  , identifierColumn :: PG.Query
  }

instance
    ( CQRS.WritableEvent event
    , Exc.MonadError CQRS.Error m
    , MonadIO m
    , Ord identifier
    , PG.From.FromField identifier
    , PG.To.ToField identifier
    , PG.From.FromRow metadata
    , PG.To.ToRow metadata
    , PG.From.FromField (CQRS.EncodingFormat event)
    , PG.To.ToField (CQRS.EncodingFormat event)
    ) => CQRS.Stream m (Stream identifier metadata event) where

  type EventType       (Stream identifier metadata event) = event
  type EventIdentifier (Stream identifier metadata event) = identifier
  type EventMetadata   (Stream identifier metadata event) = metadata

  writeEventWithMetadata = streamWriteEventWithMetadata
  streamEvents           = streamStreamEvents

-- FIXME: catch IO exceptions

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
  eRes <- liftIO . Pool.withResource connectionPool $ \conn -> do
    ids <- PG.query conn insertQuery $
      metadata :. PG.Only (CQRS.encodeEvent event)
    pure $ case ids of
      [PG.Only identifier] -> Right identifier
      _ -> Left $ show (length ids) ++ " events were inserted"
  either (Exc.throwError . CQRS.EventWriteError) pure eRes

streamStreamEvents
  :: forall identifier metadata event m.
     ( CQRS.WritableEvent event
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
      eEvents <- liftIO . Pool.withResource connectionPool $ \conn -> do
        rows <- fetchBatch conn lastFetchedIdentifier
        pure $ rows <&> \(PG.Only identifier :. metadata :. PG.Only encEvent) ->
          case CQRS.decodeEvent encEvent of
            Left err -> Left $ CQRS.EventDecodingError err
            Right event ->
              Right $ CQRS.EventWithContext identifier metadata event

      -- Yield events accumulating the last event identifier.
      mLfi <- (\f -> foldlM f Nothing eEvents) $ const $ \case
        Left err -> Exc.throwError err
        Right event -> do
          Pipes.yield event
          pure . Just . CQRS.identifier $ event

      -- Loop unless we're done.
      when (length eEvents == batchSize) $ go mLfi

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
                ([sql|{identifierColumn} > ?|], i)
            , CQRS._untilEvent bounds' <&> \i ->
                ([sql|{identifierColumn} <= ?|], i)
            ]
          whereClause =
            mconcat . intersperse [sql| AND |] . map fst $ conditions
          params = map snd conditions
          query
            | whereClause == mempty = selectQuery
            | otherwise =
                [sql| SELECT * FROM ({selectQuery}) AS _ WHERE {whereClause}|]
      PG.query conn query params

    batchSize :: Int
    batchSize = 100
