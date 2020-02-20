{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.CQRS.PostgreSQL.StreamFamily
  ( StreamFamily(..)
  , makeStreamFamily
  ) where

import Control.Concurrent
import Control.Concurrent.MVar    (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception
import Control.Monad              (void)
import Control.Monad.Trans        (MonadIO(..))
import Data.List                  (intersperse)
import Data.Proxy                 (Proxy(..))
import Database.PostgreSQL.Simple ((:.)(..))
import System.Mem.Weak            (Weak, deRefWeak, mkWeak)

import qualified Control.Concurrent.STM                  as STM
import qualified Control.Monad.Except                    as Exc
import qualified Data.ByteString                         as BS
import qualified Database.PostgreSQL.Simple              as PG
import qualified Database.PostgreSQL.Simple.FromField    as PG.From
import qualified Database.PostgreSQL.Simple.FromRow      as PG.From
import qualified Database.PostgreSQL.Simple.Notification as PG
import qualified Database.PostgreSQL.Simple.ToField      as PG.To
import qualified Pipes

import Database.CQRS.PostgreSQL.Stream

import qualified Database.CQRS as CQRS

-- | Family of event streams stored in a PostgreSQL relation.
--
-- Each stream should have a unique stream identifier and event identifiers must
-- be unique within a stream, but not necessarily across them.
--
-- 'allNewEvents' starts a new thread which reads notifications on the given
-- channel and writes them to a transactional bounded queue (a 'TBQueue') which
-- is then consumed by the returned 'Producer'. The maximum size of this queue
-- is hard-coded to 100. Should an exception be raised in the listening thread,
-- it is thrown back by the producer.
data StreamFamily streamId eventId metadata event = StreamFamily
  { connectionPool         :: forall a. (PG.Connection -> IO a) -> IO a
  , relation               :: PG.Query
  , notificationChannel    :: PG.Query
  , parseNotification      :: BS.ByteString -> Either String (streamId, eventId)
  , streamIdentifierColumn :: PG.Query
  , eventIdentifierColumn  :: PG.Query
  , metadataColumns        :: [PG.Query]
  , eventColumn            :: PG.Query
  }

makeStreamFamily
  :: (forall a. (PG.Connection -> IO a) -> IO a)
  -> PG.Query
  -> PG.Query
  -> (BS.ByteString -> Either String (streamId, eventId))
  -> PG.Query
  -> PG.Query
  -> [PG.Query]
  -> PG.Query
  -> StreamFamily streamId eventId metadata event
makeStreamFamily = StreamFamily

instance
    ( CQRS.Event event
    , Exc.MonadError CQRS.Error m
    , MonadIO m
    , PG.From.FromField eventId
    , PG.From.FromField streamId
    , PG.From.FromField (CQRS.EncodingFormat event)
    , PG.From.FromRow metadata
    , PG.To.ToField eventId
    , PG.To.ToField streamId
    )
    => CQRS.StreamFamily m (StreamFamily streamId eventId metadata event) where

  type StreamType (StreamFamily streamId eventId metadata event) =
    Stream eventId metadata event

  type StreamIdentifier (StreamFamily streamId eventId metadata event) =
    streamId

  getStream              = streamFamilyGetStream
  allNewEvents           = streamFamilyAllNewEvents
  latestEventIdentifiers = streamFamilyLastEventIdentifiers

streamFamilyGetStream
  :: forall streamId eventId metadata event m.
     ( MonadIO m
     , PG.To.ToField streamId
     )
  => StreamFamily streamId eventId metadata event
  -> streamId
  -> m (Stream eventId metadata event)
streamFamilyGetStream StreamFamily{..} streamId =
    pure $
      makeStream' connectionPool selectQuery insertQuery eventIdentifierColumn

  where
    selectQuery :: (PG.Query, PG.Only streamId)
    selectQuery = (selectQueryTpl, PG.Only streamId)

    selectQueryTpl :: PG.Query
    selectQueryTpl =
      "SELECT "
      <> eventIdentifierColumn <> ", " <> metadataList <> ", " <> eventColumn
      <> " FROM " <> relation
      <> " WHERE " <> streamIdentifierColumn
      <> " = ? ORDER BY "
      <> eventIdentifierColumn <> " ASC"

    insertQuery :: (PG.Query, PG.Only streamId)
    insertQuery = (insertQueryTpl, PG.Only streamId)

    insertQueryTpl :: PG.Query
    insertQueryTpl =
      "INSERT INTO "
      <> relation <> "("
      <> streamIdentifierColumn <> ", "
      <> metadataList <> ", "
      <> eventColumn <> ") VALUES (?, "
      <> metadataMarks <> ", ?) RETURNING "
      <> eventIdentifierColumn

    metadataList :: PG.Query
    metadataList =
      mconcat . intersperse "," $ metadataColumns

    metadataMarks :: PG.Query
    metadataMarks =
      mconcat . intersperse "," . map (const "?") $ metadataColumns

data GCKey = GCKey

streamFamilyAllNewEvents
  :: forall streamId eventId metadata event m a.
     ( CQRS.Event event
     , Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.From.FromField eventId
     , PG.From.FromField streamId
     , PG.From.FromField (CQRS.EncodingFormat event)
     , PG.From.FromRow metadata
     , PG.To.ToField eventId
     , PG.To.ToField streamId
     )
  => StreamFamily streamId eventId metadata event
  -> m (Pipes.Producer
        [ ( streamId
          , Either
              (eventId, String) (CQRS.EventWithContext eventId metadata event)
          ) ]
        m a)
streamFamilyAllNewEvents StreamFamily{..} = liftIO $ do
    let gcKey = GCKey
    queue     <- STM.newTBQueueIO 100
    queueWeak <- mkWeak gcKey queue Nothing
    mvar      <- startListeningThread queueWeak
    pure $ producer gcKey mvar queue

  where
    -- Start the listening thread and return an 'MVar' of potential error of the
    -- thread once it has started.
    startListeningThread
      :: Weak (STM.TBQueue (streamId, eventId))
      -> IO (MVar CQRS.Error)
    startListeningThread queueWeak = do
      errorMVar   <- newEmptyMVar
      startedMVar <- newEmptyMVar
      _ <- forkFinally (listen startedMVar queueWeak) $ \eErr -> do
        putMVar startedMVar () -- Unblock it.
        putMVar errorMVar $ case eErr of
          Left  err -> CQRS.NewEventsStreamingError . show $ err
          Right err -> err
      takeMVar startedMVar -- Wait to be sure it started to listen.
      pure errorMVar

    -- Entry point of the thread. It gets a connection, starts listening for
    -- notifications and signals it has started to the parent thread.
    listen
      :: MVar ()
      -> Weak (STM.TBQueue (streamId, eventId))
      -> IO CQRS.Error
    listen startedMVar queueWeak =
      connectionPool $ \conn -> do
        eThreadRes <- Exc.runExceptT $ do
          eRes <- liftIO $
            (Right <$> PG.execute_ conn ("LISTEN " <> notificationChannel))
              `catches`
                [ handleError (Proxy @PG.FormatError) CQRS.NewEventsStreamingError
                , handleError (Proxy @PG.SqlError)    CQRS.NewEventsStreamingError
                ]
          either Exc.throwError (\_ -> pure ()) eRes
          liftIO $ putMVar startedMVar ()
          handleNotifications conn queueWeak

        case eThreadRes of
          Left err -> do
            _ <- try @SomeException $ PG.execute_ conn "UNLISTEN *"
            pure err
          Right () ->
            pure $ CQRS.NewEventsStreamingError "listening thread terminated"

    -- Once the listening thread has initialised itself, it runs this code over
    -- and over again.
    handleNotifications
      :: PG.Connection
      -> Weak (STM.TBQueue (streamId, eventId))
      -> Exc.ExceptT CQRS.Error IO ()
    handleNotifications conn queueWeak = do
      -- Get notifications first even if the queue might not exist anymore.
      -- Otherwise, this thread would *not* have a pointer to the queue only
      -- between the recursion call and the first line of the function, i.e.
      -- maybe not enough time for the garbage collector to run.
      notif  <- liftIO $ PG.getNotification conn
      mQueue <- liftIO $ deRefWeak queueWeak
      case mQueue of
        Nothing -> pure () -- The queue has been garbage collected.
        Just queue -> do
          case parseNotification (PG.notificationData notif) of
            Left err ->
              Exc.throwError . CQRS.NewEventsStreamingError $
                "error decoding notification: " ++ err
            Right pair ->
              liftIO . STM.atomically . STM.writeTBQueue queue $ pair
          handleNotifications conn queueWeak

    -- Producer that repeatedly checks the listening thread is still alive and
    -- fetch corresponding events to the notifications.
    producer
      :: GCKey
      -> MVar CQRS.Error -- Error from listening thread.
      -> STM.TBQueue (streamId, eventId)
      -> Pipes.Producer
          [ ( streamId
            , Either
                (eventId, String) (CQRS.EventWithContext eventId metadata event)
            ) ]
          m a
    producer gcKey mvar queue = do
      -- Check the listening thread is still running.
      mErr <- liftIO $ tryTakeMVar mvar
      maybe (pure ()) Exc.throwError mErr

      -- Get some events or none after timeout just in case the listening
      -- thread died in the meantime (we don't want to block forever.)
      events <- liftIO $ race
        (\tmvar -> STM.atomically $ do
          events <- (:) <$> STM.readTBQueue queue <*> STM.flushTBQueue queue
          STM.putTMVar tmvar . Right $ events)
        (\tmvar -> do
          threadDelay 1000000 -- 1 second
          STM.atomically . STM.putTMVar tmvar . Right $ [])

      fetchEvents events
      producer gcKey mvar queue

    fetchEvents
      :: [(streamId, eventId)]
      -> Pipes.Producer
          [ ( streamId
            , Either
                (eventId, String) (CQRS.EventWithContext eventId metadata event)
            ) ]
          m ()
    fetchEvents [] = Pipes.yield []
    fetchEvents pairs = do
      let pairs' = map (uncurry Pair) pairs
      eRows <- liftIO . connectionPool $ \conn ->
        (Right <$> PG.query conn fetchQuery (PG.Only (PG.In pairs')))
          `catches`
            [ handleError (Proxy @PG.FormatError) CQRS.EventRetrievalError
            , handleError (Proxy @PG.QueryError)  CQRS.EventRetrievalError
            , handleError (Proxy @PG.ResultError) CQRS.EventRetrievalError
            , handleError (Proxy @PG.SqlError)    CQRS.EventRetrievalError
            ]

      rows <- either Exc.throwError pure eRows

      Pipes.yield $
        map
          (\(PG.Only streamId :. PG.Only identifier
             :. metadata :. PG.Only encEvent) ->
            case CQRS.decodeEvent encEvent of
              Left err -> (streamId, Left (identifier, err))
              Right event ->
                ( streamId
                , Right (CQRS.EventWithContext identifier metadata event)
                )
          ) rows

    fetchQuery :: PG.Query
    fetchQuery =
      "SELECT "
      <> streamIdentifierColumn <> ", "
      <> eventIdentifierColumn <> ", "
      <> metadataList <> ", "
      <> eventColumn
      <> " FROM " <> relation
      <> " WHERE (" <> streamIdentifierColumn <> ", "
      <> eventIdentifierColumn
      <> ") IN ? ORDER BY " <> eventIdentifierColumn <> " ASC"

    metadataList :: PG.Query
    metadataList =
      mconcat . intersperse "," $ metadataColumns

streamFamilyLastEventIdentifiers
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.From.FromField eventId
     , PG.From.FromField streamId
     )
  => StreamFamily streamId eventId metadata event
  -> Pipes.Producer (streamId, eventId) m ()
streamFamilyLastEventIdentifiers StreamFamily{..} = do
    eIds <- liftIO . connectionPool $ \conn -> do
      (Right <$> PG.query_ conn query)
        `catches`
          [ handleError (Proxy @PG.FormatError) CQRS.EventRetrievalError
          , handleError (Proxy @PG.QueryError)  CQRS.EventRetrievalError
          , handleError (Proxy @PG.ResultError) CQRS.EventRetrievalError
          , handleError (Proxy @PG.SqlError)    CQRS.EventRetrievalError
          ]

    either Exc.throwError Pipes.each eIds

  where
    query :: PG.Query
    query =
      "SELECT "
      <> streamIdentifierColumn <> ", "
      <> "max(" <> eventIdentifierColumn
      <> ") FROM " <> relation
      <> " GROUP BY " <> streamIdentifierColumn

handleError
  :: forall e e' a proxy. (Exception e, Show e)
  => proxy e -> (String -> e') -> Handler (Either e' a)
handleError _ f = Handler $ pure . Left . f . show @e

-- | Run two threads concurrently and return the result of the first one to
-- write to the givem 'TMVar'. If the first thread to finish does so because
-- it throws an exception, the exception is rethrown in the main process.
race
  :: (STM.TMVar (Either SomeException a) -> IO ())
  -> (STM.TMVar (Either SomeException a) -> IO ())
  -> IO a
race f g = do
    tmvar <- STM.newEmptyTMVarIO
    (tid, tid') <- mask $ \restore ->
      (,) <$> run f tmvar restore <*> run g tmvar restore
    eRes <- STM.atomically . STM.readTMVar $ tmvar
    killThread tid
    killThread tid'
    either throw pure eRes

  where
    run
      :: (STM.TMVar (Either SomeException a) -> IO ())
      -> STM.TMVar (Either SomeException a)
      -> (forall b. IO b -> IO b)
      -> IO ThreadId
    run h tmvar restore =
      forkIO $
        restore (h tmvar)
          `catch` (void . STM.atomically . STM.tryPutTMVar tmvar . Left)

data Pair a b = Pair a b

instance (PG.To.ToField a, PG.To.ToField b) => PG.To.ToField (Pair a b) where
  toField (Pair x y) =
    PG.To.Many
      [ PG.To.Plain "ROW("
      , PG.To.toField x
      , PG.To.Plain ","
      , PG.To.toField y
      , PG.To.Plain ")"
      ]
