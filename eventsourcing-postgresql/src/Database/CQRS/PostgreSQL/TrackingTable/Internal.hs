{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}

module Database.CQRS.PostgreSQL.TrackingTable.Internal where

import Control.Applicative ((<|>))
import Control.Exception   (catches)
import Control.Monad       ((<=<))
import Control.Monad.Trans (MonadIO (..))
import Data.Proxy          (Proxy (..))

import Database.CQRS.PostgreSQL.Internal (handleError)

import qualified Control.Monad.Except                 as Exc
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To

import qualified Database.CQRS as CQRS

data TrackingTable streamId eventId st = TrackingTable
  { connectionPool :: forall r. (PG.Connection -> IO r) -> IO r
  , relation       :: PG.Query
  }

getTrackedStateWithConn
  :: forall streamId eventId st.
     ( PG.From.FromField eventId
     , PG.From.FromField st
     , PG.From.FromField streamId
     , PG.To.ToField eventId
     , PG.To.ToField st
     , PG.To.ToField streamId
     )
  => PG.Connection
  -> PG.Query
  -> streamId
  -> IO (Either CQRS.Error (CQRS.TrackedState eventId st))
getTrackedStateWithConn conn relation streamId = do
    (Right <$> getState)
    `catches`
      [ handleError (Proxy @PG.FormatError) CQRS.TrackingTableError
      , handleError (Proxy @PG.SqlError)    CQRS.TrackingTableError
      , handleError (Proxy @PG.QueryError)  CQRS.TrackingTableError
      , handleError (Proxy @PG.ResultError) CQRS.TrackingTableError
      ]

  where
    getState :: IO (CQRS.TrackedState eventId st)
    getState = do
      let req =
            "SELECT event_id, failed_event_id, failed_message, state FROM "
            <> relation <> " WHERE stream_id = ?"
      rows <- PG.query conn req (PG.Only streamId)
      pure $ case rows of
        [(Just eventId, Nothing, Nothing, SomeState state)] ->
          CQRS.SuccessAt eventId state
        [(mEventId, Just failedAt, Just err, oState)] ->
          CQRS.FailureAt
            ((,) <$> mEventId <*> fromOptionalState oState) failedAt err
        _ -> CQRS.NeverRan

handlePgErrors
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     )
  => IO a -> m a
handlePgErrors f =
  Exc.liftEither <=< liftIO $ do
    (Right <$> f)
    `catches`
      [ handleError (Proxy @PG.FormatError) CQRS.TrackingTableError
      , handleError (Proxy @PG.SqlError)    CQRS.TrackingTableError
      , handleError (Proxy @PG.QueryError)  CQRS.TrackingTableError
      , handleError (Proxy @PG.ResultError) CQRS.TrackingTableError
      ]

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
