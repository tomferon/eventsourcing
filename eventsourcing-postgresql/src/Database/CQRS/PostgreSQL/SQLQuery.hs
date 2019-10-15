{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}

module Database.CQRS.PostgreSQL.SQLQuery
  ( SQLQuery(..)
  ) where

import Control.Monad.Trans (MonadIO(..))

import qualified Database.PostgreSQL.Simple         as PG
import qualified Database.PostgreSQL.Simple.FromRow as PG.From
import qualified Database.PostgreSQL.Simple.ToRow   as PG.To

import qualified Database.CQRS as CQRS

-- | A wrapper around a SELECT query that instantiates 'ReadModel' so that it
-- can be used by the application layer without said layer to be aware of SQL.
-- The implementation can then be swapped for something else, e.g. for tests.
data SQLQuery req resp = SQLQuery
  { connectionPool :: forall a. (PG.Connection -> IO a) -> IO a
  , queryTemplate  :: PG.Query
  }

instance
    {-# OVERLAPPABLE #-}
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