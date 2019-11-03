{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Database.CQRS.PostgreSQL.Projection
  ( Projection
  , runProjectionWith
  , fromTabularDataActions
  , makeSqlAction
  ) where

import Control.Exception
import Control.Monad              ((<=<), forever, unless)
import Control.Monad.Trans        (MonadIO(..), lift)
import Data.Hashable              (Hashable)
import Data.List                  (intersperse)
import Data.Monoid                (getLast)
import Data.Proxy                 (Proxy(..))
import Data.String                (fromString)
import Database.PostgreSQL.Simple ((:.)(..))
import Pipes                      ((>->))

import qualified Control.Monad.Except as Exc
import qualified Control.Monad.Identity as Id
import qualified Data.Bifunctor as Bifunctor
import qualified Data.Functor.Compose as Comp
import qualified Data.HashMap.Strict as HM
import qualified Database.PostgreSQL.Simple as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.ToField as PG.To
import qualified Database.PostgreSQL.Simple.ToRow as PG.To
import qualified Database.PostgreSQL.Simple.Types as PG
import qualified Pipes
import qualified Pipes.Prelude as Pipes

import qualified Database.CQRS as CQRS
import qualified Database.CQRS.TabularData as CQRS.Tab

type Projection event = CQRS.EffectfulProjection event SqlAction

type SqlAction = (PG.Query, [PG.To.Action])

runProjectionWith
  :: forall streamFamily action m.
     ( CQRS.Stream m (CQRS.StreamType streamFamily)
     , CQRS.StreamFamily m streamFamily
     , Exc.MonadError CQRS.Error m
     , Hashable (CQRS.StreamIdentifier streamFamily)
     , MonadIO m
     , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , Ord (CQRS.StreamIdentifier streamFamily)
     , PG.From.FromField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> streamFamily
  -> CQRS.EffectfulProjection
      ( CQRS.StreamIdentifier streamFamily
      , CQRS.EventWithContext' (CQRS.StreamType streamFamily)
      ) action
  -> PG.Query -- ^ Relation name used to keep track of where the projection is.
  -> ([action] -> [SqlAction])
  -- ^ Transform the custom actions into SQL actions, possibly optimising them.
  -> m ()
runProjectionWith connectionPool streamFamily projection versionRelation
                  transform = do
    newEvents <- CQRS.allNewEvents streamFamily
    Pipes.runEffect $ do
      CQRS.latestEventIdentifiers streamFamily >-> catchUp
      newEvents
        >-> groupByStream
        >-> projectionPipe
        >-> executeActions

  where
    catchUp
      :: Pipes.Consumer
          ( CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ()
    catchUp = do
      (streamId, eventId) <- Pipes.await
      stream <- lift $ CQRS.getStream streamFamily streamId
      state <-
        getLastEventId connectionPool versionRelation streamFamily streamId

      lift . Pipes.runEffect $ case state of
        NeverRan -> catchUp' streamId stream mempty
        SuccessAt lastSuccesfulEventId
          | lastSuccesfulEventId < eventId ->
              catchUp' streamId stream (CQRS.afterEvent lastSuccesfulEventId)
          | otherwise -> pure ()
        -- We are catching up, so maybe the executable was restarted and this
        -- stream won't fail this time.
        FailureAt (Just lastSuccessfulEventId) _ ->
          catchUp' streamId stream (CQRS.afterEvent lastSuccessfulEventId)
        FailureAt Nothing _ ->
          catchUp' streamId stream mempty

    catchUp'
      :: CQRS.StreamIdentifier streamFamily
      -> CQRS.StreamType streamFamily
      -> CQRS.StreamBounds' (CQRS.StreamType streamFamily)
      -> Pipes.Effect m ()
    catchUp' streamId stream bounds =
      CQRS.streamEvents stream bounds
        >-> Pipes.map (streamId,)
        >-> projectionPipe
        >-> executeActions

    groupByStream
      :: Pipes.Pipe
          [ ( CQRS.StreamIdentifier streamFamily
            , Either
                (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
                (CQRS.EventWithContext' (CQRS.StreamType streamFamily))
            ) ]
          ( CQRS.StreamIdentifier streamFamily
          , [ Either
                (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
                (CQRS.EventWithContext' (CQRS.StreamType streamFamily)) ]
          )
          m ()
    groupByStream = forever $ do
      events <- Pipes.await
      let eventsByStream =
            HM.toList . HM.fromListWith (++) . map (fmap pure) $ events
      Pipes.each eventsByStream

    projectionPipe
      :: Pipes.Pipe
          ( CQRS.StreamIdentifier streamFamily
          , [ Either
                (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
                (CQRS.EventWithContext' (CQRS.StreamType streamFamily)) ]
          )
          ( [SqlAction]
          , CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ()
    projectionPipe = forever $ do
      (streamId, eEvents) <- Pipes.await

      -- "Healthy" events up until the first error if any. We want to process
      -- the events before throwing the error so that chunking as no effect on
      -- semantics.
      let (events, mFirstError) = stopOnLeft eEvents
          actions =
            mconcat
            . Id.runIdentity
            . mapM (projection . (streamId,))
            $ events

      unless (null actions) $ do
        -- There is a last event, otherwise actions would be empty.
        let latestEventId = CQRS.identifier . last $ events
        Pipes.yield (transform actions, streamId, latestEventId)

      case mFirstError of
        Nothing -> pure ()
        Just (eventId, err) -> do
          Exc.liftEither <=< liftIO . connectionPool $ \conn -> do
            let (query, values) =
                  updateLastEventId
                    versionRelation streamFamily streamId eventId (Just err)
            (const (Right ()) <$> PG.execute conn query values)
              `catches`
                [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
                , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
                ]

    executeActions
      :: Pipes.Consumer
          ( [SqlAction]
          , CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ()
    executeActions = forever $ do
      (actions, streamId, eventId) <- Pipes.await
      let (query, values) =
            appendSqlActions
              [ ("BEGIN", [])
              , appendSqlActions actions
              , updateLastEventId
                  versionRelation streamFamily streamId eventId Nothing
              , ("COMMIT", [])
              ]

      Exc.liftEither <=< liftIO . connectionPool $ \conn -> do
        eRes <-
          (Right <$> PG.execute conn query values)
            `catches`
              [ handleError (Proxy @PG.FormatError) id
              , handleError (Proxy @PG.SqlError)    id
              ]

        case eRes of
          Left err -> do
            let (uquery, uvalues) =
                  updateLastEventId
                    versionRelation streamFamily streamId eventId (Just err)
            (const (Right ()) <$> PG.execute conn uquery uvalues)
              `catches`
                [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
                , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
                ]
          Right _ -> pure $ Right ()

updateLastEventId
  :: ( PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => PG.Query
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> CQRS.EventIdentifier (CQRS.StreamType streamFamily)
  -> Maybe String
  -> SqlAction
updateLastEventId versionRelation _ streamId eventId mErr =
  let (updates, updateValues, insertValues) = case mErr of
        Nothing ->
          ( "event_id = ?, failed_event_id = NULL, failed_message = NULL"
          , [PG.To.toField eventId]
          , PG.To.toRow (streamId, eventId, PG.Null, PG.Null)
          )
        Just err ->
          ( "failed_event_id = ?, failed_message = ?"
          , PG.To.toRow (eventId, err)
          , PG.To.toRow (streamId, PG.Null, eventId, err)
          )
      query =
        "INSERT INTO " <> versionRelation
        <> " (stream_id, event_id, failed_event_id, failed_message)"
        <> " VALUES (?, ?, ?, ?) ON CONFLICT DO UPDATE SET "
        <> updates <> " WHERE stream_id = ?"
  in
  makeSqlAction query $
    insertValues ++ updateValues ++ [PG.To.toField streamId]

data ProjectionState identifier
  = NeverRan
  | SuccessAt identifier
  | FailureAt (Maybe identifier) identifier -- ^ Last succeeded at, failed at.

getLastEventId
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.From.FromField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> m (ProjectionState
        (CQRS.EventIdentifier (CQRS.StreamType streamFamily)))
getLastEventId connectionPool versionRelation _ streamId =
  Exc.liftEither <=< liftIO $
    connectionPool $ \conn -> do
      let query =
            "SELECT event_id, failed_event_id FROM "
            <> versionRelation <> " WHERE stream_id = ?"
      rows <- PG.query conn query (PG.Only streamId)
      pure . Right $ case rows of
        [(Just eventId, Nothing)] -> SuccessAt eventId
        [(mEventId, Just failedAt)] -> FailureAt mEventId failedAt
        _ -> NeverRan

    `catches`
      [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
      , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
      ]

handleError
  :: forall e e' a proxy. (Exception e, Show e)
  => proxy e -> (String -> e') -> Handler (Either e' a)
handleError _ f = Handler $ pure . Left . f . show @e

appendSqlActions :: [SqlAction] -> SqlAction
appendSqlActions = \case
    [] -> ("", [])
    action : actions -> foldl step action actions
  where
    step :: SqlAction -> SqlAction -> SqlAction
    step (q1,v1) (q2,v2) = (q1 <> ";" <> q2, v1 ++ v2)

fromTabularDataActions
  :: forall cols. CQRS.Tab.AllColumns PG.To.ToField cols
  => PG.Query -- ^ Relation name.
  -> [CQRS.Tab.TabularDataAction cols]
  -> [SqlAction]
fromTabularDataActions relation =
    map fromTabularDataAction . CQRS.Tab.optimiseActions

  where
    fromTabularDataAction :: CQRS.Tab.TabularDataAction cols -> SqlAction
    fromTabularDataAction  = \case
      CQRS.Tab.Insert tuple ->
        let (identifiers, values) =
              unzip
              . CQRS.Tab.toList @PG.To.ToField
                  (\name (Id.Identity x) ->
                    (fromString @PG.Identifier name, PG.To.toField x))
              $ tuple
            questionMarks =
              "(" <> mconcat (intersperse "," (map (const "?") values)) <> ")"
            query =
              "INSERT INTO " <> relation <> questionMarks
              <> " VALUES " <> questionMarks
        in
        makeSqlAction query (identifiers :. values)

      CQRS.Tab.Update updates conds ->
        let (updatesQuery, updatesValues) =
              Bifunctor.bimap (mconcat . intersperse ",") mconcat
              . unzip
              . CQRS.Tab.toList @PG.To.ToField
                  (\name value -> case getLast value of
                    Nothing -> ("", [])
                    Just x  ->
                      ( "? = ?"
                      , [ PG.To.toField (fromString @PG.Identifier name)
                        , PG.To.toField x
                        ]
                      ))
              $ updates

            (whereStmtQuery, whereStmtValues) =
              makeWhereStatement conds

            values = updatesValues ++ whereStmtValues
            query =
              "UPDATE " <> relation <> " SET " <> updatesQuery <> whereStmtQuery
        in
        (query, values)

      CQRS.Tab.Delete conds ->
        let (whereStmtQuery, whereStmtValues) =
              makeWhereStatement conds
            query = "DELETE FROM " <> relation <> whereStmtQuery
        in
        (query, whereStmtValues)

    makeWhereStatement
      :: CQRS.Tab.Tuple (Comp.Compose [] CQRS.Tab.Condition) cols
      -> (PG.Query, [PG.To.Action])
    makeWhereStatement =
      Bifunctor.bimap
        (\cs -> if null cs
          then ""
          else mconcat (" WHERE" : intersperse " AND " cs))
        mconcat
      . unzip
      . mconcat
      . CQRS.Tab.toList @PG.To.ToField
          (\name value ->
              map (makeCond (PG.To.toField
                            (fromString @PG.Identifier name)))
                  (Comp.getCompose value))

    makeCond
      :: PG.To.ToField a
      => PG.To.Action -> CQRS.Tab.Condition a -> (PG.Query, [PG.To.Action])
    makeCond name = \case
      CQRS.Tab.Equal x -> ("? = ?", [name, PG.To.toField x])
      CQRS.Tab.NotEqual x -> ("? <> ?", [name, PG.To.toField x])
      CQRS.Tab.LowerThan x -> ("? < ?", [name, PG.To.toField x])
      CQRS.Tab.LowerThanOrEqual x -> ("? <= ?", [name, PG.To.toField x])
      CQRS.Tab.GreaterThan x -> ("? > ?", [name, PG.To.toField x])
      CQRS.Tab.GreaterThanOrEqual x -> ("? >= ?", [name, PG.To.toField x])

makeSqlAction :: PG.To.ToRow r => PG.Query -> r -> SqlAction
makeSqlAction query r = (query, PG.To.toRow r)

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