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
  , executeSqlActions
  , executeCustomActions
  , fromTabularDataActions
  , makeSqlAction
  ) where

import Control.Exception
import Control.Monad              ((<=<), forever, forM_, unless)
import Control.Monad.Trans        (MonadIO(..), lift)
import Data.Hashable              (Hashable)
import Data.List                  (intersperse)
import Data.Monoid                (getLast)
import Data.Proxy                 (Proxy(..))
import Data.String                (fromString)
import Database.PostgreSQL.Simple ((:.)(..))
import Pipes                      ((>->))

import qualified Control.Monad.Except                 as Exc
import qualified Control.Monad.Identity               as Id
import qualified Control.Monad.State.Strict           as St
import qualified Data.Bifunctor                       as Bifunctor
import qualified Data.Functor.Compose                 as Comp
import qualified Data.HashMap.Strict                  as HM
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.Types     as PG
import qualified Pipes
import qualified Pipes.Prelude                        as Pipes

import Database.CQRS.PostgreSQL.Internal

import qualified Database.CQRS as CQRS
import qualified Database.CQRS.TabularData as CQRS.Tab

type Projection event = CQRS.EffectfulProjection event SqlAction

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
  -> (streamFamily
      -> (forall r. (PG.Connection -> IO r) -> IO r)
      -> PG.Query
      -> Pipes.Consumer
          ( [action]
          , CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ())
  -- ^ Commit the custom actions. See 'executeSqlActions' for 'SqlAction's.
  -- This consumer is expected to update the tracking table accordingly.
  -> m ()
runProjectionWith connectionPool streamFamily projection trackingTable
                  executeActions = do
    newEvents <- CQRS.allNewEvents streamFamily
    Pipes.runEffect $ do
      CQRS.latestEventIdentifiers streamFamily >-> catchUp
      newEvents
        >-> groupByStream
        >-> projectionPipe
        >-> executeActions streamFamily connectionPool trackingTable

  where
    catchUp
      :: Pipes.Consumer
          ( CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ()
    catchUp = forever $ do
      (streamId, eventId) <- Pipes.await
      stream <- lift $ CQRS.getStream streamFamily streamId
      state <-
        getLastEventId connectionPool trackingTable streamFamily streamId

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
        >-> executeActions streamFamily connectionPool trackingTable

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
          ( [action]
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
        Pipes.yield (actions, streamId, latestEventId)

      case mFirstError of
        Nothing -> pure ()
        Just (eventId, err) -> do
          Exc.liftEither <=< liftIO . connectionPool $ \conn -> do
            let (query, values) =
                  upsertTrackingTable
                    trackingTable streamFamily streamId eventId (Just err)
            (const (Right ()) <$> PG.execute conn query values)
              `catches`
                [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
                , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
                ]

-- | Execute the SQL actions and update the tracking table in one transaction.
--
-- The custom actions are transformed into a list of SQL actions by the given
-- function. See 'fromTabularDataActions' for an example.
executeSqlActions
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => ([action] -> [SqlAction])
  -> streamFamily
  -> (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> Pipes.Consumer
      ( [action]
      , CQRS.StreamIdentifier streamFamily
      , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
      ) m ()
executeSqlActions transform streamFamily connectionPool trackingTable =
  forever $ do
    (actions, streamId, eventId) <- Pipes.await

    let sqlActions = transform actions
        (query, values) =
          appendSqlActions
            [ ("BEGIN", [])
            , appendSqlActions sqlActions
            , upsertTrackingTable
                trackingTable streamFamily streamId eventId Nothing
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
                upsertTrackingTable
                  trackingTable streamFamily streamId eventId (Just err)
          (const (Right ()) <$> PG.execute conn uquery uvalues)
            `catches`
              [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
              , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
              ]
        Right _ -> pure $ Right ()

-- | Execute custom actions by calling the runner function on each action in
-- turn and updating the tracking table accordingly.
executeCustomActions
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => (action -> m (Either String (m ())))
  -- ^ Run an action returning either an error or a rollback action.
  -- If any of the rollback actions fail, the others are not run.
  -- Rollback actions are run in reversed order.
  -> streamFamily
  -> (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> Pipes.Consumer
      ( [action]
      , CQRS.StreamIdentifier streamFamily
      , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
      ) m ()
executeCustomActions runAction streamFamily connectionPool trackingTable =
  forever $ do
    (actions, streamId, eventId) <- Pipes.await

    (eRes, rollbackActions) <- lift . flip St.runStateT [] . Exc.runExceptT $
      forM_ actions $ \action -> do
        errOrRollback <- lift . lift . runAction $ action
        case errOrRollback of
          Left err -> Exc.throwError err
          Right rollbackAction -> St.modify' (rollbackAction :)

    lift $ case eRes of
      Left err -> do
        doUpsertTrackingTable
          connectionPool trackingTable streamFamily streamId eventId (Just err)
        sequence_ rollbackActions

      Right () ->
        doUpsertTrackingTable
          connectionPool trackingTable streamFamily streamId eventId Nothing

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
  -> m (TrackedState
        (CQRS.EventIdentifier (CQRS.StreamType streamFamily)))
getLastEventId connectionPool trackingTable streamFamily streamId =
  Exc.liftEither <=< liftIO $
    (Right
     <$> getTrackedState connectionPool trackingTable streamFamily streamId)
    `catches`
      [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
      , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
      ]

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
          else mconcat (" WHERE " : intersperse " AND " cs))
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
