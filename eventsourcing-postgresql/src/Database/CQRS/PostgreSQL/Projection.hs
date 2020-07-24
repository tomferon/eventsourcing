{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
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
import Control.Monad              ((<=<), forever, forM_, unless, void)
import Control.Monad.Trans        (MonadIO(..), lift)
import Data.Hashable              (Hashable)
import Data.List                  (intersperse)
import Data.Monoid                (Last(..))
import Data.Proxy                 (Proxy(..))
import Data.String                (fromString)
import Data.Tuple                 (swap)
import Database.PostgreSQL.Simple ((:.)(..))
import Pipes                      ((>->))

import qualified Control.Monad.Except                 as Exc
import qualified Control.Monad.Identity               as Id
import qualified Control.Monad.State.Strict           as St
import qualified Data.Bifunctor                       as Bifunctor
import qualified Data.HashMap.Strict                  as HM
import qualified Database.PostgreSQL.Simple           as PG
import qualified Database.PostgreSQL.Simple.FromField as PG.From
import qualified Database.PostgreSQL.Simple.ToField   as PG.To
import qualified Database.PostgreSQL.Simple.Types     as PG
import qualified Pipes

import Database.CQRS.PostgreSQL.Internal

import qualified Database.CQRS as CQRS
import qualified Database.CQRS.TabularData as CQRS.Tab

type Projection event st = CQRS.EffectfulProjection event st SqlAction

runProjectionWith
  :: forall streamFamily action m st.
     ( CQRS.Stream m (CQRS.StreamType streamFamily)
     , CQRS.StreamFamily m streamFamily
     , Exc.MonadError CQRS.Error m
     , Hashable (CQRS.StreamIdentifier streamFamily)
     , MonadIO m
     , Ord (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , Ord (CQRS.StreamIdentifier streamFamily)
     , PG.From.FromField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.From.FromField st
     , PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     , PG.To.ToField st
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> streamFamily
  -> (CQRS.StreamIdentifier streamFamily -> st)
     -- ^ Initialise state when no events have been processed yet.
  -> CQRS.EffectfulProjection
      (CQRS.EventWithContext' (CQRS.StreamType streamFamily))
      st action
  -> PG.Query -- ^ Relation name used to keep track of where the projection is.
  -> (streamFamily
      -> (forall r. (PG.Connection -> IO r) -> IO r)
      -> PG.Query
      -> Pipes.Consumer
          ( st
          , [action]
          , CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ())
  -- ^ Commit the custom actions. See 'executeSqlActions' for 'SqlAction's.
  -- This consumer is expected to update the tracking table accordingly.
  -> m ()
runProjectionWith connectionPool streamFamily initState projection trackingTable
                  executeActions = do
    newEvents <- CQRS.allNewEvents streamFamily
    Pipes.runEffect $ do
      CQRS.latestEventIdentifiers streamFamily >-> catchUp
      newEvents
        >-> groupByStream
        >-> familyProjectionPipe
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
        NeverRan -> catchUp' streamId stream mempty (initState streamId)
        SuccessAt lastSuccesfulEventId st
          | lastSuccesfulEventId < eventId ->
              catchUp' streamId stream (CQRS.afterEvent lastSuccesfulEventId) st
          | otherwise -> pure ()
        -- We are catching up, so maybe the executable was restarted and this
        -- stream won't fail this time.
        FailureAt (Just (lastSuccessfulEventId, st)) _ ->
          catchUp' streamId stream (CQRS.afterEvent lastSuccessfulEventId) st
        FailureAt Nothing _ ->
          catchUp' streamId stream mempty (initState streamId)

    catchUp'
      :: CQRS.StreamIdentifier streamFamily
      -> CQRS.StreamType streamFamily
      -> CQRS.StreamBounds' (CQRS.StreamType streamFamily)
      -> st
      -> Pipes.Effect m ()
    catchUp' streamId stream bounds st =
      CQRS.streamEvents stream bounds
        >-> streamProjectionPipe streamId st
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

    familyProjectionPipe
      :: Pipes.Pipe
          ( CQRS.StreamIdentifier streamFamily
          , [ Either
                (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
                (CQRS.EventWithContext' (CQRS.StreamType streamFamily)) ]
          )
          ( st
          , [action]
          , CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ()
    familyProjectionPipe = forever $ do
      (streamId, eEvents) <- Pipes.await

      state <-
        getLastEventId connectionPool trackingTable streamFamily streamId

      let filterEventsAfter eventId = filter $ \case
            Left (eventId', _) -> eventId' > eventId
            Right CQRS.EventWithContext{ CQRS.identifier = eventId' } ->
              eventId' > eventId

      case state of
        NeverRan ->
          void $ coreProjectionPipe streamId (initState streamId) eEvents
        SuccessAt lastSuccesfulEventId st ->
          void
            . coreProjectionPipe streamId st
            . filterEventsAfter lastSuccesfulEventId
            $ eEvents
        -- This is used after catching up. If it's still marked as failed, all
        -- hope is lost.
        FailureAt _ _ -> pure ()

    streamProjectionPipe
      :: CQRS.StreamIdentifier streamFamily
      -> st
      -> Pipes.Pipe
          [ Either
              (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
              (CQRS.EventWithContext' (CQRS.StreamType streamFamily)) ]
          ( st
          , [action]
          , CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m ()
    streamProjectionPipe streamId st = do
      eEvents <- Pipes.await
      mSt' <- coreProjectionPipe streamId st eEvents
      case mSt' of
        Just st' -> streamProjectionPipe streamId st'
        Nothing -> pure ()

    coreProjectionPipe
      :: CQRS.StreamIdentifier streamFamily
      -> st
      -> [ Either
            (CQRS.EventIdentifier (CQRS.StreamType streamFamily), String)
            (CQRS.EventWithContext' (CQRS.StreamType streamFamily)) ]
      -> Pipes.Pipe
          a -- It's not supposed to consume any data.
          ( st
          , [action]
          , CQRS.StreamIdentifier streamFamily
          , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
          ) m (Maybe st) -- Nothing in case of failure.
    coreProjectionPipe streamId st eEvents = do
      -- "Healthy" events up until the first error if any. We want to process
      -- the events before throwing the error so that chunking as no effect on
      -- semantics.
      let (events, mFirstError) = stopOnLeft eEvents
          (st', actions) =
            fmap mconcat
              . swap
              . flip St.runState st
              . mapM projection
              $ events

      unless (null actions) $ do
        -- There is a last event, otherwise actions would be empty.
        let latestEventId = CQRS.identifier . last $ events
        Pipes.yield (st', actions, streamId, latestEventId)

      case mFirstError of
        Nothing -> pure Nothing
        Just (eventId, err) ->
          Exc.liftEither <=< liftIO . connectionPool $ \conn -> do
            let (query, values) =
                  upsertTrackingTable
                    trackingTable streamFamily streamId eventId
                    (Left err :: Either String st)
            (const (Right (Just st')) <$> PG.execute conn query values)
              `catches`
                [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
                , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
                ]

-- | Execute the SQL actions and update the tracking table in one transaction.
--
-- The custom actions are transformed into a list of SQL actions by the given
-- function. See 'fromTabularDataActions' for an example.
executeSqlActions
  :: forall streamFamily action m st.
     ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     , PG.To.ToField st
     )
  => ([action] -> [SqlAction])
  -> streamFamily
  -> (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> Pipes.Consumer
      ( st
      , [action]
      , CQRS.StreamIdentifier streamFamily
      , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
      ) m ()
executeSqlActions transform streamFamily connectionPool trackingTable =
  forever $ do
    (st, actions, streamId, eventId) <- Pipes.await

    let sqlActions = transform actions
        (query, values) =
          appendSqlActions
            [ ("BEGIN", [])
            , appendSqlActions sqlActions
            , upsertTrackingTable
                trackingTable streamFamily streamId eventId (Right st)
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
                  trackingTable streamFamily streamId eventId
                  (Left err :: Either String st)
          (const (Right ()) <$> PG.execute conn uquery uvalues)
            `catches`
              [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
              , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
              ]
        Right _ -> pure $ Right ()

-- | Execute custom actions by calling the runner function on each action in
-- turn and updating the tracking table accordingly.
executeCustomActions
  :: forall streamFamily action m st.
     ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     , PG.To.ToField st
     )
  => (action -> m (Either String (m ())))
  -- ^ Run an action returning either an error or a rollback action.
  -- If any of the rollback actions fail, the others are not run.
  -- Rollback actions are run in reversed order.
  -> streamFamily
  -> (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> Pipes.Consumer
      ( st
      , [action]
      , CQRS.StreamIdentifier streamFamily
      , CQRS.EventIdentifier (CQRS.StreamType streamFamily)
      ) m ()
executeCustomActions runAction streamFamily connectionPool trackingTable =
  forever $ do
    (st, actions, streamId, eventId) <- Pipes.await

    (eRes, rollbackActions) <- lift . flip St.runStateT [] . Exc.runExceptT $
      forM_ actions $ \action -> do
        errOrRollback <- lift . lift . runAction $ action
        case errOrRollback of
          Left err -> Exc.throwError err
          Right rollbackAction -> St.modify' (rollbackAction :)

    lift $ case eRes of
      Left err -> do
        doUpsertTrackingTable
          connectionPool trackingTable streamFamily streamId eventId
          (Left err :: Either String st)
        sequence_ rollbackActions

      Right () ->
        doUpsertTrackingTable
          connectionPool trackingTable streamFamily streamId eventId (Right st)

getLastEventId
  :: ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.From.FromField (CQRS.EventIdentifier (CQRS.StreamType streamFamily))
     , PG.From.FromField st
     , PG.To.ToField (CQRS.StreamIdentifier streamFamily)
     )
  => (forall r. (PG.Connection -> IO r) -> IO r)
  -> PG.Query
  -> streamFamily
  -> CQRS.StreamIdentifier streamFamily
  -> m (TrackedState
        (CQRS.EventIdentifier (CQRS.StreamType streamFamily)) st)
getLastEventId connectionPool trackingTable streamFamily streamId =
  Exc.liftEither <=< liftIO $
    (Right
      <$> getTrackedState connectionPool trackingTable streamFamily streamId)
    `catches`
      [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
      , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
      ]

fromTabularDataActions
  :: FromTabularDataAction cols
  => PG.Query -- ^ Relation name.
  -> [CQRS.Tab.TabularDataAction cols]
  -> [SqlAction]
fromTabularDataActions = map . fromTabularDataAction

class FromTabularDataAction cols where
  fromTabularDataAction
    :: PG.Query -> CQRS.Tab.TabularDataAction cols -> SqlAction

instance
    forall keyCols cols.
    ( CQRS.Tab.AllColumns
        PG.To.ToField (CQRS.Tab.Flatten ('CQRS.Tab.WithUniqueKey keyCols cols))
    , CQRS.Tab.AllColumns PG.To.ToField keyCols
    , CQRS.Tab.AllColumns PG.To.ToField cols
    , CQRS.Tab.MergeSplitTuple keyCols cols
    )
    => FromTabularDataAction ('CQRS.Tab.WithUniqueKey keyCols cols) where

  fromTabularDataAction relation = \case
    CQRS.Tab.Insert tuple ->
      makeInsertSqlAction @('CQRS.Tab.WithUniqueKey keyCols cols) relation tuple
    CQRS.Tab.Update updates conds ->
      makeUpdateSqlAction
        @('CQRS.Tab.WithUniqueKey keyCols cols)
        relation updates conds
    CQRS.Tab.Upsert tuple -> makeUpsertSqlAction @keyCols @cols relation tuple
    CQRS.Tab.Delete conds ->
      makeDeleteSqlAction
        @('CQRS.Tab.WithUniqueKey keyCols cols)
        relation conds

instance
    forall cols.
    CQRS.Tab.AllColumns PG.To.ToField cols
    => FromTabularDataAction ('CQRS.Tab.Flat cols) where

  fromTabularDataAction relation = \case
    CQRS.Tab.Insert tuple ->
      makeInsertSqlAction @('CQRS.Tab.Flat cols) relation tuple
    CQRS.Tab.Update updates conds ->
      makeUpdateSqlAction @('CQRS.Tab.Flat cols) relation updates conds
    CQRS.Tab.Delete conds ->
      makeDeleteSqlAction @('CQRS.Tab.Flat cols) relation conds

makeInsertSqlAction
  :: forall (cols :: CQRS.Tab.Columns).
     CQRS.Tab.AllColumns PG.To.ToField (CQRS.Tab.Flatten cols)
  => PG.Query -> CQRS.Tab.Tuple Id.Identity cols -> SqlAction
makeInsertSqlAction relation tuple =
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

makeUpdateSqlAction
  :: forall (cols ::  CQRS.Tab.Columns).
     CQRS.Tab.AllColumns PG.To.ToField (CQRS.Tab.Flatten cols)
  => PG.Query
  -> CQRS.Tab.Tuple Last cols
  -> CQRS.Tab.Tuple CQRS.Tab.Conditions cols
  -> SqlAction
makeUpdateSqlAction relation updates conds =
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

      (whereStmtQuery, whereStmtValues) = makeWhereStatement @cols conds

      values = updatesValues ++ whereStmtValues
      query =
        "UPDATE " <> relation <> " SET " <> updatesQuery <> whereStmtQuery
  in
  (query, values)

makeUpsertSqlAction
  :: forall keyCols cols.
     ( CQRS.Tab.AllColumns PG.To.ToField keyCols
     , CQRS.Tab.AllColumns PG.To.ToField cols
     , CQRS.Tab.MergeSplitTuple keyCols cols
     )
  => PG.Query
  -> CQRS.Tab.Tuple Id.Identity ('CQRS.Tab.WithUniqueKey keyCols cols)
  -> SqlAction
makeUpsertSqlAction relation tuple =
  let toSqlValues
        :: forall flatCols. CQRS.Tab.AllColumns PG.To.ToField flatCols
        => CQRS.Tab.FlatTuple Id.Identity flatCols
        -> [(PG.Identifier, PG.To.Action)]
      toSqlValues =
        CQRS.Tab.toList @PG.To.ToField
          (\name (Id.Identity x) ->
            (fromString @PG.Identifier name, PG.To.toField x))
      (keyTuple, otherTuple) = CQRS.Tab.splitTuple @keyCols @cols tuple
      keyPairs = toSqlValues keyTuple
      (keyIdentifiers, keyValues) = unzip keyPairs
      otherPairs = toSqlValues otherTuple
      (otherIdentifiers, otherValues) = unzip otherPairs

      mkQuestionMarks xs =
        mconcat (intersperse "," (map (const "?") xs))
      keyQuestionMarks = mkQuestionMarks keyValues
      rowQuestionMarks =
        "(" <> mkQuestionMarks (keyValues ++ otherValues) <> ")"

      mkValues =
        foldr (\(name, value) acc -> PG.To.toField name : value : acc) []
      updateSetValues = mkValues otherPairs
      updateWhereValues = mkValues keyPairs

      query =
        "INSERT INTO " <> relation <> rowQuestionMarks
        <> " VALUES " <> rowQuestionMarks
        <> " ON CONFLICT " <> keyQuestionMarks <> " DO UPDATE SET "
        <> mconcat
            (intersperse ", " (map (const "? = ?") otherIdentifiers))
        <> " WHERE "
        <> mconcat
            (intersperse " AND " (map (const "? = ?") keyIdentifiers))

  in
  makeSqlAction query $
    keyIdentifiers :. otherIdentifiers :. keyValues :. otherValues
    :. keyIdentifiers :. updateSetValues :. updateWhereValues

makeDeleteSqlAction
  :: forall (cols :: CQRS.Tab.Columns).
     CQRS.Tab.AllColumns PG.To.ToField (CQRS.Tab.Flatten cols)
  => PG.Query
  -> CQRS.Tab.Tuple CQRS.Tab.Conditions cols
  -> SqlAction
makeDeleteSqlAction relation conds =
  let (whereStmtQuery, whereStmtValues) =
        makeWhereStatement @cols conds
      query = "DELETE FROM " <> relation <> whereStmtQuery
  in
  (query, whereStmtValues)

makeWhereStatement
  :: forall (cols :: CQRS.Tab.Columns).
     CQRS.Tab.AllColumns PG.To.ToField (CQRS.Tab.Flatten cols)
  => CQRS.Tab.Tuple CQRS.Tab.Conditions cols
  -> (PG.Query, [PG.To.Action])
makeWhereStatement =
  Bifunctor.bimap
    (\cs -> if null cs
      then ""
      else mconcat (" WHERE " : intersperse " AND " cs))
    mconcat
  . unzip
  . mconcat
  . CQRS.Tab.toList @PG.To.ToField @(CQRS.Tab.Flatten cols)
      (\name value ->
        map
          (makeCond (PG.To.toField (fromString @PG.Identifier name)))
          (CQRS.Tab.getConditions value))

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
