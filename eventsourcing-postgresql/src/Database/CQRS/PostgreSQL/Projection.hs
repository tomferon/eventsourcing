{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE KindSignatures       #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.CQRS.PostgreSQL.Projection
  ( Projection
  , executeSqlActions
  , executeCustomActions
  , fromTabularDataActions
  ) where

import Control.Exception
import Control.Monad              (forM_, (<=<))
import Control.Monad.Trans        (MonadIO (..), lift)
import Data.List                  (intersperse)
import Data.Proxy                 (Proxy (..))
import Data.String                (fromString)
import Database.PostgreSQL.Simple ((:.) (..))

import qualified Control.Monad.Except               as Exc
import qualified Control.Monad.Identity             as Id
import qualified Control.Monad.State.Strict         as St
import qualified Data.Bifunctor                     as Bifunctor
import qualified Database.PostgreSQL.Simple         as PG
import qualified Database.PostgreSQL.Simple.ToField as PG.To
import qualified Database.PostgreSQL.Simple.Types   as PG

import Database.CQRS.PostgreSQL.Internal
import Database.CQRS.PostgreSQL.TrackingTable

import qualified Database.CQRS             as CQRS
import qualified Database.CQRS.TabularData as CQRS.Tab

type Projection event st = CQRS.Projection event st SqlAction

-- | Execute the SQL actions and update the tracking table in one transaction.
--
-- The custom actions are transformed into a list of SQL actions by the given
-- function. See 'fromTabularDataActions' for an example.
executeSqlActions
  :: forall streamId eventId action m st.
     ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField eventId
     , PG.To.ToField streamId
     , PG.To.ToField st
     )
  => ([action] -> [SqlAction])
  -> (forall r. (PG.Connection -> IO r) -> IO r)
  -> TrackingTable streamId eventId st
  -> (st, [action], streamId, eventId)
  -> m ()
executeSqlActions transform connectionPool trackingTable
  (st, actions, streamId, eventId) = do

  let sqlActions = transform actions
      (query, values) =
        appendSqlActions
          [ ("BEGIN", [])
          , appendSqlActions sqlActions
          , upsertTrackingTable
              trackingTable streamId eventId (Right st)
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
                trackingTable streamId eventId
                (Left err :: Either String st)
        (Right () <$ PG.execute conn uquery uvalues)
          `catches`
            [ handleError (Proxy @PG.FormatError) CQRS.ProjectionError
            , handleError (Proxy @PG.SqlError)    CQRS.ProjectionError
            ]
      Right _ -> pure $ Right ()

-- | Execute custom actions by calling the runner function on each action in
-- turn and updating the tracking table accordingly.
executeCustomActions
  :: forall streamId eventId action m st.
     ( Exc.MonadError CQRS.Error m
     , MonadIO m
     , PG.To.ToField eventId
     , PG.To.ToField streamId
     , PG.To.ToField st
     )
  => (action -> m (Either String (m ())))
  -- ^ Run an action returning either an error or a rollback action.
  -- If any of the rollback actions fail, the others are not run.
  -- Rollback actions are run in reversed order.
  -> TrackingTable streamId eventId st
  -> (st, [action], streamId, eventId)
  -> m ()
executeCustomActions runAction trackingTable
  (st, actions, streamId, eventId) = do

  (eRes, rollbackActions) <- flip St.runStateT [] . Exc.runExceptT $
    forM_ actions $ \action -> do
      errOrRollback <- lift . lift . runAction $ action
      case errOrRollback of
        Left err             -> Exc.throwError err
        Right rollbackAction -> St.modify' (rollbackAction :)

  case eRes of
    Left err -> do
      doUpsertTrackingTable trackingTable streamId eventId
        (Left err :: Either String st)
      sequence_ rollbackActions

    Right () ->
      doUpsertTrackingTable trackingTable streamId eventId (Right st)

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
  -> CQRS.Tab.Tuple CQRS.Tab.Update cols
  -> CQRS.Tab.Tuple CQRS.Tab.Conditions cols
  -> SqlAction
makeUpdateSqlAction relation updates conds =
  let (updatesQuery, updatesValues) =
        Bifunctor.bimap (mconcat . intersperse ",") mconcat
        . unzip
        . CQRS.Tab.toList @PG.To.ToField
            (\name update -> case update of
              CQRS.Tab.NoUpdate -> ("", [])
              CQRS.Tab.Set x ->
                ( "? = ?"
                , [ PG.To.toField (fromString @PG.Identifier name)
                  , PG.To.toField x
                  ]
                )
              CQRS.Tab.Plus x ->
                ( "? = ? + ?"
                , [ PG.To.toField (fromString @PG.Identifier name)
                  , PG.To.toField (fromString @PG.Identifier name)
                  , PG.To.toField x
                  ]
                )
              CQRS.Tab.Minus x ->
                ( "? = ? - ?"
                , [ PG.To.toField (fromString @PG.Identifier name)
                  , PG.To.toField (fromString @PG.Identifier name)
                  , PG.To.toField x
                  ]
                )
             )
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
  CQRS.Tab.Equal x              -> ("? = ?", [name, PG.To.toField x])
  CQRS.Tab.NotEqual x           -> ("? <> ?", [name, PG.To.toField x])
  CQRS.Tab.LowerThan x          -> ("? < ?", [name, PG.To.toField x])
  CQRS.Tab.LowerThanOrEqual x   -> ("? <= ?", [name, PG.To.toField x])
  CQRS.Tab.GreaterThan x        -> ("? > ?", [name, PG.To.toField x])
  CQRS.Tab.GreaterThanOrEqual x -> ("? >= ?", [name, PG.To.toField x])
