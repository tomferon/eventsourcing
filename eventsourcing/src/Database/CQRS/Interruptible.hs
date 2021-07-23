{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}

module Database.CQRS.Interruptible
  ( InterruptibleStreamFamily(..)
  , timingOutStreamFamily
  ) where

import Control.Monad
import Control.Monad.Trans
import Pipes               ((>->))

import qualified Control.Concurrent.STM as STM
import qualified Data.Time              as T
import qualified Pipes

import qualified Database.CQRS as CQRS

-- | Wrap a stream family together with a function that determines whether the
-- new events producer should be interrupted.
--
-- This can be useful for projections which should stop after a certain time
-- for example. See 'timingOutStreamFamily'.
data InterruptibleStreamFamily f fam = InterruptibleStreamFamily
  { streamFamily :: fam
  , interrupt
      :: [ ( CQRS.StreamIdentifier fam
           , Either
              (CQRS.EventIdentifier (CQRS.StreamType fam), String)
              (CQRS.EventWithContext' (CQRS.StreamType fam)))
         ] -> f Bool
  }

instance
  forall m fam.
  ( CQRS.StreamFamily m fam
  , Monad m
  ) => CQRS.StreamFamily m (InterruptibleStreamFamily m fam) where

  type StreamType (InterruptibleStreamFamily _ fam) = CQRS.StreamType fam
  type StreamIdentifier (InterruptibleStreamFamily _ fam) = CQRS.StreamIdentifier fam

  getStream InterruptibleStreamFamily{..} streamID =
    CQRS.getStream streamFamily streamID

  allNewEvents InterruptibleStreamFamily{..} = do
    underlying <- CQRS.allNewEvents streamFamily
    let passthroughOrInterrupt = do
          batch <- Pipes.await
          Pipes.yield batch
          check <- lift $ interrupt batch
          unless check passthroughOrInterrupt
    pure $ underlying >-> passthroughOrInterrupt

  latestEventIdentifiers InterruptibleStreamFamily{..} = do
    CQRS.latestEventIdentifiers streamFamily

-- | Stop producing events after a certain time without any new event coming in.
--
-- This relies on the assumption that the underlying stream family produces
-- empty batches on a regular basis.
timingOutStreamFamily
  :: MonadIO m
  => STM.TVar T.UTCTime
  -> T.NominalDiffTime
  -> fam
  -> InterruptibleStreamFamily m fam
timingOutStreamFamily tvar diff streamFamily =
  InterruptibleStreamFamily{..}

  where
    interrupt :: MonadIO m => [a] -> m Bool
    interrupt events = liftIO $ do
      now <- T.getCurrentTime
      limit <- STM.readTVarIO tvar
      STM.atomically . STM.writeTVar tvar $ T.addUTCTime diff now
      pure $ null events && now > limit
