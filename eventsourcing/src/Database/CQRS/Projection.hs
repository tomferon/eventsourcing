{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQRS.Projection
  ( Projection
  , Aggregator
  , EffectfulProjection
  , TaskManager
  , runAggregator
  ) where

import Control.Monad.Trans (lift)
import Pipes ((>->))

import qualified Control.Monad.State.Strict as St
import qualified Control.Monad.Identity     as Id
import qualified Data.HashMap.Strict        as HM
import qualified Pipes

import Database.CQRS.Stream

-- | A projection is simply a function consuming events and producing results
-- in an environment @f@.
type Projection f event a =
  event -> f a

-- | Projection aggregating a state in memory.
type Aggregator event agg =
  event -> St.State agg ()

-- | Projection returning actions that can be batched and executed.
--
-- This can be used to batch changes to tables in a database for example.
type EffectfulProjection event action =
  event -> Id.Identity [action]

-- | Projection deriving a list of commands from a stream of events.
--
-- Each command is identified by a unique key. This key is used when persisting
-- the commands and synchronising work between task runners.
type TaskManager event key command =
  Aggregator event (HM.HashMap key command)

-- | Run an 'Aggregator' on events from a stream starting with a given state.
runAggregator
  :: forall m stream aggregate.
     ( Monad m
     , Stream m stream
     )
  => Aggregator (EventWithContext' stream) aggregate
  -> stream
  -> StreamBounds' stream
  -> aggregate
  -> m (aggregate, Maybe (EventIdentifier stream))
runAggregator aggregator stream bounds initState = do
  flip St.execStateT (initState, Nothing) . Pipes.runEffect $
    Pipes.hoist lift (streamEvents stream bounds)
    >->
    aggregatorPipe

  where
    aggregatorPipe
      :: Pipes.Pipe
          (EventWithContext' stream) Pipes.X
          (St.StateT (aggregate, Maybe (EventIdentifier stream)) m) ()
    aggregatorPipe = do
      ewc <- Pipes.await
      St.modify' $ \(aggregate, _) ->
        let aggregate' = St.execState (aggregator  ewc) aggregate
        in (aggregate', Just (identifier ewc))