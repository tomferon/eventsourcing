module Database.CQRS.Projection
  ( Projection(..)
  , runProjectionStep
  , runProjectionUntilDrained
  , indexed
  ) where

import qualified Control.Monad.State as St
import Data.Hashable (Hashable)
import Data.Maybe (fromMaybe)
import qualified Data.HashMap.Strict as HM

import Database.CQRS.Event

-- | A projection is simply a function that "executes" some action on an event
-- in a environment @f@.
--
-- For example, if @f@ instantiates @MonadState Int@ and @a@ is @()@, then the
-- projection is a function modifying a state of type @Int@ depending on events
-- coming in. It could be
--
-- @
--  data MyEvent = EventA | EventB
--
--  bCounter :: MonadState Int f => Projection f MyEvent ()
--  bCounter = Projection $ \case
--    EventA -> modify' (+1)
--    EventB -> pure ()
-- @
newtype Projection f e a
  = Projection { unProjection :: e -> f a }

-- | Run a projection on the next event in a stream.
runProjectionStep
  :: (EventStream m e s, Monad m)
  => Projection m e a -> s -> m (Maybe a, s)
runProjectionStep proj stream = do
  (mEvent, stream') <- nextEvent stream
  x <- traverse (unProjection proj) mEvent
  pure (x, stream')

-- | Run a projection until the stream is drained.
runProjectionUntilDrained
  :: (EventStream m e s, Monad m, Monoid a)
  => Projection m e a -> s -> m (a, s)
runProjectionUntilDrained =
    go mempty
  where
    go
      :: (EventStream m e s, Monad m)
      => a -> Projection m e a -> s -> m (a, s)
    go acc proj stream = do
      (mEvent, stream') <- nextEvent stream
      case mEvent of
        Nothing -> pure (acc, stream')
        Just event -> do
          acc' <- unProjection proj event
          go acc' proj stream'

-- | Build a projection that maintains individual state for each key of events
-- in a hash map.
--
-- For example, a stream of events about different entities may have events
-- containing the entity ID they relate to. This function could be used to
-- transform a projection from events for a single entity into a projection
-- from events for all entities.
indexed
  :: (Eq k, Hashable k, Monad m, Monoid v)
  => (e -> k) -- | Function to extract the key associated to an event.
  -> Projection (St.StateT v m) e a -- | Projection from events with the same key.
  -> Projection (St.StateT (HM.HashMap k v) m) e a
indexed getKey proj =
  Projection $ \event -> do
    let key = getKey event
    hm <- St.get
    let old = fromMaybe mempty $ HM.lookup key hm
    (x, new) <- St.lift $ St.runStateT (unProjection proj event) old
    St.put $ HM.insert key new hm
    pure x
