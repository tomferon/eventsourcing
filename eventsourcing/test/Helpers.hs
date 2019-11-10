{-# LANGUAGE FlexibleContexts #-}

module Helpers
  ( collect
  ) where

import Control.Monad (forever)
import Control.Monad.Trans (lift)
import Pipes ((>->))

import qualified Control.Monad.Writer as W
import qualified Pipes

collect :: Monad m => Pipes.Producer a m () -> m [a]
collect producer =
    fmap snd . W.runWriterT . Pipes.runEffect $
      Pipes.hoist lift producer >-> collector
  where
    collector :: W.MonadWriter [a] m => Pipes.Consumer a m ()
    collector = forever $ do
      x <- Pipes.await
      W.tell [x]