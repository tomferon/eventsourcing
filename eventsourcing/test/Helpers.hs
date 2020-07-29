{-# LANGUAGE FlexibleContexts #-}

module Helpers
  ( collect
  , interleaveSeqs
  ) where

import Control.Monad (forever)
import Control.Monad.Trans (lift)
import Hedgehog hiding (collect)
import Pipes ((>->))

import qualified Control.Monad.Writer as W
import qualified Hedgehog.Gen         as Gen
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

interleaveSeqs :: [a] -> [a] -> Gen [a]
interleaveSeqs = go id
  where
    go :: ([a] -> [a]) -> [a] -> [a] -> Gen [a]
    go acc [] [] = pure $ acc []
    go acc [] ys = pure $ acc ys
    go acc xs [] = pure $ acc xs
    go acc (x:xs) (y:ys) =
      Gen.choice
        [ go (acc . (x:)) xs (y:ys)
        , go (acc . (y:)) (x:xs) ys
        ]
