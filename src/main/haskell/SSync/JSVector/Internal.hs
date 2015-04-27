module SSync.JSVector.Internal where

import Control.Applicative ((<$>))

newtype VectorMonad a = VectorMonad { runVectorMonad :: IO a }

instance Monad VectorMonad where
  return = VectorMonad . return
  (VectorMonad a) >>= b = VectorMonad $ a >>= runVectorMonad . b

instance Applicative VectorMonad where
  pure = VectorMonad . pure
  (VectorMonad f) <*> (VectorMonad v) = VectorMonad (f <*> v)

instance Functor VectorMonad where
  fmap f (VectorMonad v) = VectorMonad (fmap f v)

