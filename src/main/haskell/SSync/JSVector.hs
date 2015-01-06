module SSync.JSVector (Vector, (!), create, empty) where

import System.IO.Unsafe

import qualified SSync.JSVectorM as VM
import SSync.JSVector.Internal

newtype Vector = Vector VM.Vector

instance Show Vector where
  show _ = "[...]"

foreign import javascript unsafe "$1[$2]" idx :: VM.Vector -> Int -> Int

(!) :: Vector -> Int -> Int
(Vector v) ! i = idx v i
{-# INLINE (!) #-}

-- This unsafePerformIO / unsafeFreeze is safe as long as users don't
-- break the VectorMonad abstraction (which is why it lives in
-- SSync.JSVector.Internal instead of SSync.JSVectorM).
create :: VectorMonad VM.Vector -> Vector
create v = unsafeFreeze (unsafePerformIO $ runVectorMonad v)

unsafeFreeze :: VM.Vector -> Vector
unsafeFreeze = Vector

empty :: Vector
empty = create (VM.new 0)
{-# NOINLINE empty #-}
