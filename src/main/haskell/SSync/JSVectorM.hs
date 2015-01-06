module SSync.JSVectorM (Vector, VectorMonad, (!), write, set, new) where

import qualified GHCJS.Types as T

import Control.Monad (forM_)

import SSync.JSVector.Internal

type Vector = T.JSRef ()

foreign import javascript unsafe "$1[$2]" idx :: Vector -> Int -> IO Int
foreign import javascript unsafe "$1[$2] = $3" update :: Vector -> Int -> Int -> IO ()
foreign import javascript unsafe "new Int32Array($1)" newRaw :: Int -> IO Vector
foreign import javascript unsafe "$1.length" vLen :: Vector -> IO Int

(!) :: Vector -> Int -> VectorMonad Int
v ! i = VectorMonad $ idx v i
{-# INLINE (!) #-}

write :: Vector -> Int -> Int -> VectorMonad ()
write v i x = VectorMonad $ update v i x
{-# INLINE write #-}

set :: Vector -> Int -> VectorMonad ()
set v x = VectorMonad $ do -- Wow, JS typed arrays don't have a "fill"? (well, FF >=37 does, but nothing else)
  len <- vLen v
  forM_ [1..len-1] $ \i -> runVectorMonad $ write v i x
{-# INLINE set #-}

new :: Int -> VectorMonad Vector
new len = VectorMonad $ newRaw len
{-# INLINE new #-}
