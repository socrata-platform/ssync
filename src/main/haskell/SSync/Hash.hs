{-# LANGUAGE CPP #-}

module SSync.Hash (
  withHashM
, HashT
, update
, digest
, digestSize
, NoSuchHashAlgorithm(..)
) where

#ifdef STUPID_HASH

import SSync.StupidHash

#else

import SSync.DecentHash

#endif
