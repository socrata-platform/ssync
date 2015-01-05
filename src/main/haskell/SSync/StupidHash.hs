{-# LANGUAGE RankNTypes, ScopedTypeVariables, RecordWildCards, NamedFieldPuns, DeriveDataTypeable, OverloadedStrings, InstanceSigs #-}

module SSync.StupidHash (
  withHashM
, HashT
, update
, digest
, digestSize
, NoSuchHashAlgorithm(..)
) where

import qualified Data.Digest.Pure.MD5 as MD5 -- pureMD5 is 10x faster than Crypto's MD5
import qualified Data.ByteString as BS
import qualified Data.DList as DL
import Control.Monad.State.Strict
import Data.Serialize (encode)

import Control.Exception
import Data.Typeable
import Data.Text (Text)
import Data.Tagged (untag, Tagged)
import Crypto.Types (BitLength)

data NoSuchHashAlgorithm = NoSuchHashAlgorithm Text deriving (Show, Typeable)

instance Exception NoSuchHashAlgorithm

data S = S !(DL.DList BS.ByteString) !Int !MD5.MD5Context

type HashT m = StateT S m

withHashM :: (Monad m) => Text -> HashT m b -> m b
withHashM "MD5" op = flip evalStateT (S DL.empty targetBlockSizeBytes MD5.initialCtx) op
withHashM other _ = throw $ NoSuchHashAlgorithm other

targetBlockSizeBytes :: Int
targetBlockSizeBytes = untag (MD5.blockLength :: Tagged MD5.MD5Digest BitLength) `div` 8

-- | Feed the current hash with some bytes.
update :: (Monad m) => BS.ByteString -> HashT m ()
update bs = unless (BS.null bs) $ do
  (S soFar remaining ctx) <- get
  let soFar' = DL.snoc soFar bs
      remaining' = remaining - BS.length bs
  if remaining' <= 0
    then let (chunks, lastChunk) = chunk targetBlockSizeBytes $ DL.toList soFar'
             ctx' = MD5.updateCtx ctx chunks
         in if BS.null lastChunk
            then put (S DL.empty targetBlockSizeBytes ctx')
            else put (S (DL.singleton lastChunk) (targetBlockSizeBytes - BS.length lastChunk) ctx')
    else put (S soFar' remaining' ctx)

chunk :: Int -> [BS.ByteString] -> (BS.ByteString, BS.ByteString)
chunk target bs =
  let flattened = BS.concat bs
      splitPoint = BS.length flattened - (BS.length flattened `rem` target)
  in BS.splitAt splitPoint flattened

digest :: (Monad m) => HashT m BS.ByteString
digest = do
  (S bytes _ ctx) <- get
  let d = MD5.finalize ctx (BS.concat $ DL.toList bytes)
  return $ encode d

-- | Returns the size of the current hash's digest, in bytes.  Note:
-- this is not a fast operation.
digestSize :: (Monad m) => (HashT m Int)
digestSize = do
  d <- digest
  return $ BS.length d
