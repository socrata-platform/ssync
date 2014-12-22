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
import qualified Data.Digest.SHA1 as SHA1
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString (ByteString)
import Control.Monad.State.Strict
import Data.Serialize (encode)

import Control.Exception
import Data.Typeable
import Data.Text (Text)
import Data.Word (Word8, Word32)
import qualified Data.DList as DL
import Data.Bits (shiftR)

data NoSuchHashAlgorithm = NoSuchHashAlgorithm Text deriving (Show, Typeable)

instance Exception NoSuchHashAlgorithm

type S = ([ByteString] -> ByteString, (DL.DList ByteString))

type HashT m = StateT S m

withHashM :: (Monad m) => Text -> HashT m b -> m b
withHashM alg op = flip evalStateT (hasher alg, DL.empty) op

-- | Feed the current hash with some bytes.
update :: (Monad m) => BS.ByteString -> HashT m ()
update bs = modify (addBytes bs)

addBytes :: ByteString -> S -> S
addBytes bs (f, dl) = (f, DL.snoc dl bs)

-- | Returns the current value of the hash.
digest :: (Monad m) => HashT m BS.ByteString
digest = do
  (f, bytes') <- get
  let bytes = DL.toList bytes'
  return $ f bytes

-- | Returns the size of the current hash's digest, in bytes.  Note:
-- this is not a fast operation.
digestSize :: (Monad m) => (HashT m Int)
digestSize = do
  d <- digest
  return $ BS.length d

hasher :: Text -> [ByteString] -> ByteString
hasher "MD5" = encode . MD5.md5 . BSL.fromChunks
hasher "SHA1" = shaRes . SHA1.hash . concatMap BS.unpack
hasher other = throw $ NoSuchHashAlgorithm other

shaRes :: SHA1.Word160 -> ByteString
shaRes (SHA1.Word160 a b c d e) =
  BS.pack $ buildWord a $ buildWord b $ buildWord c $ buildWord d $ buildWord e []

buildWord :: Word32 -> [Word8] -> [Word8]
buildWord w tl = (fromIntegral $ w `shiftR` 24) : (fromIntegral $ w `shiftR` 16) : (fromIntegral $ w `shiftR` 8) : fromIntegral w : tl
