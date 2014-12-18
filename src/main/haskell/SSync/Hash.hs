{-# LANGUAGE RankNTypes, ScopedTypeVariables, RecordWildCards, NamedFieldPuns, DeriveDataTypeable, OverloadedStrings, InstanceSigs #-}

module SSync.Hash (
  withHashM
, HashT
, update
, digest
, digestSize
, NoSuchHashAlgorithm(..)
) where

import qualified Crypto.Hash as C
import qualified Data.ByteString as BS
import Data.Byteable (toBytes)
import Control.Monad.State.Strict

import Control.Exception
import Data.Typeable
import Data.Text (Text)

data NoSuchHashAlgorithm = NoSuchHashAlgorithm Text deriving (Show, Typeable)

instance Exception NoSuchHashAlgorithm

type HashT m b = forall a. C.HashAlgorithm a => StateT (C.Context a) m b

withHashM :: (Monad m) => Text -> HashT m b -> m b
withHashM alg op = withHashM' alg $ \ctx -> flip evalStateT ctx op

-- | Feed the current hash with some bytes.
update :: (Monad m) => BS.ByteString -> HashT m ()
update bs = modify (flip C.hashUpdate bs)

-- | Returns the current value of the hash.
digest :: (Monad m) => HashT m BS.ByteString
digest = do
  ctx <- get
  return $ toBytes $ C.hashFinalize ctx

-- | Returns the size of the current hash's digest, in bytes.  Note:
-- this is not a fast operation.
digestSize :: (Monad m) => (forall a. C.HashAlgorithm a => StateT (C.Context a) m Int)
digestSize = do
  d <- digest
  return $ BS.length d

withHashM' :: (Monad m) => Text -> (forall a. C.HashAlgorithm a => C.Context a -> m b) -> m b
withHashM' "MD4" f = withMD4 f
withHashM' "MD5" f = withMD5 f
withHashM' "SHA1" f = withSHA1 f
withHashM' "SHA256" f = withSHA256 f
withHashM' "SHA512" f = withSHA512 f
withHashM' other _ = throw $ NoSuchHashAlgorithm other

withMD4, withMD5, withSHA1, withSHA256, withSHA512 :: (Monad m) => (forall a. C.HashAlgorithm a => C.Context a -> m b) -> m b
withMD4 f = f (C.hashInit :: C.Context C.MD4)
withMD5 f = f (C.hashInit :: C.Context C.MD5)
withSHA1 f = f (C.hashInit :: C.Context C.SHA1)
withSHA256 f = f (C.hashInit :: C.Context C.SHA256)
withSHA512 f = f (C.hashInit :: C.Context C.SHA512)
