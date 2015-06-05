{-# LANGUAGE CPP, OverloadedStrings, FlexibleContexts #-}

module SSync.Hash (
  forName
, HashAlgorithm(..)
, HashState
, HashT
, withHashT
, initState
, update
, digest
, digestSize
, updateS
, digestS
, digestSizeS
, hexString
, withHashState
, withHashState'
) where

import Control.Monad (liftM)
import Control.Monad.State.Strict (StateT, MonadState, evalStateT, get, put, modify)
import Control.Monad.Trans (lift)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import Data.Text (Text)

#ifdef STUPID_HASH

import Crypto.Types (BitLength)
import qualified Data.DList as DL
import qualified Data.Digest.Pure.MD5 as MD5 -- pureMD5 is 10x faster than Crypto's MD5
import Data.Serialize (encode)
import Data.Tagged (untag, Tagged)

data HashAlgorithm = MD5 deriving (Read, Show, Eq, Ord, Bounded, Enum)

initState :: HashAlgorithm -> HashState
initState MD5 = fromCtx MD5.initialCtx DL.empty targetBlockSizeBytes
  where fromCtx :: MD5.MD5Context -> DL.DList ByteString -> Int -> HashState
        fromCtx ctx leftovers remaining =
          ctx `seq` HashState { update = abstractUpdate ctx leftovers remaining
                              , digest = abstractFinalize ctx leftovers
                              }
        abstractUpdate ctx leftovers remaining new =
          let leftovers' = DL.snoc leftovers new
              remaining' = remaining - BS.length new
          in if remaining' <= 0
             then let (chunks, lastChunk) = chunk targetBlockSizeBytes $ leftoversBytes leftovers'
                      ctx' = MD5.updateCtx ctx chunks
                  in if BS.null lastChunk
                     then fromCtx ctx' DL.empty targetBlockSizeBytes
                     else fromCtx ctx' (DL.singleton lastChunk) (targetBlockSizeBytes - BS.length lastChunk)
             else fromCtx ctx leftovers' remaining'
        abstractFinalize ctx leftovers = encode $ MD5.finalize ctx (leftoversBytes leftovers)
        leftoversBytes = BS.concat . DL.toList
        chunk target bs =
          let splitPoint = BS.length bs - (BS.length bs `rem` target)
          in BS.splitAt splitPoint bs
        targetBlockSizeBytes = untag (MD5.blockLength :: Tagged MD5.MD5Digest BitLength) `div` 8

forName :: Text -> Maybe HashAlgorithm
forName "MD5" = Just MD5
forName _ = Nothing

#else

import qualified Crypto.Hash as C
import Data.Byteable (toBytes)

data HashAlgorithm = MD4 | MD5 | SHA1 | SHA256 | SHA512 deriving (Read, Show, Eq, Ord, Bounded, Enum)

initState :: HashAlgorithm -> HashState
initState alg = result
  where result = case alg of
                  MD4 -> fromCtx (C.hashInit :: C.Context C.MD4)
                  MD5 -> fromCtx (C.hashInit :: C.Context C.MD5)
                  SHA1 -> fromCtx (C.hashInit :: C.Context C.SHA1)
                  SHA256 -> fromCtx (C.hashInit :: C.Context C.SHA256)
                  SHA512 -> fromCtx (C.hashInit :: C.Context C.SHA512)
        fromCtx :: (C.HashAlgorithm a) => C.Context a -> HashState
        fromCtx ctx = ctx `seq` HashState { update = abstractUpdate ctx
                                          , digest = abstractFinalize ctx
                                          }
        abstractUpdate :: (C.HashAlgorithm a) => C.Context a -> ByteString -> HashState
        abstractUpdate ctx = fromCtx . C.hashUpdate ctx
        abstractFinalize :: (C.HashAlgorithm a) => C.Context a -> ByteString
        abstractFinalize = toBytes . C.hashFinalize

forName :: Text -> Maybe HashAlgorithm
forName "MD4" = Just MD4
forName "MD5" = Just MD5
forName "SHA1" = Just SHA1
forName "SHA256" = Just SHA256
forName "SHA512" = Just SHA512
forName _ = Nothing

#endif

data HashState = HashState { update :: ByteString -> HashState
                           , digest :: ByteString
                           }

class DigestSizable a where
  digestSize :: a -> Int

instance DigestSizable HashState where
  digestSize = BS.length . digest

instance DigestSizable HashAlgorithm where
  digestSize = digestSize . initState

-- this is deliberately non-opaque
type HashT = StateT HashState

class HashInit a where
  startState :: a -> HashState

instance HashInit HashAlgorithm where
  startState = initState

instance HashInit HashState where
  startState = id

withHashT :: (Monad m, HashInit h) => h -> HashT m a -> m a
withHashT = flip evalStateT . startState

updateS :: (MonadState HashState m) => ByteString -> m ()
updateS bs = modify (flip update bs)

digestS :: (MonadState HashState m) => m ByteString
digestS = liftM digest get

digestSizeS :: (MonadState HashState m) => m Int
digestSizeS = liftM digestSize get

hexString :: ByteString -> ByteString
hexString = B16.encode

withHashState :: (Monad m) => (HashState -> m (HashState, a)) -> HashT m a
withHashState act = do
  s0 <- get
  (s1, res) <- lift $ act s0
  put s1
  return res

withHashState' :: (Monad m) => (HashState -> m HashState) -> HashT m ()
withHashState' act = do
  s0 <- get
  s1 <- lift $ act s0
  put s1
