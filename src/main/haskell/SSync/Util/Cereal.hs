{-# LANGUAGE DeriveDataTypeable, LambdaCase, RankNTypes, ScopedTypeVariables #-}

module SSync.Util.Cereal (
  getVarInt
, putVarInt
, MalformedVarInt(MalformedVarInt)
, sinkGet'
, consumeAndHash
, getShortString
, getLazyBytes
) where

import Conduit
import Control.Applicative ((<$>), (<*>))
import Control.Monad (unless)
import Control.Monad.Except (ExceptT(..), throwError, runExceptT)
import Data.Bits (shiftL, shiftR, (.|.), (.&.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.DList as DL
import Data.Maybe (fromMaybe)
import Data.Serialize.Get (runGetPartial, Result(..), Get, getWord8, getBytes, remaining, lookAhead)
import Data.Serialize.Put (Putter, putWord8)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)
import Data.Typeable (Typeable)
import Data.Word (Word32)

import SSync.Hash (HashT, updateS)
import SSync.Util (awaitNonEmpty, dropRight)

-- | Thrown when 'getVarInt' fails to find a terminal byte within 10
-- bytes.  Note: this is _not_ thrown if the end of input is reached
-- before terminal byte is seen; that's a normal failure.
--
-- I hate throwing exceptions from pure code, but Cereal doesn't really
-- give a better way to report machine-inspectable errors.
data MalformedVarInt = MalformedVarInt
                     deriving (Eq, Show, Typeable)

-- $setup
-- The code examples in this module require GHC's `OverloadedStrings`
-- extension:
--
-- >>> :set -XOverloadedStrings

-- | Decode a protobuf-format variable-length unsigned 32-bit integer.  It will accept
-- an encoded 64-bit integer but drop the upper 32 bits.
--
-- >>> runGet getVarInt "\xa9\xb9\x9f\x05"
-- Right 11001001
--
-- >>> runGet getVarInt "\xbd\x84\xb4\x8f\xc5\x29" -- actually 1427303629373
-- 1374487101
--
-- Attempting to consume more than 10 bytes (the maximum a 64-bit integer can
-- produce) causes decoding to fail by throwing a 'MalformedVarInt'.
--
-- >>> return (show $ runGet getVarInt "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00") `catch` (\MalformedVarInt -> return "oops"
-- "oops"
--
-- It is the inverse of putVarInt.
--
-- prop> runGet getVarInt (runPut $ putVarInt x) == Right x
getVarInt :: ExceptT MalformedVarInt Get Word32
getVarInt = next 0 (next 7 (next 14 (next 21 (next 28 (remainder 5))))) 0
-- equivalent to
--    foldr next (remainder 5) [0,7..28] 0
-- but produces nice straight-through core

getIntegralByte :: Get Word32
getIntegralByte = fromIntegral <$> getWord8
{-# INLINE getIntegralByte #-}

next :: Int -> (Word32 -> ExceptT MalformedVarInt Get Word32) -> Word32 -> ExceptT MalformedVarInt Get Word32
next o n v = do
  b <- lift getIntegralByte
  if b .&. 0x80 == 0
    then return $ v .|. (b `shiftL` o)
    else do
      let v' = v .|. ((b .&. 0x7f) `shiftL` o)
      n v'
{-# INLINE next #-}

remainder :: Int -> Word32 -> ExceptT MalformedVarInt Get Word32
remainder 0 _ = throwError MalformedVarInt
remainder n r = do
  b <- lift getIntegralByte
  if b .&. 0x80 == 0
    then return r
    else remainder (n-1) r

-- | Encode a 'Word32' in the protobuf variable-length encoding.
--
-- >>> runPut $ putVarInt 11001001
-- "\169\185\159\ENQ"
--
-- >>> runGet getVarInt $ runPut $ putVarInt 11001001
-- Right 11001001
putVarInt :: Putter Word32
putVarInt i =
  if i < 0x80
  then putWord8 $ fromIntegral i
  else do
    putWord8 $ fromIntegral (i .|. 0x80)
    putVarInt $ i `shiftR` 7

sinkGet' :: (Monad m) => Get r -> Consumer ByteString m (Either String r)
sinkGet' g = go (runGetPartial g)
  where go step =
          awaitNonEmpty >>= \case
            Just chunk ->
              handle step chunk go
            Nothing ->
              handle step BS.empty (const $ return $ Left "Unexpected EOF")
        handle step block recur =
          case step block of
            Done r leftovers -> do
              unless (BS.null leftovers) $ leftover leftovers
              return $ Right r
            Partial cont ->
              recur cont
            Fail msg _ ->
              return $ Left msg

consumeAndHash :: forall m o a e. (Monad m) => e -> ExceptT e Get a -> ExceptT e (HashT (ConduitM ByteString o m)) a
consumeAndHash eofError = ExceptT . (continue . runGetPartial) . runExceptT
  where continue f = do
          bs <- fromMaybe BS.empty <$> lift awaitNonEmpty
          (loop <*> f) bs
        loop :: ByteString -> Result (Either e a) -> HashT (ConduitM ByteString o m) (Either e a)
        loop _ (Fail _ _) = return $ Left eofError
        loop bs (Partial f) = do
          updateS bs
          continue f
        loop _ (Done (Left e) _) =
          return $ Left e
        loop bs (Done (Right r) l) = do
          updateS $ dropRight (BS.length l) bs
          lift $ leftover l
          return $ Right r

getShortString :: Get Text
getShortString = do
  len <- getWord8
  bs <- getBytes $ fromIntegral len
  return $ decodeUtf8 bs

-- Cereal's getLazyByteString actually produces a single chunk; we'll
-- postpone reconstitution until later.  This is named via analogy with
-- getBytes / getByteString; getLazyBytes doesn't copy.
getLazyBytes :: Int -> Get BSL.ByteString
getLazyBytes n = BSL.fromChunks <$> loop n DL.empty
  where loop want acc = do
          available <- remaining
          if available >= want
            then do
              finalAcc <- DL.snoc acc <$> getBytes want
              return $ DL.toList finalAcc
            else do
              acc' <- DL.snoc acc <$> getBytes available
              _ <- lookAhead getWord8 -- ensure the next chunk is available
              loop (want - available) acc'
