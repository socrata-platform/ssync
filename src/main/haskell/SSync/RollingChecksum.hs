{-# LANGUAGE RecordWildCards, NamedFieldPuns, BangPatterns #-}
module SSync.RollingChecksum (
  RollingChecksum
, init
, value
, value16
, forBlock
, roll
) where

import Prelude hiding (init)

import Data.Bits (shiftR, shiftL, xor, (.&.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.Word (Word32, Word8)
import Data.Int (Int8)

data RollingChecksum = RC { _rcBlockSize :: {-# UNPACK #-} !Word32
                          , _rc :: {-# UNPACK #-} !Word32
                          } deriving (Show)

init :: Word32 -> RollingChecksum
init blockSize = RC blockSize 0

value :: RollingChecksum -> Word32
value RC{..} = _rc
{-# INLINE value #-}

value16 :: RollingChecksum -> Word32
value16 RC{..} = (_rc `xor` (_rc `shiftR` 16)) .&. 0xffff
{-# INLINE value16 #-}

-- | Computes the checksum of the block at the start of the
-- 'ByteString'.  To check a block somewhere other than the start,
-- 'BS.drop' the front off of it it yourself.
forBlock :: RollingChecksum -> ByteString -> RollingChecksum
forBlock RC{_rcBlockSize} bs =
  let a = aSum _rcBlockSize bs .&. 0xffff
      b = bSum _rcBlockSize bs .&. 0xffff
  in RC{ _rc = a + (b `shiftL` 16), .. }

-- | This is a due to a stupid bug in the Java implementation; Java bytes are
-- signed, and so when they're extended to word-size they need to fill
-- the upper bits appropriately.  The bug does not affect the
-- "rolling" property of the rolling checksum, which is why it wasn't
-- detected until now.
signExtend :: Word8 -> Word32
signExtend = (fromIntegral :: Int8 -> Word32) . (fromIntegral :: Word8 -> Int8)

aSum :: Word32 -> ByteString -> Word32
aSum blockSize bs = go 0 0
  where limit = BS.length bs `min` fromIntegral blockSize
        go !i !a | i < limit = go (i+1) (a + signExtend (BS.unsafeIndex bs i))
                 | otherwise = a

bSum :: Word32 -> ByteString -> Word32
bSum blockSize bs = go 0 0
  where limit = BS.length bs `min` fromIntegral blockSize
        go :: Int -> Word32 -> Word32
        go !i !b | i < limit = go (i+1) (b + (blockSize - fromIntegral i) * signExtend (BS.unsafeIndex bs i))
                 | otherwise = b

roll :: RollingChecksum -> Word8 -> Word8 -> RollingChecksum
roll RC{..} oldByte newByte =
  let ob = signExtend oldByte
      a = ((_rc .&. 0xffff) - ob + signExtend newByte) .&. 0xffff
      b = ((_rc `shiftR` 16) - _rcBlockSize * ob + a) .&. 0xffff
  in RC { _rc = a + (b `shiftL` 16), .. }
{-# INLINE roll #-}
