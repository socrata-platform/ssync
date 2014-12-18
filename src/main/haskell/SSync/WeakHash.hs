{-# LANGUAGE RecordWildCards, NamedFieldPuns, BangPatterns #-}
module SSync.WeakHash (
  WeakHash
, init
, value
, value16
, forBlock
, roll
) where

import Prelude hiding (init)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.ByteString (ByteString)
import Data.Word
import Data.Bits

data WeakHash = WeakHash { _whBlockSize :: {-# UNPACK #-} !Word32
                         , _wh :: {-# UNPACK #-} !Word32
                         -- , _whB :: {-# UNPACK #-} !Word32
                         } deriving (Show)

init :: Word32 -> WeakHash
init blockSize = WeakHash blockSize 0

value :: WeakHash -> Word32
value WeakHash{..} = _wh --A + (_whB `shiftL` 16)
{-# INLINE value #-}

value16 :: WeakHash -> Word32
value16 WeakHash{..} = (_wh `xor` (_wh `shiftR` 16)) .&. 0xffff
{-# INLINE value16 #-}

-- | Computes the checksum of the block at the start of the
-- 'ByteString'.  To check a block somewhere other than the start,
-- 'BS.drop' the front off of it it yourself.
forBlock :: WeakHash -> ByteString -> WeakHash
forBlock WeakHash{_whBlockSize} bs =
  let a = aSum _whBlockSize bs .&. 0xffff
      b = bSum _whBlockSize bs .&. 0xffff
  in WeakHash{ _wh = a + (b `shiftL` 16), .. }

aSum :: Word32 -> ByteString -> Word32
aSum blockSize bs = go 0 0
  where limit = BS.length bs `min` fromIntegral blockSize
        go !i !a | i < limit = go (i+1) (a + fromIntegral (BS.unsafeIndex bs i))
                 | otherwise = a

bSum :: Word32 -> ByteString -> Word32
bSum blockSize bs = go 0 0
  where limit = BS.length bs `min` fromIntegral blockSize
        go :: Int -> Word32 -> Word32
        go !i !b | i < limit = go (i+1) (b + (blockSize - fromIntegral i) * fromIntegral (BS.unsafeIndex bs i))
                 | otherwise = b

roll :: WeakHash -> Word8 -> Word8 -> WeakHash
roll WeakHash{..} oldByte newByte =
  let ob = fromIntegral oldByte
      a = ((_wh .&. 0xffff) - ob + fromIntegral newByte) .&. 0xffff
      b = ((_wh `shiftR` 16) - _whBlockSize * ob + a) .&. 0xffff
  in WeakHash { _wh = a + (b `shiftL` 16), .. }
{-# INLINE roll #-}
