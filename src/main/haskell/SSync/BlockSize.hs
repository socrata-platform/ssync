module SSync.BlockSize (
  BlockSize
, blockSize
, blockSize'
, blockSizeWord
) where

import Data.Maybe (fromMaybe)
import Data.Word (Word32)

import SSync.Constants (maxBlockSize)

newtype BlockSize = BlockSize Word32 deriving (Eq, Ord)

blockSize :: Int -> Maybe BlockSize
blockSize i | i < 1 = Nothing
            | i > fromIntegral maxBlockSize = Nothing
            | otherwise = Just (BlockSize $ fromIntegral i)

blockSize' :: Int -> BlockSize
blockSize' = fromMaybe (error "block size out of bounds") . blockSize

blockSizeWord :: BlockSize -> Word32
blockSizeWord (BlockSize w) = w

instance Show BlockSize where
  show (BlockSize bs) = "blockSize' " ++ show bs

instance Bounded BlockSize where
  minBound = BlockSize 1
  maxBound = BlockSize maxBlockSize

instance Enum BlockSize where
  toEnum = blockSize'
  fromEnum (BlockSize bs) = fromIntegral bs
