{-# LANGUAGE TemplateHaskell #-}

module SSync.BlockSize (
  BlockSize
, blockSize
, mkBlockSize
, blockSizeWord
) where

import Data.Maybe (fromMaybe)
import Data.Word (Word32)
import Language.Haskell.TH

import SSync.Constants (maxBlockSize)

newtype BlockSize = BlockSize Word32 deriving (Eq, Ord)

blockSize :: Int -> Maybe BlockSize
blockSize i | i < 1 = Nothing
            | i > fromIntegral maxBlockSize = Nothing
            | otherwise = Just (BlockSize $ fromIntegral i)

mkBlockSize :: Int -> Q Exp
mkBlockSize i =
  case blockSize i of
    Nothing -> error ("Invalid block size: " ++ show i)
    Just _ -> [| BlockSize (fromIntegral i) |]

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
