module SSync.Chunk (
  Chunk(..)
) where

import qualified Data.ByteString.Lazy as BSL
import Data.Word(Word32)

data Chunk = Block Word32
           | Data BSL.ByteString
             deriving (Show)
