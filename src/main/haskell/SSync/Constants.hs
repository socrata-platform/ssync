module SSync.Constants (
  maxBlockSize
, maxSignatureBlockSize
) where

import Data.Word (Word32)

maxBlockSize :: Word32
maxBlockSize = 10*1024*1024

maxSignatureBlockSize :: Word32
maxSignatureBlockSize = 0xffff

