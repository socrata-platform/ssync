module SSync.Util (rechunk, encodeVarInt, awaitNonEmpty, dropRight, orThrow) where

import Control.Exception (Exception)
import Control.Monad ((<=<))
import Control.Monad.Catch (MonadThrow, throwM)
import Control.Monad.Except (ExceptT, runExceptT)
import Data.Bits (shiftR, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import Data.Monoid ((<>))
import Data.Word (Word32)

import SSync.Util.Conduit

encodeVarInt :: Word32 -> BS.Builder
encodeVarInt i = go i
  where go value =
          if value < 128
          then BS.word8 (fromIntegral value)
          else BS.word8 (fromIntegral value .|. 0x80) <> go (value `shiftR` 7)

dropRight :: Int -> ByteString -> ByteString
dropRight n bs = BS.take (BS.length bs - n) bs

orThrow :: (MonadThrow m, Exception e) => ExceptT e m r -> m r
orThrow = either throwM return <=< runExceptT
