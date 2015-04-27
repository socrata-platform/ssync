{-# LANGUAGE ScopedTypeVariables, LambdaCase, BangPatterns #-}

module SSync.Util (rechunk, encodeVarInt, awaitNonEmpty, dropRight, orThrow) where

import Conduit
import Control.Exception (Exception)
import Control.Monad (unless, (<=<))
import Control.Monad.Except (ExceptT, runExceptT)
import Data.Bits (shiftR, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.DList as DL
import Data.Monoid (mconcat, (<>))
import Data.Word (Word32)

rechunk :: forall m. (Monad m) => Int -> Conduit BS.ByteString m ByteString
rechunk targetSize = go DL.empty 0
  where go :: DL.DList BS.ByteString -> Int -> Conduit BS.ByteString m ByteString
        go pfx !count =
          await >>= \case
            Just bs -> do
              let allBytes = DL.snoc pfx bs
                  here = BS.length bs
                  !total = count + here
              if total >= targetSize
                then send (asStrict allBytes)
                else go allBytes total
            Nothing -> do
              let bs = asStrict pfx
              unless (BS.null bs) $ yield bs
        asStrict = mconcat . DL.toList
        send bs = do
          let (toSend, toKeep) = BS.splitAt targetSize bs
          yield toSend
          if BS.length toKeep >= targetSize
            then send toKeep
            else go (DL.singleton $ toKeep) (fromIntegral $ BS.length toKeep)

encodeVarInt :: Word32 -> BS.Builder
encodeVarInt i = go i
  where go value =
          if value < 128
          then BS.word8 (fromIntegral value)
          else BS.word8 (fromIntegral value .|. 0x80) <> go (value `shiftR` 7)

awaitNonEmpty :: (Monad m) => Consumer ByteString m (Maybe ByteString)
awaitNonEmpty = await >>= \case
  Just bs | BS.null bs -> awaitNonEmpty
  other -> return other

dropRight :: Int -> ByteString -> ByteString
dropRight n bs = BS.take (BS.length bs - n) bs

orThrow :: (MonadThrow m, Exception e) => ExceptT e m r -> m r
orThrow = either throwM return <=< runExceptT
