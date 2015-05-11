{-# LANGUAGE ScopedTypeVariables, LambdaCase, BangPatterns #-}

module SSync.Util.Conduit (
  awaitNonEmpty
, rechunk
) where

import Conduit
import Control.Monad (unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Monoid (mconcat)
import qualified Data.DList as DL

awaitNonEmpty :: (Monad m) => Consumer ByteString m (Maybe ByteString)
awaitNonEmpty = await >>= \case
  Just bs | BS.null bs -> awaitNonEmpty
  other -> return other

rechunk :: forall m. (Monad m) => Int -> Conduit ByteString m ByteString
rechunk targetSize = go DL.empty 0
  where go :: DL.DList ByteString -> Int -> Conduit ByteString m ByteString
        go pfx !count =
          awaitNonEmpty >>= \case
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
