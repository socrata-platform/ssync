{-# LANGUAGE RankNTypes, GADTs, BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables, LambdaCase, NamedFieldPuns, OverloadedStrings #-}

module SSync.Patch where

import qualified Data.DList as DL
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Conduit
import Data.Word
import Data.Monoid
import Control.Monad.Identity

import SSync.SignatureTable
import qualified SSync.WeakHash as WH
import qualified SSync.DataQueue as DQ
import SSync.Hash (HashT)

data Chunk = Block Word32
           | Data BSL.ByteString
           | End
             deriving (Show)

data SizedBuilder = SizedBuilder (DL.DList ByteString) {-# UNPACK #-} !Int
sbEmpty :: SizedBuilder
sbEmpty = SizedBuilder DL.empty 0

sizedByteString :: ByteString -> SizedBuilder
sizedByteString bs = SizedBuilder (DL.singleton bs) (BS.length bs)

builderLength :: SizedBuilder -> Int
builderLength (SizedBuilder _ l) = l

builder :: SizedBuilder -> (DL.DList ByteString)
builder (SizedBuilder b _) = b

atLeastBlockSizeOrEnd :: (Monad m) => Int -> ByteString -> ConduitM ByteString a m ByteString
atLeastBlockSizeOrEnd target pfx = go (BS.byteString pfx) (BS.length pfx)
  where go sb !len =
          if target <= len
            then return $ BSL.toStrict $ BS.toLazyByteString sb
            else await >>= \case
              Nothing -> return $ BSL.toStrict $ BS.toLazyByteString sb
              Just bs -> go (sb <> BS.byteString bs) (len + BS.length bs)

awaitNonEmpty :: (Monad m) => Consumer ByteString m (Maybe ByteString)
awaitNonEmpty = await >>= \case
  Just bs | BS.null bs -> awaitNonEmpty
          | otherwise -> return $ Just bs
  Nothing -> return Nothing

patchComputer :: (Monad m) => ParsedST -> Conduit ByteString m Chunk
patchComputer pst = go
  where go = do
          initBS <- atLeastBlockSizeOrEnd blockSizeI ""
          sb <- fromChunk initBS sbEmpty
          yieldData sb
          yield End
        fromChunk initBS sb =
          if (BS.null initBS)
          then return sb
          else
            let initQ = DQ.create initBS 0 (min blockSizeI (BS.length initBS) - 1)
                wh0 = WH.forBlock (WH.init blockSize) initBS
            in loop wh0 initQ sb
        blockSize = pstBlockSize pst
        blockSizeI = pstBlockSizeI pst
        hashComputer :: HashT Identity ByteString -> ByteString
        hashComputer = runIdentity . strongHashComputer pst
        loop wh q sb =
          -- DQ.validate "loop 1" q
          -- Is there a block at the current position in the queue?
          case findBlock pst wh (hashComputer $ DQ.hashBlock q) of
            Just b -> do
              -- Yes; add the data we've skipped, send the block ID
              -- itself, and then start over.
              blockFound q b sb
            Nothing -> do
              -- no; move forward 1 byte (which might entail dropping a block from the front
              -- of the queue; if that happens, it's data).
              attemptSlide wh q sb
        blockFound q b sb = do
          sb' <- addData blockSizeI (DQ.beforeBlock q) sb
          yieldBlock b sb'
          nextBS <- atLeastBlockSizeOrEnd blockSizeI $ DQ.afterBlock q
          fromChunk nextBS sbEmpty
        attemptSlide wh q sb =
          case DQ.slide q of
            Just (dropped, !q') ->
              -- DQ.validate "loop 2" q'
              let !wh' = WH.roll wh (DQ.firstByteOfBlock q) (DQ.lastByteOfBlock q')
              in case dropped of
                Just bs ->
                  addData blockSizeI bs sb >>= loop wh' q'
                Nothing ->
                  loop wh' q' sb
            Nothing ->
              -- can't even do that; we need more from upstream
              fetchMore wh q sb
        fetchMore wh q sb = do
          awaitNonEmpty >>= \case
            Just nextBlock ->
              -- ok good.  By adding that block we might drop one from the queue;
              -- if so, send it as data.
              let (dropped, !q') = DQ.addBlock q nextBlock
                  !wh' = WH.roll wh (DQ.firstByteOfBlock q) (DQ.lastByteOfBlock q')
              in case dropped of
                Just bs ->
                  addData blockSizeI bs sb >>= loop wh' q'
                Nothing ->
                  loop wh' q' sb
            Nothing ->
              -- Nothing!  Ok, we're in the home stretch now.
              finish wh q sb
        finish wh q sb = do
          -- sliding just failed, so let's slide off.  Again, this can
          -- cause a block to be dropped.
          case DQ.slideOff q of
            (dropped, Just !q') -> do
              -- DQ.validate "finish" q'
              sb' <- case dropped of
                Just bs ->
                  addData blockSizeI bs sb
                Nothing ->
                  return sb
              let !wh' = WH.roll wh (DQ.firstByteOfBlock q) 0
              case findBlock pst wh' (hashComputer $ DQ.hashBlock q') of
                Just b -> do
                  sb'' <- addData blockSizeI (DQ.beforeBlock q') sb'
                  yieldBlock b sb''
                  return sbEmpty
                Nothing ->
                  finish wh' q' sb'
            -- Done!
            (Just dropped, Nothing) -> do
              addData blockSizeI dropped sb
            (Nothing, Nothing) ->
              return sb

yieldBlock :: (Monad m) => Word32 -> SizedBuilder -> Producer m Chunk
yieldBlock i sb = do
  yieldData sb
  yield $ Block i

yieldData :: (Monad m) => SizedBuilder -> Producer m Chunk
yieldData (SizedBuilder pendingL pendingS) =
  unless (pendingS == 0) $
    yield $ Data $ BSL.fromChunks $ DL.toList pendingL

addData :: (Monad m) => Int ->ByteString -> SizedBuilder -> ConduitM a Chunk m SizedBuilder
addData blockSize bs sb@(SizedBuilder pendingL pendingS) =
  if BS.null bs
  then return sb
  else
    let newSize = pendingS + BS.length bs
        newList = DL.snoc pendingL bs
    in if newSize < blockSize
      then return $ SizedBuilder newList newSize
      else let bs64 = fromIntegral blockSize
               loop converted = do
                 yield $ Data $ BSL.take bs64 converted
                 let remaining = BSL.drop bs64 converted
                 if BSL.length remaining < bs64
                   then return $ sizedByteString $ BSL.toStrict remaining
                   else loop remaining
           in loop $ BSL.fromChunks $ DL.toList newList
