{-# LANGUAGE RankNTypes, GADTs, BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables, LambdaCase, NamedFieldPuns, OverloadedStrings #-}

module SSync.Patch where

import qualified Data.DList as DL
import Prelude hiding (mapM_)
import Data.Foldable (mapM_)
import Control.Applicative
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as BSL
import Conduit
import Data.Word
import Data.Monoid
import Control.Monad.State.Strict (StateT)
import Control.Monad.Reader (ReaderT)
import Control.Monad.Reader.Class (ask)
import Control.Monad.State.Strict (get)
import Control.Monad (unless)
import Control.Monad.State.Strict (put)
import Data.Time

import SSync.SignatureTable
import qualified SSync.WeakHash as WH
import qualified SSync.DataQueue as DQ
import SSync.Hash (HashT)

data Chunk = Block Word32
           | Data BSL.ByteString
           | End
             deriving (Show)

type ChunkifyT m r = ConduitM ByteString Chunk (StateT SizedBuilder m) r

data SizedBuilder = SizedBuilder (DL.DList ByteString) {-# UNPACK #-} !Int
sbEmpty :: SizedBuilder
sbEmpty = SizedBuilder DL.empty 0

sizedByteString :: ByteString -> SizedBuilder
sizedByteString bs = SizedBuilder (DL.singleton bs) (BS.length bs)

builderLength :: SizedBuilder -> Int
builderLength (SizedBuilder _ l) = l

builder :: SizedBuilder -> (DL.DList ByteString)
builder (SizedBuilder b _) = b

atLeastBlockSizeOrEnd :: (MonadIO m) => Int -> ByteString -> ChunkifyT m ByteString
atLeastBlockSizeOrEnd target pfx = go (BS.byteString pfx) (BS.length pfx)
  where go builder !len = do
          if target <= len
            then return $ BSL.toStrict $ BS.toLazyByteString builder
            else await >>= \case
              Nothing -> return $ BSL.toStrict $ BS.toLazyByteString builder
              Just bs -> go (builder <> BS.byteString bs) (len + BS.length bs)

awaitNonEmpty :: (Monad m) => ChunkifyT m (Maybe ByteString)
awaitNonEmpty = await >>= \case
  Just bs | BS.null bs -> awaitNonEmpty
          | otherwise -> return $ Just bs
  Nothing -> return Nothing

patchComputer :: (MonadIO m) => ParsedST -> Conduit ByteString m Chunk
patchComputer pst = evalStateC sbEmpty $ go
  where go = do
          liftIO $ putStrLn "Go!"
          initBS <- atLeastBlockSizeOrEnd blockSizeI ""
          liftIO $ putStrLn "No really!"
          fromChunk initBS
          yieldData
          yield End
        fromChunk initBS = do
          unless (BS.null initBS) $ do
            let initQ = DQ.create initBS 0 (min blockSizeI (BS.length initBS) - 1)
                wh0 = WH.forBlock (WH.init blockSize) initBS
            loop wh0 initQ
        blockSize = pstBlockSize pst
        blockSizeI = pstBlockSizeI pst
        hashComputer :: HashT Identity ByteString -> ByteString
        hashComputer = runIdentity . strongHashComputer pst
        loop wh q = do
          -- DQ.validate "loop 1" q
          -- Is there a block at the current position in the queue?
          case findBlock pst wh (hashComputer $ DQ.hashBlock q) of
            Just b -> do
              -- Yes; add the data we've skipped, send the block ID
              -- itself, and then start over.
              blockFound q b
            Nothing -> do
              -- no; move forward 1 byte (which might entail dropping a block from the front
              -- of the queue; if that happens, it's data).
              attemptSlide wh q
        blockFound q b = do
          addData blockSizeI $ DQ.beforeBlock q
          yieldBlock b
          nextBS <- atLeastBlockSizeOrEnd blockSizeI $ DQ.afterBlock q
          fromChunk nextBS
        attemptSlide wh q =
          case DQ.slide q of
            Just (dropped, q') -> do
              -- DQ.validate "loop 2" q'
              mapM_ (addData blockSizeI) dropped
              let wh' = WH.roll wh (DQ.firstByteOfBlock q) (DQ.lastByteOfBlock q')
              loop wh' q'
            Nothing ->
              -- can't even do that; we need more from upstream
              fetchMore wh q
        fetchMore wh q = do
          liftIO $ putStrLn "Hello!"
          awaitNonEmpty >>= \case
            Just nextBlock -> do
              -- ok good.  By adding that block we might drop one from the queue;
              -- if so, send it as data.
              let (dropped, q') = DQ.addBlock q nextBlock
              -- DQ.validate "loop 3" q'
              mapM_ (addData blockSizeI) dropped
              let wh' = WH.roll wh (DQ.firstByteOfBlock q) (DQ.lastByteOfBlock q')
              loop wh' q'
            Nothing ->
              -- Nothing!  Ok, we're in the home stretch now.
              finish wh q
        finish wh q = do
          -- sliding just failed, so let's slide off.  Again, this can
          -- cause a block to be dropped.
          case DQ.slideOff q of
            (dropped, Just q') -> do
              -- DQ.validate "finish" q'
              mapM_ (addData blockSizeI) dropped
              let wh' = WH.roll wh (DQ.firstByteOfBlock q) 0
              case findBlock pst wh' (hashComputer $ DQ.hashBlock q') of
                Just b -> do
                  addData blockSizeI $ DQ.beforeBlock q'
                  yieldBlock b
                Nothing ->
                  finish wh' q'
            -- Done!
            (dropped, Nothing) -> do
              mapM_ (addData blockSizeI) dropped

yieldBlock :: (MonadIO m) => Word32 -> ChunkifyT m ()
yieldBlock i = do
  yieldData
  yield $ Block i

yieldData :: (MonadIO m) => ChunkifyT m ()
yieldData = do
  SizedBuilder pendingL pendingS <- get
  unless (pendingS == 0) $ do
    yield $ Data $ BSL.fromChunks $ DL.toList pendingL
    put sbEmpty

time :: (MonadIO m) => String -> m a -> m a
time label op = do
  start <- liftIO getCurrentTime
  res <- op
  end <- liftIO getCurrentTime
  let diff = diffUTCTime end start
  liftIO $ putStrLn $ label ++ " : " ++ show diff
  return res

timez :: (MonadIO m) => String -> m a -> m a
timez _ op = op

addData :: (MonadIO m) => Int ->ByteString -> ChunkifyT m ()
addData blockSize bs = do
  SizedBuilder pendingL pendingS <- get
  let newSize = pendingS + BS.length bs
      newList = DL.snoc pendingL bs
  if newSize < blockSize
    then put $ SizedBuilder newList newSize
    else let bs64 = fromIntegral blockSize
             loop converted = do
               yield $ Data $ BSL.take bs64 converted
               let remaining = BSL.drop bs64 converted
               if BSL.length remaining < bs64
                 then put $ sizedByteString $ BSL.toStrict remaining
                 else loop remaining
         in loop $ BSL.fromChunks $ DL.toList newList
