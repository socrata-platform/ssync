{-# LANGUAGE CPP #-}

#ifdef __GHCJS__

{-# LANGUAGE OverloadedStrings, RecursiveDo, MultiWayIf, RankNTypes, LambdaCase, BangPatterns, ScopedTypeVariables #-}

module Main (main) where

import Control.Exception
import Control.Concurrent hiding (yield)
import Control.Concurrent.STM

import qualified GHCJS.Types as T
import qualified GHCJS.Foreign as F
import qualified Data.Text as TXT
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Map.Strict as Map
import Control.Monad (join, (<=<), unless)
import qualified Data.DList as DL
import Data.Word (Word32)
import Data.Int (Int64)
import Data.Bits
import Data.Monoid

import Conduit
import Data.Conduit.Attoparsec

import SSync.SignatureTable
import SSync.Patch
import SSync.Hash

import TSBQueue
import Request
import Response
import Types

foreign import javascript unsafe "onmessage = $1" onmessage :: T.JSFun (T.JSRef JSEvent -> IO b) -> IO ()
foreign import javascript unsafe "postMessage($1)" postMessage :: T.JSRef JSResponse -> IO ()

type TaskMap = Map.Map JobId TaskInfo

data TaskInfo = TaskInfo { taskJobId :: JobId
                         , taskTargetChunkSize :: Int
                         , taskDataBox :: TSBQueue DataFeed
                         , taskResponseQueue :: TQueue ResponseFeed -- queue because it needs to never block
                         , taskThread :: ThreadId
                         , taskMap :: TVar TaskMap
                         }

initialTaskInfo :: JobId -> Int -> TSBQueue DataFeed -> TQueue ResponseFeed -> ThreadId -> TVar TaskMap -> TaskInfo
initialTaskInfo = TaskInfo

sendResponse :: Response -> IO ()
sendResponse = postMessage <=< serializeResponse

raiseIE :: TaskInfo ->SomeException -> IO ()
raiseIE ti e = case fromException e of
                Just ThreadKilled ->
                  return ()
                Just other ->
                  sendResponse $ InternalError (taskJobId ti) (TXT.pack $ show other)
                Nothing ->
                  sendResponse $ InternalError (taskJobId ti) (TXT.pack $ show e)

recvLoop :: TaskInfo -> Source IO ByteString
recvLoop ti = do
  msg <- lift $ atomically $ readTSBQueue (taskDataBox ti)
  case msg of
   DataChunk bs -> do
     yield bs
     recvLoop ti
   DataFinished -> do
     return ()

gatheringSignature :: TaskInfo -> IO SignatureTable
gatheringSignature ti = do
  stE <- recvLoop ti $$ sinkParserEither signatureTableParser
  case stE of
   Right st -> do
     sendResponse $ SigComplete (taskJobId ti)
     return st
   Left err -> do
     sendResponse $ SigError (taskJobId ti) err
     throwIO ThreadKilled

varInt :: Word32 -> BS.Builder
varInt i = go i
  where go value =
          if value < 128
          then BS.word8 (fromIntegral value)
          else BS.word8 (fromIntegral value .|. 0x80) <> go (value `shiftR` 7)

serializeChunk :: Chunk -> BSL.ByteString
serializeChunk (Block i) = BS.toLazyByteString (BS.word8 0 <> varInt i)
serializeChunk (Data d) = BS.toLazyByteString (BS.word8 1 <> varInt (fromIntegral $ BSL.length d) <> BS.lazyByteString d)
serializeChunk End = BSL.singleton 255

frame :: (Monad m) => Word32 -> Conduit BSL.ByteString m BSL.ByteString
frame blockSize = do
  yield "\003MD5"
  checksum <- withHashM "MD5" $ do
    let targetSizeBytes = BS.toLazyByteString $ varInt blockSize
    mapM_ update $ BSL.toChunks targetSizeBytes
    lift $ yield targetSizeBytes
    let loop =
          lift await >>= \case
            Just bs -> do
              mapM_ update $ BSL.toChunks bs
              lift $ yield bs
              loop
            Nothing ->
              digest
    loop
  yield $ BSL.fromStrict checksum

rechunk :: forall m. (MonadIO m) => Int -> Conduit BSL.ByteString m ByteString
rechunk targetSize = go DL.empty 0
  where go :: DL.DList BSL.ByteString -> Int64 -> Conduit BSL.ByteString m ByteString
        go pfx !count =
          await >>= \case
            Just bs -> do
              let allBytes = DL.snoc pfx bs
                  here = BSL.length bs
                  !total = count + here
              if total >= targetSize64
                then send (asStrict allBytes)
                else go allBytes total
            Nothing -> do
              let bs = asStrict pfx
              unless (BS.null bs) $ yield bs
        asStrict = mconcat . concatMap BSL.toChunks . DL.toList
        targetSize64 = fromIntegral targetSize :: Int64
        send bs = do
          let (toSend, toKeep) = BS.splitAt targetSize bs
          yield toSend
          if BS.length toKeep >= targetSize
            then send toKeep
            else go (DL.singleton $ BSL.fromStrict toKeep) (fromIntegral $ BS.length toKeep)

acceptPatchChunkAck :: TaskInfo -> IO ()
acceptPatchChunkAck ti = do
  resp <- atomically $ readTQueue (taskResponseQueue ti)
  case resp of -- just here to warn if another case gets added to ResponseFeed
   ResponseAccepted -> return ()

sendChunks :: TaskInfo -> Sink ByteString IO ()
sendChunks ti = await >>= \case
  Just firstChunk -> do
    -- in order to maximize parallelism, we want to delay the
    -- taskResponseQueue read by one chunk.  Thus the await-then-
    -- awaitForever structure.
    lift $ sendResponse $ PatchChunk (taskJobId ti) firstChunk
    awaitForever $ \bytes -> lift $ do
      acceptPatchChunkAck ti
      sendResponse $ PatchChunk (taskJobId ti) bytes
    lift $ acceptPatchChunkAck ti
  Nothing ->
    return ()

computingPatch :: TaskInfo -> SignatureTable -> IO ()
computingPatch ti st = do
  recvLoop ti $$ patchComputer st $= mapC serializeChunk $= frame (stBlockSize st) $= rechunk (taskTargetChunkSize ti) $= sendChunks ti
  sendResponse $ PatchComplete (taskJobId ti)

nullaryPatch :: TaskInfo -> SignatureTable -> IO ()
nullaryPatch ti st = do
  recvLoop ti $$ mapC BSL.fromStrict $= rechunk (stBlockSizeI st) $= (mapC (serializeChunk . Data . BSL.fromStrict) >> yield (serializeChunk End)) $= frame (stBlockSize st) $= rechunk (taskTargetChunkSize ti) $= sendChunks ti
  sendResponse $ PatchComplete (taskJobId ti)

worker :: (forall a. IO a -> IO a) -> TaskInfo -> IO () -> IO ()
worker unmask ti op = handle (raiseIE ti) (unmask op) `finally` (atomically $ removeJob ti)

stage0 :: TMVar Bool -> IO () -> IO ()
stage0 v act = do
  goAhead <- atomically $ takeTMVar v
  if goAhead then act else return ()

sizeDataFeed :: DataFeed -> Int
sizeDataFeed (DataChunk bs) = BS.length bs
sizeDataFeed DataFinished = 0

createJob :: JobId -> Int -> JobHasSig -> TVar TaskMap -> IO ()
createJob i targetSize hasSig reg = mask_ $ mdo
  guard <- newEmptyTMVarIO
  workerQueue <- newTSBQueueIO targetSize sizeDataFeed
  responseQueue <- newTQueueIO
  let taskInfo = initialTaskInfo i targetSize workerQueue responseQueue potentialWorker reg
      w = case hasSig of
        JobHasSig -> gatheringSignature taskInfo >>= computingPatch taskInfo
        JobHasNoSig -> nullaryPatch taskInfo emptySignature
  potentialWorker <- forkIOWithUnmask (\u -> stage0 guard $ worker u taskInfo w)
  join $ atomically $ do
    m <- readTVar reg
    case Map.lookup i m of
      Nothing -> do
        putTMVar guard True
        writeTVar reg $ Map.insert i taskInfo m
        return $ sendResponse (JobCreated i)
      Just _ -> do
        putTMVar guard False
        return $ alreadyExistsJob i

jobMessage :: JobId -> DataFeed -> TVar TaskMap -> IO ()
jobMessage jid req reg =
  join $ atomically $ do
    m <- readTVar reg
    case Map.lookup jid m of
      Just taskInfo -> do
        writeTSBQueue (taskDataBox taskInfo) req
        case req of
         DataChunk _ -> return $ sendResponse $ ChunkAccepted jid
         DataFinished -> return $ return ()
      Nothing ->
        return $ noSuchJob jid

responseMessage :: JobId -> ResponseFeed -> TVar TaskMap -> IO ()
responseMessage jid r reg = do
  join $ atomically $ do
    m <- readTVar reg
    case Map.lookup jid m of
     Just taskInfo -> do
       writeTQueue (taskResponseQueue taskInfo) r
       return $ return ()
     Nothing ->
       return $ noSuchJob jid

noSuchJob, alreadyExistsJob :: JobId -> IO ()
noSuchJob jid = sendResponse (BadSequencing jid "job does not exist")
alreadyExistsJob jid = sendResponse (BadSequencing jid "job already exists")

removeJob :: TaskInfo -> STM ()
removeJob ti = do
  m <- readTVar (taskMap ti)
  writeTVar (taskMap ti) $ Map.delete (taskJobId ti) m

killJob :: JobId -> TVar TaskMap -> IO ()
killJob jid reg = join $ atomically $ do
  m <- readTVar reg
  case Map.lookup jid m of
   Just ti -> do
     removeJob ti
     drainTSBQueue (taskDataBox ti)
     return $ do
       -- putStrLn $ "Killing job " ++ show jid ++ ", which exists"
       killThread (taskThread ti)
   Nothing ->
     return $ do
       -- putStrLn $ "Not killing job " ++ show jid ++ ", which did not exist"
       return () -- killing a nonexistant task is ok

main :: IO ()
main = do
  registrations <- newTVarIO Map.empty
  callback <- F.asyncCallback1 F.AlwaysRetain $ \ev -> do
    req <- extractRequest ev
    -- putStrLn $ "got message from client: " ++ show req
    case req of
     NewJob i targetSize hasSig ->
       createJob i targetSize hasSig registrations
     TerminateJob i ->
       killJob i registrations
     DataJob i df ->
       jobMessage i df registrations
     ResponseJob i r ->
       responseMessage i r registrations
  onmessage callback

#else

{-# LANGUAGE OverloadedStrings #-}
module Main where

import System.Environment

import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
import Control.Monad.IO.Class
import Data.Conduit
import Conduit

import SSync.SignatureTable
import SSync.SignatureComputer
import SSync.PatchComputer
import SSync.PatchApplier
import SSync.Hash
import qualified Filesystem.Path.CurrentOS as FP

main :: IO ()
main = do
  -- print $ runGet getVarInt "\xa9\xb9\x9f\x05"
  -- print $ runGet getVarInt "\xbd\x84\xb4\x8f\xc5\x29"

  let toOverwrite = BS.concat (replicate 100001 "abcdefghijklmnopqrs")
      with = "abcdeqabcdefghijglksfghijdfjglkdfjsgkldjfgabcdeklgfdsjgldfjfghiabcdefghijklmnopqrs"
      chunks _ "" = []
      chunks n bs = let (a, b) = BS.splitAt n bs
                    in a : chunks n b
      chunkFinder bs num = let start = (fromIntegral bs) * num
                           in if start < fromIntegral (BS.length toOverwrite)
                              then do
                                let chunk = BS.take bs $ BS.drop (fromIntegral start) toOverwrite
                                putStrLn $ "returning chunk #" ++ show num ++ ": " ++ show chunk
                                return $ Just chunk
                              else do
                                putStrLn "chunk not found :("
                                return Nothing
      sigProducer = produceSignatureTable MD5 MD5 (blockSize' 10)
  putStrLn "Begin: signature table chunk sizes"
  yieldMany (chunks 200 toOverwrite) $$ sigProducer $= awaitForever (liftIO . print . BS.length)
  putStrLn "End: signatureTable chunk sizes"
  st <- yieldMany (chunks 200 toOverwrite) $$ sigProducer $= consumeSignatureTable
  putStrLn "Begin: patch"
  yield with $$ patchComputer' st $= awaitForever (liftIO . print)
  putStrLn "End: patch"
  xs <- yieldMany (chunks 1024 with) $$ patchComputer st $= patchApplier chunkFinder $= sinkList
  print xs

  args <- getArgs
  case args of
   [dat, sig] ->
     go dat sig
   _ -> do
     argv0 <- getProgName
     putStrLn $ "Usage: " ++ argv0 ++ " DATFILE SIGFILE"

go :: String -> String -> IO ()
go dat sig = do
  l <- runResourceT $ sourceFile (FP.decodeString sig) $$ consumeSignatureTable
  -- print $ smallify l
  if True
    then do
      runResourceT $ sourceFile (FP.decodeString dat) $$ patchComputer' l $= sinkNull -- (awaitForever $ liftIO . printChunk)
    else do
      bs <- BSL.readFile dat
      mapM_ yield (BSL.toChunks bs) $$ {- (awaitForever $ \b -> do { liftIO $ print $ BS.length b; yield b }) $= -} patchComputer' l $= (awaitForever $ liftIO . printChunk)

printChunk :: (MonadIO m) => Chunk -> m ()
printChunk (Data bytes) =  liftIO $ putStrLn $ "Data " ++ show (BSL.length bytes)
printChunk other = liftIO $ print other

#endif
