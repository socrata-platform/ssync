{-# LANGUAGE CPP #-}

#ifdef __GHCJS__

{-# LANGUAGE OverloadedStrings, RecursiveDo, MultiWayIf, RankNTypes, LambdaCase, BangPatterns, ScopedTypeVariables #-}

module Main (main) where

import GHC.Exts( IsString(..) )

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

import SSync.SignatureTable
import SSync.PatchComputer
import SSync.Util.Conduit (rechunk)

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
  stE <- try $ recvLoop ti $$ consumeSignatureTable
  case stE of
   Right st -> do
     sendResponse $ SigComplete (taskJobId ti)
     return st
   Left err -> do
     sendResponse $ SigError (taskJobId ti) err
     throwIO ThreadKilled

acceptPatchChunkAck :: TaskInfo -> IO ()
acceptPatchChunkAck ti = do
  resp <- atomically $ readTQueue (taskResponseQueue ti)
  case resp of -- just here to warn if another case gets added to ResponseFeed
   ResponseAccepted -> return ()

sendChunks :: TaskInfo -> Sink ByteString IO ()
sendChunks ti = rechunk (taskTargetChunkSize ti) $= sendloop
  where sendloop = await >>= \case
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
  recvLoop ti $$ patchComputer st $= sendChunks ti
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

createJob :: JobId -> Int -> TVar TaskMap -> IO ()
createJob i targetSize reg = mask_ $ mdo
  guard <- newEmptyTMVarIO
  workerQueue <- newTSBQueueIO targetSize sizeDataFeed
  responseQueue <- newTQueueIO
  let taskInfo = initialTaskInfo i targetSize workerQueue responseQueue potentialWorker reg
      w = gatheringSignature taskInfo >>= computingPatch taskInfo
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
     NewJob i targetSize ->
       createJob i targetSize registrations
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

import qualified Filesystem.Path.CurrentOS as FP
import qualified Data.ByteString.Lazy as BSL
import Conduit

import SSync.SignatureTable
import SSync.PatchComputer

tap :: (MonadIO m, Show b) => (a -> b) -> Conduit a m a
tap f = awaitForever $ \a -> do
  liftIO . putStrLn . show $ f a
  yield a

main :: IO ()
main = do
  putStrLn "gnu!"
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
  if True
    then do
      runResourceT $ sourceFile (FP.decodeString dat) $$ patchComputer' l $= (awaitForever $ liftIO . printChunk)
    else do
      bs <- BSL.readFile dat
      mapM_ yield (BSL.toChunks bs) $$ {- (awaitForever $ \b -> do { liftIO $ print $ BS.length b; yield b }) $= -} patchComputer' l $= (awaitForever $ liftIO . printChunk)

printChunk :: (MonadIO m) => Chunk -> m ()
printChunk (Data bytes) =  liftIO $ putStrLn $ "Data " ++ show (BSL.length bytes)
printChunk other = liftIO $ print other

#endif
