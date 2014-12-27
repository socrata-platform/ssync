{-# LANGUAGE CPP #-}

#ifdef __GHCJS__

{-# LANGUAGE OverloadedStrings, RecursiveDo, MultiWayIf, RankNTypes, LambdaCase, BangPatterns, ScopedTypeVariables #-}

module Main (main) where

import Control.Applicative
import Control.Exception
import Control.Concurrent hiding (yield)
import Control.Concurrent.STM

import qualified GHCJS.Types as T
import qualified GHCJS.Foreign as F
import qualified Data.Text as TXT
import Data.Text (Text)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Unsafe as BS
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

import GHC.Ptr

data JSEvent
data JSRequest
data JSResponse
data JSByteArray
data JSArrayBuffer

foreign import javascript unsafe "onmessage = $1" onmessage :: T.JSFun (T.JSRef JSEvent -> IO b) -> IO ()
foreign import javascript unsafe "postMessage($1)" postMessage :: T.JSRef JSResponse -> IO ()
foreign import javascript unsafe "$1.data" evData :: T.JSRef JSEvent -> IO (T.JSRef JSRequest)
foreign import javascript unsafe "$1.id" jsReqId :: T.JSRef JSRequest -> IO Int
foreign import javascript unsafe "$1.command" jsReqCommand :: T.JSRef JSRequest -> IO T.JSString
foreign import javascript unsafe "$1.bytes" jsReqBytes :: T.JSRef JSRequest -> IO (T.JSRef JSByteArray)
foreign import javascript unsafe "$1.size" jsReqSize :: T.JSRef JSRequest -> IO Int
foreign import javascript unsafe "$r = { id : $1, command : $2 } " jsRespNoData :: Int -> T.JSString -> IO (T.JSRef JSResponse)
foreign import javascript unsafe "$r = { id : $1, command : $2, bytes: $3 } " jsRespData :: Int -> T.JSString -> T.JSRef JSByteArray -> IO (T.JSRef JSResponse)
foreign import javascript unsafe "$r = { id : $1, command : $2, text: $3 } " jsRespText :: Int -> T.JSString -> T.JSString -> IO (T.JSRef JSResponse)

foreign import javascript unsafe "$1.buffer" jsByteArrayBuffer :: T.JSRef JSByteArray -> IO (T.JSRef JSArrayBuffer)
foreign import javascript unsafe "$1.byteOffset" jsByteArrayOffset :: T.JSRef JSByteArray -> IO Int
foreign import javascript unsafe "$1.length" jsByteArrayLength :: T.JSRef JSByteArray -> IO Int

newtype JobId = JobId Int deriving (Show, Eq, Ord)

data DataFeed = DataChunk ByteString -- ^ Send a block of bytes to the worker; the meaning (sig or data) depends on the phase.  Receives 'ChunkAccepted' in response.
              | DataFinished -- ^ Finished sending data, move to the next phase.  Receives either 'SigComplete' or 'PatchChunks'/'PatchComplete' in response
              deriving (Show)

data ResponseFeed = ResponseAccepted -- ^ Sent from JS in response to a PatchChunk message
                  deriving (Show)

data Request = NewJob JobId Int JobHasSig -- ^ Establish a job context; 'JobCreated' is sent in response.
             | TerminateJob JobId -- ^ Terminate a job context prematurely; nothing is sent back.
             | DataJob JobId DataFeed -- ^ See 'DataFeed'
             | ResponseJob JobId ResponseFeed -- ^ See 'ResponseFeed'
             deriving (Show)

reqNEW, reqNEWFAKESIG, reqTERMINATE, reqDATA, reqDATADONE, reqRESPACCEPTED :: Text
reqNEW = "NEW"
reqNEWFAKESIG = "NEWFAKESIG"
reqTERMINATE = "TERM"
reqDATA = "DATA"
reqDATADONE = "DONE"
reqRESPACCEPTED = "CHUNKACCEPTED"

data TSBQueue a = TSBQueue Int (TVar Int) (a -> Int) (TQueue (a, Int))
newTSBQueueIO :: Int -> (a -> Int) -> IO (TSBQueue a)
newTSBQueueIO maxTotalSize sizer = do
  underlying <- newTQueueIO
  current <- newTVarIO 0
  return $ TSBQueue maxTotalSize current sizer underlying

writeTSBQueue :: TSBQueue a -> a -> STM ()
writeTSBQueue (TSBQueue limit current sizer underlying) a = do
  c <- readTVar current
  let size = sizer a
      new = c + size
  unless (new <= limit) retry
  writeTVar current new
  writeTQueue underlying (a, size)

readTSBQueue :: TSBQueue a -> STM a
readTSBQueue (TSBQueue _ current _ underlying) = do
  (res, size) <- readTQueue underlying
  modifyTVar current (subtract size)
  return res

data JobHasSig = JobHasSig | JobHasNoSig
               deriving (Show)

data Response = JobCreated JobId -- ^ Sent in response to a job context; phase is now "gathering signature" or "computing patch" depending on whether the job had a signature.
              | ChunkAccepted JobId -- ^ Sent in response to 'DataChunk'
              | SigComplete JobId -- ^ Sent in response to 'DataFinished' in the "gathering signature" phase.  Phase is now "computing patch".
              | SigError JobId ParseError -- ^ Sent at any time during "gathering signature"; the job is terminated.
              | PatchChunk JobId ByteString -- ^ Sent (asynchronously!) during "computing patch".
              | PatchComplete JobId -- ^ Sent in response to 'DataFinished' in the "computing patch" phase.  Phase is now "terminated".
              | BadSequencing JobId Text -- ^ Sent in response to any unexpected message
              | InternalError JobId Text -- ^ Sent in response to a crash
              deriving (Show)

respCREATED, respCHUNKACCEPTED, respSIGCOMPLETE, respSIGERROR, respPATCHCHUNK, respPATCHCOMPLETE, respBADSEQ, respINTERNALERROR :: T.JSString
respCREATED = "CREATED"
respCHUNKACCEPTED = "CHUNKACCEPTED"
respSIGCOMPLETE = "SIGCOMPLETE"
respSIGERROR = "SIGERROR"
respPATCHCHUNK = "CHUNK"
respPATCHCOMPLETE = "COMPLETE"
respBADSEQ = "BADSEQ"
respINTERNALERROR = "ERRORERRORDOESNOTCOMPUTE"

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

serializeResponse :: Response -> IO (T.JSRef JSResponse)
serializeResponse resp =
  case resp of
    JobCreated (JobId jid) -> jsRespNoData jid respCREATED
    ChunkAccepted (JobId jid) -> jsRespNoData jid respCHUNKACCEPTED
    SigComplete (JobId jid) -> jsRespNoData jid respSIGCOMPLETE
    SigError (JobId jid) msg -> jsRespText jid respSIGERROR $ F.toJSString $ errorMessage msg
    PatchChunk (JobId jid) bs -> uint8ArrayOfByteString bs >>= jsRespData jid respPATCHCHUNK
    PatchComplete (JobId jid) -> jsRespNoData jid respPATCHCOMPLETE
    BadSequencing (JobId jid) msg -> jsRespText jid respBADSEQ $ F.toJSString msg
    InternalError (JobId jid) msg -> jsRespText jid respINTERNALERROR $ F.toJSString msg

deserializeRequest :: T.JSRef JSRequest -> IO Request
deserializeRequest jsReq = do
  jid <- JobId `fmap` jsReqId jsReq
  cmd <- F.fromJSString `fmap` jsReqCommand jsReq
  if | cmd == reqDATA -> do
         rawByteArray <- jsReqBytes jsReq
         buffer <- jsByteArrayBuffer rawByteArray
         offset <- jsByteArrayOffset rawByteArray
         len <- jsByteArrayLength rawByteArray
         bs <- F.bufferByteString offset len buffer
         return $ DataJob jid $ DataChunk bs
     | cmd == reqNEW -> do
         size <- jsReqSize jsReq
         return $ NewJob jid size JobHasSig
     | cmd == reqNEWFAKESIG -> do
         size <- jsReqSize jsReq
         return $ NewJob jid size JobHasNoSig
     | cmd == reqTERMINATE ->
         return $ TerminateJob jid
     | cmd == reqDATADONE -> do
         return $ DataJob jid $ DataFinished
     | cmd == reqRESPACCEPTED -> do
         return $ ResponseJob jid ResponseAccepted
     | otherwise ->
         error $ "unknown command " ++ TXT.unpack cmd

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
        JobHasNoSig -> computingPatch taskInfo emptySignature
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

drainTSBQueue :: TSBQueue a -> STM ()
drainTSBQueue q = do
  e <- (Just <$> readTSBQueue q) <|> return Nothing
  case e of
   Just _ -> drainTSBQueue q
   Nothing -> return ()

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

foreign import javascript unsafe "new Uint8Array($1_1.buf, $1_2, $2)" extractBA :: Ptr a -> Int -> IO (T.JSRef JSByteArray)

uint8ArrayOfByteString :: ByteString -> IO (T.JSRef JSByteArray)
uint8ArrayOfByteString bs =
  BS.unsafeUseAsCString bs $ \ptr ->
    extractBA ptr (BS.length bs)

main :: IO ()
main = do
  registrations <- newTVarIO Map.empty
  callback <- F.asyncCallback1 F.AlwaysRetain $ \ev -> do
    req <- evData ev >>= deserializeRequest
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

import qualified Data.Attoparsec.ByteString as AP
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Control.Monad.IO.Class
import Data.Conduit

import SSync.SignatureTable
import SSync.Patch


main :: IO ()
main = do
  let sig = "twocol-large.ssig"
      dat = "/home/robertm/di2-data/twocol/twocol.csv"
  l <- -- AP.parseOnly signatureTableParser `fmap` BS.readFile sig >>= \(Right l) -> return l
    return emptySignature
  print $ smallify l
  if True
    then do
      bs <- BS.readFile dat
      yield bs $$ patchComputer l $= (awaitForever $ liftIO . printChunk)
    else do
      bs <- BSL.readFile dat
      mapM_ yield (BSL.toChunks bs) $$ {- (awaitForever $ \b -> do { liftIO $ print $ BS.length b; yield b }) $= -} patchComputer l $= (awaitForever $ liftIO . printChunk)

printChunk :: (MonadIO m) => Chunk -> m ()
printChunk (Data bytes) =  liftIO $ putStrLn $ "Data " ++ show (BSL.length bytes)
printChunk other = liftIO $ print other

#endif
