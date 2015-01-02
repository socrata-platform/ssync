{-# LANGUAGE OverloadedStrings, MultiWayIf #-}

module Request (Request(..), JobHasSig(..), ResponseFeed(..), extractRequest) where

import qualified GHCJS.Types as T
import qualified GHCJS.Foreign as F

import Data.Text (Text)
import qualified Data.Text as TXT

import Control.Monad ((<=<))

import Types

data JSRequest
data JSByteArray
data JSArrayBuffer

foreign import javascript unsafe "$1.data" evData :: T.JSRef JSEvent -> IO (T.JSRef JSRequest)
foreign import javascript unsafe "$1.id" jsReqId :: T.JSRef JSRequest -> IO Int
foreign import javascript unsafe "$1.command" jsReqCommand :: T.JSRef JSRequest -> IO T.JSString
foreign import javascript unsafe "$1.bytes" jsReqBytes :: T.JSRef JSRequest -> IO (T.JSRef JSByteArray)
foreign import javascript unsafe "$1.size" jsReqSize :: T.JSRef JSRequest -> IO Int

foreign import javascript unsafe "$1.buffer" jsByteArrayBuffer :: T.JSRef JSByteArray -> IO (T.JSRef JSArrayBuffer)
foreign import javascript unsafe "$1.byteOffset" jsByteArrayOffset :: T.JSRef JSByteArray -> IO Int
foreign import javascript unsafe "$1.length" jsByteArrayLength :: T.JSRef JSByteArray -> IO Int

data JobHasSig = JobHasSig | JobHasNoSig
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

extractRequest :: T.JSRef JSEvent -> IO Request
extractRequest = deserializeRequest <=< evData

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
