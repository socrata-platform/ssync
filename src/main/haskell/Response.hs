{-# LANGUAGE OverloadedStrings #-}
module Response (Response(..), JSResponse, serializeResponse) where

import qualified GHCJS.Types as T
import qualified GHCJS.Foreign as F
import GHC.Ptr (Ptr)

import Data.Text (Text)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.Conduit.Attoparsec (ParseError, errorMessage)

import Types

data JSResponse
data JSByteArray

foreign import javascript unsafe "$r = { id : $1, command : $2 } " jsRespNoData :: Int -> T.JSString -> IO (T.JSRef JSResponse)
foreign import javascript unsafe "$r = { id : $1, command : $2, bytes: $3 } " jsRespData :: Int -> T.JSString -> T.JSRef JSByteArray -> IO (T.JSRef JSResponse)
foreign import javascript unsafe "$r = { id : $1, command : $2, text: $3 } " jsRespText :: Int -> T.JSString -> T.JSString -> IO (T.JSRef JSResponse)

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

foreign import javascript unsafe "new Uint8Array($1_1.buf, $1_2, $2)" extractBA :: Ptr a -> Int -> IO (T.JSRef JSByteArray)

uint8ArrayOfByteString :: ByteString -> IO (T.JSRef JSByteArray)
uint8ArrayOfByteString bs =
  BS.unsafeUseAsCString bs $ \ptr ->
    extractBA ptr (BS.length bs)

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
