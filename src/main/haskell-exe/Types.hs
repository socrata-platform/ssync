module Types where

import Data.ByteString (ByteString)

data JSEvent

newtype JobId = JobId Int deriving (Show, Eq, Ord)

data DataFeed = DataChunk ByteString -- ^ Send a block of bytes to the worker; the meaning (sig or data) depends on the phase.  Receives 'ChunkAccepted' in response.
              | DataFinished -- ^ Finished sending data, move to the next phase.  Receives either 'SigComplete' or 'PatchChunks'/'PatchComplete' in response
              deriving (Show)
