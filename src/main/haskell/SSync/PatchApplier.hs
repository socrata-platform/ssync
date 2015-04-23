{-# LANGUAGE LambdaCase, DeriveDataTypeable, ScopedTypeVariables #-}

module SSync.PatchApplier (patchApplier, PatchException(..)) where

import SSync.PatchComputer
import SSync.Hash
import SSync.Util
import SSync.Util.Cereal

import SSync.Constants
import Control.Applicative
import Data.Word
import Control.Monad
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)
import Control.Exception (Exception)
import Data.Typeable (Typeable)

import Conduit
import Data.Serialize.Get (Get, getWord8, getByteString, getLazyByteString)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL

type ChunkProvider m = Int -> Word32 -> m (Maybe ByteString) -- blocksize/number

data PatchException = UnexpectedEOF
                    | ExpectedEOF
                    | MalformedInteger
                    | BlockSizeTooLarge Word32
                    | DataBlockTooLarge Word32
                    | UnknownBlockType Word8
                    | UnknownBlock Word32
                    | UnknownChecksum Text
                    | ChecksumMismatch
                    deriving (Show, Typeable)
instance Exception PatchException

consumeAndHash' :: (MonadThrow m) => Get (Either PatchException a) -> HashT (ConduitM ByteString o m) a
consumeAndHash' = consumeAndHash UnexpectedEOF

patchApplier :: (MonadThrow m) => ChunkProvider m -> Conduit ByteString m ByteString
patchApplier chunkProvider = do
  checksumAlg <- join $ either (const $ throwM UnexpectedEOF) (either throwM return) <$> sinkGet' getChecksumAlgorithm
  expectedDigest <- withHashT checksumAlg $ do
    blockSize <- consumeAndHash' getBlockSize
    when (blockSize > maxBlockSize) $ throwM (BlockSizeTooLarge blockSize)
    let blockSizeI = fromIntegral blockSize -- we know it'll fit in an Int now
    process blockSize (chunkProvider blockSizeI)
    digestS
  actualDigest <- join $ either (const $ throwM UnexpectedEOF) return <$> sinkGet' (getByteString $ BS.length expectedDigest)
  when (expectedDigest /= actualDigest) $ do
    throwM ChecksumMismatch
  awaitNonEmpty >>= \case
    Nothing -> return ()
    Just _ -> throwM ExpectedEOF

process :: (MonadThrow m) => Word32 -> (Word32 -> m (Maybe ByteString)) -> HashT (ConduitM ByteString ByteString m) ()
process blockSize chunkProvider = do
  chunk <- consumeAndHash' (getChunk blockSize)
  case chunk of
    Just (Data bytes) ->
      mapM_ (lift . yield) (BSL.toChunks bytes) >> process blockSize chunkProvider
    Just (Block num) ->
      lift (lift $ chunkProvider num) >>= \case
        Just bytes -> lift (yield bytes) >> process blockSize chunkProvider
        Nothing -> throwM $ UnknownBlock num
    Nothing ->
      return ()

getChecksumAlgorithm :: Get (Either PatchException HashAlgorithm)
getChecksumAlgorithm = do
  l <- getWord8
  bs <- getByteString $ fromIntegral l
  let hashAlgName = decodeUtf8 bs
  case forName hashAlgName of
   Just hashAlg -> return $ Right hashAlg
   Nothing -> return $ Left $ UnknownChecksum hashAlgName

getBlockSize :: Get (Either PatchException Word32)
getBlockSize = do
  getVarInt >>= \case
    Left _ -> return $ Left MalformedInteger
    Right blockSize ->
      if blockSize > maxBlockSize
      then return $ Left $ BlockSizeTooLarge blockSize
      else return $ Right blockSize

getChunk :: Word32 -> Get (Either PatchException (Maybe Chunk))
getChunk blockSize =
  getWord8 >>= \case
    0 ->
      either (const $ Left MalformedInteger) (Right . Just . Block) <$> getVarInt
    1 -> do
      getVarInt >>= \case
        Left MalformedVarInt -> return $ Left MalformedInteger
        Right len ->
          if len > blockSize
          then return $ Left $ DataBlockTooLarge len
          else Right . Just . Data <$> getLazyByteString (fromIntegral len)
    255 ->
      return $ Right Nothing
    other ->
      return $ Left $ UnknownBlockType other

