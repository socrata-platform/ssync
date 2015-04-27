{-# LANGUAGE LambdaCase, DeriveDataTypeable, ScopedTypeVariables #-}

module SSync.PatchApplier (patchApplier, PatchException(..)) where

import Conduit
import Control.Applicative ((<$>))
import Control.Exception (Exception)
import Control.Monad (when)
import Control.Monad.Except (ExceptT(..), withExceptT, throwError, runExceptT)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Serialize.Get (Get, getWord8, getBytes)
import Data.Text (Text)
import Data.Typeable (Typeable)
import Data.Word (Word8, Word32)

import SSync.Constants
import SSync.Hash
import SSync.PatchComputer
import SSync.Util
import SSync.Util.Cereal hiding (consumeAndHash, getVarInt)
import qualified SSync.Util.Cereal as SC

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

consumeAndHash :: (Monad m) => ExceptT PatchException Get a -> ExceptT PatchException (HashT (ConduitM ByteString o m)) a
consumeAndHash = SC.consumeAndHash UnexpectedEOF

getVarInt :: ExceptT PatchException Get Word32
getVarInt = withExceptT fixup SC.getVarInt
  where fixup MalformedVarInt = MalformedInteger

fixupEOF :: String -> PatchException
fixupEOF _ = UnexpectedEOF

withHashTEx :: (Monad m) => HashAlgorithm -> ExceptT e (HashT m) a -> ExceptT e m a
withHashTEx ha = ExceptT . withHashT ha . runExceptT

patchApplier :: (MonadThrow m) => ChunkProvider m -> Conduit ByteString m ByteString
patchApplier chunkProvider = orThrow $ do
  checksumAlgName <- withExceptT fixupEOF $ ExceptT (sinkGet' getShortString)
  checksumAlg <- maybe (throwError $ UnknownChecksum checksumAlgName) return (forName checksumAlgName)
  expectedDigest <- withHashTEx checksumAlg $ do
    blockSize <- consumeAndHash getBlockSize
    when (blockSize > maxBlockSize) $ throwError (BlockSizeTooLarge blockSize)
    let blockSizeI = fromIntegral blockSize -- we know it'll fit in an Int now
    process blockSize (chunkProvider blockSizeI)
    digestS
  actualDigest <- withExceptT fixupEOF $ ExceptT (sinkGet' $ getBytes $ BS.length expectedDigest)
  when (expectedDigest /= actualDigest) $ do
    throwError ChecksumMismatch
  lift awaitNonEmpty >>= \case
    Nothing -> return ()
    Just _ -> throwError ExpectedEOF

process :: (Monad m) => Word32 -> (Word32 -> m (Maybe ByteString)) -> ExceptT PatchException (HashT (ConduitM ByteString ByteString m)) ()
process blockSize chunkProvider =
  consumeAndHash (getChunk blockSize) >>= \case
    Just (Data bytes) ->
      mapM_ (lift . lift . yield) (BSL.toChunks bytes) >> process blockSize chunkProvider
    Just (Block num) ->
      (lift . lift . lift) (chunkProvider num) >>= \case
        Just bytes -> (lift . lift . yield) bytes >> process blockSize chunkProvider
        Nothing -> throwError $ UnknownBlock num
    Nothing ->
      return ()

getBlockSize :: ExceptT PatchException Get Word32
getBlockSize = do
  blockSize <- getVarInt
  when (blockSize > maxBlockSize) $ throwError (BlockSizeTooLarge blockSize)
  return blockSize

getChunk :: Word32 -> ExceptT PatchException Get (Maybe Chunk)
getChunk blockSize =
  lift getWord8 >>= \case
    0 ->
      Just . Block <$> getVarInt
    1 -> do
      len <- getVarInt
      when (len > blockSize) $ throwError (DataBlockTooLarge len)
      Just . Data <$> lift (getLazyBytes (fromIntegral len))
    255 ->
      return Nothing
    other ->
      throwError $ UnknownBlockType other

