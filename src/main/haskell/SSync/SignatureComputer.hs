{-# LANGUAGE LambdaCase, ViewPatterns, RankNTypes #-}

module SSync.SignatureComputer (
  produceSignatureTable
, signatureTableSize
, BlockSize
, blockSize
, blockSize'
, blockSizeWord
, HashAlgorithm(..)
, hashForName
, nameForHash
) where

import Conduit
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Monoid ((<>), Sum(..))
import Data.Serialize.Put (runPut, putWord32be, putByteString)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import Data.Word (Word32)

import SSync.Hash
import SSync.Util
import SSync.Util.Cereal
import SSync.Constants
import SSync.BlockSize
import qualified SSync.RollingChecksum as RC

hashForName :: Text -> Maybe HashAlgorithm
hashForName = forName

nameForHash :: HashAlgorithm -> Text
nameForHash = name

produceAndHash :: (Monad m) => HashState -> ConduitM ByteString ByteString m HashState
produceAndHash s0 = execStateC s0 $ awaitForever $ \bs -> do
  updateS bs
  yield bs

produceShortString :: (Monad m) => String -> Producer m ByteString
produceShortString s =
  let bs = encodeUtf8 . T.pack $ s
  in yield $ (BS.singleton . fromIntegral . BS.length $ bs) <> bs

produceVarInt :: (Monad m) => Word32 -> Producer m ByteString
produceVarInt = yield . runPut . putVarInt

-- each signature-block represents as close to 1MB of source data as possible
signatureBlockSizeForBlockSize :: Word32 -> Word32
signatureBlockSizeForBlockSize blockSz = min (1 + ((1024*1024) `div` blockSz)) maxSignatureBlockSize

-- receives blocks of data, produces blocks of signatures
sigs :: (Monad m) => Word32 -> Word32 -> HashAlgorithm -> Conduit ByteString m ByteString
sigs blockSz sigsPerBlock hashAlg = go 0 $ return ()
  where go sigsSoFar sigData =
          if sigsSoFar == sigsPerBlock
          then do
            yield . runPut $ putVarInt sigsSoFar >> sigData
            go 0 $ return ()
          else
            await >>= \case
              Just block -> do
                let weak = RC.value . RC.forBlock rcZero $ block
                    strong = digest . update strongZero $ block
                go (sigsSoFar + 1) (sigData >> putWord32be weak >> putByteString strong)
              Nothing ->
                yield . runPut $ putVarInt sigsSoFar >> sigData
        rcZero = RC.init blockSz
        strongZero = initState hashAlg

produceSignatureTableUnframed :: (Monad m) => HashAlgorithm -> BlockSize -> Conduit ByteString m ByteString
produceSignatureTableUnframed strongHashAlg (blockSizeWord -> blockSz) = do
  let sigBlockSize = signatureBlockSizeForBlockSize blockSz
  produceVarInt blockSz
  produceShortString . T.unpack $ name strongHashAlg
  produceVarInt sigBlockSize
  rechunk (fromIntegral blockSz) $= sigs blockSz sigBlockSize strongHashAlg

produceSignatureTable :: (Monad m) => HashAlgorithm -> HashAlgorithm -> BlockSize -> Conduit ByteString m ByteString
produceSignatureTable checksumAlg strongHashAlg blockSz = do
  produceShortString . T.unpack $ name checksumAlg
  d <- withHashT checksumAlg $ do
    withHashState' $ \hs -> produceSignatureTableUnframed strongHashAlg blockSz $= produceAndHash hs
    digestS
  yield d

-- | Returns the length (in bytes) of the signature table that would be computed
-- for a file of a given length using the given hash algorithms and
-- block size.  This is useful for (e.g.) setting a @Content-Length@ header on
-- an HTTP message containing a signature table.
signatureTableSize :: HashAlgorithm -> HashAlgorithm -> BlockSize -> Integer -> Integer
signatureTableSize checksumAlg strongHashAlg (blockSizeWord -> blockSz) fileLen =
  let hashedPart = do
        produceVarInt blockSz
        produceShortString . T.unpack $ name strongHashAlg
        produceVarInt (signatureBlockSizeForBlockSize blockSz)
      headerFooter = do
        produceShortString . T.unpack $ name checksumAlg
        d <- withHashT checksumAlg $ do
          withHashState' $ \hs -> hashedPart $= produceAndHash hs
          digestS
        yield d
      headerFooterSize = fromIntegral . getSum . runIdentity $ headerFooter $$ foldMapC (Sum . BS.length)
      sigsPerBlock = fromIntegral $ signatureBlockSizeForBlockSize blockSz
      blocks = (fileLen `div` fromIntegral blockSz) + partialBlocks
      partialBlocks = if fileLen `rem` fromIntegral blockSz /= 0
                      then 1
                      else 0
      fullSigBlocks = blocks `div` sigsPerBlock
      leftoverSigs = blocks `rem` sigsPerBlock
      sigSize = fromIntegral $ 4 + digestSize strongHashAlg
      varIntSize i = fromIntegral . getSum . runIdentity $ (produceVarInt (fromIntegral i) $$ foldMapC (Sum . BS.length))
      fullSigBlockLength = fullSigBlocks * (varIntSize sigsPerBlock + sigsPerBlock * sigSize)
      partialSigBlockLength = varIntSize leftoverSigs + leftoverSigs * sigSize
  in headerFooterSize + fullSigBlockLength + partialSigBlockLength

