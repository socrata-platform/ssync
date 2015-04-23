 {-# LANGUAGE CPP, RankNTypes, ScopedTypeVariables, DeriveDataTypeable, BangPatterns, RecordWildCards, NamedFieldPuns, MultiWayIf, LambdaCase, OverloadedStrings #-}

module SSync.SignatureTable (
  SignatureTable
, smallify
, stBlockSize
, stBlockSizeI
, stStrongAlg
, findBlock
, strongHashComputer
, emptySignature
, emptySignature'
, consumeSignatureTable
, SignatureTableException(..)
) where

import SSync.Hash
import SSync.Constants
import qualified SSync.RollingChecksum as RC
import SSync.Util (awaitNonEmpty)
import SSync.Util.Cereal (consumeAndHash)

import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import qualified Data.Vector.Algorithms.Intro as MV
#ifdef STUPID_VECT
import qualified SSync.JSVector as PV
import qualified SSync.JSVectorM as MPV
#else
import qualified Data.Vector.Primitive as PV
import qualified Data.Vector.Primitive.Mutable as MPV
#endif
import Data.Foldable
import Data.Bits
import Data.Monoid
import Control.Applicative ((<$>))
import qualified Data.Sequence as Seq
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.Word (Word32)
import Control.Monad (join, unless)
import Data.Text.Encoding (decodeUtf8)
import Data.Text (Text)
import Control.Monad.ST
import Data.Serialize.Get (Get, getWord8, getWord32be, getByteString)
import SSync.Util.Cereal (getVarInt, MalformedVarInt(MalformedVarInt), sinkGet')
import Conduit
import Data.Typeable (Typeable)
import Control.Exception (Exception)

#ifdef TRACING
import qualified Debug.Trace as DT

t :: (Show a) => String -> a -> a
t label x = DT.trace (label ++ " : " ++ show x) x
#endif

#ifdef STUPID_VECT
type PVI = PV.Vector
#else
type PVI = PV.Vector Int
#endif

data SignatureTableException = UnexpectedEOF
                               -- ^ The signature file was truncated.
                             | MalformedInteger
                               -- ^ A variable-length number was longer than 10 bytes.
                             | UnknownChecksum Text
                               -- ^ The checksum algorithm specified in the signature file is unknown.
                             | UnknownStrongHash Text
                               -- ^ The strong hash algorithm specified in the signature file is unknown.
                             | ChecksumMismatch
                               -- ^ The signature file had an invalid checksum.
                             | ExpectedEOF
                               -- ^ There was data after the signature file's checksum.
                             | InvalidBlockSize Word32
                               -- ^ The block size specified in the signature file was too large.
                             | InvalidMaxSignatureBlockSize Word32
                               -- ^ The maximum size of a signature block in the signature file was too large.
                             | InvalidSignatureBlockSize Word32
                               -- ^ The size of a signature block was larger than the file's specified maximum.
                             deriving (Show, Typeable)
instance Exception SignatureTableException

getShortString :: Get Text
getShortString = do
  len <- getWord8
  bs <- getByteString $ fromIntegral len
  return $ decodeUtf8 bs

data BlockSpec = BlockSpec { bsEntry :: {-# UNPACK #-} !Word32
                           , bsChecksum :: {-# UNPACK #-} !Word32
                           , bsStrongHash :: !ByteString
                           } deriving (Show)

data SignatureTable = ST { stBlockSize :: {-# UNPACK #-} !Word32 -- \ The same, but sometime's it's more
                         , stBlockSizeI :: {-# UNPACK #-} !Int   -- / convenient to have one than the other
                         , stStrongAlg :: !HashAlgorithm
                         , stBlocks :: !(V.Vector BlockSpec)
                         -- ^ BlockSpecs ordered by (weak16 checksum,
                         -- checksum, entry)
                         , stChecksumLookup :: !PVI
                         -- ^ pairs of indices into stBlocks for each
                         -- possible hash16(checksum) value,
                         -- indicating the start and end of the range
                         -- in 'stBlocks'.
                         } deriving (Show)

smallify :: SignatureTable -> SignatureTable
smallify ST{..} =
  ST { stBlockSize
     , stBlockSizeI
     , stStrongAlg
     , stBlocks = V.empty
     , stChecksumLookup = PV.empty
     }

emptySignature :: SignatureTable
emptySignature = emptySignature' maxBlockSize

emptySignature' :: Word32 -> SignatureTable
emptySignature' bs = ST { stBlockSize = bs
                        , stBlockSizeI = fromIntegral bs
                        , stStrongAlg = MD5
                        , stBlocks = V.empty
                        , stChecksumLookup = PV.create $ do
                            v <- MPV.new $ 1 `shiftL` 17
                            MPV.set v 0
                            return v
                       }

getBlock :: Int -> Word32 -> Get BlockSpec
getBlock hashSize n = do
  check <- getWord32be
  strong <- getByteString hashSize
  return $ BlockSpec n check strong

getBlocks :: Word32 -> Int -> Seq.Seq BlockSpec -> Get (Either SignatureTableException (Seq.Seq BlockSpec))
getBlocks maxSigs hashSize !pfx = do
  getVarInt >>= \case
    Left MalformedVarInt -> return $ Left MalformedInteger
    Right sigsThisBlock ->
      if sigsThisBlock > maxSigs
      then return $ Left (InvalidSignatureBlockSize sigsThisBlock)
      else if sigsThisBlock == 0
        then return $ Right pfx
        else do
          let startBlock = fromIntegral $ Seq.length pfx
          blocks <- Seq.fromList `fmap` sequence (map (getBlock hashSize) [startBlock .. startBlock + sigsThisBlock - 1])
          if sigsThisBlock == maxSigs
            then getBlocks maxSigs hashSize (pfx <> blocks)
            else return $ Right (pfx <> blocks)

copyToVector :: Seq.Seq a -> forall s. ST s (MV.STVector s a)
copyToVector xs = do
  v <- MV.new (Seq.length xs)
  forM_ [0 .. Seq.length xs - 1] $ \i ->
    MV.write v i (Seq.index xs i)
  return v

checksums :: BlockSpec -> BlockSpec -> Ordering
checksums (BlockSpec n1 rc1 _) (BlockSpec n2 rc2 _) = do
  case compare (hash16 rc1) (hash16 rc2) of
    EQ ->
      case compare rc1 rc2 of
        EQ -> compare n1 n2
        other -> other
    other ->
      other

indexBlocks :: V.Vector BlockSpec -> PVI
indexBlocks blocks =
  PV.create $ do
    v <- MPV.new (1 `shiftL` 17)
    MPV.set v 0
    -- scan across 'blocks' partitioning it by hash16
    let end = V.length blocks
        loop pos =
          if pos /= end
             then scanFrom pos
             else return v
        scanFrom pos = do
          let h16 = hash16 $ bsChecksum $ blocks V.! pos
              chunk = V.takeWhile (\b -> hash16 (bsChecksum b) == h16) $ V.drop pos blocks
              afterChunk = pos + V.length chunk
          MPV.write v (2 * fromIntegral h16) pos
          MPV.write v (1 + 2 * fromIntegral h16) afterChunk
          loop afterChunk
    loop 0

getSignatureTable :: Get (Either SignatureTableException SignatureTable)
getSignatureTable = do
  getVarInt >>= \case
    Left MalformedVarInt -> return $ Left MalformedInteger
    Right blockSize ->
      if blockSize > maxBlockSize
      then return $ Left $ InvalidBlockSize blockSize
      else do
        strongHashAlgName <- getShortString
        case forName strongHashAlgName of
          Nothing ->
            return $ Left (UnknownStrongHash strongHashAlgName)
          Just strongHashAlg -> do
            let strongHashSize = digestSize strongHashAlg
            getVarInt >>= \case
              Left MalformedVarInt -> return $ Left MalformedInteger
              Right sigsPerBlock ->
                if sigsPerBlock > maxSignatureBlockSize
                then return $ Left (InvalidMaxSignatureBlockSize sigsPerBlock)
                else do
                  getBlocks sigsPerBlock strongHashSize Seq.empty >>= \case
                    Left err -> return $ Left err
                    Right blocksUnsorted -> do
                      let blocksSorted = V.create $ do
                            v <- copyToVector blocksUnsorted
                            MV.sortBy checksums v
                            return v
                          blocksIndexed = indexBlocks blocksSorted
                      return $ Right (ST blockSize (fromIntegral blockSize) strongHashAlg blocksSorted blocksIndexed)

consumeAndHash' :: (MonadThrow m) => Get (Either SignatureTableException a) -> HashT (ConduitM ByteString o m) a
consumeAndHash' = consumeAndHash UnexpectedEOF

consumeSignatureTable :: MonadThrow m => Consumer ByteString m SignatureTable
consumeSignatureTable = do
  checksumAlgName <- join $ either (const $ throwM UnexpectedEOF) return <$> sinkGet' getShortString
  case forName checksumAlgName of
   Nothing ->
     throwM $ UnknownChecksum checksumAlgName
   Just checksumAlg -> do
     (st, d) <- withHashT checksumAlg $ do
       st <- consumeAndHash' getSignatureTable
       d <- digestS
       return (st, d)
     checksum <- join $ either (const $ throwM UnexpectedEOF) return <$> sinkGet' (getByteString $ BS.length d)
     unless (d == checksum) $ throwM ChecksumMismatch
     awaitNonEmpty >>= \case
       Nothing -> return st
       Just _ -> throwM ExpectedEOF

hash16 :: Word32 -> Int
hash16 x = 0xffff .&. fromIntegral (x `xor` (x `shiftR` 16))
{-# INLINE hash16 #-}

strongHashComputer :: (Monad m) => SignatureTable -> (HashT m ByteString) -> m ByteString
strongHashComputer st op = withHashT (stStrongAlg st) op

-- | Finds the preexisting block corresponding to the block with the
-- given weak and strong hashes.  Note: this does not evaluate the
-- strong hash unless the weak hash matches.
findBlock :: SignatureTable -> RC.RollingChecksum -> ByteString -> Maybe Word32
findBlock st@ST{..} rc strongHash =
  let rcv = RC.value rc
      h16 = hash16 rcv
      potentialsListIdx = h16 `shiftL` 1
      start = stChecksumLookup PV.! potentialsListIdx
      end = stChecksumLookup PV.! (potentialsListIdx + 1)
  in if start == end
     then Nothing
     else let p = findFirstWeakEntry st start end rcv
          in if p == -1
             then Nothing
             else findStrongHashMatch st p end strongHash rcv
{-# INLINE findBlock #-}

linearProbeThreshold :: Int
linearProbeThreshold = 8

findFirstWeakEntry :: SignatureTable -> Int -> Int -> Word32 -> Int
findFirstWeakEntry st@ST{..} start end target = go start end
  where go p e =
          if e - p < linearProbeThreshold
          then linearProbe st p e target
          else let m = (p + e) `shiftR` 1 -- Not quite safe in general, but if + can overflow we have other problems
                   h = bsChecksum $ stBlocks V.! m
               in if | h < target -> go (m+1) e
                     | h > target -> go p m
                     | otherwise -> go p (m+1) -- found one, but it might not be the _first_ one

linearProbe :: SignatureTable -> Int -> Int -> Word32 -> Int
linearProbe ST{..} start end target = go start
  where go p | p == end = -1
             | bsChecksum (stBlocks V.! p) == target = p
             | otherwise = go (p+1)

findStrongHashMatch :: SignatureTable -> Int -> Int -> ByteString -> Word32 -> Maybe Word32
findStrongHashMatch ST{..} start end target weakTarget = go start
  where go p =
          if p == end
          then Nothing
          else let block = stBlocks V.! p
               in if | bsChecksum block /= weakTarget -> Nothing
                     | bsStrongHash block == target -> Just $ bsEntry block
                     | otherwise -> go (p+1)
