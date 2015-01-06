{-# LANGUAGE CPP, RankNTypes, ScopedTypeVariables, DeriveDataTypeable, BangPatterns, RecordWildCards, NamedFieldPuns, MultiWayIf, LambdaCase, OverloadedStrings #-}

module SSync.SignatureTable (
  SignatureTable
, smallify
, stBlockSize
, stBlockSizeI
, stStrongAlg
, signatureTableParser
, findBlock
, strongHashComputer
, emptySignature
) where

import SSync.Hash
import qualified SSync.RollingChecksum as RC

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
import Control.Monad.Trans
import Data.Monoid
import qualified Data.Sequence as Seq
import Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString as AP
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.Word (Word8, Word32)
import Control.Monad (unless, when)
import Data.Text.Encoding (decodeUtf8)
import Data.Text (Text)
import Control.Monad.ST

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

shortStringNoCS :: Parser Text
shortStringNoCS = do
  len <- AP.anyWord8
  bs <- AP.take $ fromIntegral len
  return $ decodeUtf8 bs

bytesNoCS :: Int -> Parser ByteString
bytesNoCS = AP.take

varInt :: HashT Parser Word32
varInt = do -- we want to take at most 10 bytes stopping at the first one without the MSB set
  bsRaw <- lift takeVarIntBytes
  update bsRaw
  return $ intify bsRaw

int4 :: HashT Parser Word32
int4 = do -- we want to take at most 10 bytes stopping at the first one without the MSB set
  bs <- lift $ AP.take 4
  update bs
  return $ (fromIntegral (BS.index bs 0) `shiftL` 24) .|. (fromIntegral (BS.index bs 1) `shiftL` 16) .|. (fromIntegral (BS.index bs 2) `shiftL` 8) .|. (fromIntegral (BS.index bs 3))

intify :: ByteString -> Word32
intify bsRaw =
  let bs = BS.take 5 bsRaw
  in BS.foldr (\w acc -> (acc `shiftL` 7) .|. (fromIntegral w .&. 0x7f)) 0 bs

takeVarIntBytes :: Parser ByteString
takeVarIntBytes = AP.scan 0 step >>= checkVAB
  where step :: Int -> Word8 -> Maybe Int
        step (-1) _ = Nothing
        step 9 _ = Nothing
        step n b = if b .&. 0x80 == 0
                   then Just (-1)
                   else Just (n + 1)
        checkVAB bs = do
          if BS.null bs
            then AP.anyWord8 >> return bs -- at EOF; force an error
            else do
              unless (BS.last bs .&. 0x80 == 0) $ fail $ "malformed varint " ++ show bs
              return bs

shortString :: HashT Parser Text
shortString = do
  len <- anyWord8_h
  bs <- take_h $ fromIntegral len
  return $ decodeUtf8 bs

anyWord8_h :: HashT Parser Word8
anyWord8_h = do
  b <- lift AP.anyWord8
  update $ BS.singleton b
  return b

take_h :: Int -> HashT Parser ByteString
take_h n = do
  bs <- lift $ AP.take n
  update bs
  return bs

maxBlockSize :: Word32
maxBlockSize = 10*1024*1024

maxSignatureBlockSize :: Word32
maxSignatureBlockSize = 0xffff

data BlockSpec = BlockSpec { bsEntry :: {-# UNPACK #-} !Word32
                           , bsChecksum :: {-# UNPACK #-} !Word32
                           , bsStrongHash :: !ByteString
                           } deriving (Show)

data SignatureTable = ST { stBlockSize :: {-# UNPACK #-} !Word32 -- \ The same, but sometime's it's more
                         , stBlockSizeI :: {-# UNPACK #-} !Int   -- / convenient to have one than the other
                         , stStrongAlg :: !Text
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
emptySignature = ST { stBlockSize = maxBlockSize
                    , stBlockSizeI = fromIntegral maxBlockSize
                    , stStrongAlg = "MD5"
                    , stBlocks = V.empty
                    , stChecksumLookup = PV.create $ do
                        v <- MPV.new $ 1 `shiftL` 17
                        MPV.set v 0
                        return v
                    }

receiveBlock :: Int -> Word32 -> HashT Parser BlockSpec
receiveBlock hashSize n = do
  check <- int4
  strong <- take_h hashSize
  return $ BlockSpec n check strong

receiveBlocks :: Word32 -> Int -> (Seq.Seq BlockSpec) -> HashT Parser (Seq.Seq BlockSpec)
receiveBlocks maxSigs hashSize !pfx = do
  sigsThisBlock <- varInt
  when (sigsThisBlock > maxSigs) $ lift $ fail "invalid signature block size"
  if sigsThisBlock == 0
    then return pfx
    else do
      let startBlock = fromIntegral $ Seq.length pfx
      blocks <- Seq.fromList `fmap` sequence (map (receiveBlock hashSize) [startBlock .. startBlock + sigsThisBlock - 1])
      if sigsThisBlock == maxSigs
        then
          receiveBlocks maxSigs hashSize (pfx <> blocks)
        else
          return (pfx <> blocks)

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

signatureTableParser :: Parser SignatureTable
signatureTableParser = do
  checksumAlg <- shortStringNoCS
  (st, d) <- withHashM checksumAlg $ do
    blockSize <- varInt
    when (blockSize > maxBlockSize) $ fail "invalid block size"
    strongHashAlg <- shortString
    strongHashSize <- withHashM strongHashAlg digestSize
    sigsPerBlock <- varInt
    when (sigsPerBlock > maxSignatureBlockSize) $ lift $ fail "invalid max signature block size"
    blocksUnsorted <- receiveBlocks sigsPerBlock strongHashSize Seq.empty
    let blocksSorted = V.create $ do
          -- ok, we have a 'Seq BlockSpec' and we want a sorted 'MVector BlockSpec'
          v <- copyToVector blocksUnsorted
          MV.sortBy checksums v
          return v
        blocksIndexed = indexBlocks blocksSorted
    d <- digest
    return (ST blockSize (fromIntegral blockSize) strongHashAlg blocksSorted blocksIndexed, d)
  checksum <- bytesNoCS $ BS.length d
  unless (d == checksum) $ fail "checksum mismatch"
  AP.peekWord8 >>= \case
    Nothing -> return st
    Just _ -> fail "Expected EOF"

hash16 :: Word32 -> Int
hash16 x = 0xffff .&. fromIntegral (x `xor` (x `shiftR` 16))
{-# INLINE hash16 #-}

strongHashComputer :: (Monad m) => SignatureTable -> (HashT m ByteString) -> m ByteString
strongHashComputer st op = withHashM (stStrongAlg st) op

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
