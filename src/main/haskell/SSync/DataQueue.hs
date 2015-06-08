{-# LANGUAGE RankNTypes, MultiWayIf #-}

module SSync.DataQueue (
  DataQueue
, create
, validate
, firstByteOfBlock
, lastByteOfBlock
, afterBlock
, beforeBlock
, slide
, slideOff
, atEnd
, addBlock
, hashBlock
) where

{-

For the scanning process, we're interested in the following operations:
 * "Give me the first byte of the current block and the last byte of the
    block offset by 1"
 * "Hash the current block"
 * "Give me all the bytes after the current block"

So we will represent this as the following:
 * If the current block is contained within a single chunk, a constructor
     SingleChunk ByteString firstIndex lastIndex
 * If the current block is NOT contained within a single chunk, a constructor
     PolyChunk ByteString firstIndex possiblyEmptyChunkQueue ByteString lastIndex
Note that both indices are INCLUSIVE, and in the latter case it is relative to
the start of the last chunk.

-}

import Control.Monad (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word (Word8)

import SSync.SimpleQueue
import SSync.Hash

-- None of these bytestrings may be empty!
data DataQueue = SingleChunk !ByteString !Int !Int
               | PolyChunk !ByteString !Int !(SimpleQueue ByteString) !ByteString !Int
               deriving (Show)

-- We'll always initially populate in single-chunk mode.  This incurs
-- a little copying, but not too much.
create :: ByteString -> Int -> Int -> DataQueue
create bs a b
  | BS.null bs = error "create: empty bytestring"
  | a < 0 || a >= BS.length bs = error "create: first position out of range"
  | b < a || b >= BS.length bs = error "create: second position out of range"
  | otherwise = SingleChunk bs a b

validate :: (Monad m) => String -> DataQueue -> m ()
validate label (SingleChunk bs hd tl)
  | hd > tl = error $ label ++ ": single: hd > tl"
  | hd < 0 = error $ label ++ ": single: hd < 0"
  | tl >= BS.length bs = error $ label ++ ": single: tl > len"
  | otherwise = return ()
validate label (PolyChunk bs1 hd _ bs2 tl)
  | hd < 0 = error $ label ++ ": poly: hd < 0"
  | hd >= BS.length bs1 = error $ label ++ ": poly: hd > len"
  | tl < 0 = error $ label ++ ": poly: tl < 0"
  | tl >= BS.length bs2 = error $ label ++ ": poly: tl > len"
  | otherwise = return ()

firstByteOfBlock :: DataQueue -> Word8
firstByteOfBlock (SingleChunk bs hd _) = BS.index bs hd
firstByteOfBlock (PolyChunk bs hd _ _ _) = BS.index bs hd

lastByteOfBlock :: DataQueue -> Word8
lastByteOfBlock (SingleChunk bs _ tl) = BS.index bs tl
lastByteOfBlock (PolyChunk _ _ _ bs tl) = BS.index bs tl

atEnd :: DataQueue -> Bool
atEnd (SingleChunk bs _ tl) = atEnd' bs tl
atEnd (PolyChunk _ _ _ bs tl) = atEnd' bs tl

atEnd' :: ByteString -> Int -> Bool
atEnd' bs tl = tl == BS.length bs - 1

afterBlock :: DataQueue -> ByteString
afterBlock (SingleChunk bs _ tl) = BS.drop (tl + 1) bs
afterBlock (PolyChunk _ _ _ bs tl) = BS.drop (tl + 1) bs

beforeBlock :: DataQueue -> ByteString
beforeBlock (SingleChunk bs hd _) = BS.take hd bs
beforeBlock (PolyChunk bs hd _ _ _) = BS.take hd bs

slide :: DataQueue -> Maybe (Maybe ByteString, DataQueue)
slide (SingleChunk bs hd tl) | atEnd' bs tl = Nothing
                             | otherwise = Just (Nothing, SingleChunk bs (hd+1) (tl+1))
slide (PolyChunk bs1 hd q bs2 tl) | atEnd' bs2 tl =
                                      Nothing
                                  | atEnd' bs1 hd =
                                      case deq q of
                                        Just (q', bs1') ->
                                          Just (Just bs1, PolyChunk bs1' 0 q' bs2 (tl+1))
                                        Nothing ->
                                          Just (Just bs1, SingleChunk bs2 0 (tl+1))
                                  | otherwise =
                                      Just (Nothing, PolyChunk bs1 (hd+1) q bs2 (tl+1))
{-# INLINE slide #-}

-- | Slide only the head of the block.
slideOff :: DataQueue -> (Maybe ByteString, Maybe DataQueue)
slideOff (SingleChunk bs hd tl) | atEnd' bs hd = (Just bs, Nothing)
                                | otherwise = (Nothing, Just $ SingleChunk bs (hd+1) tl)
slideOff (PolyChunk bs1 hd q bs2 tl) | atEnd' bs1 hd =
                                         case deq q of
                                           Just (q', bs1') ->
                                             (Just bs1, Just $ PolyChunk bs1' 0 q' bs2 tl)
                                           Nothing ->
                                             (Just bs1, Just $ SingleChunk bs2 0 tl)
                                     | otherwise =
                                         (Nothing, Just $ PolyChunk bs1 (hd+1) q bs2 tl)
{-# INLINE slideOff #-}

-- | Add a block, sliding too.  Behavior is undefined if not 'atEnd'
addBlock :: DataQueue -> ByteString -> (Maybe ByteString, DataQueue)
addBlock (SingleChunk bs hd tl) bs2 | atEnd' bs tl =
                                        if atEnd' bs hd -- this won't ever happen in practice
                                        then (Just bs, SingleChunk bs2 0 0)
                                        else (Nothing, PolyChunk bs (hd+1) empty bs2 0)
                                    | otherwise =
                                        error "Not at end of block"
addBlock (PolyChunk bs1 hd q bs2 tl) bs3 | atEnd' bs2 tl =
                                             if atEnd' bs1 hd
                                             then case deq q of
                                                   Nothing ->
                                                     (Just bs1, PolyChunk bs2 0 empty bs3 0)
                                                   Just (q', bs1') ->
                                                     (Just bs1, PolyChunk bs1' 0 (enq q' bs2) bs3 0)
                                             else (Nothing, PolyChunk bs1 (hd+1) (enq q bs2) bs3 0)
                                         | otherwise =
                                             error "Not at end of block"
{-# INLINE addBlock #-}

hashBlock :: (Monad m) => DataQueue -> HashT m ByteString
hashBlock (SingleChunk bs hd tl) = do
  updateS $ BS.take (1 + tl - hd) $ BS.drop hd bs
  digestS
hashBlock (PolyChunk bs1 hd q bs2 tl) = do
  updateS $ BS.drop hd bs1
  forM_ (q2list q) updateS
  updateS $ BS.take (tl+1) bs2
  digestS
