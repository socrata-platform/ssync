{-# LANGUAGE OverloadedStrings, LambdaCase #-}
module Main where

import Prelude hiding (FilePath)

import Filesystem.Path.CurrentOS (FilePath)
import Data.Maybe (fromMaybe)
import qualified Filesystem.Path.CurrentOS as FP
import qualified Filesystem as FS
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T
import Conduit
import Options.Applicative
import System.IO (hClose, hSeek, SeekMode(AbsoluteSeek))
import Control.Monad.Trans.Resource (allocate)

import SSync.SignatureTable
import SSync.PatchComputer
import SSync.SignatureComputer
import SSync.PatchApplier

tap :: (MonadIO m, Show b) => (a -> b) -> Conduit a m a
tap f = awaitForever $ \a -> do
  liftIO . putStrLn . show $ f a
  yield a

data SigOptions = SigOptions { sigChecksumAlgorithm :: Maybe HashAlgorithm
                             , sigStrongHashAlgorithm :: Maybe HashAlgorithm
                             , sigBlockSize :: Maybe BlockSize
                             , sigInFile :: Maybe FilePath
                             , sigOutFile :: Maybe FilePath
                             } deriving (Show)

data DiffOptions = DiffOptions { diffInFile :: FilePath
                               , diffSigFile :: Maybe FilePath
                               , diffOutFile :: Maybe FilePath
                               } deriving (Show)

data PatchOptions = PatchOptions { patchInFile :: FilePath
                                 , patchPatchFile :: Maybe FilePath
                                 , patchOutFile :: Maybe FilePath
                                 } deriving (Show)

data Command = GenerateSignature SigOptions
             | GenerateDiff DiffOptions
             | ApplyPatch PatchOptions
             deriving Show

hashAlg :: ReadM HashAlgorithm
hashAlg = (hashForName . T.pack <$> str) >>= \case
  Just ha -> return ha
  Nothing -> readerError "Unknown hash algorithm"

blkSz :: ReadM BlockSize
blkSz = do
  xs <- reads <$> str
  case xs of
   [(n, "")] ->
     case blockSize n of
      Just bs -> return bs
      Nothing -> readerError "Number out of range"
   _ ->
     readerError "Not a number"

file :: ReadM FilePath
file = FP.fromText . T.pack <$> str

sigOptions :: Parser SigOptions
sigOptions = SigOptions <$> optional (option hashAlg (long "chk" <> metavar "ALGORITHM" <> help "Checksum algorithm"))
                        <*> optional (option hashAlg (long "strong" <> metavar "ALGORITHM" <> help "Strong hash algorithm"))
                        <*> optional (option blkSz (long "bs" <> metavar "BLOCKSIZE" <> help "Block size"))
                        <*> optional (argument file (metavar "INFILE" <> action "file"))
                        <*> optional (argument file (metavar "OUTFILE" <> action "file"))

diffOptions :: Parser DiffOptions
diffOptions = DiffOptions <$> argument file (metavar "INFILE" <> action "file")
                          <*> optional (argument file (metavar "SIGFILE" <> action "file"))
                          <*> optional (argument file (metavar "OUTFILE" <> action "file"))

patchOptions :: Parser PatchOptions
patchOptions = PatchOptions <$> argument file (metavar "INFILE" <> action "file")
                            <*> optional (argument file (metavar "PATCHFILE" <> action "file"))
                            <*> optional (argument file (metavar "OUTFILE" <> action "file"))

commandParser :: Parser Command
commandParser = subparser (
  command "sig" (info (GenerateSignature <$> sigOptions) (progDesc "Generate a signature from a file")) <>
  command "diff" (info (GenerateDiff <$> diffOptions) (progDesc "Generate a patch from a file and a signature")) <>
  command "patch" (info (ApplyPatch <$> patchOptions) (progDesc "Apply a patch to a file to produce a new file"))
 )

stdinOr :: (MonadResource m) => Maybe FilePath -> Source m ByteString
stdinOr = maybe stdinC sourceFile

stdoutOr :: (MonadResource m) => Maybe FilePath -> Sink ByteString m ()
stdoutOr = maybe stdoutC sinkFile

main :: IO ()
main = execParser (info (helper <*> commandParser) fullDesc) >>= \case
  GenerateSignature (SigOptions chkAlg strongAlg bs inOpt outOpt) ->
    runResourceT $ stdinOr inOpt $$ produceSignatureTable (fromMaybe MD5 chkAlg) (fromMaybe MD5 strongAlg) (fromMaybe (blockSize' 102400) bs) $= stdoutOr outOpt
  GenerateDiff (DiffOptions inFile sigOpt outOpt) ->
    runResourceT $ do
      st <- stdinOr sigOpt $$ consumeSignatureTable
      sourceFile inFile $$ patchComputer st $= stdoutOr outOpt
  ApplyPatch (PatchOptions inFile diffOpt outOpt) ->
    runResourceT $ do
      (_, chunkFile) <- allocate (FS.openFile inFile FS.ReadMode) hClose
      let chunkProvider bs bn = liftIO $ do
            hSeek chunkFile AbsoluteSeek (fromIntegral bs * fromIntegral bn)
            result <- BS.hGet chunkFile bs
            if BS.null result
              then return Nothing
              else return $ Just result
      stdinOr diffOpt $$ patchApplier chunkProvider $= stdoutOr outOpt
