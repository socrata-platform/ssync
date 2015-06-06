{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}

module Main (
  main
) where

import Conduit
import Control.Applicative ((<$>))
import System.Exit (exitSuccess, exitFailure)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Maybe (mapMaybe)
import Data.Foldable (foldMap)
import Data.Monoid (Sum(..))
import Test.QuickCheck (quickCheckAll, Arbitrary, arbitrary, shrink, elements, choose, frequency)
import Test.QuickCheck.Instances ()

import SSync.SignatureComputer

instance Arbitrary HashAlgorithm where
  arbitrary = elements [minBound..maxBound]

instance Arbitrary BlockSize where
  arbitrary = blockSize' . fromIntegral <$> frequency [ (3, choose (blockSizeWord minBound, 100))
                                                      , (1, choose (101, blockSizeWord maxBound))
                                                      ]
  shrink bs = mapMaybe (blockSize . fromIntegral) (shrink $ blockSizeWord bs)

prop_sigLen :: HashAlgorithm -> HashAlgorithm -> BlockSize -> [ByteString] -> Bool
prop_sigLen chk strong bs bytes =
  let lenSum = Sum . fromIntegral . BS.length
      realLen = getSum . runIdentity $ yieldMany bytes $$ produceSignatureTable chk strong bs $= foldMapC lenSum
      estLen = signatureTableSize chk strong bs (getSum $ foldMap lenSum bytes)
  in realLen == estLen

return []

main :: IO ()
main = do
  success <- $quickCheckAll
  if success then exitSuccess else exitFailure
