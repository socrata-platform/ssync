{-# OPTIONS_GHC -fno-warn-orphans #-}

import Test.Tasty
import Test.Tasty.QuickCheck (testProperty, Arbitrary, arbitrary, elements, frequency, choose, shrink)
import Test.Tasty.Runners.AntXML (antXMLRunner)

import Conduit
import Control.Applicative ((<$>))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Maybe (mapMaybe)
import Data.Foldable (foldMap)
import Data.Monoid (Sum(..))
import Test.QuickCheck.Instances ()

import SSync.SignatureComputer

instance Arbitrary HashAlgorithm where
  arbitrary = elements [minBound..maxBound]

instance Arbitrary BlockSize where
  arbitrary = blockSize' . fromIntegral <$> frequency [ (3, choose (blockSizeWord minBound, 100))
                                                      , (1, choose (101, blockSizeWord maxBound))
                                                      ]
  shrink bs = mapMaybe (blockSize . fromIntegral) (shrink $ blockSizeWord bs)

main :: IO ()
main = defaultMainWithIngredients (antXMLRunner : defaultIngredients) tests

tests :: TestTree
tests = testSignatureTableSize

testSignatureTableSize :: TestTree
testSignatureTableSize =
  testProperty "length (computeSignatureTable bytes) == signatureTableSize bytes" prop
  where prop :: HashAlgorithm -> HashAlgorithm -> BlockSize -> [ByteString] -> Bool
        prop chk strong bs bytes =
          let lenSum = Sum . fromIntegral . BS.length
              realLen = getSum . runIdentity $ yieldMany bytes $$ produceSignatureTable chk strong bs $= foldMapC lenSum
              estLen = signatureTableSize chk strong bs (getSum $ foldMap lenSum bytes)
          in realLen == estLen
