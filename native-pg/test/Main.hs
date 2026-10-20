module Main (main) where

import qualified CoreSpec
import qualified TransportSpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec $ do
  TransportSpec.spec
  CoreSpec.spec
