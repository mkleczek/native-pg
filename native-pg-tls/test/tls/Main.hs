module Main (main) where

import qualified TlsSpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec TlsSpec.spec
