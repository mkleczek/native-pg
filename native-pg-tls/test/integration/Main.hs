module Main (main) where

import qualified IntegrationSpec
import TestCluster (withCluster)
import Test.Hspec (hspec)

main :: IO ()
main = withCluster $ \socketPath port -> hspec (IntegrationSpec.spec socketPath port)
