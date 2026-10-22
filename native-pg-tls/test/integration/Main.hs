module Main (main) where

import qualified OperationContextSpec
import qualified IntegrationSpec
import TestCluster (withCluster)
import Skeletest.Internal.Paths (setOriginalDirectory)
import Skeletest.Main (runSkeletest)
import System.Directory (getCurrentDirectory)

main :: IO ()
main = do
  -- The generated runner normally initializes source locations. This runner
  -- registers a spec parameterized by the cluster instead; Cabal runs it in
  -- the package directory, also used for the test certificates.
  setOriginalDirectory =<< getCurrentDirectory
  -- Keep setup outside the runner, so cluster failures cannot become XFAILs.
  withCluster $ \socketPath port ->
    runSkeletest [] [("test/integration/IntegrationSpec.hs", IntegrationSpec.spec socketPath port), ("test/integration/OperationContextSpec.hs", OperationContextSpec.spec port)]
