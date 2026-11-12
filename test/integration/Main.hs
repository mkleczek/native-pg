module Main (main) where

import qualified BufferSpec
import qualified IntegrationSpec
import qualified ScopeTransactionSpec
import Skeletest.Internal.Paths (setOriginalDirectory)
import Skeletest.Main (runSkeletest)
import System.Directory (getCurrentDirectory)
import TestCluster (withCluster)
import qualified TransactionSpec

main :: IO ()
main = do
  -- The generated runner normally initializes source locations. This runner
  -- registers a spec parameterized by the cluster instead; Cabal runs it in
  -- the package directory, also used for the test certificates.
  setOriginalDirectory =<< getCurrentDirectory
  -- Keep setup outside the runner, so cluster failures cannot become XFAILs.
  -- Every spec is run under every runner of TestPrograms, and every group of them is marked as an expected
  -- failure until the v2 code is there: a cluster that does not start is still a failure, and so is a group that passes.
  withCluster $ \socketPath port ->
    runSkeletest
      []
      [ ("test/integration/IntegrationSpec.hs", IntegrationSpec.spec socketPath port),
        ("test/integration/TransactionSpec.hs", TransactionSpec.spec socketPath),
        ("test/integration/ScopeTransactionSpec.hs", ScopeTransactionSpec.spec socketPath port),
        ("test/integration/BufferSpec.hs", BufferSpec.spec socketPath port)
      ]
