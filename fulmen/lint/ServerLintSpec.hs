-- | The structure of the server's program, which the lint of "Network.Streaming.Lint" looks at: it has no finding that a
-- part of the program doesn't allow, with a reason.
module ServerLintSpec (spec) where

import Network.Streaming.Lint (Report (..), lint, renderReport)
import ServerProgram (withServer)
import Skeletest

spec :: Spec
spec = describe "the server's program" $ do
  it "has no finding that a part of it doesn't allow" $ do
    report <- withServer lint
    lines (renderReport report {reportAllowed = []}) `shouldBe` ["0 findings, 0 allowed"]
