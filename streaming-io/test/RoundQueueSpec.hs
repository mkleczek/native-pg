module RoundQueueSpec (spec) where

import Control.Exception (IOException, evaluate, throw, try)
import qualified Network.Streaming.Internal.RoundQueue as Queue
import Skeletest
import System.IO.Error (ioeGetErrorString)

spec :: Spec
spec = describe "the pending round queue" $ do
  it "keeps individual actions and fork batches in insertion order" $ do
    let first = Queue.push Queue.empty (0 :: Int)
        second = Queue.pushBatch first [1, 2] id
        third = Queue.push second 3
        fourth = Queue.pushBatch third [] id
        fifth = Queue.pushBatch fourth [4] id
        final = Queue.pushBatch fifth [5, 6, 7] id
    Queue.takeReady final `shouldBe` [0 .. 7]

  it "forces the batch spine before resuming the scheduler" $ do
    let batch = () : throw (userError "end of fork batch")
    outcome <- try @IOException (evaluate (Queue.pushBatch Queue.empty batch (const ())))
    either ioeGetErrorString (const "the scheduler resumed") outcome `shouldBe` "end of fork batch"

  it "leaves the staged actions unevaluated" $ do
    let batch = [throw (userError "forced fork payload") :: ()]
    Queue.pushBatch Queue.empty batch (length . Queue.takeReady) `shouldBe` 1
