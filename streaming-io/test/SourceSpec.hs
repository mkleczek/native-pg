{-# LANGUAGE OverloadedStrings #-}

module SourceSpec (spec) where

import Control.Concurrent (forkIO, threadDelay)
import FakeStreams
import Network.Streaming.Source
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "pull sources" $ do
  describe "pull" $ do
    it "gives a ready chunk without watching the source" $ do
      source <- newFakeSource [Pulled "abc", Ended]
      pull source `shouldReturn` Just "abc"
      pullCount source `shouldReturn` 1
      watchCounts source `shouldReturn` (0, 0)

    it "gives Nothing once the source has ended" $ do
      source <- newFakeSource [Pulled "abc", Ended]
      pull source `shouldReturn` Just "abc"
      pull source `shouldReturn` Nothing

    it "waits on the watch of a pending source and pulls again once it fires" $ do
      source <- newClosedSource [Pulled "abc", Ended]
      _ <- forkIO (threadDelay 20000 *> openSource source)
      timeout 2000000 (pull source) `shouldReturn` Just (Just "abc")
      -- A pending pull and the pull after the watch fired.
      pullCount source `shouldReturn` 2
      watchCounts source `shouldReturn` (1, 1)

    it "waits again when a pull after the watch finds nothing" $ do
      source <- newFakeSource [Pending, Pending, Pulled "abc"]
      timeout 2000000 (pull source) `shouldReturn` Just (Just "abc")
      pullCount source `shouldReturn` 3
      watchCounts source `shouldReturn` (2, 2)

    it "releases its watch when interrupted" $ do
      source <- newClosedSource [Pulled "abc"]
      timeout 50000 (pull source) `shouldReturn` Nothing
      watchCounts source `shouldReturn` (1, 1)

  describe "a source of any kind" $ do
    it "is pulled through its dictionary" $ do
      source <- newFakeSource [Pulled "abc", Ended]
      case AnySource source of
        AnySource inner -> do
          pull inner `shouldReturn` Just "abc"
          pull inner `shouldReturn` Nothing
