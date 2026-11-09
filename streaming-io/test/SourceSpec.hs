{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module SourceSpec (spec) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (IOException, try)
import Control.Monad (when)
import Data.IORef (atomicModifyIORef', modifyIORef', newIORef, readIORef)
import FakeStreams
import Network.Streaming.Source
import Network.Streaming.Watch (awaitWatch)
import Skeletest
import qualified Skeletest.Predicate as P
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

    it "releases its watch when interrupted" $ do
      source <- newClosedSource [Pulled "abc"]
      timeout 50000 (pull source) `shouldReturn` Nothing
      watchCounts source `shouldReturn` (1, 1)

  describe "pullWith" $ do
    -- What the pull gave: the chunk, 'Nothing' at the end, or the stall's own result.
    let chunkOf = pure . \case Pulled chunk -> Just (Just chunk); Ended -> Just Nothing

    it "gives a ready chunk to the first continuation, without calling the stall" $ do
      source <- newFakeSource [Pulled "abc", Ended]
      pullWith source chunkOf (\_ _ -> fail "unexpected stall") `shouldReturn` Just (Just "abc")
      pullWith source chunkOf (\_ _ -> fail "unexpected stall") `shouldReturn` Just Nothing

    it "calls the stall with a registration that registers nothing until it is run" $ do
      source <- newClosedSource [Pulled "abc", Ended]
      pullWith
        source
        chunkOf
        ( \register again -> do
            watchCounts source `shouldReturn` (0, 0)
            openSource source *> awaitWatch register *> again
        )
        `shouldReturn` Just (Just "abc")
      watchCounts source `shouldReturn` (1, 1)

    it "pulls again where it is when the stall runs again, and each pull stalls afresh" $ do
      source <- newClosedSource [Pulled "abc", Ended]
      calls <- newIORef (0 :: Int)
      let stall _ again = do
            modifyIORef' calls (+ 1)
            n <- readIORef calls
            when (n == 3) (openSource source)
            again
      pullWith source chunkOf stall `shouldReturn` Just (Just "abc")
      readIORef calls `shouldReturn` 3
      -- Three pulls that found nothing, and the one that gave the chunk.
      pullCount source `shouldReturn` 4

    it "gives what the stall gives when it doesn't run again, and the next pull starts from the top" $ do
      source <- newClosedSource [Pulled "abc", Ended]
      pullWith source chunkOf (\_ _ -> pure (Just (Just "stopped"))) `shouldReturn` Just (Just "stopped")
      pullCount source `shouldReturn` 1
      openSource source
      pullWith source chunkOf (\_ _ -> fail "unexpected stall") `shouldReturn` Just (Just "abc")

    it "lets the exception of the stall out, and doesn't pull again" $ do
      source <- newClosedSource [Pulled "abc", Ended]
      result <- try @IOException (pullWith source chunkOf (\_ _ -> ioError (userError "wait failed")))
      either show (const "pulled") result `shouldBe` "user error (wait failed)"
      pullCount source `shouldReturn` 1

  describe "a source as a waiting source" $ do
    it "is pulled until it has a chunk, with a wait that always runs again" $ do
      source <- newClosedSource [Pulled "abc", Ended]
      waited <- newIORef (0 :: Int)
      let wait register = modifyIORef' waited (+ 1) *> openSource source *> awaitWatch register
      pullWaiting (Waited source) wait `shouldReturn` Just "abc"
      pullWaiting (Waited source) wait `shouldReturn` Nothing
      readIORef waited `shouldReturn` 1

    it "lets the exception of the wait out" $ do
      source <- newClosedSource [Pulled "abc"]
      try @IOException (pullWaiting (Waited source) (\_ -> ioError (userError "wait failed")))
        `shouldSatisfy` P.returns (P.left P.anything)

  describe "retrying" $ do
    it "pulls, and stalls and pulls again only while the pull finds nothing" $ do
      results <- newIORef [Nothing, Nothing, Just (Pulled "abc")]
      stalls <- newIORef (0 :: Int)
      let pulling = atomicModifyIORef' results (\case [] -> ([], Just Ended); next : rest -> (rest, next))
      retrying pulling (fail "unexpected registration") (\found -> pure (found `seq` ())) (\_ again -> modifyIORef' stalls (+ 1) *> again)
        `shouldReturn` ()
      length <$> readIORef results `shouldReturn` 0
      readIORef stalls `shouldReturn` 2

  describe "a source of any kind" $ do
    it "is pulled through its dictionary" $ do
      source <- newFakeSource [Pulled "abc", Ended]
      case AnySource source of
        AnySource inner -> do
          pull inner `shouldReturn` Just "abc"
          pull inner `shouldReturn` Nothing
