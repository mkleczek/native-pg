{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}

module CoresSpec (spec) where

import Control.Arrow ((>>>))
import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Monad (when)
import Data.List (sort)
import Network.Directions.PingPong
import Network.Streaming.Classes hiding (within)
import Network.Streaming.Combinators (concurrently)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited
import Network.Streaming.Loop (Report (..), runLoop)
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "cores" $ do
  describe "tickets that are made as the program runs" $ do
    it "let two parts ping-pong on one core, with every arrow, over tickets of the arrow's own core" $ do
      runThreads (pingPong 1000) () `shouldReturn` ()
      fst <$> runLoop (pingPong 1000) () `shouldReturn` ()
      fst <$> runCompiled (pingPong 1000) () `shouldReturn` ()
      fst <$> runDelimited (pingPong 1000) () `shouldReturn` ()

    it "let two parts ping-pong on one core over tickets of any core" $ do
      runThreads (pingPongAt @'Anywhere 1000) () `shouldReturn` ()
      fst <$> runDelimited (pingPongAt @'Anywhere 1000) () `shouldReturn` ()
      fst <$> runCompiled (pingPongAt @'Anywhere 1000) () `shouldReturn` ()

    it "let two parts ping-pong on two cores over tickets of any core, and in two threads" $ do
      ensureCores
      handoff <- newHandoff
      results <- within (runCores 2 (pure handoff) (\shared core -> if core == 0 then find shared >>> client 1000 else leave shared >>> server 1000))
      results `shouldBe` Just [(), ()]
      threadHandoff <- newHandoff
      together' <- within (runThreads (concurrently (find threadHandoff >>> client 1000) (leave threadHandoff >>> server 1000)) ())
      together' `shouldBe` Just ((), ())

  describe "a core that waits in its poller" $ do
    it "is woken by a give from another core within a few milliseconds, every time" $ do
      ensureCores
      wake <- newWake
      let samplesWanted = 40
      reports <- within (runCoresReport 2 (pure wake) (\shared core -> if core == 0 then sleeper shared samplesWanted else waker shared 3000 samplesWanted))
      waits <- case reports of
        Just (((), report) : _) -> pure (reportWaits report)
        _ -> failTest "the run didn't end"
      -- Core 0 blocked in its poller for (nearly) every sample.
      (waits >= samplesWanted - 5) `shouldBe` True
      latencies <- sort <$> wakeLatencies wake
      length latencies `shouldBe` samplesWanted
      -- The median is far below a millisecond; the slowest tenth, on a busy host, still well under 10 ms.
      (latencies !! (samplesWanted `div` 2) < 1000000) `shouldBe` True
      (latencies !! (samplesWanted * 9 `div` 10) < 10000000) `shouldBe` True

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

within :: IO a -> IO (Maybe a)
within = timeout 60000000
