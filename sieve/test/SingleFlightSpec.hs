{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | The loads that callers share: the callers that miss the same key while it loads wait for the loader's value, as tickets of
-- the arrow, and a load that fails or is cancelled makes one of them the next loader. A waiter can be cancelled, which doesn't
-- disturb the load.
module SingleFlightSpec (spec) where

import Control.Arrow ((>>>), (>>^), (^>>), (|||))
import Control.Exception (throwIO)
import Control.Monad (forM_, when)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (sort)
import Data.Sieve
import Network.Streaming.Classes
import Data.Tuple (swap)
import Network.Streaming.Combinators (alongside, done, tap)
import Skeletest
import Support

spec :: Spec
spec = describe "single flight" $ forM_ setups $ \setup@(Setup name _ make _ _ run) -> describe name $ do
  itCase setup [Table, Cached, SingleFlight] "shares one load among the callers that miss the same key, and counts each request once" $ do
    loads <- counter
    told <- newLog
    results <- newLog
    cache <- make ((ints 4) {requested = noteIn told . show})
    let call = const (cache, 1) ^>> cached (tenTimes loads >>> pause (millis 100)) accept >>> now (noteIn results . show)
    () <- run (together (replicate 8 call))
    readIORef loads `shouldReturn` 1
    readLog results `shouldReturn` replicate 8 "10"
    readLog told `shouldReturn` replicate 8 "False"
    run (valuesOf (tenTimes loads) cache [1]) `shouldReturn` [10]
    readIORef loads `shouldReturn` 1

  itCase setup [Table, Cached] "loads different keys at once, while the hits of another key go on" $ do
    loads <- counter
    started <- (,) <$> newIORef False <*> newIORef False
    cache <- make (ints 4)
    (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [0])
    -- Each loader waits until the other has begun, so that they only finish if the loads are not one after the other.
    let loading own other = tap (now (const (writeIORef own True)) >>> spinUntil (readIORef other)) >>^ (* 10)
        callWith loader key = const (cache, key) ^>> cached loader accept >>> done
        hits = valuesOf (tenTimes loads) cache (replicate 50 0) >>> done
    finished <-
      promptly (run (within (secs 20) (together [callWith (loading (fst started) (snd started)) 1, callWith (loading (snd started) (fst started)) 2, hits])))
    finished `shouldBe` Just (Just ())
    entries cache >>= \found -> sort found `shouldBe` [(0, 0), (1, 10), (2, 20)]

  itCase setup [Table, Cached, SingleFlight] "makes one waiter the next loader when the load fails, and the failure is the loader's" $ do
    loads <- counter
    results <- newLog
    cache <- make (ints 4)
    let loader =
          (alongside (now (const (bump loads))) >>^ swap)
            >>> pause (millis 100)
            >>> now (\(k, n) -> if n == 0 then throwIO (userError "load failed") else pure (k * 10 + n))
        call = const (cache, 1) ^>> cached loader accept
    () <- run
      ( together
          ( (tryA call >>> now (noteIn results . either show show))
              : replicate 3 (later (millis 30) call >>> now (noteIn results . show))
          )
      )
    readLog results `shouldReturn` ["user error (load failed)", "11", "11", "11"]
    readIORef loads `shouldReturn` 2

  itCase setup [Table, Cached, SingleFlight] "makes one waiter the next loader when the load is cancelled" $ do
    loads <- counter
    results <- newLog
    cache <- make (ints 4)
    let loader = (alongside (now (const (bump loads))) >>^ swap) >>> (route ^>> (pause (secs 30) ||| pause (millis 20)))
        route (k, n) = if n == 0 then Left k else Right (k * 10 + n)
        call = const (cache, 1) ^>> cached loader accept
    () <- run
      ( together
          ( (within (millis 100) call >>> now (noteIn results . show))
              : replicate 3 (later (millis 30) call >>> now (noteIn results . show))
          )
      )
    readLog results `shouldReturn` ["Nothing", "11", "11", "11"]
    readIORef loads `shouldReturn` 2

  itCase setup [Table, Cached, SingleFlight] "lets a waiter be cancelled without disturbing the load or the other waiters" $ do
    loads <- counter
    results <- newLog
    cache <- make (ints 4)
    let call = const (cache, 1) ^>> cached (tenTimes loads >>> pause (millis 200)) accept
    () <- run
      ( together
          [ call >>> now (noteIn results . show),
            later (millis 30) (within (millis 50) call) >>> now (noteIn results . show),
            later (millis 30) call >>> now (noteIn results . show)
          ]
      )
    sort <$> readLog results `shouldReturn` ["10", "10", "Nothing"]
    readIORef loads `shouldReturn` 1
    run (valuesOf (tenTimes loads) cache [1]) `shouldReturn` [10]
    readIORef loads `shouldReturn` 1

  itCase setup [Table, Cached] "gives the callers the value that is cached when the load finishes, if another way put it there" $ do
    results <- newLog
    cache <- make (ints 4)
    let loader = tap (now (\_ -> () <$ store cache 1 99)) >>> pause (millis 60) >>^ const 5
        call = const (cache, 1) ^>> cached loader accept >>> now (noteIn results . show)
    () <- run (together [call, later (millis 20) call])
    readLog results `shouldReturn` ["99", "99"]
    entries cache `shouldReturn` [(1, 99)]

  itCase setup [Table, Cached, SingleFlight] "lets a waiter's hook ask for the value to be loaded again, which the loader's isn't" $ do
    loads <- counter
    results <- newLog
    cache <- make (ints 4)
    asked <- newIORef False
    let loader = tenTimes loads >>> pause (millis 80) >>> now (\v -> (v +) <$> readIORef loads)
        hookOnce _ v = readIORef asked >>= \seen -> if seen then pure (Use v) else Reload <$ writeIORef asked True
        call hook = const (cache, 1) ^>> cached loader hook >>> now (noteIn results . show)
    () <- run (together [call accept, later (millis 20) (call hookOnce)])
    readIORef loads `shouldReturn` 2
    sort <$> readLog results `shouldReturn` ["11", "12"]

  describe "values that are not kept" $ do
    itCase setup [Table, Cached, SingleFlight, Owned] "hands the value of a load that lost the race for its key to the eviction listener, once, and serves the waiters with the value that is cached" $ do
      told <- newLog
      results <- newLog
      cache <- make ((ints 4) {evicted = noteEvicted told})
      let loader = pause (millis 40) >>> tap (now (\_ -> () <$ store cache 1 99)) >>^ const 5
          call = const (cache, 1) ^>> cached loader accept >>> now (noteIn results . show)
      () <- run (together [call, later (millis 10) call, later (millis 20) call])
      readLog results `shouldReturn` ["99", "99", "99"]
      readLog told `shouldReturn` ["(1,5)"]
      entries cache `shouldReturn` [(1, 99)]

    itCase setup [Table, Cached, SingleFlight, Owned] "tells the eviction listener of an entry once when the loader and a waiter both ask for a reload of it" $ do
      told <- newLog
      loads <- counter
      results <- newLog
      cache <- make ((ints 4) {evicted = noteEvicted told})
      askedByLoader <- newIORef False
      askedByWaiter <- newIORef False
      let loader = now (\k -> (k * 10 +) <$> bump loads) >>> pause (millis 80)
          hookOnce asked _ v = readIORef asked >>= \seen -> if seen then pure (Use v) else Reload <$ writeIORef asked True
          call asked = const (cache, 1) ^>> cached loader (hookOnce asked) >>> now (noteIn results . show)
      () <- run (together [call askedByLoader, later (millis 20) (call askedByWaiter)])
      readIORef loads `shouldReturn` 2
      readLog results `shouldReturn` ["11", "11"]
      readLog told `shouldReturn` ["(1,10)"]
      entries cache `shouldReturn` [(1, 11)]

    itCase setup [Table, Cached, SingleFlight] "tells the eviction listener of each entry that a resize drops while a load runs, once, and caches the value that is loaded" $ do
      told <- newLog
      cache <- make ((ints 4) {evicted = noteEvicted told})
      mapM_ (\k -> store cache k (k * 10)) [1, 2, 3]
      let loader = pause (millis 60) >>^ (* 10)
      () <- run (together [const (cache, 10) ^>> cached loader accept >>> done, later (millis 20) (now (const (resize cache 1)))])
      sort <$> readLog told `shouldReturn` ["(1,10)", "(2,20)", "(3,30)"]
      entries cache `shouldReturn` [(10, 100)]

    itCase setup [Table, Cached, SingleFlight] "tells the eviction listener of each entry that a flush drops while a load runs, once, and caches the value that is loaded" $ do
      told <- newLog
      cache <- make ((ints 4) {evicted = noteEvicted told})
      mapM_ (\k -> store cache k (k * 10)) [1, 2]
      let loader = pause (millis 60) >>^ (* 10)
      () <- run (together [const (cache, 10) ^>> cached loader accept >>> done, later (millis 20) (now (const (flush cache)))])
      readLog told `shouldReturn` ["(1,10)", "(2,20)"]
      entries cache `shouldReturn` [(10, 100)]
      flush cache
      readLog told `shouldReturn` ["(1,10)", "(2,20)", "(10,100)"]

    itCase setup [Table, Cached, SingleFlight, Owned] "serves the waiters of a load whose insertion fails in an eviction listener, and the failure is the loader's" $ do
      results <- newLog
      cache <- make ((ints 1) {evicted = \k _ -> when (k == 0) (throwIO (userError "listener failed"))})
      _ <- store cache 0 0
      let call = const (cache, 1) ^>> cached ((* 10) ^>> pause (millis 80)) accept
      finished <-
        promptly
          ( run
              ( within
                  (secs 5)
                  ( together
                      [ tryA call >>> now (noteIn results . either show show),
                        later (millis 20) call >>> now (noteIn results . show)
                      ]
                  )
              )
          )
      finished `shouldBe` Just (Just ())
      sort <$> readLog results `shouldReturn` ["10", "user error (listener failed)"]
      entries cache `shouldReturn` [(1, 10)]
