{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | The teardown of evicted values: the eviction listener queues them without waiting, and a part of the program retires each
-- one's lease and then runs the owner's arrow on it.
module TeardownSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Control.Monad (forM_)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Sieve
import Data.Sieve.Lease
import Data.Sieve.Teardown
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, tap)
import Skeletest
import Support

spec :: Spec
spec = describe "teardown" $ forM_ setups $ \setup@(Setup name _ make newLease' newGraveyard' run) -> describe name $ do
  itCase setup [Lease, Teardown] "tears a value down once the requests in flight have left, and runs the owner's arrow on it" $ do
    graveyard <- newGraveyard'
    lease <- newLease'
    logged <- newLog
    let owner = now (\((tenant, _), how) -> noteIn logged ("torn down " <> tenant <> " " <> show how))
        part = const graveyard ^>> teardown snd (secs 10) owner
        request =
          now (const (enter lease >> bury graveyard ("a", lease)))
            >>> pause (millis 60)
            >>> now (const (noteIn logged "request done" >> leave lease))
            >>> spinUntil (isDown graveyard)
    (_ :: Either () ()) <- run (race part request)
    readLog logged `shouldReturn` ["request done", "torn down a Drained"]
    outstanding graveyard `shouldReturn` 0

  itCase setup [Lease, Teardown] "tears down every value that was buried, also those that were before the part began, in order" $ do
    graveyard <- newGraveyard'
    logged <- newLog
    leases <- mapM (const newLease') [1 .. 5 :: Int]
    forM_ (zip [1 :: Int ..] leases) $ \(i, lease) -> bury graveyard (show i, lease)
    outstanding graveyard `shouldReturn` 5
    let owner = now (\((tenant, _), how) -> noteIn logged ("torn down " <> tenant <> " " <> show how))
    (_ :: Either () ()) <- run (race (const graveyard ^>> teardown snd (secs 10) owner) (spinUntil (isDown graveyard)))
    readLog logged `shouldReturn` ["torn down " <> show i <> " Drained" | i <- [1 .. 5 :: Int]]
    outstanding graveyard `shouldReturn` 0

  itCase setup [Lease, Teardown] "wakes the part that waits for a value, and doesn't run the owner's arrow for a lease that was retired before" $ do
    graveyard <- newGraveyard'
    logged <- newLog
    retiredBefore <- newLease'
    fresh <- newLease'
    (_ :: Retirement) <- run (const retiredBefore ^>> retire (secs 1))
    let owner = now (\((tenant, _), how) -> noteIn logged ("torn down " <> tenant <> " " <> show how))
        burying = pause (millis 40) >>> now (const (bury graveyard ("retired before", retiredBefore) >> bury graveyard ("fresh", fresh)))
    (_ :: Either () ()) <- run (race (const graveyard ^>> teardown snd (secs 10) owner) (burying >>> spinUntil (isDown graveyard)))
    readLog logged `shouldReturn` ["torn down fresh Drained"]
    outstanding graveyard `shouldReturn` 0

  itCase setup [Lease, Teardown] "runs the owner's arrow when a request stays past the deadline, and says so" $ do
    graveyard <- newGraveyard'
    lease <- newLease'
    logged <- newLog
    let owner = now (\((tenant, _), how) -> noteIn logged ("torn down " <> tenant <> " " <> show how))
        request =
          now (const (enter lease >> bury graveyard ("a", lease)))
            >>> pause (millis 250)
            >>> now (const (noteIn logged "request done" >> leave lease))
    (_ :: Either () ()) <- run (race (const graveyard ^>> teardown snd (millis 50) owner) (request >>> spinUntil (isDown graveyard)))
    readLog logged `shouldReturn` ["torn down a TimedOut", "request done"]
    inFlight lease `shouldReturn` 0

  itCase setup [Lease, Teardown] "loses no value when the part that waits for one is cancelled" $ do
    graveyard <- newGraveyard'
    lease <- newLease'
    logged <- newLog
    let owner = now (\((tenant, _), how) -> noteIn logged ("torn down " <> tenant <> " " <> show how))
        part = const graveyard ^>> teardown snd (secs 10) owner
    run (within (millis 40) part) `shouldReturn` Nothing
    bury graveyard ("a", lease)
    (_ :: Either () ()) <- run (race part (spinUntil (isDown graveyard)))
    readLog logged `shouldReturn` ["torn down a Drained"]

  itCase setup [Table, Cached, Lease, Teardown] "tears down what a cache evicts" $ do
    graveyard <- newGraveyard'
    logged <- newLog
    cache <- make ((config 1) {evicted = \_ (tenant, lease) -> bury graveyard (tenant :: String, lease)})
    let owner = now (\((tenant, _), how) -> noteIn logged ("torn down " <> tenant <> " " <> show how))
        loader = now (\k -> (,) ("tenant " <> show (k :: Int)) <$> newLease')
        lookups = traverse (\k -> const (cache, k) ^>> cached loader accept) [1, 2, 3]
    (_ :: Either () ()) <- run (race (const graveyard ^>> teardown snd (secs 10) owner) (lookups >>> done >>> spinUntil (isDown graveyard)))
    readLog logged `shouldReturn` ["torn down tenant 1 Drained", "torn down tenant 2 Drained"]

  describe "values that the cache does not keep" $ do
    itCase setup [Table, Cached, Lease, Teardown, Owned] "tears down a tenant that lost the race for its key, once, and not the one that won" $ do
      graveyard <- newGraveyard'
      logged <- newLog
      counted <- counter
      cache <- make ((config 4) {evicted = \_ tenant' -> bury graveyard tenant'})
      let owner = now (\((label, _), how) -> noteIn logged ("torn down " <> label <> " " <> show how))
          loader = now (\k -> bump counted >>= \n -> (,) ("tenant " <> show (k :: Int) <> ", loaded " <> show n) <$> newLease') >>> tap (now (\_ -> newLease' >>= \won -> () <$ store cache 1 ("won", won)))
          call = const (cache, 1) ^>> cached loader (\_ (label, lease) -> pure (Use (label, lease)))
      run (withPart (const graveyard ^>> teardown snd (secs 10) owner) (call >>> tap (done >>> spinUntil (isDown graveyard)))) `shouldReturn` "the scenario ended"
      readLog logged `shouldReturn` ["torn down tenant 1, loaded 0 Drained"]
      map (fst . snd) <$> entries cache `shouldReturn` ["won"]
      outstanding graveyard `shouldReturn` 0

    itCase setup [Table, Cached, Lease, Teardown, Owned] "tears down a tenant that a reload removes, once" $ do
      graveyard <- newGraveyard'
      logged <- newLog
      counted <- counter
      asked <- newIORef False
      cache <- make ((config 4) {evicted = \_ tenant' -> bury graveyard tenant'})
      let owner = now (\((label, _), how) -> noteIn logged ("torn down " <> label <> " " <> show how))
          loader = now (\k -> bump counted >>= \n -> (,) ("tenant " <> show (k :: Int) <> ", loaded " <> show n) <$> newLease')
          hookOnce _ tenant' = readIORef asked >>= \seen -> if seen then pure (Use (fst tenant')) else Reload <$ writeIORef asked True
          call = const (cache, 1) ^>> cached loader hookOnce
      run (withPart (const graveyard ^>> teardown snd (secs 10) owner) (call >>> tap (done >>> spinUntil (isDown graveyard)))) `shouldReturn` "the scenario ended"
      readLog logged `shouldReturn` ["torn down tenant 1, loaded 0 Drained"]
      map (fst . snd) <$> entries cache `shouldReturn` ["tenant 1, loaded 1"]
      outstanding graveyard `shouldReturn` 0

    itCase setup [Table, Cached, Lease, Teardown] "tears down a tenant once when its owner buried it already, and a lookup finds it retired and reloads it" $ do
      graveyard <- newGraveyard'
      logged <- newLog
      counted <- counter
      cache <- make ((config 4) {evicted = \_ tenant' -> bury graveyard tenant'})
      let owner = now (\((label, _), how) -> noteIn logged ("torn down " <> label <> " " <> show how))
          loader = now (\k -> bump counted >>= \n -> (,) ("tenant " <> show (k :: Int) <> ", loaded " <> show n) <$> newLease')
          hook _ (label, lease) = enter lease >>= \entered -> pure (if entered then Use (label, lease) else Reload)
          call = const (cache, 1) ^>> cached loader hook
          -- Loads the tenant and is done with it, buries it as its owner does when it kills it, and waits for its teardown, and then
          -- looks it up again.
          scenario = call >>> now (\(_, lease) -> leave lease >> peek cache 1 >>= maybe (pure ()) (bury graveyard)) >>> spinUntil (isDown graveyard) >>> call >>> tap (done >>> spinUntil (isDown graveyard))
      run (withPart (const graveyard ^>> teardown snd (secs 10) owner) scenario) `shouldReturn` "the scenario ended"
      readLog logged `shouldReturn` ["torn down tenant 1, loaded 0 Drained"]
      map (fst . snd) <$> entries cache `shouldReturn` ["tenant 1, loaded 1"]
      outstanding graveyard `shouldReturn` 0

-- | Whether every value that was buried is torn down.
isDown :: Graveyard c v -> IO Bool
isDown graveyard = (== 0) <$> outstanding graveyard
