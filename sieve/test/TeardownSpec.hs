{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | The teardown of evicted values: the eviction listener queues them without waiting, and a part of the program retires each
-- one's lease and then runs the owner's arrow on it.
module TeardownSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Control.Monad (forM_)
import Data.Sieve
import Data.Sieve.Lease
import Data.Sieve.Teardown
import Network.Streaming.Classes
import Network.Streaming.Combinators (done)
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

-- | Whether every value that was buried is torn down.
isDown :: Graveyard c v -> IO Bool
isDown graveyard = (== 0) <$> outstanding graveyard
