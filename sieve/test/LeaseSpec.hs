{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | The lease: requests that enter and leave without waiting, and a retirement that refuses new requests and waits, within a
-- deadline, for those in flight.
module LeaseSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Control.Monad (forM_, when)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (sort)
import Data.Sieve.Lease
import Network.Streaming.Classes
import Network.Streaming.Combinators (times)
import Skeletest
import Support

spec :: Spec
spec = describe "lease" $ forM_ setups $ \setup@(Setup name _ _ newLease' _ run) -> describe name $ do
  itCase setup [Lease] "counts the requests that are in flight" $ do
    lease <- newLease'
    ((,) <$> enter lease <*> enter lease) `shouldReturn` (True, True)
    inFlight lease `shouldReturn` 2
    leave lease
    inFlight lease `shouldReturn` 1
    leave lease
    inFlight lease `shouldReturn` 0
    isRetired lease `shouldReturn` False

  itCase setup [Lease] "retires at once when no request is in flight, refuses the requests after, and is retired once" $ do
    lease <- newLease'
    run (const lease ^>> retire (secs 5)) `shouldReturn` Drained
    isRetired lease `shouldReturn` True
    enter lease `shouldReturn` False
    inFlight lease `shouldReturn` 0
    run (const lease ^>> retire (secs 5)) `shouldReturn` AlreadyRetired

  itCase setup [Lease] "waits for the request in flight to leave" $ do
    lease <- newLease'
    entered <- newIORef False
    logged <- newLog
    let holder = now (const (enter lease >>= writeIORef entered)) >>> pause (millis 80) >>> now (const (noteIn logged "left" >> leave lease))
        retirer = spinUntil (readIORef entered) >>> const lease ^>> retire (secs 10) >>> now (noteIn logged . show)
    () <- run (together [holder, retirer])
    readLog logged `shouldReturn` ["left", "Drained"]
    inFlight lease `shouldReturn` 0

  itCase setup [Lease] "gives up at the deadline, and a request that leaves after is harmless" $ do
    lease <- newLease'
    entered <- newIORef False
    logged <- newLog
    let holder = now (const (enter lease >>= writeIORef entered)) >>> pause (millis 300) >>> now (const (noteIn logged "left" >> leave lease))
        retirer = spinUntil (readIORef entered) >>> const lease ^>> retire (millis 60) >>> now (noteIn logged . show)
    () <- run (together [holder, retirer])
    readLog logged `shouldReturn` ["TimedOut", "left"]
    inFlight lease `shouldReturn` 0
    isRetired lease `shouldReturn` True

  itCase setup [Lease] "stays retired when the part that waits is cancelled" $ do
    lease <- newLease'
    entered <- newIORef False
    logged <- newLog
    let holder = now (const (enter lease >>= writeIORef entered)) >>> pause (millis 200) >>> now (const (noteIn logged "left" >> leave lease))
        retirer = spinUntil (readIORef entered) >>> within (millis 40) (const lease ^>> retire (secs 10)) >>> now (noteIn logged . show)
    () <- run (together [holder, retirer])
    readLog logged `shouldReturn` ["Nothing", "left"]
    isRetired lease `shouldReturn` True
    run (const lease ^>> retire (secs 5)) `shouldReturn` AlreadyRetired

  itCase setup [Lease] "has one of two retirements wait, and the other is told that the lease was retired" $ do
    lease <- newLease'
    entered <- newIORef False
    logged <- newLog
    let holder = now (const (enter lease >>= writeIORef entered)) >>> pause (millis 80) >>> now (const (leave lease))
        retirer = spinUntil (readIORef entered) >>> const lease ^>> retire (secs 10) >>> now (noteIn logged . show)
    () <- run (together [holder, retirer, retirer])
    sort <$> readLog logged `shouldReturn` ["AlreadyRetired", "Drained"]

  itCase setup [Lease] "refuses a request that comes after the retirement, which the retirement doesn't wait for" $ do
    lease <- newLease'
    entered <- newIORef False
    logged <- newLog
    let holder = now (const (enter lease >>= writeIORef entered)) >>> pause (millis 120) >>> now (const (noteIn logged "left" >> leave lease))
        retirer = spinUntil (readIORef entered) >>> const lease ^>> retire (secs 10) >>> now (noteIn logged . ("retirement " <>) . show)
        late = spinUntil (isRetired lease) >>> now (const (enter lease >>= noteIn logged . ("late request " <>) . show))
    () <- run (together [holder, retirer, late])
    readLog logged `shouldReturn` ["late request False", "left", "retirement Drained"]
    inFlight lease `shouldReturn` 0

  itCase setup [Lease] "ends drained, with no request left in flight, when requests race the retirement" $ do
    lease <- newLease'
    admitted <- counter
    refused <- counter
    ensureCores
    let request = times 300 (now (const (enter lease)) >>> giveWay >>> now (\ok -> if ok then leave lease >> () <$ bump admitted else () <$ bump refused))
        retirer = later (millis 1) (const lease ^>> retire (secs 10)) >>> now (\how -> when (how /= Drained) (failTest ("the retirement ended " <> show how)))
    () <- run (together [request, request, request, retirer])
    inFlight lease `shouldReturn` 0
    isRetired lease `shouldReturn` True
    (+) <$> readIORef admitted <*> readIORef refused `shouldReturn` 900
