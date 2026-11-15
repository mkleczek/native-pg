{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PatternSynonyms #-}

-- | The shared cache and the lease on several cores at once: readers that hammer one key and several keys while a writer
-- inserts, evicts, removes, flushes and resizes, which never get a value of another key; and requests that enter the lease
-- of a tenant that a teardown races, which never use it after its teardown ran. Each runs with the parts on the cores of
-- 'runCores', and with the parts as threads.
module StressSpec (spec) where

import Control.Arrow ((>>>), (>>^), (^>>))
import Control.Monad (forM_, replicateM, when)
import Data.Bool (bool)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List (nub)
import Data.Sieve
import Data.Sieve.Lease
import Data.Sieve.Teardown
import Network.Streaming.Classes
import Network.Streaming.Combinators (alongside, done, iterateA, tap, times, pattern Again, pattern Finish)
import Data.Tuple (swap)
import Network.Streaming.Delimited (runCores)
import Network.Streaming.Threads (runThreads)
import Skeletest
import Support

-- * Readers and a writer

-- | The cache, which the readers and the writer share, with what they count. A value is its key and the number of the load or
-- the store that made it, so that a value of another key is told at once.
data World = World
  { worldCache :: Cache 'AllCores Int (Int, Int),
    worldSerial :: IORef Int,
    worldWrong :: IORef Int,
    worldLookups :: IORef Int,
    worldStop :: IORef Bool
  }

newWorld :: IO World
newWorld = World <$> newSharedCache (config 8) <*> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef False

-- | Looks the keys up, one after the other, until the writer has finished, and counts a value that is not the key's.
readerOf :: (Runtime arr) => World -> (Int -> Int) -> arr () ()
readerOf world keyOf =
  iterateA (nextKey >>> alongside lookingUp >>> now (\found -> check found >> bool (Again ()) (Finish ()) <$> readIORef (worldStop world)))
  where
    nextKey = now (const (keyOf <$> bump (worldLookups world)))
    lookingUp = (\k -> (worldCache world, k)) ^>> cached loader accept
    loader = alongside (now (const (bump (worldSerial world)))) >>^ swap
    check ((valueKey, _), k) = when (valueKey /= k) (() <$ bump (worldWrong world))

-- | Stores, removes, peeks, resizes and flushes at random for this many turns, and tells the readers to stop.
writerOf :: (Runtime arr) => World -> Int -> arr () ()
writerOf world turns = times turns (now (const write)) >>> now (const (writeIORef (worldStop world) True))
  where
    cache = worldCache world
    write = do
      n <- bump (worldSerial world)
      let key = (n * 7919 + n `div` 3) `mod` 24
      case n `mod` 40 of
        r
          | r < 22 -> () <$ store cache key (key, n)
          | r < 30 -> () <$ remove cache key
          | r < 37 -> () <$ peek cache key
          | r < 39 -> resize cache (4 + (n `div` 40) `mod` 9)
          | otherwise -> when (n `mod` 400 == 39) (flush cache)

-- | What each run has to show: no wrong hit, and a table that holds each key once, with its own value.
checkWorld :: World -> IO ()
checkWorld world = do
  readIORef (worldWrong world) `shouldReturn` 0
  looked <- readIORef (worldLookups world)
  (looked > 1000) `shouldBe` True
  found <- entries (worldCache world)
  nub (map fst found) `shouldBe` map fst found
  forM_ found $ \(k, (valueKey, _)) -> valueKey `shouldBe` k
  capacity' <- size (worldCache world)
  (capacity' <= 12) `shouldBe` True

-- * Requests and teardowns

-- | A tenant: its lease, and whether its teardown ran.
data Tenant = Tenant {tenantLease :: Lease 'AllCores, tenantDown :: IORef Bool}

-- | The cache of tenants, the graveyard that the evictions fill, and what the run counts. Each key has a count of the loaders
-- that run at once.
data Tenants = Tenants
  { tenantsCache :: Cache 'AllCores Int Tenant,
    tenantsGraveyard :: Graveyard 'AllCores Tenant,
    tenantsCreated :: IORef [Tenant],
    tenantsViolations :: IORef Int,
    tenantsUsed :: IORef Int,
    tenantsFinished :: IORef Int,
    tenantsLoading :: [IORef Int]
  }

keysOfTenants :: Int
keysOfTenants = 30

newTenants :: IO Tenants
newTenants = do
  graveyard <- newSharedGraveyard
  cache <- newSharedCache ((config 4) {evicted = \_ tenant -> bury graveyard tenant})
  Tenants cache graveyard <$> newIORef [] <*> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> replicateM keysOfTenants (newIORef 0)

-- | A request for the tenants of the keys in turn, this many times: it enters the lease of the tenant that the cache gives,
-- uses the tenant, giving way in between, and leaves. It counts a use of a tenant whose teardown ran.
requests :: (Runtime arr) => Tenants -> (Int -> Int) -> Int -> IORef Int -> arr () ()
requests tenants keyOf turns position = times turns (nextKey >>> lookingUp >>> tap (now checkUp) >>> giveWay >>> now (\tenant -> checkUp tenant >> leave (tenantLease tenant) >> () <$ bump (tenantsUsed tenants))) >>> now (const (() <$ bump (tenantsFinished tenants)))
  where
    nextKey = now (const (keyOf <$> bump position))
    lookingUp = (\k -> (tenantsCache tenants, k)) ^>> cached loader hook
    -- The loader of a key never runs at the same time as another loader of it.
    loader =
      tap (now (\k -> atomicModifyIORef' (tenantsLoading tenants !! k) (\n -> (n + 1, n)) >>= \others -> when (others /= 0) (() <$ bump (tenantsViolations tenants))))
        >>> giveWay
        >>> now (\k -> atomicModifyIORef' (tenantsLoading tenants !! k) (\n -> (n - 1, ())) >> newTenant)
    newTenant = do
      tenant <- Tenant <$> newSharedLease <*> newIORef False
      atomicModifyIORef' (tenantsCreated tenants) (\created -> (tenant : created, ()))
      pure tenant
    hook _ tenant = enter (tenantLease tenant) >>= \entered -> pure (if entered then Use tenant else Reload)
    checkUp tenant = readIORef (tenantDown tenant) >>= \down -> when down (() <$ bump (tenantsViolations tenants))

-- | The teardown part and, beside it, what ends it: once the requests are finished, the cache is flushed and every tenant that
-- was evicted is torn down.
teardownAfter :: (Runtime arr) => Tenants -> Int -> arr () ()
teardownAfter tenants requesters = race part ending >>> done
  where
    part = const (tenantsGraveyard tenants) ^>> teardown tenantLease (secs 30) (now owner)
    owner (tenant, how) = do
      when (how /= Drained) (() <$ bump (tenantsViolations tenants))
      writeIORef (tenantDown tenant) True
    ending =
      spinUntil ((== requesters) <$> readIORef (tenantsFinished tenants))
        >>> now (const (flush (tenantsCache tenants)))
        >>> spinUntil ((== 0) <$> outstanding (tenantsGraveyard tenants))

checkTenants :: Tenants -> IO ()
checkTenants tenants = do
  readIORef (tenantsViolations tenants) `shouldReturn` 0
  readIORef (tenantsUsed tenants) >>= \used -> (used > 1000) `shouldBe` True
  created <- readIORef (tenantsCreated tenants)
  forM_ created $ \tenant -> do
    readIORef (tenantDown tenant) `shouldReturn` True
    inFlight (tenantLease tenant) `shouldReturn` 0
  outstanding (tenantsGraveyard tenants) `shouldReturn` 0
  size (tenantsCache tenants) `shouldReturn` 0

spec :: Spec
spec = describe "stress" $ do
  describe "a writer and readers of one key and of several keys" $ do
    itNeeds [Table, Cached, SingleFlight, Shared] "give no reader a value of another key, with the parts on three cores" $ do
      ensureCores
      world <- newWorld
      results <-
        promptly
          ( runCores 3 (pure world) $ \shared core -> case core of
              0 -> readerOf shared (const 0)
              1 -> readerOf shared (`mod` 16)
              _ -> writerOf shared 150000
          )
      results `shouldBe` Just [(), (), ()]
      checkWorld world

    itNeeds [Table, Cached, SingleFlight, Shared] "give no reader a value of another key, with the parts as threads" $ do
      ensureCores
      world <- newWorld
      results <- promptly (runThreads (together [readerOf world (const 0), readerOf world (`mod` 16), readerOf world (`mod` 5), writerOf world 150000]) ())
      results `shouldBe` Just ()
      checkWorld world

  describe "requests that enter the lease of a tenant, and the teardown that races them" $ do
    itNeeds [Table, Cached, SingleFlight, Shared, Lease, Teardown] "never use a tenant after its teardown ran, with the parts on four cores" $ do
      ensureCores
      tenants <- newTenants
      positions <- replicateM 3 (newIORef 0)
      results <-
        promptly
          ( runCores 4 (pure tenants) $ \shared core -> case core of
              0 -> requests shared (`mod` 10) 40000 (positions !! 0)
              1 -> requests shared (\n -> (n * 3) `mod` 10) 40000 (positions !! 1)
              2 -> requests shared (\n -> (n * 7) `mod` keysOfTenants) 40000 (positions !! 2)
              _ -> teardownAfter shared 3
          )
      results `shouldBe` Just [(), (), (), ()]
      checkTenants tenants

    itNeeds [Table, Cached, SingleFlight, Shared, Lease, Teardown] "never use a tenant after its teardown ran, with the parts as threads" $ do
      ensureCores
      tenants <- newTenants
      positions <- replicateM 3 (newIORef 0)
      results <-
        promptly
          ( runThreads
              ( together
                  [ requests tenants (`mod` 10) 40000 (positions !! 0),
                    requests tenants (\n -> (n * 3) `mod` 10) 40000 (positions !! 1),
                    requests tenants (\n -> (n * 7) `mod` keysOfTenants) 40000 (positions !! 2),
                    teardownAfter tenants 3
                  ]
              )
              ()
          )
      results `shouldBe` Just ()
      checkTenants tenants
