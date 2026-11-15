{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | 'cached': a hit, a miss that loads, the hook and the reload it asks for, the listeners, and the keys that are stored. The
-- same programs, written once for any arrow and any sharing of the cache that suit each other, run under every setup.
module CachedSpec (spec) where

import Control.Arrow ((^>>))
import Control.Exception (throwIO)
import Control.Monad (forM_)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Sieve
import Network.Streaming.Classes (Effect (..), Scope (..))
import Network.Streaming.Combinators (tap)
import Skeletest
import Support

-- | A key that is prepared for storage by marking it, as a view of a buffer is copied: equal when the numbers are.
data Tagged = Tagged Bool Int deriving (Show)

instance Eq Tagged where Tagged _ a == Tagged _ b = a == b

instance Key Tagged where hashKey (Tagged _ n) = n

spec :: Spec
spec = describe "cached" $ forM_ setups $ \setup@(Setup name _ make _ _ run) -> describe name $ do
  itCase setup [Table, Cached] "loads a missing value, finds it afterwards, and tells each request once" $ do
    loads <- counter
    told <- newLog
    cache <- make ((ints 4) {requested = noteIn told . show})
    run (valuesOf (tenTimes loads) cache [1, 1, 2, 1]) `shouldReturn` [10, 10, 20, 10]
    readIORef loads `shouldReturn` 2
    readLog told `shouldReturn` ["False", "True", "False", "True"]

  itCase setup [Table, Cached] "runs the hook once for each call, on a value that is found and on one that was just loaded" $ do
    loads <- counter
    runs <- counter
    cache <- make (ints 4)
    let hook _ v = (Use (v + 1)) <$ bump runs
    run (traverse (\k -> const (cache, k) ^>> cached (tenTimes loads) hook) [1, 1, 2]) `shouldReturn` [11, 11, 21]
    readIORef runs `shouldReturn` 3
    readIORef loads `shouldReturn` 2

  itCase setup [Table, Cached] "gives the hook the time of the arrow's clock, which never goes back" $ do
    cache <- make (ints 4)
    loads <- counter
    seen <- newIORef []
    let hook instant v = Use v <$ modifyIORef' seen (instant :)
    (_ :: [Int]) <- run (traverse (\k -> const (cache, k) ^>> cached (tenTimes loads) hook) [1, 1, 1])
    instants <- reverse <$> readIORef seen
    length instants `shouldBe` 3
    and (zipWith (<=) instants (drop 1 instants)) `shouldBe` True

  itCase setup [Table, Cached] "evicts the values it loaded when it is full, and loads them again" $ do
    loads <- counter
    cache <- make (ints 1)
    run (valuesOf (now (\_ -> (+ 1) <$> bump loads)) cache [1, 2, 1]) `shouldReturn` [1, 2, 3]

  itCase setup [Table, Cached] "tells the eviction listener of what it evicts" $ do
    told <- newLog
    cache <- make ((ints 1) {evicted = \k v -> noteIn told (show (k :: Int, v :: Int))})
    loads <- counter
    (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [1, 2, 3])
    readLog told `shouldReturn` ["(1,10)", "(2,20)"]

  itCase setup [Table, Cached] "doesn't cache a load that failed, and the failure is the caller's" $ do
    loads <- counter
    cache <- make (ints 4)
    let loader = now (\k -> bump loads >>= \attempt -> if attempt == 0 then throwIO (userError "load failed") else pure (k * 10 + attempt))
        call k = const (cache, k) ^>> cached loader accept
    results <- run ((,,) <$> tryA (call 1) <*> call 1 <*> call 1)
    case results of
      (Left failure, second', third) -> (show failure, second', third) `shouldBe` ("user error (load failed)", 11, 11)
      (Right value, _, _) -> failTest ("the first call gave " <> show (value :: Int))
    readIORef loads `shouldReturn` 2

  describe "the hook asks for a reload" $ do
    itCase setup [Table, Cached] "loads the value again, and keeps what it loaded" $ do
      loads <- counter
      cache <- make (ints 4)
      -- The first value of a key is stale: it is a ten, and the hook wants a twenty or more.
      let hook _ v = pure (if v < 15 then Reload else Use v)
          loader = now (\k -> (\n -> k * 10 * (n + 1)) <$> bump loads)
          call k = const (cache, k) ^>> cached loader hook
      run ((,) <$> call 1 <*> call 1) `shouldReturn` (20, 20)
      readIORef loads `shouldReturn` 2
      entries cache `shouldReturn` [(1, 20)]

    itCase setup [Table, Cached] "removes the entry that it found, and only if it is still that entry" $ do
      loads <- counter
      cache <- make (ints 4)
      replaced <- newIORef False
      -- While the hook of the first call decides, another entry takes the place of the one it found: the reload
      -- must not remove it.
      let hook _ v = do
            done' <- readIORef replaced
            if done'
              then pure (Use v)
              else do
                writeIORef replaced True
                _ <- remove cache 1
                _ <- store cache 1 99
                pure Reload
      run (const (cache, 1) ^>> cached (tenTimes loads) hook) `shouldReturn` 99
      readIORef loads `shouldReturn` 1
      peek cache 1 `shouldReturn` Just 99
      entries cache `shouldReturn` [(1, 99)]

    itCase setup [Table, Cached] "counts the request once, as a hit" $ do
      told <- newLog
      loads <- counter
      cache <- make ((ints 4) {requested = noteIn told . show})
      (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [1])
      replaced <- newIORef False
      let hook _ v = readIORef replaced >>= \seen -> if seen then pure (Use v) else Reload <$ writeIORef replaced True
      (_ :: Int) <- run (const (cache, 1) ^>> cached (tenTimes loads) hook)
      readLog told `shouldReturn` ["False", "True"]

  describe "thrashing" $ do
    itCase setup [Table, Cached] "tells the listener of a miss of a key among the last evicted ones, once" $ do
      thrashes <- counter
      loads <- counter
      cache <- make ((ints 1) {thrashed = Just (() <$ bump thrashes)})
      (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [1, 2, 1, 1])
      readIORef thrashes `shouldReturn` 1
      -- 3 was never cached, and 2 is not the last evicted key any more.
      (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [3, 2])
      readIORef thrashes `shouldReturn` 1

    itCase setup [Table, Cached] "doesn't tell of a miss of a key that was flushed or reloaded" $ do
      thrashes <- counter
      loads <- counter
      cache <- make ((ints 16) {thrashed = Just (() <$ bump thrashes)})
      reloaded <- newIORef False
      let hook _ v = readIORef reloaded >>= \seen -> if seen then pure (Use v) else Reload <$ writeIORef reloaded True
      (_ :: Int) <- run (const (cache, 1) ^>> cached (tenTimes loads) hook)
      flush cache
      (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [1])
      readIORef thrashes `shouldReturn` 0

    itCase setup [Table, Cached] "remembers as many evicted keys as the capacity" $ do
      thrashes <- counter
      loads <- counter
      cache <- make ((ints 3) {thrashed = Just (() <$ bump thrashes)})
      (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [1, 2, 3, 4, 5])
      -- 1 and 2 were evicted, and both are among the last three.
      (_ :: [Int]) <- run (valuesOf (tenTimes loads) cache [1, 2])
      readIORef thrashes `shouldReturn` 2

  describe "keys that are prepared for storage" $ do
    itCase setup [Table, Cached] "are stored prepared, and found by an unprepared one" $ do
      loads <- counter
      cache <- make ((config 4) {prepareKey = \(Tagged _ n) -> Tagged True n})
      let loader = (\(Tagged _ n) -> n * 10) ^>> tap (now (const (() <$ bump loads)))
          call k = const (cache, k) ^>> cached loader accept
      run ((,) <$> call (Tagged False 1) <*> call (Tagged False 1)) `shouldReturn` (10, 10)
      readIORef loads `shouldReturn` 1
      stored <- entries cache
      [(t, v) | (Tagged t _, v) <- stored] `shouldBe` [(True, 10)]
