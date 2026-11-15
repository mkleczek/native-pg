{-# LANGUAGE DataKinds #-}

-- | What a hit costs in allocation: a lookup that finds its key takes no lock and writes at most the visited byte, and it
-- allocates next to nothing, as an IO step and as a step of a program. A program is run at the arrow that it is for, where it
-- is built, so that it is specialised to the arrow as the programs of a server are.
module AllocationSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Data.Int (Int64)
import Data.IORef (IORef)
import Data.Sieve
import GHC.Conc (getAllocationCounter)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, tap, times)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.Threads (runThreads)
import Skeletest
import Support

-- | How many bytes each hit may allocate, besides what the loop of the program allocates for each of its turns: 16 in each arrow,
-- the box of what the hook gives. The bound has room for one more.
hitBytes :: Int64
hitBytes = 32

hits :: Int
hits = 100000

-- | The bytes that the arrow allocates on the thread that runs it, which every arrow runs on the calling thread.
allocating :: (Runtime arr) => arr () () -> arr () Int64
allocating body = now (const getAllocationCounter) >>> tap (done >>> body) >>> now (\before -> (before -) <$> getAllocationCounter)

-- | What a program of hits allocates, and what a loop of the same length that looks nothing up does: the second is what the
-- first has besides its hits.
measured :: (Runtime arr, Reaching c arr) => Cache c Int Int -> IORef Int -> arr () (Int64, Int64)
measured cache loads = warmed >>> ((,) <$> allocating (times hits hit) <*> allocating (times hits baseline))
  where
    warmed = tap (done >>> times 1000 hit)
    hit = const (cache, 1) ^>> cached (tenTimes loads) accept >>> done
    baseline = const (cache, 1 :: Int) ^>> done

-- | A cache with the key 1 in it, and the counter of its loads.
withHit :: (Config Int Int -> IO (Cache c Int Int)) -> IO (Cache c Int Int, IORef Int)
withHit make = do
  cache <- make (ints 16)
  loads <- counter
  _ <- store cache 1 10
  pure (cache, loads)

-- | What each hit allocates, which is told and bounded.
bounded :: String -> (Int64, Int64) -> IO ()
bounded name (withHits, without) = do
  let perHit = (withHits - without) `div` fromIntegral hits
  putStrLn (name <> ": a hit of cached allocates " <> show perHit <> " bytes")
  (perHit <= hitBytes) `shouldBe` True

spec :: Spec
spec = describe "a hit" $ do
  itNeeds [Table] "allocates next to nothing when it is looked up in IO" $ do
    cache <- newCache (config 16)
    _ <- store cache (1 :: Int) (10 :: Int)
    let loop :: Int -> IO ()
        loop 0 = pure ()
        loop n = peek cache 1 >>= \found -> found `seq` loop (n - 1)
    loop 1000
    before <- getAllocationCounter
    loop hits
    after <- getAllocationCounter
    let perHit = (before - after) `div` fromIntegral hits
    putStrLn ("a hit of peek allocates " <> show perHit <> " bytes")
    (perHit <= 32) `shouldBe` True

  describe "allocates little when it is looked up by a program, beyond the program's own loop" $ do
    itNeeds [Table, Cached] "with threads, and a shared cache" $ do
      (cache, loads) <- withHit newSharedCache
      runThreads (measured cache loads) () >>= bounded "threads, shared cache"

    itNeeds [Table, Cached] "with delimited parts, and a shared cache" $ do
      (cache, loads) <- withHit newSharedCache
      (fst <$> runDelimited (measured cache loads) ()) >>= bounded "delimited, shared cache"

    itNeeds [Table, Cached] "with delimited parts, and a cache of one core" $ do
      (cache, loads) <- withHit newCache
      (fst <$> runDelimited (measured cache loads) ()) >>= bounded "delimited, cache of one core"

    itNeeds [Table, Cached] "with compiled parts, and a shared cache" $ do
      (cache, loads) <- withHit newSharedCache
      (fst <$> runCompiled (measured cache loads) ()) >>= bounded "compiled, shared cache"

    itNeeds [Table, Cached] "with compiled parts, and a cache of one core" $ do
      (cache, loads) <- withHit newCache
      (fst <$> runCompiled (measured cache loads) ()) >>= bounded "compiled, cache of one core"
