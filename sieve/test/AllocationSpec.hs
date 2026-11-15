{-# LANGUAGE DataKinds #-}

-- | What a hit costs in allocation: a lookup that finds its key takes no lock and writes at most the visited byte, and it
-- allocates next to nothing, as an IO step and as a step of a program.
module AllocationSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Control.Monad (forM_)
import Data.Int (Int64)
import Data.Sieve
import GHC.Conc (getAllocationCounter)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, tap, times)
import Skeletest
import Support

-- | How many bytes each hit may allocate, besides what the loop of the program allocates for each of its turns.
hitBytes :: Int64
hitBytes = 160

hits :: Int
hits = 100000

-- | The bytes that the arrow allocates on the thread that runs it, which every arrow runs on the calling thread.
allocating :: (Runtime arr) => arr () () -> arr () Int64
allocating body = now (const getAllocationCounter) >>> tap (done >>> body) >>> now (\before -> (before -) <$> getAllocationCounter)

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

  forM_ setups $ \setup@(Setup name _ make _ _ run) -> describe name $
    itCase setup [Table, Cached] "allocates little when it is looked up by a program, beyond the program's own loop" $ do
      cache <- make (ints 16)
      loads <- counter
      _ <- run (valuesOf (tenTimes loads) cache [1])
      let hit = const (cache, 1) ^>> cached (tenTimes loads) accept >>> done
          baseline = const (cache, 1 :: Int) ^>> done
      _ <- run (allocating (times 1000 hit))
      withHit <- run (allocating (times hits hit))
      without <- run (allocating (times hits baseline))
      let perHit = (withHit - without) `div` fromIntegral hits
      putStrLn (name <> ": a hit of cached allocates " <> show perHit <> " bytes")
      (perHit <= hitBytes) `shouldBe` True
