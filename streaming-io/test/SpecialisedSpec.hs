{-# LANGUAGE RankNTypes #-}

-- The test measures allocation per arrow step, so its pure and IO steps must stay separate.
{- HLINT ignore "Fuse into now" -}

-- | Checks that a program written for any arrow is specialised to the arrow that runs it. A step called through a class
-- dictionary is an unknown call that allocates a thunk, so an unspecialised step costs tens of bytes.
--
-- The program is an overloaded recursive loop, which GHC does not inline, like a server's functions. Each runner runs it,
-- and the allocation per iteration is the difference between a long run and a short one. "Network.Streaming.Delimited"
-- runs programs of type @forall s. Delimited s a b@, and their calls are specialised only when the runner is inlined.
-- The suite runs with @-T@ ("-with-rtsopts" in its cabal stanza) so that allocation is counted.
module SpecialisedSpec (spec) where

import Control.Arrow (ArrowChoice, arr, first, returnA, (>>>), (^>>), (|||))
import Control.Monad (forM_, void)
import Data.Word (Word64)
import GHC.Stats (allocated_bytes, getRTSStats)
import Network.Streaming.Classes (Effect (..))
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Threads
import Skeletest
import System.Mem (performMinorGC)

-- | An arrow, and how it runs the loop this many times.
data Runner = Runner String (Int -> IO ())

runners :: [Runner]
runners =
  [ Runner "threads" (\count -> void (runThreads (counting count) ())),
    Runner "delimited, by runDelimited" (\count -> void (runDelimited (counting count) ())),
    Runner "delimited, by runCores" (\count -> void (runCores 1 (pure ()) (\_ _ -> counting count))),
    Runner "compiled" (\count -> void (runCompiled (counting count) ()))
  ]

-- | The loop, from the number of iterations: a step of each kind that a program is made of, and a choice that is the loop's
-- end. It is overloaded and recursive, so that it is not inlined: it is specialised where it is called, or it is not.
countdown :: (ArrowChoice arr, Effect arr) => arr Int Int
countdown = (\n -> if n <= 0 then Left n else Right (n, n)) ^>> (returnA ||| (first (arr (subtract 1)) >>> fst ^>> now pure >>> countdown))

counting :: (ArrowChoice arr, Effect arr) => Int -> arr () Int
counting count = const count ^>> countdown

spec :: Spec
spec = describe "a program written for any arrow" $ forM_ runners $ \(Runner name run) ->
  it ("is specialised to the arrow, so that its steps allocate under 64 bytes each: " <> name) $ do
    -- The first run is for the code to be loaded and the nurseries to be as they will be.
    run 1000
    few <- allocatedBy (run 1000)
    many <- allocatedBy (run 101000)
    -- The allocation of a specialised loop is so small that the difference of the runs can be below zero.
    let perIteration = (fromIntegral many - fromIntegral few) / 100000 :: Double
    if perIteration < 64
      then pure ()
      else failTest ("an iteration allocates " <> show (round perIteration :: Int) <> " bytes")

-- | The bytes that the action allocated: the runtime counts a nursery when it is collected, so a collection is made on both sides.
allocatedBy :: IO () -> IO Word64
allocatedBy action = do
  performMinorGC
  before <- allocated_bytes <$> getRTSStats
  action
  performMinorGC
  after <- allocated_bytes <$> getRTSStats
  pure (after - before)
