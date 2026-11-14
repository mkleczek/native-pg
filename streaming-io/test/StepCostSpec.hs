{-# LANGUAGE RankNTypes #-}

-- | What a step costs, which doesn't depend on how many parts a program has, or on how deep in scopes a part is: the same
-- number of steps takes about the same time among thousands of parts as among a few, and deep in scopes as at the top.
-- Each case compares the two, as the allocation guard of the cursors does, with a margin wide enough for a noisy host: a
-- cost that grows with the number of parts or with the depth is a factor of tens, and not of one and a half.
--
-- The schedulers that run every part on one thread take a turn of no length, so that every step is a point where the
-- part gives way, and looks for a cancellation, as the steps of a part do when the other parts have been waiting.
module StepCostSpec (spec) where

import Control.Arrow (ArrowChoice, returnA, (>>>))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, try)
import Control.Monad (forM_, replicateM)
import Data.Word (Word64)
import Network.Streaming.Classes (Annotate, Concurrent (..), GiveWay (..), Within, monotonicNanoseconds, secs)
import qualified Network.Streaming.Classes as Classes (within)
import Network.Streaming.Combinators (done, times)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Internal.Scheduler (Turn (..))
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

-- | What the cases' programs need of an arrow.
type Program = forall arr. (ArrowChoice arr, GiveWay arr, Concurrent arr, Within arr, Annotate arr) => arr () ()

-- | What puts a part in many scopes: deadlines, or pairs, one of whose halves does nothing.
data Nesting = Deadlines | Pairs

-- | An arrow, how the part that is deep in scopes gets there, the steps of a run among parts and of a run in scopes, and how
-- it runs a program. The steps are numbers that make a run take the time of some hundred milliseconds, at the shallowest.
data Runner = Runner String Nesting Int Int (Program -> IO ())

-- | A deadline is a frame of the stack that a part of "Network.Streaming.Delimited" captures each time it gives way, which
-- is a cost of its own that grows with the depth, so its part is put in pairs. A deadline of "Network.Streaming.Loop" is a
-- race, whose parts wait for the instant: a run that ends them one by one takes time that grows as the square of the depth.
runners :: [Runner]
runners =
  [ Runner "threads" Deadlines (4096 * 96) (4096 * 320) onThreads,
    Runner "delimited" Pairs (4096 * 80) (4096 * 160) onDelimited,
    Runner "compiled" Deadlines (4096 * 80) (4096 * 190) onCompiled,
    Runner "loop" Pairs (4096 * 96) (4096 * 64) onLoop
  ]

-- | Each arrow runs a program for the unit: a function of its own, since a program is for any arrow.
onThreads, onDelimited, onCompiled, onLoop :: Program -> IO ()
onThreads program = runThreads program ()
onDelimited program = fst <$> runDelimitedTurn (Turn 0) program ()
onCompiled program = fst <$> runCompiledTurn (Turn 0) program ()
onLoop program = fst <$> runLoop program ()

spec :: Spec
spec = describe "a step" $ forM_ runners $ \(Runner name nesting amongSteps scopeSteps run) -> describe name $ do
  it "costs the same among thousands of parts as among a few" $ do
    -- The same number of steps in all: 4,096 parts that each give way a number of times, and 64 that give way 64 times as often.
    (few, many) <- leastOfThree (run (amongParts 64 amongSteps)) (run (amongParts 4096 amongSteps))
    atMostFourTimes few many

  it "costs the same deep in scopes as at the top" $ do
    (shallow, deep) <- leastOfThree (run (inScopes nesting 1 scopeSteps)) (run (inScopes nesting 1024 scopeSteps))
    atMostFourTimes shallow deep

-- | Parts that give way, this many steps in all, at once.
amongParts :: Int -> Int -> Program
amongParts parts steps = together (replicate parts (times (steps `div` parts) giveWay))

-- | A part that gives way this many times, in this many scopes: deadlines that are far off, or pairs.
inScopes :: Nesting -> Int -> Int -> Program
inScopes nesting depth steps = foldr (\_ inner -> nest nesting inner) (times steps giveWay) [1 .. depth]

-- | The arrow in one more scope.
nest :: (ArrowChoice arr, Concurrent arr, Within arr) => Nesting -> arr () () -> arr () ()
nest nesting inner = case nesting of
  Deadlines -> Classes.within (secs 3600) inner >>> done
  Pairs -> together [returnA, inner]

-- | The least time, over three runs of each, of one program and of the other, in nanoseconds. The runs alternate, so that
-- what the host does meanwhile is shared.
leastOfThree :: IO () -> IO () -> IO (Word64, Word64)
leastOfThree one other = do
  runs <- replicateM 3 ((,) <$> timed one <*> timed other)
  pure (minimum (map fst runs), minimum (map snd runs))

-- | How long the program takes, in nanoseconds. It runs on a thread of its own, which the waiting side gives up on: a
-- scheduler runs its parts masked, so a part that spins can't be interrupted.
timed :: IO () -> IO Word64
timed program = do
  result <- newEmptyMVar
  _ <- forkIO $ do
    started <- monotonicNanoseconds
    outcome <- try program
    ended <- monotonicNanoseconds
    putMVar result (either (\failure -> Left (show (failure :: SomeException))) (const (Right (ended - started))) outcome)
  timeout 60000000 (takeMVar result)
    >>= maybe (failTest "the program did not end in a minute") (either (failTest . ("the program failed: " <>)) pure)

-- | The time of the many is under four times the time of the few.
atMostFourTimes :: Word64 -> Word64 -> IO ()
atMostFourTimes few many =
  if many < 4 * few
    then pure ()
    else failTest ("the same steps took " <> show (many `div` 1000000) <> " ms, where they took " <> show (few `div` 1000000) <> " ms")
