{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | What encloses a loop costs each iteration of it: a part enters a scope, a shield, a deadline, tryA or a group once, as an
-- inbox's or a listener's is, and runs a loop in it for its life, and the loop's iterations cost what they do outside. Each
-- case runs the same loop bare and enclosed, and compares the bytes that an iteration allocates, which are the difference
-- between a long run and a short one. A loop that gives way at each step is one case, and a loop that waits at an inbox for a
-- part that posts to it is the other. The suite runs with @-T@ ("-with-rtsopts" in its cabal stanza) so that allocation is
-- counted. With @SCOPE_COST_REPORT@ set, each case says what it measured, on stderr.
--
-- A part of "Network.Streaming.Delimited" that waits captures its stack, up to the scheduler's prompt, so that each scope around
-- its loop adds the frame that runs the scope's release to every wait: one pointer, 16 bytes, which is under the tolerance. A
-- shield, a deadline, tryA and a part of a group add what they keep on the stack.
--
-- The schedulers that run every part on one thread take a turn of no length, so that every step is a point where the part
-- gives way.
module ScopeCostSpec (spec) where

import Control.Arrow (Arrow, ArrowChoice, (>>>), (^>>))
import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, tryReadMVar)
import Control.Monad (forM_, void, when)
import Data.Word (Word64)
import GHC.Stats (allocated_bytes, getRTSStats)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, tap, timesWith, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Internal.Scheduler (Turn (..))
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)
import System.Mem (performMinorGC)

-- | What the cases' programs need of an arrow.
type Program = forall arr. (ArrowChoice arr, Annotate arr, Concurrent arr, GiveWay arr, Scope arr, Within arr, Inboxes arr (Local arr)) => Int -> arr () ()

-- | An arrow, how it runs a program for the unit, with the number of iterations that it is given, what a scope may add to
-- what a part's wait copies, and the most that a bare loop may allocate each iteration.
data Runner = Runner
  { runnerName :: String,
    runnerRun :: Program -> Int -> IO (),
    -- | What the frames of one scope add to each wait, in bytes: the one pointer that the return frame of a scope holds, in
    -- the arrow whose parts capture their stack when they wait, and nothing in the others.
    runnerFrame :: Double,
    runnerCeilings :: Ceilings
  }

-- | The most that a bare loop allocates each iteration, in bytes, that gives way at each step and that waits at an inbox: what
-- the arrow allocated when the ceiling was set, and a tolerance. A change that makes a bare loop dearer says so here, as a
-- change that makes it cheaper takes the ceiling down. The ceilings of Delimited hold the catch at the base of the program, which
-- runs the releases that a failure of the first part finds, a frame of 16 bytes that the first part's waits capture.
data Ceilings = Ceilings {yieldingCeiling :: Double, exchangingCeiling :: Double}

runners :: [Runner]
runners =
  [ Runner "threads" (\program count -> void (runThreads (program count) ())) 0 (Ceilings 304 1488),
    Runner "delimited" (\program count -> void (runDelimitedTurn (Turn 0) (program count) ())) 16 (Ceilings 528 1920),
    Runner "compiled" (\program count -> void (runCompiledTurn (Turn 0) (program count) ())) 0 (Ceilings 488 1856),
    Runner "loop" (\program count -> void (runLoop (program count) ())) 0 (Ceilings 1584 5976)
  ]

-- | The most that an iteration enclosed may allocate more than one outside, in bytes: a few words, for the noise of the
-- count, which is none in practice.
tolerance :: Double
tolerance = 24

-- | Marks a case of an arrow whose parts capture their stack when they wait, which copies what the loop is enclosed in: the catch
-- of a deadline or of tryA, and what follows a part's body in its group. The mark comes off when what encloses a loop is not
-- copied, and a case that then passes unexpectedly fails the suite.
copied :: String -> String -> Spec -> Spec
copied name reason = if name == "delimited" then xfail ("delimited: " <> reason) else id

spec :: Spec
spec = do
  describe "a scope" $ forM_ runners $ \runner -> describe (runnerName runner) $ do
    let name = runnerName runner
    it "costs a loop that gives way at each step no allocation in it" $
      costsAtMost runner "scope, gives way" tolerance yielding (\count -> scoped (yielding count))

    it "costs a loop that waits at an inbox no allocation in it" $
      costsAtMost runner "scope, inbox" tolerance exchanging (exchangingIn scoped)

    when (name == "delimited") $
      it "costs a loop that waits at an inbox that a part on another core posts to no allocation in it" $ do
        cores <- getNumCapabilities
        when (cores < 2) (setNumCapabilities 2)
        outside <- perIteration (void . acrossCores id)
        inside <- perIteration (void . acrossCores scoped)
        reported name "scope, another core" outside inside
        atMostMore tolerance outside inside

    -- Each scope adds the frame of one pointer to a wait, and nothing else: a frame that grows shows here, eight times over.
    it "costs a loop in eight nested scopes no more than a frame for each" $
      costsAtMost runner "eight scopes" (tolerance + runnerFrame runner * 8) yielding (\count -> iterate scoped (yielding count) !! 8)

  describe "what else encloses a loop" $ forM_ runners $ \runner -> describe (runnerName runner) $ do
    let name = runnerName runner
    it "costs a loop in a shield no allocation in it" $
      costsAtMost runner "shield" tolerance yielding (shield . yielding)

    copied name "a part that waits captures the catch of a deadline, which a deadline adds to each wait, until stage C of the frame-free scopes" $
      it "costs a loop in a deadline no allocation in it" $
        costsAtMost runner "deadline" tolerance yielding (\count -> within (secs 3600) (yielding count) >>> done)

    -- The same cost as a deadline that the program fixes.
    xfail "withinIn is not there yet" $
      it "costs a loop in a deadline that the input gives no allocation in it" $
        costsAtMost runner "deadline, input" tolerance yielding (\count -> (secs 3600,) ^>> withinIn (yielding count) >>> done)

    copied name "a part that waits captures the catch of tryA, which tryA adds to each wait, until stage C of the frame-free scopes" $
      it "costs a loop in tryA no allocation in it" $
        costsAtMost runner "tryA" tolerance yielding (\count -> tryA (yielding count) >>> done)

    copied name "a part that waits captures the catch and what follows the body of the part of a group, which a part adds to each wait, until stage B of the frame-free scopes" $
      it "costs a loop in a part of a group no allocation in it" $
        costsAtMost runner "group" tolerance yielding (\count -> together [yielding count, done])

  describe "a bare loop" $ forM_ runners $ \runner -> describe (runnerName runner) $ do
    it "that gives way at each step allocates no more than it did" $
      allocatesAtMost runner "bare, gives way" (yieldingCeiling (runnerCeilings runner)) yielding

    it "that waits at an inbox allocates no more than it did" $
      allocatesAtMost runner "bare, inbox" (exchangingCeiling (runnerCeilings runner)) exchanging

-- | The enclosed loop allocates no more each iteration than the bare one, but for the allowance.
costsAtMost :: Runner -> String -> Double -> Program -> Program -> IO ()
costsAtMost runner label allowance bare enclosed = do
  outside <- perIteration (runnerRun runner bare)
  inside <- perIteration (runnerRun runner enclosed)
  reported (runnerName runner) label outside inside
  atMostMore allowance outside inside

-- | The bare loop allocates no more each iteration than the ceiling.
allocatesAtMost :: Runner -> String -> Double -> Program -> IO ()
allocatesAtMost runner label ceiling' bare = do
  bytes <- perIteration (runnerRun runner bare)
  reported (runnerName runner) label bytes bytes
  when (bytes > ceiling' + tolerance) $
    failTest ("an iteration allocates " <> show (round bytes :: Int) <> " bytes, where the ceiling is " <> show (round ceiling' :: Int))

-- | Says what the case measured, if the run asks for it.
reported :: String -> String -> Double -> Double -> IO ()
reported name label outside inside =
  lookupEnv "SCOPE_COST_REPORT" >>= \asked ->
    when (asked /= Nothing) (hPutStrLn stderr ("scope-cost " <> name <> " | " <> label <> " | outside " <> show (round outside :: Int) <> " | inside " <> show (round inside :: Int)))

-- | The arrow in a scope that acquires nothing and releases nothing.
scoped :: (Arrow arr, Scope arr) => arr a () -> arr a ()
scoped body = scope done done (snd ^>> body)

-- | A loop of this many iterations, which gives way once in each.
yielding :: (ArrowChoice arr, Annotate arr, GiveWay arr) => Int -> arr () ()
yielding count = timesWith count giveWay

-- | A part that collects this many values from the inbox.
collecting :: (ArrowChoice arr, Annotate arr, Inboxes arr r) => Int -> arr (Inbox arr "numbers" r Int) ()
collecting count = timesWith count (tap (collect >>> done)) >>> done

-- | A part that posts this many values to the address.
posting :: (ArrowChoice arr, Annotate arr, Inboxes arr r) => Int -> arr (Address "numbers" r Int) ()
posting count = timesWith count (tap ((,1) ^>> post >>> done)) >>> done

-- | A part that collects this many values, which another part posts, in a group on one core.
exchanging :: (ArrowChoice arr, Annotate arr, Concurrent arr, Inboxes arr (Local arr)) => Int -> arr () ()
exchanging = exchangingIn id

-- | 'exchanging', with the part that collects run by the function.
exchangingIn :: forall arr. (ArrowChoice arr, Annotate arr, Concurrent arr, Inboxes arr (Local arr)) => (arr (Inbox arr "numbers" (Local arr) Int) () -> arr (Inbox arr "numbers" (Local arr) Int) ()) -> Int -> arr () ()
exchangingIn around count = inbox @arr @(Local arr) @"numbers" >>> together [fst ^>> around (collecting count), snd ^>> posting count]

-- | The same on two cores, in an inbox of any core: the part that collects makes the inbox, and gives its address to the
-- other core, which posts to it.
acrossCores :: (forall arr a. (Arrow arr, Scope arr) => arr a () -> arr a ()) -> Int -> IO [()]
acrossCores around count = do
  shelf <- newEmptyMVar :: IO (MVar (Address "numbers" 'Anywhere Int))
  runCoresTurn (Turn 0) 2 (pure shelf) $ \shared core ->
    if core == 0
      then inbox @_ @'Anywhere @"numbers" >>> tap (now (putMVar shared . snd)) >>> fst ^>> around (collecting count)
      else untilJust (tryReadMVar shared) >>> posting count

-- | The bytes that an iteration allocates: the difference between a long run and a short one, after a run that loads the code.
perIteration :: (Int -> IO ()) -> IO Double
perIteration run = do
  run 1000
  few <- allocatedBy (run 20000)
  many <- allocatedBy (run 120000)
  pure ((fromIntegral many - fromIntegral few) / 100000)

-- | The bytes that the action allocated: the runtime counts a nursery when it is collected, so a collection is made on both
-- sides.
allocatedBy :: IO () -> IO Word64
allocatedBy action = do
  performMinorGC
  before <- allocated_bytes <$> getRTSStats
  action
  performMinorGC
  after <- allocated_bytes <$> getRTSStats
  pure (after - before)

-- | The iteration inside allocates no more than the one outside it, but for the allowance: the difference is of whole words,
-- whatever the count of a long run says of it.
atMostMore :: Double -> Double -> Double -> IO ()
atMostMore allowance outside inside =
  if fromIntegral (round ((inside - outside) / 8) * 8 :: Int) <= allowance
    then pure ()
    else failTest ("an iteration allocates " <> show (round inside :: Int) <> " bytes enclosed, where it allocates " <> show (round outside :: Int) <> " bare")
