{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | What a scope costs each iteration of a loop that runs inside it, which is nothing: a part enters a scope once, as an
-- inbox's or a listener's is, and runs a loop in it for its life, and the loop's iterations cost what they do outside it.
-- Each case runs the same loop bare and inside an empty scope, and compares the bytes that an iteration allocates, which
-- are the difference between a long run and a short one. A loop that gives way at each step is one case, and a loop that
-- waits at an inbox for a part that posts to it is the other. The suite runs with @-T@ ("-with-rtsopts" in its cabal
-- stanza) so that allocation is counted.
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
import System.Mem (performMinorGC)

-- | What the cases' programs need of an arrow.
type Program = forall arr. (ArrowChoice arr, Annotate arr, Concurrent arr, GiveWay arr, Scope arr, Inboxes arr (Local arr)) => Int -> arr () ()

-- | An arrow, whether a scope costs its loop anything in it, which is a known failure, and how it runs a program for the
-- unit, with the number of iterations that it is given.
data Runner = Runner String Bool (Program -> Int -> IO ())

runners :: [Runner]
runners =
  [ Runner "threads" False (\program count -> void (runThreads (program count) ())),
    Runner "delimited" True (\program count -> void (runDelimitedTurn (Turn 0) (program count) ())),
    Runner "compiled" False (\program count -> void (runCompiledTurn (Turn 0) (program count) ())),
    Runner "loop" False (\program count -> void (runLoop (program count) ()))
  ]

-- | The most that an iteration in a scope may allocate more than one outside, in bytes: a few words, for the noise of the
-- count, which is none in practice.
tolerance :: Double
tolerance = 24

-- | Marks a case of an arrow whose parts capture their stack when they wait, which holds the frames of their scopes: the
-- catch frame of the scope, and what follows its body, 12 words in all, are copied each time a part waits in the scope.
-- The mark comes off when a scope's frames are not copied, and a case that then passes unexpectedly fails the suite.
copied :: Bool -> Spec -> Spec
copied known = if known then xfail "delimited: a part that waits captures the frames of its scopes, which a scope adds to each wait" else id

spec :: Spec
spec = describe "a scope" $ forM_ runners $ \(Runner name costly run) -> describe name $ do
  copied costly $
    it "costs a loop that gives way at each step no allocation in it" $ do
      outside <- perIteration (run yielding)
      inside <- perIteration (run (\count -> scoped (yielding count)))
      atMostMore outside inside

  copied costly $
    it "costs a loop that waits at an inbox no allocation in it" $ do
      outside <- perIteration (run exchanging)
      inside <- perIteration (run (\count -> exchangingIn scoped count))
      atMostMore outside inside

  when (name == "delimited") $
    copied costly $
      it "costs a loop that waits at an inbox that a part on another core posts to no allocation in it" $ do
        cores <- getNumCapabilities
        when (cores < 2) (setNumCapabilities 2)
        outside <- perIteration (void . acrossCores id)
        inside <- perIteration (void . acrossCores scoped)
        atMostMore outside inside

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

-- | The iteration inside the scope allocates no more than the one outside it, but for the tolerance.
atMostMore :: Double -> Double -> IO ()
atMostMore outside inside =
  if inside - outside <= tolerance
    then pure ()
    else failTest ("an iteration allocates " <> show (round inside :: Int) <> " bytes in a scope, where it allocates " <> show (round outside :: Int) <> " outside one")
