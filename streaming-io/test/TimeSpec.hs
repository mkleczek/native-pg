{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

-- | Deadlines and the timers that keep them: the same programs for every arrow that has time, and the timer
-- wheel by itself, in time that the test makes up.
module TimeSpec (spec) where

import Control.Arrow (Arrow, ArrowChoice, arr, returnA, second, (&&&), (>>>), (>>^), (^>>))
import Control.Monad (forM, forM_, void, when)
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, newIORef, readIORef)
import Data.Word (Word64)
import qualified Network.Socket as Socket
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, keeping, localTicket, releaseWithin, sleep, tap, times)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Internal.Timers
import Network.Streaming.Loop
import Network.Streaming.Pool
import Network.Streaming.Threads
import Skeletest
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Types (Fd (..))
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Exchange arr (Local arr), Scope arr, Race arr, Time arr, Within arr, Annotate arr)

type Program a = forall arr. (Rich arr) => arr () a

data Runner = Runner String (forall a. Program a -> IO a)

-- | A program that parts of several cores share a pool in.
type PoolProgram a = forall arr. (Rich arr, Exchange arr 'Anywhere) => arr () a

data PoolRunner = PoolRunner String (forall a. PoolProgram a -> IO a)

poolRunners :: [PoolRunner]
poolRunners =
  [ PoolRunner "threads" onThreadsPool,
    PoolRunner "delimited" onDelimitedPool,
    PoolRunner "compiled" onCompiledPool
  ]

onThreads :: Program a -> IO a
onThreads program = runThreads program ()

onThreadsPool :: PoolProgram a -> IO a
onThreadsPool program = runThreads program ()

onDelimitedPool :: PoolProgram a -> IO a
onDelimitedPool program = fst <$> runDelimited program ()

onCompiledPool :: PoolProgram a -> IO a
onCompiledPool program = fst <$> runCompiled program ()

runners :: [Runner]
runners =
  [ Runner "threads" onThreads,
    Runner "delimited" (\program -> fst <$> runDelimited program ()),
    Runner "compiled" (\program -> fst <$> runCompiled program ()),
    Runner "loop" (\program -> fst <$> runLoop program ())
  ]

spec :: Spec
spec = describe "time" $ do
  forM_ runners $ \(Runner name run) -> describe name $ do
    it "gives Nothing when the deadline passes first, and releases what the body held" $ do
      noted <- newLog
      started <- monotonicNanoseconds
      result <- run (within (millis 30) (bracketed noted "r" returnA waitsForEver))
      ended <- monotonicNanoseconds
      result `shouldBe` Nothing
      readLog noted `shouldReturn` ["acquire r", "release r: cancelled"]
      (ended - started >= 30000000) `shouldBe` True
      (ended - started < 2000000000) `shouldBe` True

    -- A part of a Loop that waits for a ticket is held by the ticket and is not cancelled there.
    when (name /= "loop") $ it "gives Nothing when the deadline passes while the part waits for a ticket of the arrow's own core, and releases what the body held" $ do
      noted <- newLog
      started <- monotonicNanoseconds
      result <- run (within (millis 30) (bracketed noted "r" returnA waitsForTicket))
      ended <- monotonicNanoseconds
      result `shouldBe` Nothing
      readLog noted `shouldReturn` ["acquire r", "release r: cancelled"]
      (ended - started >= 30000000) `shouldBe` True
      (ended - started < 2000000000) `shouldBe` True

    it "gives the body's result when it ends first, and leaves no deadline behind" $ do
      run (within (secs 10) (arr (const 'x'))) `shouldReturn` Just 'x'
      run (within (secs 10) (sleep (millis 5) >>^ const 'y')) `shouldReturn` Just 'y'

    it "gives the outer deadline Nothing when it passes while an inner one is active" $ do
      run (within (millis 30) (within (secs 10) waitsForEver)) `shouldReturn` Nothing
      run (within (secs 10) (within (millis 30) waitsForEver)) `shouldReturn` Just Nothing

    it "tells the deadlines apart: the inner one passes, and the part goes on until the outer does" $ do
      noted <- newLog
      result <- run (within (millis 120) (within (millis 20) waitsForEver >>> noting noted "inner ended" >>> waitsForEver))
      result `shouldBe` Nothing
      readLog noted `shouldReturn` ["inner ended"]

    it "lets a cancellation from outside pass through it" $ do
      noted <- newLog
      winner' <- run (race (within (secs 10) waitsForEver >>> noting noted "within returned") giveWayAndEnd)
      either (const "first") (const "second") winner' `shouldBe` "second"
      readLog noted `shouldReturn` []

    it "fires a deadline that is inside a shield, whatever outside cancels" $ do
      noted <- newLog
      started <- monotonicNanoseconds
      winner' <-
        run $
          race
            (scope returnA (releaseWithin (millis 30) waitsForEver >>> noting noted "release ended" >>> done) (snd ^>> waitsForEver))
            giveWayAndEnd
      ended <- monotonicNanoseconds
      either (const "first") (const "second") winner' `shouldBe` "second"
      readLog noted `shouldReturn` ["release ended"]
      -- The release waited for its own deadline, and not for ever.
      (ended - started >= 30000000) `shouldBe` True
      (ended - started < 2000000000) `shouldBe` True

    it "sleeps for the time, which a cancellation ends" $ do
      started <- monotonicNanoseconds
      run (sleep (millis 20)) `shouldReturn` ()
      slept <- monotonicNanoseconds
      (slept - started >= 20000000) `shouldBe` True
      winner' <- run (race (sleep (secs 30)) giveWayAndEnd)
      either (const "first") (const "second") winner' `shouldBe` "second"
      ended <- monotonicNanoseconds
      (ended - slept < 5000000000) `shouldBe` True

    it "reads a clock that goes forward, and that a sleep moves" $ do
      before <- newIORef (Instant 0)
      later <- newIORef (Instant 0)
      run (clock >>> now (atomicWriteIORef before) >>> sleep (millis 5) >>> clock >>> now (atomicWriteIORef later))
      Instant earlier <- readIORef before
      Instant afterwards <- readIORef later
      (afterwards - earlier >= 5000000) `shouldBe` True

    when (name /= "loop") $ it "stops 100,000 deadlines that were set one after the other, none of which passes" $ do
      result <- withinTime (run (times 100000 (within (secs 10) returnA >>> done) >>^ const 'z'))
      result `shouldBe` Just 'z'

    -- The deadline is the first of the input, in front of what the body takes: one program, built once, has the deadline
    -- of each run.
    xfail "withinIn is not there yet" $ describe "a deadline that the input gives" $ do
      it "gives Nothing when the deadline passes first, and releases what the body held" $ do
        noted <- newLog
        started <- monotonicNanoseconds
        result <- run (given (millis 30) (bracketed noted "r" returnA waitsForEver))
        ended <- monotonicNanoseconds
        result `shouldBe` Nothing
        readLog noted `shouldReturn` ["acquire r", "release r: cancelled"]
        (ended - started >= 30000000) `shouldBe` True
        (ended - started < 2000000000) `shouldBe` True

      it "gives the body's result when it ends first, and leaves no deadline behind" $ do
        run (given (secs 10) (arr (const 'x'))) `shouldReturn` Just 'x'
        run (given (secs 10) (sleep (millis 5) >>^ const 'y')) `shouldReturn` Just 'y'

      it "takes the deadline of each run from the input, in one program that is built once" $
        run bothDeadlines `shouldReturn` (Nothing, Just 'x')

      it "gives the outer deadline Nothing when it passes while an inner one is active, whichever way each is given" $ do
        run (given (millis 30) (given (secs 10) waitsForEver)) `shouldReturn` Nothing
        run (given (secs 10) (given (millis 30) waitsForEver)) `shouldReturn` Just Nothing
        run (within (millis 30) (given (secs 10) waitsForEver)) `shouldReturn` Nothing
        run (given (secs 10) (within (millis 30) waitsForEver)) `shouldReturn` Just Nothing

      it "tells the deadlines apart: the inner one passes, and the part goes on until the outer does" $ do
        noted <- newLog
        result <- run (within (millis 120) (given (millis 20) waitsForEver >>> noting noted "inner ended" >>> waitsForEver))
        result `shouldBe` Nothing
        readLog noted `shouldReturn` ["inner ended"]

      it "lets a cancellation from outside pass through it" $ do
        noted <- newLog
        winner' <- run (race (given (secs 10) waitsForEver >>> noting noted "within returned") giveWayAndEnd)
        either (const "first") (const "second") winner' `shouldBe` "second"
        readLog noted `shouldReturn` []

      it "fires a deadline that is inside a shield, whatever outside cancels" $ do
        noted <- newLog
        started <- monotonicNanoseconds
        winner' <-
          run $
            race
              (scope returnA (done >>> shield (given (millis 30) waitsForEver) >>> noting noted "release ended" >>> done) (snd ^>> waitsForEver))
              giveWayAndEnd
        ended <- monotonicNanoseconds
        either (const "first") (const "second") winner' `shouldBe` "second"
        readLog noted `shouldReturn` ["release ended"]
        (ended - started >= 30000000) `shouldBe` True
        (ended - started < 2000000000) `shouldBe` True

      when (name /= "loop") $ it "stops 100,000 deadlines that were set one after the other, none of which passes" $ do
        result <- withinTime (run (times 100000 (given (secs 10) returnA >>> done) >>^ const 'z'))
        result `shouldBe` Just 'z'

  describe "the pool" $
    forM_ poolRunners $ \(PoolRunner name run) -> do
      it ("gives Nothing when the deadline passes while the part waits for a ticket of any core, and releases what the body held (" <> name <> ")") $ do
        noted <- newLog
        result <- run (within (millis 30) (bracketed noted "r" returnA waitsForAnywhereTicket))
        result `shouldBe` Nothing
        readLog noted `shouldReturn` ["acquire r", "release r: cancelled"]

      it ("gives a part that waits for a connection too long Nothing, and keeps no place for it (" <> name <> ")") $ do
        pool <- newPool [7 :: Int]
        -- The part holds the only connection, and asks for another with a deadline.
        taken <- run (checkout pool >>> keeping (checkoutWithin (millis 30) pool) >>> second (release pool) >>^ fst)
        taken `shouldBe` Nothing
        -- It was given back to the pool, and not to the place that was given up.
        free pool `shouldReturn` [7]

  describe "the virtual clock of a simulation" $ do
    it "runs a wait of an hour at once, and the clock is where it was waiting until" $ do
      started <- monotonicNanoseconds
      outcome <- runSimulation 1 1000 (sleep (secs 3600) >>> clock) ()
      ended <- monotonicNanoseconds
      simulatedOutcome outcome `shouldBe` Completed (Instant (3600 * 1000000000))
      (ended - started < 1000000000) `shouldBe` True

    it "wakes sleepers in the order of their deadlines, at the same instants, for every seed" $
      forM_ [1 .. 20] $ \seed -> do
        noted <- newLog
        outcome <- runSimulation seed 100000 (together [sleeper noted "three" 3, sleeper noted "one" 1, sleeper noted "two" 2]) ()
        simulatedOutcome outcome `shouldBe` Completed ()
        readLog noted `shouldReturn` ["one at 1 s", "two at 2 s", "three at 3 s"]

    it "gives a deadline Nothing at its instant, for every seed, and releases what the body held" $
      forM_ [1 .. 20] $ \seed -> do
        noted <- newLog
        outcome <- runSimulation seed 100000 (within (secs 5) (bracketed noted "r" returnA (sleep (secs 100))) >>> done >>> clock >>^ \(Instant at) -> at) ()
        simulatedOutcome outcome `shouldBe` Completed (5 * 1000000000)
        readLog noted `shouldReturn` ["acquire r", "release r: cancelled"]

    it "gives the body's result when it ends before the deadline, and then the clock is where the body left it" $
      forM_ [1 .. 20] $ \seed -> do
        outcome <- runSimulation seed 100000 (within (secs 100) (sleep (secs 5) >>^ const 'x') >>> done >>> clock) ()
        simulatedOutcome outcome `shouldBe` Completed (Instant (5 * 1000000000))

    it "gives the outer deadline Nothing when it passes first, and the inner one when that does" $
      forM_ [1 .. 20] $ \seed -> do
        outer <- runSimulation seed 100000 (within (secs 3) (within (secs 10) (sleep (secs 100)))) ()
        simulatedOutcome outer `shouldBe` Completed Nothing
        inner <- runSimulation seed 100000 (within (secs 10) (within (secs 3) (sleep (secs 100)))) ()
        simulatedOutcome inner `shouldBe` Completed (Just Nothing)

    xfail "withinIn is not there yet" $ describe "a deadline that the input gives" $ do
      it "gives Nothing at its instant, for every seed, and releases what the body held" $
        forM_ [1 .. 20] $ \seed -> do
          noted <- newLog
          outcome <- runSimulation seed 100000 (given (secs 5) (bracketed noted "r" returnA (sleep (secs 100))) >>> done >>> clock >>^ \(Instant at) -> at) ()
          simulatedOutcome outcome `shouldBe` Completed (5 * 1000000000)
          readLog noted `shouldReturn` ["acquire r", "release r: cancelled"]

      it "gives the body's result when it ends before the deadline, and then the clock is where the body left it" $
        forM_ [1 .. 20] $ \seed -> do
          outcome <- runSimulation seed 100000 (given (secs 100) (sleep (secs 5) >>^ const 'x') >>> done >>> clock) ()
          simulatedOutcome outcome `shouldBe` Completed (Instant (5 * 1000000000))

      it "gives the deadline of each run from the input, in one program that is built once" $
        forM_ [1 .. 20] $ \seed -> do
          outcome <- runSimulation seed 100000 bothDeadlines ()
          simulatedOutcome outcome `shouldBe` Completed (Nothing, Just 'x')

    it "gives the same run for the same seed, and finds that a part waits for ever when nothing will end its wait" $ do
      first' <- simulatedTrace <$> runSimulation 5 100000 (together [sleep (secs 2), sleep (secs 1)]) ()
      again <- simulatedTrace <$> runSimulation 5 100000 (together [sleep (secs 2), sleep (secs 1)]) ()
      again `shouldBe` first'
      outcome <- runSimulation 1 1000 (localTicket >>> fst ^>> await :: Loop () ()) ()
      simulatedOutcome outcome `shouldBe` Deadlocked [0]

    it "reads the clock of the run when it is not a simulation" $ do
      started <- monotonicNanoseconds
      (Instant after', _) <- runLoop (sleep (millis 20) >>> clock) ()
      ended <- monotonicNanoseconds
      (after' >= started + 20000000) `shouldBe` True
      (ended - started >= 20000000) `shouldBe` True

  describe "the timer wheel" $ do
    it "fires a timer at its millisecond and not before, in order, from every level" $ do
      let ms = 1000000 :: Word64
      wheel <- newWheel 0
      fired <- newIORef []
      -- Near, far (seconds ahead), and beyond (more than about 17 minutes).
      let deadlines = [5 * ms, 7 * ms, 900 * ms, 1500 * ms, 20 * 1000 * ms, 30 * 60 * 1000 * ms, 90 * 60 * 1000 * ms]
      forM_ (reverse deadlines) $ \deadline -> addTimer wheel deadline (atomicModifyIORef' fired (\log' -> (deadline : log', ())))
      pending wheel `shouldReturn` length deadlines
      nextDeadline wheel `shouldReturn` Just (5 * ms)
      forM_ deadlines $ \deadline -> do
        expire wheel (deadline - 1)
        readIORef fired >>= \seen -> (deadline `elem` seen) `shouldBe` False
        expire wheel deadline
        readIORef fired >>= \seen -> (deadline `elem` seen) `shouldBe` True
      reverse <$> readIORef fired `shouldReturn` deadlines
      pending wheel `shouldReturn` 0
      nextDeadline wheel `shouldReturn` Nothing

    it "doesn't fire a timer that was cancelled, and counts it as gone" $ do
      wheel <- newWheel 0
      fired <- newIORef (0 :: Int)
      keep <- addTimer wheel 10000000 (atomicModifyIORef' fired (\n -> (n + 1, ())))
      gone <- addTimer wheel 20000000 (atomicModifyIORef' fired (\n -> (n + 1, ())))
      cancelTimer wheel gone `shouldReturn` True
      cancelTimer wheel gone `shouldReturn` False
      pending wheel `shouldReturn` 1
      expire wheel 1000000000
      readIORef fired `shouldReturn` 1
      cancelTimer wheel keep `shouldReturn` False

    it "inserts and cancels 100,000 timers, and holds none of them after" $ do
      wheel <- newWheel 0
      let ms = 1000000 :: Word64
      timers <- forM [1 .. 100000] $ \n -> addTimer wheel ((n `mod` 5000) * ms + 1) (pure ())
      pending wheel `shouldReturn` 100000
      mapM_ (cancelTimer wheel) timers
      pending wheel `shouldReturn` 0
      nextDeadline wheel `shouldReturn` Nothing
      expire wheel (10000 * ms)

    it "fires a timer that a timer adds at the next expiry at the soonest" $ do
      wheel <- newWheel 0
      fired <- newIORef []
      void (addTimer wheel 1000000 (void (addTimer wheel 1000001 (atomicModifyIORef' fired (\log' -> ("second" : log', ()))))))
      expire wheel 1000000
      readIORef fired `shouldReturn` []
      expire wheel 2000000
      readIORef fired `shouldReturn` ["second"]

-- | The body with a deadline that its input gives.
given :: (Rich arr) => Duration -> arr () b -> arr () (Maybe b)
given duration body = const (duration, ()) ^>> withinIn body

-- | One body, whose deadline is the input's, which a short deadline cuts short and a long one doesn't, in one program.
bothDeadlines :: (Rich arr) => arr () (Maybe Char, Maybe Char)
bothDeadlines = (const (millis 10, ()) ^>> once) &&& (const (secs 10, ()) ^>> once)
  where
    once = withinIn (sleep (millis 100) >>^ const 'x')

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

note :: Log -> String -> IO ()
note noted entry = atomicModifyIORef' noted (\entries -> (entry : entries, ()))

noting :: (Arrow arr, Effect arr) => Log -> String -> arr a a
noting noted entry = tap (now (const (note noted entry)))

bracketed :: (Rich arr) => Log -> String -> arr () () -> arr () b -> arr () b
bracketed noted name work body =
  scope
    (noting noted ("acquire " <> name))
    (now (\(_, exit) -> note noted ("release " <> name <> ": " <> describeExit exit)) >>> work)
    (snd ^>> body)

describeExit :: Exit -> String
describeExit = \case
  Done -> "done"
  Failed failure -> "failed " <> show failure
  Cancelled' -> "cancelled"

-- | A wait that nothing ends.
-- | A wait for a ticket of the arrow's own core that nothing fulfils, and for one of any core.
waitsForTicket :: (Rich arr) => arr a ()
waitsForTicket = done >>> localTicket >>> fst ^>> await

waitsForAnywhereTicket :: forall arr a. (Rich arr, Exchange arr 'Anywhere) => arr a ()
waitsForAnywhereTicket = done >>> ticket @arr @'Anywhere >>> fst ^>> (await :: arr (Future arr 'Anywhere ()) ())

waitsForEver :: (Rich arr) => arr a ()
waitsForEver = now (const readableNever) >>> block

readableNever :: IO Event
readableNever = do
  (one, other) <- Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol
  atomicModifyIORef' kept (\sockets -> (one : other : sockets, ()))
  Readable . Fd <$> Socket.unsafeFdSocket one

kept :: IORef [Socket.Socket]
kept = unsafePerformIO (newIORef [])
{-# NOINLINE kept #-}

giveWayAndEnd :: (Rich arr) => arr () ()
giveWayAndEnd = giveWay

withinTime :: IO a -> IO (Maybe a)
withinTime = timeout 120000000

-- | A part that sleeps for the seconds, and notes at what time it woke.
sleeper :: (Arrow arr, Effect arr, Time arr, Block arr) => Log -> String -> Word64 -> arr () ()
sleeper noted name duration = sleep (secs duration) >>> clock >>> now (\(Instant at) -> note noted (name <> " at " <> show (at `div` 1000000000) <> " s"))
