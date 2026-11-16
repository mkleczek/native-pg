{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

-- | Deadlines and the timers that keep them: the same programs for every arrow that has time, and the timer
-- wheel by itself, in time that the test makes up.
module TimeSpec (spec) where

import Control.Arrow (Arrow, ArrowChoice, arr, returnA, second, (&&&), (>>>), (>>^), (^>>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (forM, forM_, replicateM, replicateM_, void, when)
import qualified Data.ByteString as B
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, newIORef, readIORef)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Stats (allocated_bytes, getRTSStats)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
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
import System.Mem (performMinorGC)
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

    -- The part that a descriptor woke runs in the round that the scheduler was in before it waited, and with a timer armed each round reads the
    -- clock: the time of that round is the time that the wait began at, which the clock arrow gives, and a grace, a deadline and a sleep count from.
    when (name /= "loop") $
      it "gives a part that a descriptor woke the time that it woke at, while a timer is armed" $ do
        (reader, writer) <- Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol
        event <- Readable . Fd <$> Socket.unsafeFdSocket reader
        written <- newIORef (0 :: Word64)
        void . forkIO $ do
          threadDelay 50000
          monotonicNanoseconds >>= atomicWriteIORef written
          SocketBytes.sendAll writer (B.singleton 120)
        result <- run (race (sleep (secs 3600)) (const event ^>> block >>> clock))
        wroteAt <- readIORef written
        case result of
          Right (Instant seen) -> (seen >= wroteAt) `shouldBe` True
          Left () -> failTest "the sleep of an hour ended first"

    when (name /= "loop") $ it "stops 100,000 deadlines that were set one after the other, none of which passes" $ do
      result <- withinTime (run (times 100000 (within (secs 10) returnA >>> done) >>^ const 'z'))
      result `shouldBe` Just 'z'

    -- The deadline is the first of the input, in front of what the body takes: one program, built once, has the deadline
    -- of each run.
    describe "a deadline that the input gives" $ do
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

    describe "a deadline that the input gives" $ do
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

    -- The scheduler asks for the next deadline each time it has nothing to run, so that a timer that is far off, as an idle timeout is,
    -- is in every idle step of a program that has one. Finding it allocates what the answer is, and not a cell for each slot that is
    -- looked at on the way.
    it "finds the next deadline of a wheel that has a timer far off without allocating for it" $ do
      let oneSecond = 1000000000 :: Word64
          calls = 1000 :: Int
      wheel <- newWheel 0
      void (addTimer wheel (30 * oneSecond) (pure ()))
      replicateM_ 10 (nextDeadline wheel)
      performMinorGC
      before <- allocated_bytes <$> getRTSStats
      replicateM_ calls (nextDeadline wheel)
      performMinorGC
      after <- allocated_bytes <$> getRTSStats
      let each = fromIntegral (after - before) / fromIntegral calls :: Double
      when (each > 128) $ failTest ("a call allocates " <> show (round each :: Int) <> " bytes")
      nextDeadline wheel `shouldReturn` Just (30 * oneSecond)

    -- It is called each time a scheduler has nothing to run, which with an idle timeout is each time a connection waits for a request, so
    -- that a timer that is far off costs no more than one that is near.
    it "finds the next deadline of a wheel that has a timer far off as fast as one that has a timer near" $ do
      let oneSecond = 1000000000 :: Word64
          calls = 20000 :: Int
          timeOf deadline = do
            wheel <- newWheel 0
            void (addTimer wheel deadline (pure ()))
            replicateM_ 100 (nextDeadline wheel)
            began <- getMonotonicTimeNSec
            replicateM_ calls (nextDeadline wheel)
            ended <- getMonotonicTimeNSec
            pure (ended - began)
      -- The least of five runs of each, so that a run that the machine was busy for doesn't count.
      near <- minimum <$> replicateM 5 (timeOf 1000000)
      far <- minimum <$> replicateM 5 (timeOf (30 * oneSecond))
      when (far > 10 * near + 5000000) $
        failTest ("a call takes " <> show (far `div` fromIntegral calls) <> " ns with a timer 30 seconds off, and " <> show (near `div` fromIntegral calls) <> " with one a millisecond off")

    -- A timer that was cancelled may still be what the wheel remembers as its nearest: the time that it gives is then at or before the
    -- nearest live one, and it is exact again once the clock has passed it.
    it "gives a time at or before the nearest live deadline after the nearest timer was cancelled, and the exact one once that time has passed" $ do
      let ms = 1000000 :: Word64
      wheel <- newWheel 0
      near <- addTimer wheel (5 * ms) (pure ())
      void (addTimer wheel (20 * ms) (pure ()))
      nextDeadline wheel `shouldReturn` Just (5 * ms)
      void (cancelTimer wheel near)
      Just remembered <- nextDeadline wheel
      (remembered <= 20 * ms) `shouldBe` True
      expire wheel (6 * ms)
      nextDeadline wheel `shouldReturn` Just (20 * ms)
      expire wheel (20 * ms)
      nextDeadline wheel `shouldReturn` Nothing

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
given duration body = const (duration, ()) ^>> timeoutA body

-- | One body, whose deadline is the input's, which a short deadline cuts short and a long one doesn't, in one program.
bothDeadlines :: (Rich arr) => arr () (Maybe Char, Maybe Char)
bothDeadlines = (const (millis 10, ()) ^>> once) &&& (const (secs 10, ()) ^>> once)
  where
    once = timeoutA (sleep (millis 100) >>^ const 'x')

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
