{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | Groups, for every arrow that runs programs: a body that spawns parts while it runs, and what the group does as they
-- end. The same programs, written once for any arrow that has what they need, are run by each.
module GroupSpec (spec) where

import Control.Applicative ((<|>))
import Control.Arrow (ArrowChoice, second, (>>>), (^>>))
import Control.Exception (SomeException)
import Control.Monad (forM_, when)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.List (sort)
import Data.Maybe (isNothing)
import Data.Word (Word64)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, keeping, localTicket, sleep, tap, timesWith, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (..), Joiner (..))
import Network.Streaming.Delimited
import Network.Streaming.Loop (Loop, Outcome (..), Simulated (..), runLoop, runSimulation)
import Network.Streaming.Threads
import Skeletest
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)
import System.Mem (performMajorGC)
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr =
  ( ArrowChoice arr,
    ArrowApplicative arr,
    Effect arr,
    GiveWay arr,
    Block arr,
    Concurrent arr,
    Exchange arr (Local arr),
    Scope arr,
    Race arr,
    Time arr,
    Within arr,
    Annotate arr
  )

type Program a = forall arr. (Rich arr) => arr () a

-- | A way to run a program: an arrow's.
data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners =
  [ Runner "threads" (\program -> runThreads program ()),
    Runner "delimited" (\program -> fst <$> runDelimited program ()),
    Runner "compiled" (\program -> fst <$> runCompiled program ()),
    Runner "loop" (\program -> fst <$> runLoop program ())
  ]

spec :: Spec
spec = describe "a group" $ do
  forM_ runners $ \(Runner name run) -> describe name $ do
    it "runs its parts at once and ends when the body and every part have ended, and a spawn returns at once" $ do
      noted <- newLog
      run (spawnsThree noted) `shouldReturn` ()
      readLog noted `shouldReturn` ["spawned all", "body ended", "part 0 ended", "part 1 ended", "part 2 ended", "group ended"]

    it "runs a part that waits for what a later part gives" $ do
      noted <- newLog
      run (awaitsALaterPart noted) `shouldReturn` ()
      readLog noted `shouldReturn` ["got 7"]

    it "cancels the body and the parts at their waits when it is cancelled from outside, and waits for them, scopes inside them released" $ do
      noted <- newLog
      run (cancelledFromOutside noted) `shouldReturn` ()
      entries <- readLog noted
      sort (take 3 entries) `shouldBe` ["body: cancelled", "part 0: cancelled", "part 1: cancelled"]
      drop 3 entries `shouldBe` ["race returned"]

    it "leaves the others and its result alone when a part fails, under AwaitAll" $ do
      noted <- newLog
      result <- run (tryA (aPartFails noted))
      describeFailure result `shouldBe` "Right ()"
      readLog noted `shouldReturn` ["good part ended"]

    it "cancels the parts when the body fails, and rethrows its failure after they have ended" $ do
      noted <- newLog
      result <- run (tryA (bodyFails noted))
      describeFailure result `shouldBe` "Left user error (body failed)"
      entries <- readLog noted
      (sort (take 2 entries), drop 2 entries) `shouldBe` (["part 0: cancelled", "part 1: cancelled"], ["group: failed user error (body failed)"])

    describe "with a joiner that keeps the results in the order of the spawns and cancels on the first failure" $ do
      it "returns the results" $ do
        result <- run (tryA collectsResults)
        describeFailure result `shouldBe` "Right ((),[10,20,30])"

      it "rethrows the failure, cancels the parts, lets the body go on, and doesn't run a part that is spawned after" $ do
        noted <- newLog
        result <- run (tryA (firstFailureCancels noted))
        describeFailure result `shouldBe` "Left user error (part 1 failed)"
        entries <- readLog noted
        sort entries `shouldBe` ["body ended", "body goes on", "part 0: cancelled"]

    -- A loop of Loop keeps a frame for each round, as its choice has no branch of its own: the case is for the arrows that run
    -- a loop in constant space.
    when (name /= "loop") $ it "spawns 100,000 parts one after the other, each of which ends at once, and holds no more than a group that spawned none" $ do
      probes <- newIORef []
      ran <- newIORef 0
      finished <- timeout 120000000 (run (spawnsMany probes ran 100000))
      finished `shouldBe` Just ()
      reverse <$> readIORef probes >>= \case
        [start, end] -> do
          reported name start end
          if end < start + 16384
            then pure ()
            else failTest ("the live heap grew from " <> show start <> " to " <> show end <> " bytes")
        found -> failTest ("the heap was taken " <> show (length found) <> " times, and not twice")

  describe "under a simulation" $
    it "is a deadlock, and says so, when it waits for a part that can never end" $ do
      outcome <- simulatedOutcome <$> runSimulation 1 100000 (waitsForAPartThatNeverEnds :: Loop () ()) ()
      case outcome of
        Deadlocked _ -> pure ()
        _ -> failTest "the simulation did not end in a deadlock"

-- | Says what the memory case measured, if the run asks for it: with @GROUP_REPORT@ set, on stderr.
reported :: String -> Word64 -> Word64 -> IO ()
reported name start end =
  lookupEnv "GROUP_REPORT" >>= \asked ->
    when (asked /= Nothing) (hPutStrLn stderr ("group " <> name <> " | live bytes at the start " <> show start <> " | at the end " <> show end))

-- | A joiner that keeps what the parts give, by the number of the spawn, and the first failure, and cancels the parts when a
-- part fails.
data Results = Results

instance Joiner Results where
  type Part Results = Int
  type Joined Results = [Int]
  type Tally Results = (IntMap.IntMap Int, Maybe SomeException)
  begin _ = (IntMap.empty, Nothing)
  ended number outcome (results, failure) = case outcome of
    Right value -> ((IntMap.insert number value results, failure), False)
    Left thrown -> ((results, failure <|> Just thrown), isNothing failure)
  joined (results, failure) = maybe (Right (IntMap.elems results)) Left failure

-- | Spawns the part on the second of the pair, and keeps the pair.
spawning :: (Rich arr) => arr x () -> arr (Group arr s AwaitAll, x) (Group arr s AwaitAll, x)
spawning part = tap (spawn part)

-- | Waits this long, in the body of a group, and keeps the pair.
pausing :: (Rich arr) => Duration -> arr (g, ()) (g, ())
pausing duration = tap (snd ^>> sleep duration)

-- | A part that waits for this many milliseconds, and notes that it ended.
lasting :: (Rich arr) => Log -> Word64 -> Int -> arr () ()
lasting noted milliseconds number = sleep (millis milliseconds) >>> noting noted ("part " <> show number <> " ended")

-- | A group of three parts that end after 40, 80 and 120 ms, in a body that notes that it spawned them all and ends after 10.
spawnsThree :: (Rich arr) => Log -> arr () ()
spawnsThree noted =
  group AwaitAll
    ( spawning (lasting noted 40 0)
        >>> spawning (lasting noted 80 1)
        >>> spawning (lasting noted 120 2)
        >>> noting noted "spawned all"
        >>> pausing (millis 10)
        >>> noting noted "body ended"
        >>> done
    )
    >>> noting noted "group ended"
    >>> done

-- | A part that waits for a ticket, which a part that is spawned after it fulfils.
awaitsALaterPart :: (Rich arr) => Log -> arr () ()
awaitsALaterPart noted =
  group
    AwaitAll
    ( second (keeping localTicket)
        >>> tap ((\(handle, ((future, _), ())) -> (handle, future)) ^>> spawn waiter)
        >>> tap ((\(handle, ((_, promise), ())) -> (handle, promise)) ^>> spawn (tap (done >>> sleep (millis 20)) >>> fulfilling))
        >>> done
    )
    >>> done
  where
    waiter = await >>> now (\value -> note noted ("got " <> show (value :: Int)))
    fulfilling = (\promise -> (promise, 7)) ^>> fulfil >>> done

-- | A scope that notes how its body ended.
noteExit :: (Rich arr) => Log -> String -> arr a b -> arr a b
noteExit noted name body = scope done (now (\((), exit) -> note noted (name <> ": " <> describeExit exit))) (snd ^>> body)

-- | A wait that nothing ends.
forEver :: (Rich arr) => arr () ()
forEver = sleep (secs 3600)

-- | A group whose body and parts wait for ever, with scopes that note how they ended, in a race with a short wait: the
-- group is cancelled from outside.
cancelledFromOutside :: (Rich arr) => Log -> arr () ()
cancelledFromOutside noted =
  race
    ( group
        AwaitAll
        ( noteExit
            noted
            "body"
            ( spawning (noteExit noted "part 0" forEver)
                >>> spawning (noteExit noted "part 1" forEver)
                >>> tap (snd ^>> forEver)
                >>> done
            )
        )
        >>> done
    )
    (sleep (millis 40))
    >>> noting noted "race returned"
    >>> done

-- | A part fails, and another ends well after it.
aPartFails :: (Rich arr) => Log -> arr () ()
aPartFails noted =
  group
    AwaitAll
    ( spawning (boom "part failed")
        >>> spawning (sleep (millis 20) >>> noting noted "good part ended")
        >>> done
    )
    >>> done

-- | The body fails after it spawned two parts that wait for ever.
bodyFails :: (Rich arr) => Log -> arr () ()
bodyFails noted =
  noteExit
    noted
    "group"
    ( group
        AwaitAll
        ( spawning (noteExit noted "part 0" forEver)
            >>> spawning (noteExit noted "part 1" forEver)
            >>> boom "body failed"
            >>> done
        )
        >>> done
    )

-- | Three parts that give 10, 20 and 30, the first of them the last to end.
collectsResults :: (Rich arr) => arr () ((), [Int])
collectsResults =
  group
    Results
    ( tap (spawn (sleep (millis 60) >>> returning 10))
        >>> tap (spawn (sleep (millis 30) >>> returning 20))
        >>> tap (spawn (returning 30))
        >>> done
    )

-- | A part that waits for ever and a part that fails at once, and then, after a wait, a part that is spawned too late.
firstFailureCancels :: (Rich arr) => Log -> arr () ((), [Int])
firstFailureCancels noted =
  group
    Results
    ( tap (spawn (noteExit noted "part 0" (forEver >>> returning 0)))
        >>> tap (spawn (boom "part 1 failed"))
        >>> noting noted "body goes on"
        >>> pausing (millis 100)
        >>> tap (spawn (noting noted "part 2 ran" >>> returning 2))
        >>> noting noted "body ended"
        >>> done
    )

-- | A part that gives the value.
returning :: (Effect arr) => a -> arr b a
returning value = now (const (pure value))

-- | Spawns this many parts, one after the other, each of which counts that it ran and ends, and takes the live heap before the
-- first and after the last has ended.
spawnsMany :: (Rich arr) => IORef [Word64] -> IORef Int -> Int -> arr () ()
spawnsMany probes ran count =
  group
    AwaitAll
    ( tap (snd ^>> probing)
        >>> timesWith count (spawning (now (const (atomicModifyIORef' ran (\n -> (n + 1, ()))))) >>> tap (snd ^>> giveWay))
        >>> tap (snd ^>> untilJust (readIORef ran >>= \n -> pure (if n >= count then Just () else Nothing)))
        >>> pausing (millis 20)
        >>> tap (snd ^>> probing)
        >>> done
    )
    >>> done
  where
    probing = now (const (performMajorGC *> (gcdetails_live_bytes . gc <$> getRTSStats) >>= \live -> atomicModifyIORef' probes (\taken -> (live : taken, ()))))

-- | A part that waits for a ticket that nothing fulfils.
waitsForAPartThatNeverEnds :: (Rich arr) => arr () ()
waitsForAPartThatNeverEnds = group AwaitAll (spawning (localTicket >>> fst ^>> await) >>> done) >>> done

-- | What a program noted.
type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

note :: Log -> String -> IO ()
note noted entry = atomicModifyIORef' noted (\entries -> (entry : entries, ()))

noting :: (Rich arr) => Log -> String -> arr a a
noting noted entry = tap (now (const (note noted entry)))

-- | Fails with the text.
boom :: (Effect arr) => String -> arr a b
boom text = now (const (ioError (userError text)))

describeFailure :: (Show a) => Either SomeException a -> String
describeFailure = either (\failure -> "Left " <> show failure) (\result -> "Right " <> show result)

describeExit :: Exit -> String
describeExit = \case
  Done -> "done"
  Failed failure -> "failed " <> show failure
  Cancelled' -> "cancelled"
