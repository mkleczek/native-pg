{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- | 'grace': a region that keeps the cancellation of a scope outside it for at most its period, counted from when the
-- cancellation arrives, and then delivers it at the next wait. The cancellation here is the end of the other half of a race
-- that the region is in, which comes at a time that the program chooses; the same programs are run by every arrow.
--
-- A time that a case measures is read from the arrow's clock, in the part that causes the cancellation and in the part that
-- is cancelled, so that the period is measured from the instant that the cancellation was made, and not from the start of the
-- program.
module GraceSpec (spec) where

import Control.Arrow (Arrow, ArrowChoice, returnA, second, (>>>), (>>^), (^>>))
import Control.Concurrent (threadDelay)
import Control.Monad (forM_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Word (Word64)
import qualified Network.Socket as Socket
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, releaseWithin, sleep, tap)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Types (Fd (..))
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Scope arr, Race arr, Time arr, Within arr, Annotate arr)

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
spec = describe "grace" $ forM_ runners $ \(Runner name run) -> describe name $ xfail "grace is not implemented yet" $ do
  it "changes nothing for a cancellation that arrives after its body has ended" $ do
    noted <- newLog
    started <- monotonicNanoseconds
    result <- inTime (run (race (grace (secs 5) returnA >>> noting noted "left the region" >>> waitsForEver) (sleep (millis 30))))
    ended <- monotonicNanoseconds
    fmap winner result `shouldBe` Just "second"
    names noted `shouldReturn` ["left the region"]
    -- The cancellation was delivered at the wait after the region, and not after its period.
    (ended - started >= 30000000) `shouldBe` True
    (ended - started < 2000000000) `shouldBe` True

  it "keeps a cancellation that arrives during its body for the period, and then delivers it at the body's next wait" $ do
    noted <- newLog
    result <- inTime (run (race (grace (millis 150) (bracketed noted "r" waitsForEver)) (causing noted "cancel")))
    fmap winner result `shouldBe` Just "second"
    events <- readLog noted
    map fst events `shouldBe` ["acquire r", "cancel", "release r: cancelled"]
    (elapsed events "cancel" "release r: cancelled" >= 150000000) `shouldBe` True
    (elapsed events "cancel" "release r: cancelled" < 3000000000) `shouldBe` True

  it "doesn't deliver it during an action that never waits, which the period ends in, but at the wait after it" $ do
    noted <- newLog
    -- The cancellation comes while the body sleeps, and the period of 100 ms ends 300 ms into the action that follows.
    let body :: Program ()
        body = bracketed noted "r" (sleep (millis 40) >>> now (const (threadDelay 300000)) >>> noting noted "action done" >>> waitsForEver)
    result <- inTime (run (race (grace (millis 100) body) (causing noted "cancel")))
    fmap winner result `shouldBe` Just "second"
    events <- readLog noted
    -- The action ran to its end, 300 ms after the part began it, whose period ended in it, and the release followed.
    map fst events `shouldBe` ["acquire r", "cancel", "action done", "release r: cancelled"]

  it "lets a body that ends within the period finish, and the cancellation is found at the next wait after it" $ do
    noted <- newLog
    let region :: Program Char
        region = grace (secs 2) (sleep (millis 60) >>> noting noted "body done" >>^ const 'x')
    started <- monotonicNanoseconds
    result <- inTime (run (race (region >>> noting noted "left the region" >>> waitsForEver) (sleep (millis 10))))
    ended <- monotonicNanoseconds
    fmap winner result `shouldBe` Just "second"
    names noted `shouldReturn` ["body done", "left the region"]
    (ended - started >= 60000000) `shouldBe` True
    (ended - started < 1500000000) `shouldBe` True

  it "keeps the cancellation until the longest period of the regions that it is in has passed, whichever is nested in the other" $
    forM_ [(millis 200, millis 20), (millis 20, millis 200)] $ \(outer, inner) -> do
      noted <- newLog
      result <- inTime (run (race (grace outer (grace inner (bracketed noted "r" waitsForEver))) (causing noted "cancel")))
      fmap winner result `shouldBe` Just "second"
      events <- readLog noted
      map fst events `shouldBe` ["acquire r", "cancel", "release r: cancelled"]
      (elapsed events "cancel" "release r: cancelled" >= 200000000) `shouldBe` True
      (elapsed events "cancel" "release r: cancelled" < 3000000000) `shouldBe` True

  it "has a release that keeps its own deadline: it is neither cut when the period ends, nor held beyond its deadline" $ do
    -- The body ends at once, and its release waits for its own deadline of 300 ms; the cancellation comes in the release, and
    -- the period of the region is over before the release is.
    noted <- newLog
    let release :: (Rich arr) => arr ((), Exit) ()
        release = noting noted "release begins" >>> releaseWithin (millis 300) waitsForEver >>> noting noted "release ended" >>> done
        body :: Program ()
        body = scope (noting noted "entered") release (snd ^>> returnA)
    result <- inTime (run (race (grace (millis 50) body >>> noting noted "left the region") (causing noted "cancel")))
    fmap winner result `shouldBe` Just "second"
    events <- readLog noted
    map fst events `shouldBe` ["entered", "release begins", "cancel", "release ended", "left the region"]
    (elapsed events "release begins" "release ended" >= 300000000) `shouldBe` True
    (elapsed events "release begins" "release ended" < 3000000000) `shouldBe` True

  it "has a release that the period cut the body for run to the end of its own deadline" $ do
    noted <- newLog
    let release :: (Rich arr) => arr ((), Exit) ()
        release = noting noted "release begins" >>> releaseWithin (millis 200) waitsForEver >>> noting noted "release ended" >>> done
        body :: Program ()
        body = scope (noting noted "entered") release (snd ^>> waitsForEver)
    result <- inTime (run (race (grace (millis 50) body) (causing noted "cancel")))
    fmap winner result `shouldBe` Just "second"
    events <- readLog noted
    map fst events `shouldBe` ["entered", "cancel", "release begins", "release ended"]
    (elapsed events "cancel" "release begins" >= 50000000) `shouldBe` True
    (elapsed events "release begins" "release ended" >= 200000000) `shouldBe` True

  it "lets a deadline inside it fire while the cancellation is kept" $ do
    noted <- newLog
    result <- inTime (run (race (grace (secs 5) (within (millis 30) waitsForEver >>> noting noted "inner deadline passed")) (sleep (millis 10))))
    fmap winner result `shouldBe` Just "second"
    names noted `shouldReturn` ["inner deadline passed"]

  it "is held by a shield inside it until the part leaves the shield, whatever the period" $ do
    noted <- newLog
    started <- monotonicNanoseconds
    result <- inTime (run (race (grace (millis 30) (shield (sleep (millis 200)) >>> noting noted "left the shield" >>> waitsForEver)) (sleep (millis 10))))
    ended <- monotonicNanoseconds
    fmap winner result `shouldBe` Just "second"
    names noted `shouldReturn` ["left the shield"]
    (ended - started >= 200000000) `shouldBe` True

  it "delivers the cancellation when the period has passed, to a part that left a shield inside it before that" $ do
    noted <- newLog
    result <- inTime (run (race (grace (millis 100) (shield (sleep (millis 30)) >>> bracketed noted "r" waitsForEver)) (causing noted "cancel")))
    fmap winner result `shouldBe` Just "second"
    events <- readLog noted
    map fst events `shouldBe` ["cancel", "acquire r", "release r: cancelled"]
    (elapsed events "cancel" "release r: cancelled" >= 100000000) `shouldBe` True
    (elapsed events "cancel" "release r: cancelled" < 3000000000) `shouldBe` True

  it "keeps a cancellation that is pending when the part enters it for the period, counted from when the cancellation arrived" $ do
    noted <- newLog
    -- The cancellation comes while the part is behind a shield, and the region is entered when the shield ends.
    result <- inTime (run (race (shield (sleep (millis 60)) >>> grace (millis 100) (bracketed noted "r" waitsForEver)) (causing noted "cancel")))
    fmap winner result `shouldBe` Just "second"
    events <- readLog noted
    map fst events `shouldBe` ["cancel", "acquire r", "release r: cancelled"]
    (elapsed events "cancel" "release r: cancelled" >= 100000000) `shouldBe` True
    (elapsed events "cancel" "release r: cancelled" < 3000000000) `shouldBe` True

  it "delivers the cancellation to every part of a group inside it, when the period has passed" $ do
    noted <- newLog
    result <- inTime (run (race (grace (millis 100) (bracketed noted "r" (together [waitsForEver, waitsForEver >>> noting noted "never"]))) (causing noted "cancel")))
    fmap winner result `shouldBe` Just "second"
    events <- readLog noted
    map fst events `shouldBe` ["acquire r", "cancel", "release r: cancelled"]
    (elapsed events "cancel" "release r: cancelled" >= 100000000) `shouldBe` True
    (elapsed events "cancel" "release r: cancelled" < 3000000000) `shouldBe` True

-- | The result of a program that ends in time, or nothing if it doesn't.
inTime :: IO a -> IO (Maybe a)
inTime = timeout 30000000

-- | What a part noted, with the time on the arrow's clock.
type Log = IORef [(String, Word64)]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [(String, Word64)]
readLog = fmap reverse . readIORef

-- | The names of what was noted, in order.
names :: Log -> IO [String]
names = fmap (map fst) . readLog

-- | Notes the text at the time of the arrow's clock.
noteAt :: (Arrow arr, Effect arr, Time arr) => Log -> arr String ()
noteAt noted = (\text -> (text, ())) ^>> second clock >>> now (\(text, Instant at) -> atomicModifyIORef' noted (\entries -> ((text, at) : entries, ())))

-- | Notes the entry, and passes on the input.
noting :: (Arrow arr, Effect arr, Time arr) => Log -> String -> arr a a
noting noted entry = tap (const entry ^>> noteAt noted)

-- | The part that causes the cancellation: it waits a little, notes the time, and ends, which cancels the other half of the race.
causing :: (Rich arr) => Log -> String -> arr () ()
causing noted entry = sleep (millis 20) >>> noting noted entry >>> done

-- | How long after the entry of the first name the entry of the second name was made, in nanoseconds.
elapsed :: [(String, Word64)] -> String -> String -> Word64
elapsed events from to = case (lookup from events, lookup to events) of
  (Just begun, Just ended) -> ended - begun
  _ -> 0

-- | A scope that notes when it is entered, and how it was left: its body is the arrow that is given.
bracketed :: (Rich arr) => Log -> String -> arr () b -> arr () b
bracketed noted name body =
  scope
    (noting noted ("acquire " <> name))
    ((\(_, exit) -> "release " <> name <> ": " <> describeExit exit) ^>> noteAt noted)
    (snd ^>> body)

describeExit :: Exit -> String
describeExit = \case
  Done -> "done"
  Failed failure -> "failed " <> show failure
  Cancelled' -> "cancelled"

winner :: Either a b -> String
winner = either (const "first") (const "second")

-- | A wait that nothing ends.
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
