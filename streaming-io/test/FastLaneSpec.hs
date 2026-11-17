{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | The fast lane of the schedulers that run every part on one thread: a part in it, whenever it is ready, runs after at most one
-- part of the normal lane, and the normal lane gets every other turn at least. The cases count turns, which are what the
-- parts do between two points to give way at, and not time: the runs take a turn of no length, so that every point to give way at ends
-- the turn, and they are built in 'Debug', where the scheduler asserts what it can. Four hundred parts that spin in the normal
-- lane make a round of four hundred turns, so that a part that waits for the next round waits four hundred turns for it.
module FastLaneSpec (spec) where

import Control.Arrow (first, (&&&), (>>>), (^>>))
import Control.Concurrent (forkIO, getNumCapabilities, setNumCapabilities, threadDelay)
import Control.Monad (forM, forM_, unless, when)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, keeping, localTicket, tap, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (..))
import Network.Streaming.Delimited
import Network.Streaming.Internal.Scheduler (Turn (..))
import Network.Streaming.Mode (Mode (..))
import Network.Streaming.Threads (runThreads)
import Skeletest
import System.Timeout (timeout)

-- | What a case's program needs of the arrow it runs in.
type Program a = forall arr. (Runtime arr) => arr () a

-- | The arrows whose schedulers have lanes, each running a program with turns of no length, in 'Debug'.
data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners =
  [ Runner "delimited" (\program -> fst <$> runDelimitedTurnM @'Debug (Turn 0) program ()),
    Runner "compiled" (\program -> fst <$> runCompiledTurnM @'Debug (Turn 0) program ())
  ]

-- | How many parts spin in the normal lane.
spinning :: Int
spinning = 400

spec :: Spec
spec = describe "the fast lane" $ forM_ runners $ \(Runner name run) -> describe name $ do
  describe "a part in it" $ do
    it "gets about half of the turns, and the normal parts get the rest" $ do
      total <- newIORef 0
      fast <- newIORef 0
      normal <- newIORef 0
      run (alternating total fast normal)
      (f, n) <- (,) <$> readIORef fast <*> readIORef normal
      let turns = f + n
      when (f * 10 < turns * 4) $ failTest ("the part in the fast lane had " <> show f <> " of " <> show turns <> " turns, which is under 40%")
      when (n * 2 < turns) $ failTest ("the normal parts had " <> show n <> " of " <> show turns <> " turns, which is under half")

    it "runs after at most one normal part when a part on its core hands it the value that it waits for" $ do
      events <- newIORef []
      finished <- newIORef False
      waited <- newIORef 0
      run (handedOver events finished waited)
      between <- turnsBetween <$> readIORef events
      when (between > 1) $ failTest (show between <> " normal parts ran between the hand-over and the part that waits for it")

    it "runs after at most one normal part when a part on another core hands it the value that it waits for" $ do
      cores <- getNumCapabilities
      when (cores < 2) (setNumCapabilities 2)
      -- The value arrives at a point of the round that the thread's delay makes arbitrary, so that each case is run again.
      counts <- forM [1 .. 10 :: Int] $ \_ -> do
        events <- newIORef []
        finished <- newIORef False
        run (handedOverFromAnotherCore events finished)
        turnsBetween <$> readIORef events
      when (maximum counts > 1) $ failTest ("normal parts ran between the hand-over and the part that waits for it, as many as " <> show counts)

  describe "mailbox order" $ forM_ [("normal", False), ("fast", True)] $ \(lane, fast) ->
    it ("drains remote arrivals in FIFO order before local work in the " <> lane <> " lane") $ do
      order <- newIORef []
      timeout 2000000 (run (mixedArrivals fast order) *> (reverse <$> readIORef order))
        `shouldReturn` Just ["remote 1", "remote 2", "local 1", "local 2"]

  describe "the region of a part" $ forM_ [Normally, ByFailure, ByCancellation] $ \ending ->
    it ("ends with the arrow, " <> describeEnding ending) $ do
      tallies <- newTallies
      run (leaving ending tallies)
      (within', total', after) <- (,,) <$> readIORef (inside tallies) <*> readIORef (leftAt tallies) <*> readIORef (afterwards tallies)
      when (total' < 200) $ failTest ("the part left the region after " <> show total' <> " turns")
      when (within' * 10 < total' * 4) $ failTest ("the part had " <> show within' <> " of " <> show total' <> " turns in the region, which is under 40%")
      when (after * 10 >= later) $ failTest ("the part had " <> show after <> " of " <> show later <> " turns after the region, which is 10% or more")

  describe "the parts that a part starts in the region" $ forM_ [Together, Racing, Spawning] $ \how ->
    it ("begin in the normal lane, when it starts them with " <> describeFork how) $ do
      tallies <- newForkTallies
      run (forking how tallies)
      (within', began, child) <- (,,) <$> readIORef (ownTurns tallies) <*> readIORef (beganAt tallies) <*> readIORef (childTurns tallies)
      when (within' * 10 < began * 4) $ failTest ("the part had " <> show within' <> " of " <> show began <> " turns in the region, which is under 40%")
      when (child * 10 >= later) $ failTest ("the part that it started had " <> show child <> " of " <> show later <> " turns, which is 10% or more")

-- | How many turns the window after the region lasts, which the parts count among them.
later :: Int
later = 2000

-- | A turn of a part: it is counted in the part's tally and in the total, unless the part's window is over, which it says.
counting :: IORef Int -> IORef Int -> IO Bool -> IO (Maybe ())
counting mine total over = over >>= \ended -> if ended then pure (Just ()) else Nothing <$ (modifyIORef' total (+ 1) *> modifyIORef' mine (+ 1))

-- | A part that takes a turn, and gives way, until its window is over.
takingTurns :: (Runtime arr) => IORef Int -> IORef Int -> IO Bool -> arr () ()
takingTurns mine total over = untilJust (counting mine total over) >>> done

-- | One part in the fast lane and the others in the normal lane, which take turns until the total has this many. The turn in which
-- the first part enters the region is in the normal lane, and it counts none.
alternating :: (Runtime arr) => IORef Int -> IORef Int -> IORef Int -> arr () ()
alternating total fast normal = together (fastLane (giveWay >>> part fast) : replicate spinning (part normal))
  where
    part mine = takingTurns mine total ((>= 4000) <$> readIORef total)

-- | What happens in the programs that hand a value over: a normal part takes a turn, the value is handed over, the part that
-- waits for it runs.
data Happening = Normal | HandedOver | Ran
  deriving (Eq)

-- | Notes the event, which parts on several cores may do.
noting :: IORef [Happening] -> Happening -> IO ()
noting events event = atomicModifyIORef' events (\noted -> (event : noted, ()))

-- | A part that takes a turn, and notes it, until the flag says to stop.
spinner :: (Runtime arr) => IORef [Happening] -> IORef Bool -> arr a ()
spinner events finished = const () ^>> untilJust (readIORef finished >>= \stop -> if stop then pure (Just ()) else Nothing <$ noting events Normal) >>> done

-- | How many normal parts took a turn between the hand-over and the run of the part that waited for it, which is none if it ran
-- first.
turnsBetween :: [Happening] -> Int
turnsBetween noted = if Ran `elem` afterwards' then length (filter (== Normal) (takeWhile (/= Ran) afterwards')) else 0
  where
    afterwards' = drop 1 (dropWhile (/= HandedOver) (reverse noted))

-- | A part in the fast lane waits for a ticket of its core, among normal parts that spin. Another normal part hands the value over, once
-- the first has been waiting for some rounds.
handedOver :: (Runtime arr) => IORef [Happening] -> IORef Bool -> IORef Int -> arr () ()
handedOver events finished waited =
  localTicket
    >>> first willAwait
    >>> together (waiter : giver : replicate spinning (spinner events finished))
  where
    waiter = fst ^>> fastLane (await >>> now (\value -> noting events Ran *> writeIORef finished True *> pure (value :: Int))) >>> done
    giver = snd ^>> keeping (untilJust (modifyIORef' waited (+ 1) *> ((\n -> if n >= 1200 then Just () else Nothing) <$> readIORef waited))) >>> now (\(_, promise) -> (promise, 1) <$ noting events HandedOver) >>> fulfil >>> done

-- | The same, with a ticket of any core, and the value handed over by a thread that waits until the part is parked and for a moment
-- more, so that the hand-over falls at some point of a round.
handedOverFromAnotherCore :: (Runtime arr) => IORef [Happening] -> IORef Bool -> arr () ()
handedOverFromAnotherCore events finished =
  ticket @_ @'Anywhere
    >>> first willAwait
    >>> tap (now ((\promise -> () <$ forkIO (hand promise)) . snd))
    >>> fst ^>> together (waiter : replicate spinning (spinner events finished))
  where
    waiter = fastLane (await >>> now (\value -> noting events Ran *> writeIORef finished True *> pure (value :: Int)) >>> done)
    hand promise = do
      runThreads untilParked promise
      threadDelay 1000
      _ <- runThreads fulfil (promise, 1 :: Int)
      noting events HandedOver

-- | Park all four receivers, then wake them in one turn, interleaving the
-- local ready list and the mailbox. Anywhere tickets post even on their own core.
mixedArrivals :: forall arr. (Runtime arr) => Bool -> IORef [String] -> arr () ()
mixedArrivals fast order =
  ((ticket @_ @'Anywhere &&& ticket @_ @'Anywhere) &&& (localTicket &&& localTicket))
    >>> together
      [ (fst . fst) ^>> receiving,
        (snd . fst) ^>> receiving,
        (fst . snd) ^>> receiving,
        (snd . snd) ^>> receiving,
        now handing
      ]
  where
    receiving :: (Exchange arr reach) => arr (Future arr reach String, Promise reach String) ()
    receiving = lane (fst ^>> await >>> now (\name -> modifyIORef' order (name :)))
    lane :: arr a b -> arr a b
    lane = if fast then fastLane else id
    handing (((_, remote1), (_, remote2)), ((_, local1), (_, local2))) = do
      taken <- sequence [fulfilNow local1 "local 1", fulfilNow remote1 "remote 1", fulfilNow local2 "local 2", fulfilNow remote2 "remote 2"]
      unless (and taken) (ioError (userError "a receiver had not parked"))

-- | How a region ends.
data Ending = Normally | ByFailure | ByCancellation

describeEnding :: Ending -> String
describeEnding Normally = "when it ends with its result"
describeEnding ByFailure = "when it ends with a failure"
describeEnding ByCancellation = "when it ends with a cancellation"

-- | The turns of the part that is in a region, and of the others.
data Tallies = Tallies
  { -- | The part's turns in the region.
    inside :: IORef Int,
    -- | The part's turns after it.
    afterwards :: IORef Int,
    -- | The total of the turns when the part left the region.
    leftAt :: IORef Int,
    total :: IORef Int,
    stopped :: IORef Bool
  }

newTallies :: IO Tallies
newTallies = Tallies <$> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef False

-- | A part that takes turns in a region, which ends in the way that is given, and then takes turns outside it, among parts that spin
-- in the normal lane. It has a thousand turns in the region, or the deadline's twenty milliseconds, and then a window of the total.
leaving :: (Runtime arr) => Ending -> Tallies -> arr () ()
leaving ending tallies = together (subject : replicate spinning (const () ^>> untilJust (readIORef (stopped tallies) >>= \stop -> if stop then pure (Just ()) else Nothing <$ modifyIORef' (total tallies) (+ 1)) >>> done))
  where
    subject =
      region
        >>> now (\_ -> readIORef (total tallies) >>= writeIORef (leftAt tallies))
        >>> takingTurns (afterwards tallies) (total tallies) ((\left now' -> now' >= left + later) <$> readIORef (leftAt tallies) <*> readIORef (total tallies))
        >>> now (\_ -> writeIORef (stopped tallies) True)
    enough = (>= 1000) <$> readIORef (inside tallies)
    region = case ending of
      Normally -> fastLane (takingTurns (inside tallies) (total tallies) enough)
      ByFailure -> tryA (fastLane (takingTurns (inside tallies) (total tallies) enough >>> now (\_ -> ioError (userError "the region failed")))) >>> done
      ByCancellation -> within (millis 20) (fastLane (takingTurns (inside tallies) (total tallies) ((>= 20000000) <$> readIORef (total tallies)))) >>> done

-- | How a part starts parts.
data Fork = Together | Racing | Spawning

describeFork :: Fork -> String
describeFork Together = "together"
describeFork Racing = "race"
describeFork Spawning = "spawn"

-- | The turns of a part in a region, of the part that it starts there, and of the others.
data ForkTallies = ForkTallies
  { ownTurns :: IORef Int,
    -- | The total of the turns when the part started the other.
    beganAt :: IORef Int,
    childTurns :: IORef Int,
    allTurns :: IORef Int,
    done' :: IORef Bool
  }

newForkTallies :: IO ForkTallies
newForkTallies = ForkTallies <$> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef 0 <*> newIORef False

-- | A part takes turns in a region, and then starts a part there that takes turns, with the way that is given, among parts that spin in
-- the normal lane.
forking :: forall arr. (Runtime arr) => Fork -> ForkTallies -> arr () ()
forking how tallies = together (subject : replicate spinning (const () ^>> untilJust (readIORef (done' tallies) >>= \stop -> if stop then pure (Just ()) else Nothing <$ modifyIORef' (allTurns tallies) (+ 1)) >>> done))
  where
    subject =
      fastLane
        ( takingTurns (ownTurns tallies) (allTurns tallies) ((>= 1000) <$> readIORef (ownTurns tallies))
            >>> now (\_ -> readIORef (allTurns tallies) >>= writeIORef (beganAt tallies))
            >>> start
        )
        >>> now (\_ -> writeIORef (done' tallies) True)
    child :: arr () ()
    child = takingTurns (childTurns tallies) (allTurns tallies) ((\began now' -> now' >= began + later) <$> readIORef (beganAt tallies) <*> readIORef (allTurns tallies))
    start = case how of
      Together -> together [child, done]
      Racing -> race child parked >>> done
      Spawning -> group AwaitAll (tap (spawn child) >>> done) >>> done

-- | A part that waits for a ticket that nothing hands a value over.
parked :: forall arr. (Runtime arr) => arr () ()
parked = (localTicket :: arr () (Future arr (Local arr) (), Promise (Local arr) ())) >>> fst ^>> await >>> done
