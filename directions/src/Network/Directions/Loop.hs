{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | Every concurrent part on one thread: an arrow is a program as data, which
-- a scheduler runs. A part that waits is the rest of its program and the value
-- going into it, so nothing is captured when it waits. The simulation runs the
-- same programs in an order a seed chooses.
module Network.Directions.Loop
  ( Loop,
    Report (..),
    runLoop,
    Outcome (..),
    Simulated (..),
    runSimulation,
    awaitFired,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..), returnA)
import Control.Category (Category (..))
import Control.Concurrent.STM (atomically, orElse, retry)
import Control.Exception (bracket, mask, onException, uninterruptibleMask_)
import Control.Monad (filterM)
import Data.Bits (shiftR, xor)
import qualified Data.Function as Function
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (partition, sort)
import Data.Word (Word64)
import Network.Directions.Classes
import Network.Streaming.Watch (Watch (..), fdReadable, fdWritable)
import Prelude hiding (id, (.))

-- | A program as data: functions, sequences, choices and concurrent pairs, and
-- its primitives: an action that never waits, a block until an event fires,
-- and giving and waiting for a ticket.
data Loop a b where
  Arr :: (a -> b) -> Loop a b
  Seq :: Loop a b -> Loop b c -> Loop a c
  First :: Loop a b -> Loop (a, c) (b, c)
  Choose :: Loop a b -> Loop (Either a c) (Either b c)
  Both :: Loop a b -> Loop a c -> Loop a (b, c)
  Now :: (a -> IO b) -> Loop a b
  Block :: Loop Event ()
  Fulfil :: Loop (IORef (Slot a), a) ()
  Await :: Loop (IORef (Slot a)) a

instance Category Loop where
  id = Arr Function.id
  (.) = flip Seq

instance Arrow Loop where
  arr = Arr
  first = First

instance ArrowChoice Loop where
  left = Choose

instance Concurrent Loop where
  concurrently = Both

instance Effect Loop where
  now = Now

-- | Nothing: the interpreter counts a step for every action that a part runs,
-- not only at a link, and gives way after 64 of them (a simulation, after each,
-- to try the orders of the parts' actions that a seed gives).
instance GiveWay Loop where
  giveWay = returnA

-- | Suspends the part: the scheduler goes on with it once the watch fires.
instance Block Loop where
  block = Block

-- | A ticket is a slot on this thread. A part that waits for it is parked in
-- it, with no watch, and goes back among those that can go on when the ticket
-- is given its value.
instance Exchange Loop where
  newtype Ticket Loop a = LoopTicket (IORef (Slot a))
  newTicket = Now (\() -> LoopTicket <$> newIORef Empty)
  fulfil = Seq (Arr (\(LoopTicket slot, value) -> (slot, value))) Fulfil
  await = Seq (Arr (\(LoopTicket slot) -> slot)) Await

-- | A ticket's slot: empty, given its value, or holding the part that waits for
-- it, as what puts the part back among those that can go on.
data Slot a = Empty | Given a | Waiter (a -> IO ())

-- | A part of a running program: a program with its input, or a value for
-- what follows, and what follows.
data Process r where
  Run :: Loop a b -> a -> Frames b r -> Process r
  Give :: b -> Frames b r -> Process r

-- | What follows a program, as data: the rest of a sequence, the other half of
-- a pair, a choice to wrap, or the join of a concurrent pair.
data Frames b r where
  Return :: Frames r r
  Then :: Loop b c -> Frames c r -> Frames b r
  Beside :: c -> Frames (b, c) r -> Frames b r
  OnLeft :: Frames (Either b c) r -> Frames b r
  JoinFirst :: IORef (Half b d) -> Frames (b, d) r -> Frames b r
  JoinSecond :: IORef (Half b d) -> Frames (b, d) r -> Frames d r

-- | What a concurrent pair has of its halves' results.
data Half b d = Neither | FirstDone b | SecondDone d

-- | What a part came to.
data Advanced r
  = Finished r
  | -- | It ended at a join, which the other half goes on from.
    Ended
  | -- | It waits for a ticket, whose slot holds it.
    Parked
  | -- | It used its steps, and can go on.
    Yielded (Process r)
  | Forked (Process r) (Process r)
  | Waits (IO Watch) (Process r)

-- | Runs a part until it waits, forks, ends, or has taken this many steps of
-- its primitives: what it came to, and how many steps it took. A part it parks
-- on a ticket goes back with the given action when the ticket has its value.
advance :: forall r. Int -> (Process r -> IO ()) -> Process r -> IO (Int, Advanced r)
advance fuel wake = go 0
  where
    go :: Int -> Process r -> IO (Int, Advanced r)
    go steps process
      | steps >= fuel = pure (steps, Yielded process)
      | otherwise = case process of
          Run program input frames -> case program of
            Arr function -> go steps (Give (function input) frames)
            Seq before after -> go steps (Run before input (Then after frames))
            First inner -> case input of
              (a, c) -> go steps (Run inner a (Beside c frames))
            Choose inner -> case input of
              Left a -> go steps (Run inner a (OnLeft frames))
              Right c -> go steps (Give (Right c) frames)
            Both one two -> do
              joined <- newIORef Neither
              pure (steps, Forked (Run one input (JoinFirst joined frames)) (Run two input (JoinSecond joined frames)))
            Now action -> action input >>= \output -> go (steps + 1) (Give output frames)
            Block -> pure (steps + 1, Waits (watchOf input) (Give () frames))
            Fulfil -> case input of
              (slot, value) ->
                readIORef slot >>= \case
                  Empty -> writeIORef slot (Given value) *> go (steps + 1) (Give () frames)
                  Waiter resume -> writeIORef slot (Given value) *> resume value *> go (steps + 1) (Give () frames)
                  Given _ -> ioError (userError "a ticket was given a value twice")
            Await ->
              readIORef input >>= \case
                Given value -> go (steps + 1) (Give value frames)
                Empty -> (steps + 1, Parked) <$ writeIORef input (Waiter (\value -> wake (Give value frames)))
                Waiter _ -> ioError (userError "two parts wait for one ticket")
          Give value frames -> case frames of
            Return -> pure (steps, Finished value)
            Then next rest -> go steps (Run next value rest)
            Beside c rest -> go steps (Give (value, c) rest)
            OnLeft rest -> go steps (Give (Left value) rest)
            JoinFirst joined rest ->
              readIORef joined >>= \case
                SecondDone d -> go steps (Give (value, d) rest)
                _ -> (steps, Ended) <$ writeIORef joined (FirstDone value)
            JoinSecond joined rest ->
              readIORef joined >>= \case
                FirstDone b -> go steps (Give (b, value) rest)
                _ -> (steps, Ended) <$ writeIORef joined (SecondDone value)

-- | An event's watch: the scheduler waits on all of its parts' at once.
watchOf :: Event -> IO Watch
watchOf = \case
  Readable fd -> fdReadable fd
  Writable fd -> fdWritable fd
  Watched register -> register

-- | What a run did: how many steps, and how many waits. In a run of 'Loop' or
-- a simulation a step is any primitive that a part ran. In a run of
-- "Network.Directions.Compiled" or "Network.Directions.Delimited" it is only
-- a point to give way at, which is a link's receive or send, a wait, or a
-- ticket given or waited for.
data Report = Report {reportSteps :: !Int, reportWaits :: !Int}
  deriving (Eq, Show)

-- | Runs the program and all of its concurrent parts on this thread. It runs
-- each part that can go on in turn, for up to 64 steps, and when none can,
-- waits once for all of their watches. Parts parked on tickets go on once the
-- tickets have their values.
runLoop :: Loop a b -> a -> IO (b, Report)
runLoop program input = do
  woken <- newIORef []
  let -- The parts that tickets woke join those that can go on.
      go report parked ready waiting = do
        risen <- reverse <$> readIORef woken
        writeIORef woken []
        step report (parked - length risen) (ready <> risen) waiting
      step report parked ready waiting = case ready of
        process : others -> do
          (steps, advanced) <- advance 64 (\woke -> modifyIORef' woken (woke :)) process
          let report' = report {reportSteps = reportSteps report + steps}
          case advanced of
            Finished value -> pure (value, report')
            Ended -> go report' parked others waiting
            Parked -> go report' (parked + 1) others waiting
            Yielded process' -> go report' parked (others <> [process']) waiting
            Forked one two -> go report' parked (others <> [one, two]) waiting
            Waits watch process' -> go report' parked others ((watch, process') : waiting)
        []
          | null waiting && parked > (0 :: Int) -> ioError (userError "runLoop: parts wait for tickets that nothing will give a value")
          | null waiting -> ioError (userError "runLoop: the program ended without a result")
          | otherwise -> do
              fired <- awaitFired (zip (map fst waiting) [0 :: Int ..])
              let (wake, still) = partition ((`elem` fired) . fst) (zip [0 ..] waiting)
              go report {reportWaits = reportWaits report + 1} parked (map (snd . snd) wake) (map snd still)
  go (Report 0 0) 0 [Run program input Return] []

-- | How a simulation ended.
data Outcome b
  = Completed b
  | -- | These parts wait for each other: none of their watches can fire, and
    -- no ticket they wait for will be given a value.
    Deadlocked [Int]
  | -- | It took the most steps it was allowed.
    GaveUp
  deriving (Eq, Show)

data Simulated b = Simulated
  { simulatedOutcome :: Outcome b,
    simulatedReport :: Report,
    -- | The parts, by number, in the order they stepped.
    simulatedTrace :: [Int]
  }

-- | Runs the program on this thread in an order the seed chooses: one step of
-- a primitive at a time, of a part picked at random among those that can go
-- on, up to the given number of steps. It never waits: when no part can go
-- on, it looks at their watches and goes on with those that have fired; if
-- none has, the parts wait for each other, or for tickets that nothing will
-- give a value, which it reports. Its links must be simulated ones, whose
-- watches depend only on what the parts did, so that a seed always gives the
-- same run.
runSimulation :: Word64 -> Int -> Loop a b -> a -> IO (Simulated b)
runSimulation seed limit program input = do
  woken <- newIORef []
  let -- The parts that tickets woke join those that can go on.
      go random next report trace parked ready waiting = do
        risen <- reverse <$> readIORef woken
        writeIORef woken []
        step random next report trace (filter (`notElem` map fst risen) parked) (ready <> risen) waiting
      step random next report trace parked ready waiting
        | reportSteps report >= limit = finish GaveUp
        | null ready && null waiting && null parked = ioError (userError "runSimulation: the program ended without a result")
        | null ready = do
            checked <- traverse (\entry@(_, (watch, _)) -> (,) entry <$> anyFired watch) waiting
            let (wake, still) = partition snd checked
            if null wake
              then finish (Deadlocked (sort (map (fst . fst) still <> parked)))
              else go random next report {reportWaits = reportWaits report + length wake} trace parked [(ident, process) | ((ident, (_, process)), _) <- wake] (map fst still)
        | otherwise = do
            let (random', pick) = splitMix random
                ((ident, process), others) = takeAt (fromIntegral (pick `mod` fromIntegral (length ready))) ready
                trace' = ident : trace
            (steps, advanced) <- advance 1 (\woke -> modifyIORef' woken ((ident, woke) :)) process
            let report' = report {reportSteps = reportSteps report + steps}
            case advanced of
              Finished value -> pure (Simulated (Completed value) report' (reverse trace'))
              Ended -> go random' next report' trace' parked others waiting
              Parked -> go random' next report' trace' (ident : parked) others waiting
              Yielded process' -> go random' next report' trace' parked ((ident, process') : others) waiting
              Forked one two -> go random' (next + 2) report' trace' parked ((next, one) : (next + 1, two) : others) waiting
              Waits watch process' -> go random' next report' trace' parked others ((ident, (watch, process')) : waiting)
        where
          finish outcome = pure (Simulated outcome report (reverse trace))
  go seed 1 (Report 0 0) [] [] [(0, Run program input Return)] []

-- | Registers the watches, waits until one has fired, and gives the values of
-- all that have fired by then. It releases them all, also if it is
-- interrupted.
awaitFired :: [(IO Watch, a)] -> IO [a]
awaitFired entries = mask $ \restore -> do
  registered <- newIORef []
  let register (make, value) = do
        watch <- make
        modifyIORef' registered (watch :)
        pure (watch, value)
      release = uninterruptibleMask_ (readIORef registered >>= mapM_ watchCancel)
  fired <-
    ( do
        watches <- traverse register entries
        restore (atomically (foldr (orElse . watchReady . fst) retry watches))
        filterM (hasFired . fst) watches
    )
      `onException` release
  map snd fired <$ release

-- | Whether the event has fired, without waiting.
anyFired :: IO Watch -> IO Bool
anyFired make = bracket make watchCancel hasFired

hasFired :: Watch -> IO Bool
hasFired watch = atomically ((True <$ watchReady watch) `orElse` pure False)

-- | The element at the index, and the others in their order.
takeAt :: Int -> [a] -> (a, [a])
takeAt index list = case splitAt index list of
  (before, chosen : after) -> (chosen, before <> after)
  (_, []) -> error "takeAt: the index is past the end"

-- | SplitMix64: the next state, and a random number.
splitMix :: Word64 -> (Word64, Word64)
splitMix state = (state', mixed `xor` (mixed `shiftR` 31))
  where
    state' = state + 0x9e3779b97f4a7c15
    z = (state' `xor` (state' `shiftR` 30)) * 0xbf58476d1ce4e5b9
    mixed = (z `xor` (z `shiftR` 27)) * 0x94d049bb133111eb
