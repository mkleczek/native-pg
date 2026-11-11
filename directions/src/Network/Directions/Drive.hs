{-# LANGUAGE LambdaCase #-}

-- | Drivers: what runs directions, and waits for them. A direction is a stage
-- whose first stage reads nothing and whose last writes nothing. Its steps
-- never wait; the drivers differ only in how they do it.
module Network.Directions.Drive
  ( Report (..),
    runThreads,
    runLoop,
    Outcome (..),
    Simulated (..),
    runSimulation,
  )
where

import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.Chan (newChan, readChan, writeChan)
import Control.Concurrent.STM (atomically, orElse, retry)
import Control.Exception (SomeException, bracket, finally, mask, onException, throwIO, try, uninterruptibleMask_)
import Control.Monad (filterM, forM, zipWithM)
import Data.Bits (shiftR, xor)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.List (partition, sort)
import Data.Word (Word64)
import Network.Directions.Stage
import Network.Streaming.Watch (Watch (..), awaitFirst)

-- | What a run did: how many steps, and how many times directions waited.
data Report = Report {reportSteps :: !Int, reportWaits :: !Int}
  deriving (Eq, Show)

instance Semigroup Report where
  Report steps waits <> Report steps' waits' = Report (steps + steps') (waits + waits')

instance Monoid Report where
  mempty = Report 0 0

-- | A thread for each direction, which waits for its direction's events. A
-- failure of any direction stops all of them, and is rethrown.
runThreads :: [Stage] -> IO Report
runThreads stages = do
  closed <- closedGap
  results <- newChan
  threads <- forM (zip [0 ..] stages) $ \(index, stage) ->
    forkIO (try (begin closed index stage >>= alone mempty) >>= writeChan results)
  let collect report left
        | left == 0 = pure report
        | otherwise =
            readChan results >>= \case
              Left (failure :: SomeException) -> throwIO failure
              Right done -> collect (report <> done) (left - 1)
  collect mempty (length stages) `finally` mapM_ killThread threads
  where
    alone report direction =
      stepOnce direction >>= \case
        Moved direction' -> alone (tick report) direction'
        Waiting direction' events -> do
          awaitFirst [(event, ()) | event <- events]
          alone (tick report <> Report 0 1) direction'
        Ended -> pure (tick report)

-- | All directions on this thread. It steps each that can go on in turn, and
-- when none can, waits once for the events of all of them, and goes on with
-- those whose events fired.
runLoop :: [Stage] -> IO Report
runLoop stages = do
  closed <- closedGap
  directions <- zipWithM (begin closed) [0 ..] stages
  go mempty directions []
  where
    go report ready waiting = case ready of
      direction : others ->
        stepOnce direction >>= \case
          Moved direction' -> go (tick report) (others <> [direction']) waiting
          Waiting direction' events -> go (tick report) others ((direction', events) : waiting)
          Ended -> go (tick report) others waiting
      []
        | null waiting -> pure report
        | otherwise -> do
            woken <- awaitFired [(event, indexOf direction) | (direction, events) <- waiting, event <- events]
            let (wake, still) = partition ((`elem` woken) . indexOf . fst) waiting
            go (report <> Report 0 1) (map fst wake) still

-- | How a simulation ended.
data Outcome
  = Completed
  | -- | These directions wait for each other: none of their events can fire.
    Deadlocked [Int]
  | -- | It took the most steps it was allowed.
    GaveUp
  deriving (Eq, Show)

data Simulated = Simulated
  { simulatedOutcome :: Outcome,
    simulatedReport :: Report,
    -- | The directions, in the order they stepped.
    simulatedTrace :: [Int]
  }

-- | All directions on this thread, in an order the seed chooses: each step
-- goes to one of the directions that can go on, picked at random, up to the
-- given number of steps. It never waits. When no direction can go on, it looks
-- at their events and goes on with those whose events have fired; if none
-- has, the directions wait for each other, which it reports. Its links must be
-- simulated ones, whose events depend only on what the directions did, so
-- that a seed always gives the same run.
runSimulation :: Word64 -> Int -> [Stage] -> IO Simulated
runSimulation seed limit stages = do
  closed <- closedGap
  directions <- zipWithM (begin closed) [0 ..] stages
  go seed mempty [] directions []
  where
    go random report trace ready waiting
      | reportSteps report >= limit = finish GaveUp
      | null ready && null waiting = finish Completed
      | null ready = do
          checked <- traverse (\entry -> (,) entry <$> anyFired (snd entry)) waiting
          let (wake, still) = partition snd checked
          if null wake
            then finish (Deadlocked (sort (map (indexOf . fst . fst) still)))
            else go random (report <> Report 0 (length wake)) trace (map (fst . fst) wake) (map fst still)
      | otherwise = do
          let (random', pick) = splitMix random
              (direction, others) = takeAt (fromIntegral (pick `mod` fromIntegral (length ready))) ready
              trace' = indexOf direction : trace
          stepOnce direction >>= \case
            Moved direction' -> go random' (tick report) trace' (direction' : others) waiting
            Waiting direction' events -> go random' (tick report) trace' others ((direction', events) : waiting)
            Ended -> go random' (tick report) trace' others waiting
      where
        finish outcome = pure (Simulated outcome report (reverse trace))

-- | A direction made ready to run: its number, its state, and its step with
-- its closed ends.
data Running = forall s. Running !Int !s (s -> IO (Step s))

-- | What one step of a direction gave.
data Stepped = Moved Running | Waiting Running [IO Watch] | Ended

begin :: Gap -> Int -> Stage -> IO Running
begin closed index (Stage start step) = do
  state <- start
  pure (Running index state (\s -> step s (inputOf closed) (outputOf closed)))

stepOnce :: Running -> IO Stepped
stepOnce (Running index state step) =
  step state >>= \case
    Again state' -> pure (Moved (Running index state' step))
    Await state' wants -> Waiting (Running index state' step) <$> eventsOf index wants
    Done -> pure Ended

-- | The events a direction waits for. Nothing is before or after it, so it
-- can't wait for anything else.
eventsOf :: Int -> [Want] -> IO [IO Watch]
eventsOf index wants = case [event | Event event <- wants] of
  [] -> ioError (userError ("direction " <> show index <> " waits for nothing that can wake it"))
  events -> pure events

indexOf :: Running -> Int
indexOf (Running index _ _) = index

tick :: Report -> Report
tick report = report {reportSteps = reportSteps report + 1}

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

-- | Whether any of the events has fired, without waiting.
anyFired :: [IO Watch] -> IO Bool
anyFired = fmap or . traverse (\make -> bracket make watchCancel hasFired)

-- | Whether the watch has fired, without waiting.
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
