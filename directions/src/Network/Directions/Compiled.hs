{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | Every concurrent part on one thread, as 'Network.Directions.Loop' runs
-- them, but compiled: an arrow is a function that takes what follows it as a
-- continuation, so a program's composition is function composition, which GHC
-- inlines. A part that waits is a closure, which can't be looked into, and is
-- built only where the part waits.
module Network.Directions.Compiled
  ( Compiled,
    runCompiled,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (partition)
import Network.Directions.Classes
import Network.Directions.Loop (Report (..), awaitFired)
import Network.Streaming.Watch (Watch)
import Prelude hiding (id, (.))

-- | An arrow as a function of its input and of what follows it.
newtype Compiled a b = Compiled (Run -> a -> (b -> IO Step) -> IO Step)

-- | What a part comes to, for the scheduler.
data Step
  = -- | It ended, at a join or with the program's result.
    Ended
  | -- | It waits for the watch, and goes on with the action once it fires.
    Suspended (IO Watch) (IO Step)
  | -- | Two parts that go on at once.
    Forked (IO Step) (IO Step)
  | -- | It waits for a ticket, whose slot holds it.
    Parked
  | -- | It used its steps, and can go on.
    Yielded (IO Step)

-- | What the parts of a run share: the steps a part has left before it
-- yields, the parts that tickets woke, and the steps taken.
data Run = Run {runFuel :: IORef Int, runWoken :: IORef [IO Step], runSteps :: IORef Int}

instance Category Compiled where
  id = Compiled (\_ value next -> next value)
  {-# INLINE id #-}
  Compiled after . Compiled before = Compiled (\run value next -> before run value (\middle -> after run middle next))
  {-# INLINE (.) #-}

instance Arrow Compiled where
  arr function = Compiled (\_ value next -> next (function value))
  {-# INLINE arr #-}
  first (Compiled inner) = Compiled (\run (a, c) next -> inner run a (\b -> next (b, c)))
  {-# INLINE first #-}

instance ArrowChoice Compiled where
  left (Compiled inner) = Compiled $ \run choice next -> case choice of
    Left a -> inner run a (next . Left)
    Right c -> next (Right c)
  {-# INLINE left #-}

instance Effect Compiled where
  now action = Compiled (\run value next -> action value >>= \output -> stepped run (next output))
  {-# INLINE now #-}

instance Block Compiled where
  block = Compiled (\run register next -> counted run (Suspended register (next ())))
  {-# INLINE block #-}

instance Concurrent Compiled where
  concurrently (Compiled one) (Compiled two) = Compiled $ \run value next -> do
    joined <- newIORef Neither
    pure $
      Forked
        (one run value (\b -> readIORef joined >>= \case SecondDone c -> next (b, c); _ -> Ended <$ writeIORef joined (FirstDone b)))
        (two run value (\c -> readIORef joined >>= \case FirstDone b -> next (b, c); _ -> Ended <$ writeIORef joined (SecondDone c)))

-- | What a concurrent pair has of its halves' results.
data Half b c = Neither | FirstDone b | SecondDone c

-- | A ticket is a slot on this thread: a part that waits for it is parked in
-- it, as its continuation, and goes back among those that can go on when the
-- ticket is given its value.
instance Exchange Compiled where
  newtype Ticket Compiled a = CompiledTicket (IORef (Slot a))
  newTicket = CompiledTicket <$> newIORef Empty
  fulfil (CompiledTicket slot) = Compiled $ \run value next ->
    readIORef slot >>= \case
      Empty -> writeIORef slot (Given value) *> stepped run (next ())
      Waiter resume -> do
        writeIORef slot (Given value)
        modifyIORef' (runWoken run) (resume value :)
        stepped run (next ())
      Given _ -> ioError (userError "a ticket was given a value twice")
  await (CompiledTicket slot) = Compiled $ \run () next ->
    readIORef slot >>= \case
      Given value -> stepped run (next value)
      Empty -> Parked <$ writeIORef slot (Waiter next)
      Waiter _ -> ioError (userError "two parts wait for one ticket")

data Slot a = Empty | Given a | Waiter (a -> IO Step)

-- | Counts a step of a primitive, and goes on, or yields once the part has
-- used its steps.
stepped :: Run -> IO Step -> IO Step
stepped run continue = do
  modifyIORef' (runSteps run) (+ 1)
  remaining <- readIORef (runFuel run)
  if remaining <= 1 then pure (Yielded continue) else writeIORef (runFuel run) (remaining - 1) *> continue

-- | Counts a step of a primitive that suspends.
counted :: Run -> Step -> IO Step
counted run step = step <$ modifyIORef' (runSteps run) (+ 1)

-- | Runs the program and all of its concurrent parts on this thread, as
-- 'Network.Directions.Loop.runLoop' does: each part that can go on in turn,
-- for up to 64 steps, and when none can, one wait for all of their watches.
runCompiled :: Compiled a b -> a -> IO (b, Report)
runCompiled (Compiled program) input = do
  run <- Run <$> newIORef 64 <*> newIORef [] <*> newIORef 0
  result <- newIORef Nothing
  let finished =
        readIORef result >>= \case
          Just value -> (\steps -> Just (value, steps)) <$> readIORef (runSteps run)
          Nothing -> pure Nothing
      -- The parts that tickets woke join those that can go on.
      go waits parked ready waiting = do
        risen <- reverse <$> readIORef (runWoken run)
        writeIORef (runWoken run) []
        next waits (parked - length risen) (ready <> risen) waiting
      next waits parked ready waiting = case ready of
        part : others -> do
          writeIORef (runFuel run) 64
          step <- part
          finished >>= \case
            Just (value, steps) -> pure (value, Report steps waits)
            Nothing -> case step of
              Ended -> go waits parked others waiting
              Parked -> go waits (parked + 1) others waiting
              Yielded part' -> go waits parked (others <> [part']) waiting
              Forked one two -> go waits parked (others <> [one, two]) waiting
              Suspended watch part' -> go waits parked others ((watch, part') : waiting)
        []
          | null waiting && parked > (0 :: Int) -> ioError (userError "runCompiled: parts wait for tickets that nothing will give a value")
          | null waiting -> ioError (userError "runCompiled: the program ended without a result")
          | otherwise -> do
              fired <- awaitFired (zip (map fst waiting) [0 :: Int ..])
              let (wake, still) = partition ((`elem` fired) . fst) (zip [0 ..] waiting)
              go (waits + 1) parked (map (snd . snd) wake) (map snd still)
  go 0 0 [program run input (\value -> Ended <$ writeIORef result (Just value))] []
