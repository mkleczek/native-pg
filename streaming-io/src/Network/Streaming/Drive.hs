{-# LANGUAGE LambdaCase #-}

-- | The loop of a caller that tries without waiting and waits only when it
-- is blocked.
module Network.Streaming.Drive
  ( Progress (..),
    Polled (..),
    drive,
    poll,
  )
where

import Network.Streaming.Link (Interest, Link (..))

-- | What a step made of a try.
data Progress s a
  = -- | Done, with the result.
    Ready a
  | -- | Made progress: try again before waiting, since what it wants may
    -- already be ready.
    Again s
  | -- | Can't go on until what the interest includes may be retried.
    Blocked s Interest

-- | Runs the step from a state until it is ready, and waits for the link
-- when it is blocked. It is the only recursion and the only 'awaitAny' of its
-- users: a step never waits, and after a wait it tries again everything it
-- is blocked on, since readiness is a hint. It is inlined into its users, so
-- that their step's results are matched where they are made and 'Progress'
-- is never allocated.
drive :: Link link => link -> (s -> IO (Progress s a)) -> s -> IO a
drive link step = loop
  where
    loop s =
      step s >>= \case
        Ready a -> pure a
        Again s' -> loop s'
        Blocked s' interest -> awaitAny link interest *> loop s'
{-# INLINE drive #-}

-- | What 'poll' found.
data Polled a
  = -- | The step was ready, with the result.
    Got a
  | -- | The step was blocked on this interest, which 'poll' didn't wait for.
    Stalled Interest

-- | Runs the step from a state as 'drive' does, and gives up where 'drive' would
-- wait: with the interest the step was blocked on, which its caller may wait
-- for or watch, as it likes, and then try again from the start, since
-- readiness is a hint. It never waits itself. It is inlined into its users for
-- the reason 'drive' is.
poll :: (s -> IO (Progress s a)) -> s -> IO (Polled a)
poll step = loop
  where
    loop s =
      step s >>= \case
        Ready a -> pure (Got a)
        Again s' -> loop s'
        Blocked _ interest -> pure (Stalled interest)
{-# INLINE poll #-}
