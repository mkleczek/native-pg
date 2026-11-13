{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | The mode that an arrow is built in, as part of its type, like the reach of a ticket: 'Release' observes nothing of a
-- run, and 'Debug' observes what the build asks for. Code that the arrow runs and that observes something says so through
-- 'Instrumented' at the mode of its arrow, so that in 'Release' the call is a constant that specialisation removes.
module Network.Streaming.Mode
  ( Mode (..),
    ModeOf,
    Instrumented (..),
  )
where

import Data.Kind (Type)
import Network.Streaming.Counters (Counter, Counts, increment, incrementBy, snapshot)

-- | What a build observes of its runs. There is nothing to observe in 'Release'.
data Mode = Debug | Release

-- | The mode of an arrow. A mode is the arrow's own: "Network.Streaming.Loop" is always 'Release', and an arrow that
-- wraps another has the other's.
type family ModeOf (arr :: Type -> Type -> Type) :: Mode

-- | What a mode lets code observe of a run: the hooks are called in 'Debug' and do nothing in 'Release', where each is a
-- constant, so that a program that is built for a known mode has none of them. They are called with the mode of the arrow
-- that the code runs in, @bump \@(ModeOf arr)@, or with the mode that a run is made in.
class Instrumented (m :: Mode) where
  -- | Adds one to the count of the counter of "Network.Streaming.Counters".
  bump :: Counter -> IO ()

  -- | Adds to the count of the counter.
  bumpBy :: Counter -> Int -> IO ()

  -- | Runs the action, which only a build that observes needs.
  debugging :: IO () -> IO ()

  -- | The counts, which a build that observes nothing has none of.
  counted :: IO (Maybe Counts)

instance Instrumented 'Debug where
  bump = increment
  {-# INLINE bump #-}
  bumpBy = incrementBy
  {-# INLINE bumpBy #-}
  debugging = id
  {-# INLINE debugging #-}
  counted = Just <$> snapshot

instance Instrumented 'Release where
  bump _ = pure ()
  {-# INLINE bump #-}
  bumpBy _ _ = pure ()
  {-# INLINE bumpBy #-}
  debugging _ = pure ()
  {-# INLINE debugging #-}
  counted = pure Nothing
