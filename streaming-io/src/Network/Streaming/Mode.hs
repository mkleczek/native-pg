{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | The mode that an arrow is built in, as part of its type, like the reach of a ticket: 'Release' observes nothing of a
-- run, and 'Debug' observes what the build asks for. Code that the arrow runs and that observes something says so through
-- 'Instrumented' at the mode of its arrow, so that in 'Release' the call is a constant that specialisation removes.
module Network.Streaming.Mode
  ( Mode (..),
    ModeOf,
    Instrumented,
  )
where

import Data.Kind (Type)

-- | What a build observes of its runs. There is nothing to observe in 'Release'.
data Mode = Debug | Release

-- | The mode of an arrow. A mode is the arrow's own: "Network.Streaming.Loop" is always 'Release', and an arrow that
-- wraps another has the other's.
type family ModeOf (arr :: Type -> Type -> Type) :: Mode

-- | What a mode lets code observe of a run, which is a hook that is called in 'Debug' and does nothing in 'Release'.
class Instrumented (m :: Mode)

instance Instrumented 'Debug

instance Instrumented 'Release
