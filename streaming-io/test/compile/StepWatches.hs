{-# LANGUAGE DataKinds #-}

-- The raw watch is restricted outside the library and its tests, and this fixture needs it.
{- HLINT ignore "Avoid restricted function" -}
module StepWatches where

import Control.Arrow ((^>>))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, undoing)

-- | A step that waits until the receiver is parked, which waits.
watching :: (Steps arr, Exchange arr 'Anywhere) => arr () ()
watching = const undefined ^>> untilParked @_ @'Anywhere @Int

-- | A step that can wait, which undoing refuses.
rejected :: (Steps arr) => arr () ()
rejected = undoing watching done
