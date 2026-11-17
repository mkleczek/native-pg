{-# LANGUAGE DataKinds #-}

-- The raw hand is restricted outside the library and its tests, and this fixture needs it.
{- HLINT ignore "Avoid restricted function" -}
module StepHands where

import Control.Arrow ((^>>))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, undoing)

-- | A step that hands a value over and waits until the receiver takes it, which waits.
handing :: (Steps arr, Exchange arr 'Anywhere) => arr () (Maybe Int)
handing = const undefined ^>> handWaiting @_ @'Anywhere

-- | A step that can wait, which undoing refuses.
rejected :: (Steps arr) => arr () (Maybe Int)
rejected = undoing handing done
