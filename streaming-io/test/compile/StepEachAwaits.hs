{-# LANGUAGE DataKinds #-}

-- The raw await is restricted outside the library and its tests, and this fixture needs it.
{- HLINT ignore "Avoid restricted function" -}
module StepEachAwaits where

import Control.Arrow ((^>>))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, each, undoing)

-- | A step that awaits a future, which waits.
awaiting :: (Steps arr, Exchange arr 'Anywhere) => arr () Int
awaiting = const undefined ^>> await @_ @'Anywhere

-- | The step runs it on each element, which is a loop that can wait now, and undoing refuses it.
rejected :: (Steps arr) => arr [()] [Int]
rejected = undoing (each awaiting) done
