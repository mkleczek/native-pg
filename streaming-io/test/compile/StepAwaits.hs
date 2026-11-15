{-# LANGUAGE DataKinds #-}

-- The raw await is restricted outside the library and its tests, and this fixture needs it.
{- HLINT ignore "Avoid restricted function" -}
module StepAwaits where

import Control.Arrow ((^>>))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, undoing)

-- | A step that awaits a future, which waits.
awaiting :: (Steps arr, Exchange arr 'Anywhere) => arr () Int
awaiting = const undefined ^>> await @_ @'Anywhere

-- | A step that can wait, which undoing refuses.
rejected :: (Steps arr) => arr () Int
rejected = undoing awaiting done
