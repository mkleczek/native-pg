module UndoingSteps where

import Control.Arrow (arr, (>>>), (>>^), (^>>))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, keeping, on, through, undoing, _1, _2)

-- | A step made of what can't wait: an action, a pure step, the clock, a scope, a shield and a failure that is caught.
effects :: (Steps arr) => arr Int Int
effects = now pure >>> (+ 1) ^>> clocked >>> scoped >>> caught
  where
    clocked = keeping clock >>^ snd
    scoped = scope (now pure) done (arr snd)
    caught = shield (tryA (now pure) >>^ either (const 0) id)

-- | The step and the cleanup work on parts of the input, picked by lenses.
undone :: (Steps arr) => arr (Int, (Int, String)) (Int, (Int, String))
undone = undoing (through (_2 . _1) effects >>> through _1 effects) (on (_2 . _1) (effects >>> done))
