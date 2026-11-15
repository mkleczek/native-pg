module StepGivesWay where

import Network.Streaming.Classes
import Network.Streaming.Combinators (done, undoing)

-- | A step that gives way to the other parts, which is a wait.
yielding :: (Steps arr, GiveWay arr) => arr () ()
yielding = giveWay

-- | A step that can wait, which undoing refuses.
rejected :: (Steps arr) => arr () ()
rejected = undoing yielding done
