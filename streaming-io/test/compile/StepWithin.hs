module StepWithin where

import Control.Arrow (returnA)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, undoing)

-- | A step with a deadline, which can wait.
bounded :: (Steps arr, Within arr) => arr () (Maybe ())
bounded = within (secs 1) returnA

-- | A step that can wait, which undoing refuses.
rejected :: (Steps arr) => arr () (Maybe ())
rejected = undoing bounded done
