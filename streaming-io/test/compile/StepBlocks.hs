module StepBlocks where

import Control.Arrow ((^>>))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, undoing)
import System.Posix.Types (Fd (..))

-- | A step that blocks until a descriptor is readable.
blocking :: (Steps arr, Block arr) => arr () ()
blocking = const (Readable (Fd 0)) ^>> block

-- | A step that can wait, which undoing refuses.
rejected :: (Steps arr) => arr () ()
rejected = undoing blocking done
