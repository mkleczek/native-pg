module UndoingEach where

import Control.Arrow ((>>>))
import Data.Monoid (Sum (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, each, folding, traversal, undoing, _Just)

-- | The step and the cleanup run an effect that can't wait on each element, which is a loop that can't wait either.
eachStep :: (Steps arr) => arr [Int] [Int]
eachStep = undoing (each (now pure)) (each (now pure) >>> done)

-- | The same for each focus of a traversal.
traversalStep :: (Steps arr) => arr (Maybe Int) (Maybe Int)
traversalStep = undoing (traversal traverse (now pure)) (traversal traverse (now pure) >>> done)

-- | The same for each part of a fold.
foldingStep :: (Steps arr) => arr [Int] (Sum Int)
foldingStep = undoing (folding id (now (pure . Sum))) (folding id (now (pure . Sum)) >>> done)

-- | The same for the case of a prism.
prismStep :: (Steps arr) => arr (Maybe Int) (Maybe Int)
prismStep = undoing (_Just (now pure)) (_Just (now pure) >>> done)
