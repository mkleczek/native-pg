{-# LANGUAGE DataKinds #-}

module NamedTwiceApart where

import Control.Arrow (arr)
import Network.Streaming.Classes
import Network.Streaming.Named

-- | A name that newRecord is given twice, with another name between the two.
apart :: (Steps arr) => arr () ("a" := Int, ("b" := Int, "a" := Int))
apart = newRecord ["a", "b", "a"] <$> arr (const 1) <*> arr (const 2) <*> arr (const 3)
