{-# LANGUAGE DataKinds #-}

module NamedTwice where

import Control.Arrow (arr)
import Network.Streaming.Classes
import Network.Streaming.Named

-- | A name that newRecord is given twice.
twice :: (Steps arr) => arr () ("size" := Int, "size" := Int)
twice = newRecord ["size", "size"] <$> arr (const 3) <*> arr (const 4)
