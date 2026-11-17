{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPatternWhole where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

number :: (Steps arr, HasField "i" r Int) => arr r Int
number = now (\r -> pure r.i)

-- | A pattern for an output that is neither a record nor a pair.
whole :: (Steps arr) => Flow arr ("i" := Int) ("q" := Int, "i" := Int)
whole = ["q"] <: number -< ["i"]
