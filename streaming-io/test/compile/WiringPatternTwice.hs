{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPatternTwice where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

recordOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, "y" := Int)
recordOf = now (\r -> pure (newRecord ["x", "y"] r.i (r.i + 1)))

-- | A pattern that gives two parts the name "z".
twice :: (Steps arr) => Flow arr ("i" := Int) ("z" := Int, ("z" := Int, "i" := Int))
twice = ["x" =: "z", "y" =: "z"] <: recordOf -< ["i"]
