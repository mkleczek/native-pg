{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPatternNoField where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

recordOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, "y" := String)
recordOf = now (\r -> pure (newRecord ["x", "y"] r.i (show r.i)))

-- | A pattern that names a field that the output doesn't have: it has "x" and "y", and not "w".
missing :: (Steps arr) => Flow arr ("i" := Int, "j" := Int) ("x" := Int, ("w" := String, ("i" := Int, "j" := Int)))
missing = ["x", "w"] <: recordOf -< ["i"]
