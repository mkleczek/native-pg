{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringNoName where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A binding to a name that the flow doesn't have: "k" is not one of its inputs, and no earlier step has it.
missing :: (Steps arr) => Flow arr ("i" := Int, "j" := Int) ("total" := Int, ("i" := Int, "j" := Int))
missing = "total" <: sumOf -< ["i", "j" =: "k"]
