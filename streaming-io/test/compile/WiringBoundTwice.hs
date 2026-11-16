{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringBoundTwice where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A port that two bindings have: "i" is bound to "i" and to "j".
twice :: (Steps arr) => Flow arr ("i" := Int, "j" := Int) ("total" := Int, ("i" := Int, "j" := Int))
twice = "total" <: sumOf -< ["i", "i" =: "j", "j"]
