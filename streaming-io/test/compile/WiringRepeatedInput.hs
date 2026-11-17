{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringRepeatedInput where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A step whose name is the name of an input of the flow.
repeated :: (Steps arr) => Flow arr ("i" := Int, "j" := Int) ("i" := Int, ("i" := Int, "j" := Int))
repeated = "i" <: sumOf -< ["i", "j"]
