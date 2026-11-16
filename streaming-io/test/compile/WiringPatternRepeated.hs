{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPatternRepeated where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

pairOf :: (Steps arr, HasField "i" r Int) => arr r (Int, String)
pairOf = now (\r -> pure (r.i, show r.i))

-- | A pattern with the name of an input of the flow.
repeated :: (Steps arr) => Flow arr ("i" := Int) ("i" := Int, ("r" := String, "i" := Int))
repeated = ["i", "r"] <: pairOf -< ["i"]
