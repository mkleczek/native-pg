{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPatternField where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

pairOf :: (Steps arr, HasField "i" r Int) => arr r (Int, String)
pairOf = now (\r -> pure (r.i, show r.i))

-- | A pattern that names a field of a pair, which has none.
byField :: (Steps arr) => Flow arr ("i" := Int) ("q" := Int, ("r" := String, "i" := Int))
byField = ["first" =: "q", "r"] <: pairOf -< ["i"]
