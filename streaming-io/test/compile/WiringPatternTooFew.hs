{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPatternTooFew where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

pairOf :: (Steps arr, HasField "i" r Int) => arr r (Int, String)
pairOf = now (\r -> pure (r.i, show r.i))

-- | A pattern with one name for a pair of two parts.
few :: (Steps arr) => Flow arr ("i" := Int) ("q" := Int, "i" := Int)
few = ["q"] <: pairOf -< ["i"]
