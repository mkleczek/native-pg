{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringRepeated where

import Control.Arrow ((>>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A step whose name is the name of an earlier step.
repeated :: (Steps arr) => Flow arr ("i" := Int, "j" := Int) ("a1" := Int, ("a1" := Int, ("i" := Int, "j" := Int)))
repeated = "a1" <: sumOf -< ["i", "j"] >>> "a1" <: sumOf -< ["i", "j"]
