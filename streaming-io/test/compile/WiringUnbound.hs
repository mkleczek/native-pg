{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringUnbound where

import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A step that reads the ports "i" and "j", with only "i" bound.
unbound :: (Steps arr) => Flow arr ("i" := Int, "j" := Int) ("total" := Int, ("i" := Int, "j" := Int))
unbound = "total" <: sumOf -< ["i"]
