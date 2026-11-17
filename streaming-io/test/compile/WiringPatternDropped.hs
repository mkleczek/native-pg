{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPatternDropped where

import Control.Arrow ((>>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

recordOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, "y" := Int)
recordOf = now (\r -> pure (newRecord ["x", "y"] r.i (r.i + 1)))

-- | A step that binds a field that the pattern left out: the flow has "x" and no "y".
dropped :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
dropped =
  result
    "total"
    ( ["x"] <: recordOf -< ["i"]
        >>> "total" <: sumOf -< ["i" =: "x", "j" =: "y"]
    )
