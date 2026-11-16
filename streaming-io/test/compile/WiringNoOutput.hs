{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringNoOutput where

import Control.Arrow ((>>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

twoOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, "y" := String)
twoOf = now (\r -> pure (newRecord ["x", "y"] r.i (show r.i)))

-- | A path to an output that the step "a1" doesn't give: it has "x" and "y", and not "z".
missing :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
missing =
  result
    "total"
    ( "a1" <: twoOf -< ["i"]
        >>> "total" <: sumOf -< ["i" =: "a1.z", "j"]
    )
