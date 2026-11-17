{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringFlow where

import Control.Arrow (arr, (>>>), (^>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that reads the ports "i" and "j", and gives their sum.
sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A step that gives a record of two outputs, from its port "i".
twoOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, "y" := String)
twoOf = now (\r -> pure (newRecord ["x", "y"] r.i (show r.i)))

-- | A step that gives nothing, and waits for its port "i".
noting :: (Steps arr, HasField "i" r Int) => arr r ()
noting = now (\r -> pure (r.i `seq` ()))

-- | A flow of steps that are wired by name: bare names, and bindings of a port to a name in the flow, a name bound to
-- two ports, a step whose ports are fed by two producers, a path into the output of a step, and a step that gives
-- nothing.
wired :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
wired =
  result
    "total"
    ( "first" <: sumOf -< ["i", "j"]
        >>> "pair" <: twoOf -< ["i" =: "first"]
        >>> noting -< ["i" =: "pair.x"]
        >>> "twice" <: sumOf -< ["i" =: "first", "j" =: "first"]
        >>> "total" <: sumOf -< ["i" =: "pair.x", "j"]
    )

-- | A flow over a record that is not known, which a step takes as it takes any record that has its names.
anyRecord :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
anyRecord = result "both" ("both" <: sumOf -< ["i", "j"] >>> noting -< ["i"])
