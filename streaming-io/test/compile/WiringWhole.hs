{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringWhole where

import Control.Arrow ((>>>), (|||))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that takes a number, and gives it doubled.
twice :: (Steps arr) => arr Int Int
twice = now (\n -> pure (n * 2))

-- | A step that takes a pair, and gives their sum.
sumPair :: (Steps arr) => arr (Int, Int) Int
sumPair = now (\(a, b) -> pure (a + b))

-- | A step that takes pairs that nest to the right, and gives their sum.
sumTriple :: (Steps arr) => arr (Int, (Int, Int)) Int
sumTriple = now (\(a, (b, c)) -> pure (a + b + c))

-- | A step that takes nothing, and gives a number.
unit :: (Steps arr) => arr () Int
unit = now (const (pure 1))

-- | A step that reads its port "i", and gives it.
same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

-- | A step that reads its port "i", and gives a record of two outputs.
twoOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, "y" := Int)
twoOf = now (\r -> pure (newRecord ["x", "y"] r.i (r.i + 1)))

-- | A step that gives the size of its number.
sized :: (Steps arr) => arr Int (Either ("small" := Int) ("big" := Int))
sized = now (\n -> pure (if n < 10 then inj "small" n else inj "big" n))

-- | A name that a step takes whole, with a name for the output and without, and a path; a pair and a triple of names,
-- some of them paths; the list of no name, and of one.
wired :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
wired =
  result
    "total"
    ( "first" <: twice -< "i"
        >>> twice -< "j"
        >>> "pair" <: twoOf -< ["i"]
        >>> "path" <: twice -< "pair.y"
        >>> "sum" <: sumPair -< ("i", "pair.x")
        >>> "three" <: sumTriple -< ("first", "j", "pair.y")
        >>> "none" <: unit -< []
        >>> "one" <: same -< ["i"]
        >>> "total" <: sumPair -< ("none", "one")
    )

-- | A name and a pair in the branches of a choice, in a flow that has a name for its result.
choosing :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
choosing =
  result
    "picked"
    ( "size" <: sized -< "i"
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" =: "small" ?> twice -< "small"
                ||| "big" =: "big" ?> sumPair -< ("big", "j")
            )
    )

-- | A flow over a record that is not known, which a step takes as it takes any record that has its names.
anyRecord :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
anyRecord = result "both" ("both" <: sumPair -< ("i", "j") >>> twice -< "i")
