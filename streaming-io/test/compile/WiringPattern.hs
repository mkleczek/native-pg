{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringPattern where

import Control.Arrow ((>>>), (|||))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that reads the ports "i" and "j", and gives their sum.
sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A step that gives a record of three fields, from its port "i".
recordOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, ("y" := String, "z" := Int))
recordOf = now (\r -> pure (newRecord ["x", "y", "z"] r.i (show r.i) (r.i + 1)))

-- | A step that gives a pair, from its ports "i" and "j".
pairOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r (Int, Int)
pairOf = now (\r -> pure (r.i `div` r.j, r.i `mod` r.j))

-- | A step that gives pairs that nest to the right, from its port "i".
tripleOf :: (Steps arr, HasField "i" r Int) => arr r (Int, (String, Int))
tripleOf = now (\r -> pure (r.i, (show r.i, r.i + 2)))

-- | The sum that a step gives, which a choice takes apart.
type Size = Either ("small" := Int) ("big" := Int)

-- | A step that gives the size of its port "i".
sized :: (Steps arr, HasField "i" r Int) => arr r Size
sized = now (\r -> pure (if r.i < 10 then inj "small" r.i else inj "big" r.i))

-- | Lists of names that take the output of a step apart: a record partly, a record with a field under another name, a
-- pair by position, pairs that nest to the right with one part dropped, and the names that they give are read by the
-- steps after them.
wired :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
wired =
  result
    "total"
    ( ["x", "z"] <: recordOf -< ["i"]
        >>> ["x" =: "again", "y" =: "text"] <: recordOf -< ["i"]
        >>> ["q", "r"] <: pairOf -< ["i", "j"]
        >>> ["first", "_", "last"] <: tripleOf -< ["i"]
        >>> "total" <: sumOf -< ["i" =: "x", "j" =: "q"]
    )

-- | The names of a pattern are the names of a choice's branches: the branch reads them.
picking :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
picking =
  result
    "picked"
    ( ["q", "r"] <: pairOf -< ["i", "j"]
        >>> "size" <: sized -< ["i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" ?> sumOf -< ["i" =: "small", "j" =: "q"]
                ||| "big" ?> sumOf -< ["i" =: "big", "j" =: "r"]
            )
    )

-- | A pattern in a flow over a record that is not known.
anyRecord :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
anyRecord = result "total" (["q", "r"] <: pairOf -< ["i", "j"] >>> "total" <: sumOf -< ["i" =: "q", "j" =: "r"])
