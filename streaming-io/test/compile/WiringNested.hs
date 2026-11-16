{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringNested where

import Control.Arrow ((>>>), (|||))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring


-- | A step that reads the ports "i" and "j", and gives their sum.
sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

-- | A step that reads its port "i", and gives it.
same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

-- | A step that reads no port, and gives a number.
zero :: (Steps arr) => arr r Int
zero = now (const (pure 0))

-- | A step that reads its port "i", and gives a record that has a record in a field.
connOf :: (Steps arr, HasField "i" r Int) => arr r ("socket" := ("fd" := Int, "addr" := String), "peer" := String)
connOf = now (\r -> pure (newRecord ["socket", "peer"] (newRecord ["fd", "addr"] r.i "a") "p"))

-- | A step that reads its port "i", and gives a pair that has a pair in its first part.
nestedPair :: (Steps arr, HasField "i" r Int) => arr r ((Int, Int), Int)
nestedPair = now (\r -> pure ((r.i, r.i), r.i))

-- | A step that reads its port "i", and gives a pair.
pairOf :: (Steps arr, HasField "i" r Int) => arr r (Int, String)
pairOf = now (\r -> pure (r.i, show r.i))

-- | The sum that a step gives, which a choice takes apart: a number, a pair and a record.
type Size = Either ("small" := Int) (Either ("big" := (Int, Int)) ("huge" := ("lo" := Int, "hi" := Int)))

-- | A step that gives the size of its port "i".
sized :: (Steps arr, HasField "i" r Int) => arr r Size
sized = now (\r -> pure (if r.i < 10 then inj "small" r.i else if r.i < 100 then inj "big" (r.i, r.i) else inj "huge" (newRecord ["lo", "hi"] r.i r.i)))

-- | A name that is kept whole and taken apart, and a list that takes a field of a record apart.
kept :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
kept =
  result
    "total"
    ( "conn" @: ["socket" =: ["fd", "addr"], "peer"] <: connOf -< ["i"]
        >>> "total" <: sumOf -< ["i" =: "fd", "j"]
    )

-- | A field that is kept under a name and taken apart.
under :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
under =
  result
    "total"
    ( ["socket" =: "s" @: ["fd", "addr"], "peer"] <: connOf -< ["i"]
        >>> "total" <: sumOf -< ["i" =: "fd", "j"]
    )

-- | A part of a pair that is taken apart by position, and a pair whose last name takes what is left.
positions :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
positions =
  result
    "total"
    ( ["_" @: ["x", "y"], "c"] <: nestedPair -< ["i"]
        >>> ["q"] <: pairOf -< ["i"]
        >>> ["first", "rest"] <: pairOf -< ["i"]
        >>> "total" <: sumOf -< ["i" =: "x", "j" =: "c"]
    )

-- | A case that is selected with no pattern, and cases whose value is taken by a name, by a list for a pair, and by a name
-- that is kept and takes a record apart. The names of the patterns are read by the branches, and are not names of the flow.
picking :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
picking =
  result
    "picked"
    ( "size" <: sized -< ["i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" =: "n" ?> same -< ["i" =: "n"]
                ||| "big" =: ["x", "y"] ?> sumOf -< ["i" =: "x", "j" =: "y"]
                ||| "huge" =: "h" @: ["lo", "hi"] ?> sumOf -< ["i" =: "lo", "j" =: "h.hi"]
            )
    )

-- | A case with no pattern: its value is not read.
selecting :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
selecting =
  result
    "picked"
    ( "size" <: sized -< ["i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" ?> same -< ["i"]
                ||| "big" ?> same -< ["i"]
                ||| "huge" ?> zero -< []
            )
    )
