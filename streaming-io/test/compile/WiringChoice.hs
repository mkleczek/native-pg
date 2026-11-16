{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringChoice where

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

-- | The sum that a step gives, which a choice takes apart.
type Size = Either ("small" := Int) (Either ("big" := Int) ("huge" := Int))

-- | A step that gives the size of its port "i".
sized :: (Steps arr, HasField "i" r Int) => arr r Size
sized = now (\r -> pure (if r.i < 10 then inj "small" r.i else if r.i < 100 then inj "big" r.i else inj "huge" r.i))

-- | A choice of three cases whose branches bind different names: the value of their case and an input of the flow, the
-- value of their case and a step of the flow, and nothing. Its result is named.
picking :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
picking =
  result
    "picked"
    ( "size" <: sized -< ["i"]
        >>> "twice" <: sumOf -< ["i", "j" =: "i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" =: "small" ?> same -< ["i" =: "small"]
                ||| "big" =: "big" ?> sumOf -< ["i" =: "big", "j" =: "twice"]
                ||| "huge" ?> zero -< []
            )
    )

-- | The same choice with no name for its result: the record of the flow goes on as it was.
passing :: (Steps arr) => arr ("i" := Int, "j" := Int) ("size" := Size, ("i" := Int, "j" := Int))
passing =
  flow
    ( "size" <: sized -< ["i"]
        >>> choice
          "how big?"
          "size"
          ( "small" =: "small" ?> same -< ["i" =: "small"]
              ||| "big" =: "big" ?> sumOf -< ["i" =: "big", "j"]
              ||| "huge" ?> zero -< []
          )
    )
