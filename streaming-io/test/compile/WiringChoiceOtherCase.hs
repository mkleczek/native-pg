{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringChoiceOtherCase where

import Control.Arrow ((>>>), (|||))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

sized :: (Steps arr, HasField "i" r Int) => arr r (Either ("small" := Int) ("big" := Int))
sized = now (\r -> pure (if r.i < 10 then inj "small" r.i else inj "big" r.i))

-- | A branch that binds the value of another case: a branch has the value of the case that its pattern takes apart, and of
-- no other.
picked :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
picked =
  result
    "picked"
    ( "size" <: sized -< ["i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" =: "small" ?> same -< ["i" =: "small"]
                ||| "big" ?> sumOf -< ["i" =: "small", "j"]
            )
    )
