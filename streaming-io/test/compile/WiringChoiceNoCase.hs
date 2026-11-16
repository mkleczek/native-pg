{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringChoiceNoCase where

import Control.Arrow ((>>>), (|||))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

sumOf :: (Steps arr, HasField "i" r Int, HasField "j" r Int) => arr r Int
sumOf = now (\r -> pure (r.i + r.j))

same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

sized :: (Steps arr, HasField "i" r Int, Inj "small" Int s, Inj "big" Int s) => arr r s
sized = now (\r -> pure (if r.i < 10 then inj "small" r.i else inj "big" r.i))

-- | A branch of a case that the step does not put a value in: the step has "small" and "big", and the last branch is of
-- "large".
picked :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
picked =
  result
    "picked"
    ( "size" <: sized -< ["i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" ?> same -< ["i" =: "small"]
                ||| "large" ?> sumOf -< ["i" =: "large", "j"]
            )
    )
