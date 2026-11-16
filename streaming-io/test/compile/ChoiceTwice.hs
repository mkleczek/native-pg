{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module ChoiceTwice where

import Control.Arrow (arr, (>>>), (|||))
import Network.Streaming.Classes
import Network.Streaming.Combinators (decide)
import Network.Streaming.Named

-- | A choice of two cases that have one name: the second branch could never be reached, as @inj "small"@ puts a value
-- in the first.
twice :: (Steps arr) => arr ("size" := Either ("small" := Int) ("small" := Bool)) String
twice =
  decide "how big?" (cases "size")
    >>> ("small" =: "small" ?> arr (\r -> show r.small) ||| "small" =: "small" ?> arr (\r -> show r.small))
