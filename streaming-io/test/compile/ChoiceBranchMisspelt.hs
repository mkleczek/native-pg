{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module ChoiceBranchMisspelt where

import Control.Arrow (arr, (>>>), (^>>), (|||))
import Network.Streaming.Classes
import Network.Streaming.Combinators (decide)
import Network.Streaming.Named

-- | A branch of a case that the producer doesn't put anything in: the producer has "small", "big" and "huge", and the
-- last branch is of "hugee", so that the choice of the branches has no "huge".
misspelt :: (Steps arr) => arr Int String
misspelt =
  newRecord ["req", "number"] "GET /"
    ^>> adding "size" (arr byRange)
    >>> decide "how big?" (cases "size")
    >>> ( "small" =: "small" ?> arr (\r -> r.req <> " small " <> show r.small)
            ||| "big" =: "big" ?> arr (\r -> r.req <> " big " <> show r.big)
            ||| "hugee" =: "hugee" ?> arr (\r -> r.req <> " huge " <> show r.hugee)
        )
  where
    byRange r
      | r.number < 10 = inj "small" r.number
      | r.number < 100 = inj "big" r.number
      | otherwise = inj "huge" r.number
