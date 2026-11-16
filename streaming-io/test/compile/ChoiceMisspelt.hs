{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module ChoiceMisspelt where

import Control.Arrow (arr, (>>>), (^>>), (|||))
import Network.Streaming.Classes
import Network.Streaming.Combinators (decide)
import Network.Streaming.Named

-- | A value put in a case that the choice doesn't have: the branches take "small", "big" and "huge", and the producer
-- puts it in "smal".
misspelt :: (Steps arr) => arr Int String
misspelt =
  newRecord ["req", "number"] "GET /"
    ^>> adding "size" (arr byRange)
    >>> decide "how big?" (cases "size")
    >>> ( "small" ?> arr (\r -> r.req <> " small " <> show r.small)
            ||| "big" ?> arr (\r -> r.req <> " big " <> show r.big)
            ||| "huge" ?> arr (\r -> r.req <> " huge " <> show r.huge)
        )
  where
    byRange r
      | r.number < 10 = inj "smal" r.number
      | r.number < 100 = inj "big" r.number
      | otherwise = inj "huge" r.number
