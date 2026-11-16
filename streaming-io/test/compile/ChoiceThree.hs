{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module ChoiceThree where

import Control.Arrow (arr, (>>>), (^>>), (|||))
import Network.Streaming.Classes
import Network.Streaming.Combinators (decide)
import Network.Streaming.Named

-- | A choice of three cases that a chain of branches takes apart, and a producer that puts a value in each of them.
-- Neither has a signature: the type of the choice comes from the branches.
choice :: (Steps arr) => arr Int String
choice =
  newRecord ["req", "number"] "GET /"
    ^>> adding "size" (arr byRange)
    >>> decide "how big?" (cases "size")
    >>> ( "small" ?> arr (\r -> r.req <> " small " <> show r.small)
            ||| "big" ?> arr (\r -> r.req <> " big " <> show r.big)
            ||| "huge" ?> arr (\r -> r.req <> " huge " <> show r.huge)
        )
  where
    byRange r
      | r.number < 10 = inj "small" r.number
      | r.number < 100 = inj "big" r.number
      | otherwise = inj "huge" r.number

-- | The prism of a case of a plain sum, which changes its type.
changed :: (Steps arr) => arr (Either ("small" := Int) (Either ("big" := Int) ("huge" := Int))) (Either ("small" := Int) (Either ("big" := String) ("huge" := Int)))
changed = _Case "big" (arr show)
