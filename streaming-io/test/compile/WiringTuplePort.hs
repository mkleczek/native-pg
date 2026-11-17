{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringTuplePort where

import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that takes a number, and gives it doubled.
twice :: (Steps arr) => arr Int Int
twice = now (\n -> pure (n * 2))

-- | A step that takes a pair, and gives their sum.
sumPair :: (Steps arr) => arr (Int, Int) Int
sumPair = now (\(a, b) -> pure (a + b))

-- | A port in a tuple, which has none.
bad :: (Steps arr) => Flow arr ("i" := Int, "j" := Int) ("total" := Int, ("i" := Int, "j" := Int))
bad = "total" <: sumPair -< ("i", "port" =: "j")
