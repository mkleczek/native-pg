{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringNestedBindingAs where

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

-- | A name that is kept and taken apart on the right of -<.
inputs :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
inputs = result "total" ("total" <: sumOf -< ["i" =: "x" @: ["y"], "j"])
