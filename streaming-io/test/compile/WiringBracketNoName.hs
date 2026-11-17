{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringBracketNoName where

import Control.Arrow ((>>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that reads its port "i", and gives it.
same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

-- | A release that reads the port "k".
closing :: (Steps arr, HasField "k" r Int) => arr r ()
closing = now (\r -> r.k `seq` pure ())

-- | A release that binds a name that the flow doesn't have.
missing :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
missing =
  result
    "total"
    ( "res" <: same -< ["i"] `releasedBy` closing -< ["k"]
        >>> "total" <: same -< ["i"]
    )
