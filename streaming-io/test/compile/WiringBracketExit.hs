{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringBracketExit where

import Control.Arrow ((>>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that reads its port "i", and gives it.
same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

-- | A release that reads the port "exit".
closing :: (Steps arr, HasField "exit" r Exit) => arr r ()
closing = now (\r -> r.exit `seq` pure ())

-- | A line that is released, in a flow that has the name "exit" already.
repeated :: (Steps arr) => arr ("i" := Int, "exit" := Int) Int
repeated =
  result
    "total"
    ( "res" <: same -< ["i"] `releasedBy` closing -< ["exit"]
        >>> "total" <: same -< ["i"]
    )
