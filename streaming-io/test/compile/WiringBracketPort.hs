{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringBracketPort where

import Control.Arrow ((>>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that reads its port "i", and gives it.
same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

-- | A release that takes the exit whole.
closing :: (Steps arr) => arr Exit ()
closing = now (\exit -> exit `seq` pure ())

-- | A port in a name that a release takes whole: a name has no ports.
bad :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
bad =
  result
    "total"
    ( "res" <: same -< ["i"] `releasedBy` closing -< "port" =: "exit"
        >>> "total" <: same -< ["i"]
    )
