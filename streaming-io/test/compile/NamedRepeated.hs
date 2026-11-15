{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module NamedRepeated where

import Control.Arrow (arr, (&&&), (>>>))
import Network.Streaming.Classes
import Network.Streaming.Named

-- | A name that two fields of the record have, read.
repeated :: (Steps arr) => arr () Int
repeated =
  ( is "size" (arr (const (3 :: Int)))
      &&& is "kept" (arr (const True))
      &&& is "size" (arr (const (4 :: Int)))
  )
    >>> now (\r -> pure r.size)
