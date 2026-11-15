{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module NamedMisspelt where

import Control.Arrow (arr, (&&&), (>>>))
import Network.Streaming.Classes
import Network.Streaming.Named

-- | A field that the record doesn't have: the name is misspelt.
missing :: (Steps arr) => arr () Int
missing =
  ( is "name" (arr (const "pipe"))
      &&& is "size" (arr (const (3 :: Int)))
      &&& is "kept" (arr (const True))
  )
    >>> now (\r -> pure r.count)
