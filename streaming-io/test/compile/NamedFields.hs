{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module NamedFields where

import Control.Arrow (arr, (&&&), (>>>), (>>^))
import Network.Streaming.Classes
import Network.Streaming.Named

-- | A record of three fields, each made by a step of its own.
record :: (Steps arr) => arr () ("name" := String, ("size" := Int, "kept" := Bool))
record =
  is "name" (arr (const "pipe"))
    &&& is "size" (arr (const 3))
    &&& is "kept" (arr (const True))

-- | The first, a middle and the last field of the record, read by name where a step uses them.
readByName :: (Steps arr) => arr () (String, Int, Bool)
readByName = record >>> now (\r -> pure (r.name, r.size, r.kept))

-- | A field read by 'field', which has its type from the record.
readByField :: (Steps arr) => arr () Int
readByField = record >>> field "size"

-- | A name that the record has twice is read from the first field: a field with another name is read all the same.
readOther :: (Steps arr) => arr () Bool
readOther =
  ( is "size" (arr (const (3 :: Int)))
      &&& is "kept" (arr (const True))
      &&& is "size" (arr (const (4 :: Int)))
  )
    >>^ \r -> r.kept

-- | A record that newRecord makes from three arrows, read as the one that '&&&' makes is.
made :: (Steps arr) => arr () (String, Int, Bool)
made =
  newRecord ["name", "size", "kept"]
    <$> arr (const "pipe")
    <*> arr (const 3)
    <*> arr (const True)
    >>^ \r -> (r.name, r.size, r.kept)
