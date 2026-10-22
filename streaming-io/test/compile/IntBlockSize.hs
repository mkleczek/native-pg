{-# LANGUAGE DataKinds #-}
module IntBlockSize where

import Network.Streaming.Buffer (Buffer, newBuffers)
import Network.Streaming.PositiveInt (positiveInt)

invalid :: IO [Buffer]
invalid = newBuffers (positiveInt @8) (0 :: Int)
