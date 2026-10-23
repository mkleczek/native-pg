{-# LANGUAGE DataKinds #-}
module IntBufferBudget where

import Network.Streaming.Buffer (Buffer, newBuffers)
import Network.Streaming.PositiveInt (positiveInt)

invalid :: IO (Int, [Buffer])
invalid = newBuffers (0 :: Int) (positiveInt @8)
