{-# LANGUAGE DataKinds #-}
module PositiveBuffers where

import Network.Streaming.PositiveInt
import Network.Streaming.Buffer (Buffer, newBuffer, newBuffers)

allocated :: IO Buffer
allocated = newBuffer (positiveInt @8)

batch :: IO (Int, [Buffer])
batch = newBuffers (positiveInt @16) (positiveInt @8)
