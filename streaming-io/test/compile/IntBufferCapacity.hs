module IntBufferCapacity where

import Network.Streaming.Buffer (Buffer, newBuffer)

invalid :: IO Buffer
invalid = newBuffer (0 :: Int)
