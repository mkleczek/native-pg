{-# LANGUAGE DataKinds #-}

module Zero where

import Network.Streaming.PositiveInt

value :: PositiveInt
value = positiveInt @0
