-- | Synchronous streams with borrowed chunks.
module Network.Streaming (Source (..), Sink, Body, forward) where

import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS

-- | Empty means EOF. A chunk is valid only during its callback.
newtype Source = Source { withChunk :: forall a. (ByteString -> IO a) -> IO a }

-- | Consumes the complete chunk before returning; must not retain an alias.
type Sink = ByteString -> IO ()
type Body = Sink -> IO ()

forward :: Source -> Body
forward source sink = go
  where
    go = do
      more <- withChunk source $ \chunk ->
        if BS.null chunk then pure False else sink chunk >> pure True
      when more go
