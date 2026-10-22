-- | Synchronous streams with borrowed chunks.
module Network.Streaming (Source, Sink, Body, forward) where

import Control.Monad (unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS

-- | Empty means EOF. A nonempty chunk is valid until the next pull or scope end.
type Source = IO ByteString

-- | Consumes the complete chunk before returning; must not retain an alias.
type Sink = ByteString -> IO ()
type Body = Sink -> IO ()

forward :: Source -> Body
forward source sink = go
  where
    go = do
      chunk <- source
      unless (BS.null chunk) $ sink chunk >> go
