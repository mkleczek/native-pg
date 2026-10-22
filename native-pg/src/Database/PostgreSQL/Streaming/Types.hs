-- | Length-bearing byte streams and parameter sources supplied by clients.
module Database.PostgreSQL.Streaming.Types (ByteStream (..), Parameter (..)) where

import Data.ByteString (ByteString)

-- | Bytes that arrive in chunks, with their total length known up front.
data ByteStream = ByteStream
  { byteStreamLength :: !Int,
    -- | The next chunk. An empty one means the end.
    byteStreamNext :: IO ByteString
  }

-- | A parameter value the client sends: bytes at hand, whole or in chunks,
-- or a stream it supplies while the value is sent.
data Parameter
  = ParameterBytes ByteString
  | -- | Sent one after another, each from where it is, as the serializer sends a
    -- long one, so that they aren't copied into one first.
    ParameterChunks [ByteString]
  | ParameterStream ByteStream
  | -- | Each chunk may reuse storage after the next pull. The source and
    -- its storage must remain exclusively lent until the operation ends.
    -- The sender accepts the whole chunk before pulling another.
    ParameterBorrowedStream ByteStream
