-- | Length-bearing byte streams and parameter sources supplied by clients.
module Database.PostgreSQL.Streaming.Types (ByteStream (..), Parameter (..)) where

import Data.ByteString (ByteString)
import Network.Streaming.Output (ByteStream (..))
import Network.Streaming.Source (Source)

-- | A parameter value the client sends: bytes at hand, whole or in chunks,
-- or a stream it supplies while the value is sent.
data Parameter
  = ParameterBytes ByteString
  | -- | Sent one after another, each from where it is, as the serializer sends a
    -- long one, so that they aren't copied into one first.
    ParameterChunks [ByteString]
  | ParameterStream ByteStream
  | -- | Declared length and a source, pulled as the chunks are sent. A chunk
    -- remains valid until the source is pulled again, which happens once the
    -- link has taken it.
    forall s. Source s => ParameterBorrowedStream !Int !s
