{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Sending what the client says: an output of the protocol's writers is rendered into a buffer and sent, as often
-- as the buffer is full, and the byte strings that are too long to copy are sent from where they are.
module Database.PostgreSQL.Internal.Frontend
  ( sendOutput,
    sync,
  )
where

import Control.Arrow (first, returnA, (>>>), (^>>), (|||))
import Data.ByteString (ByteString)
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output, Uncopied (..), WriteResult (..), Writer, outputWriter, runWriter)
import Database.PostgreSQL.Internal.Protocol.Messages (pattern Sync, serialize)
import Foreign.ForeignPtr (withForeignPtr)
import Network.Socket (Socket)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes (Effect (..), Link (..), Runtime, view)

-- | The output, rendered into the buffer: it is sent in as many pieces as the buffer takes, with the long byte
-- strings, which aren't copied, between them. The buffer is reused for each piece, since a send returns once its
-- pieces may be reused.
sendOutput :: (Runtime arr) => arr (Socket, Buffer, Output) ()
sendOutput = (\(sock, buffer, output) -> (sock, buffer, outputWriter output)) ^>> loop
  where
    loop =
      now render
        >>> (\(sock, buffer, sent, rest) -> ((sock, sent), (sock, buffer, rest))) ^>> first sendPieces
        >>> (\((), (sock, buffer, rest)) -> maybe (Left ()) (\writer -> Right (sock, buffer, writer)) rest) ^>> (returnA ||| loop)
    sendPieces = (\(sock, pieces) -> if null pieces then Left () else Right (sock, pieces)) ^>> (returnA ||| sendAll)
    render :: (Socket, Buffer, Writer) -> IO (Socket, Buffer, [ByteString], Maybe Writer)
    render (sock, buffer, writer) =
      withForeignPtr (bufferMemory buffer) (\pointer -> runWriter writer pointer (bufferCapacity buffer) 0) >>= \case
        Finished used -> pure (sock, buffer, copied buffer used [], Nothing)
        NeedSpace used rest -> pure (sock, buffer, copied buffer used [], Just rest)
        Yield used (UncopiedBytes long) rest -> pure (sock, buffer, copied buffer used [long], Just rest)
    copied buffer used following = [view buffer used | used > 0] <> following

-- | The Sync that ends the messages of a pipeline.
sync :: Output
sync = serialize Sync
