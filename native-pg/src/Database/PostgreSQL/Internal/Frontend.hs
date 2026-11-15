{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Sending what the client says: an output of the protocol's writers is rendered into a buffer and sent, as often
-- as the buffer is full, and the byte strings that are too long to copy are sent from where they are.
module Database.PostgreSQL.Internal.Frontend
  ( sendOutput,
    sync,
  )
where

import Control.Arrow (first, returnA, (>>>), (>>^), (^>>), (|||))
import Data.ByteString (ByteString)
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output, Uncopied (..), WriteResult (..), Writer, outputWriter, runWriter)
import Database.PostgreSQL.Internal.Protocol.Messages (pattern Sync, serialize)
import Database.PostgreSQL.Internal.Carrier (Carrier)
import Foreign.ForeignPtr (withForeignPtr)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes (Effect (..), Link (..), Runtime, view)
import Network.Streaming.Combinators (pattern Again, pattern Finish, iterateA)

-- | What is left to send: the carrier, the buffer that the writer renders into, and the writer.
data Outgoing = Outgoing {outCarrier :: Carrier, outBuffer :: Buffer, outWriter :: Writer}

-- | What a writer rendered into the buffer: the carrier to send it on, the pieces to send, and what is left to send, if
-- the writer was not finished.
data Rendered = Rendered {rendCarrier :: Carrier, rendPieces :: [ByteString], rendMore :: Maybe Outgoing}

-- | The output, rendered into the buffer: it is sent in as many pieces as the buffer takes, with the long byte
-- strings, which aren't copied, between them. The buffer is reused for each piece, since a send returns once its
-- pieces may be reused.
sendOutput :: (Runtime arr) => arr (Carrier, Buffer, Output) ()
sendOutput =
  (\(carrier, buffer, output) -> Outgoing carrier buffer (outputWriter output))
    ^>> iterateA
      ( now render
          >>> sendable
          ^>> first sendPieces
          >>^ continued
      )
  where
    sendable Rendered {rendCarrier, rendPieces, rendMore} = ((rendCarrier, rendPieces), rendMore)
    continued ((), more) = maybe (Finish ()) Again more
    sendPieces = somePieces ^>> (returnA ||| sendAll)
    somePieces (carrier, pieces) = if null pieces then Left () else Right (carrier, pieces)
    render :: Outgoing -> IO Rendered
    render Outgoing {outCarrier, outBuffer, outWriter} =
      withForeignPtr (bufferMemory outBuffer) (\pointer -> runWriter outWriter pointer (bufferCapacity outBuffer) 0) >>= \case
        Finished used -> pure (rendered used [] Nothing)
        NeedSpace used rest -> pure (rendered used [] (Just rest))
        Yield used (UncopiedBytes long) rest -> pure (rendered used [long] (Just rest))
      where
        rendered used following rest =
          Rendered
            { rendCarrier = outCarrier,
              rendPieces = copied outBuffer used following,
              rendMore = Outgoing outCarrier outBuffer <$> rest
            }
    copied buffer used following = [view buffer used | used > 0] <> following

-- | The Sync that ends the messages of a pipeline.
sync :: Output
sync = serialize Sync
