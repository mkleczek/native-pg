{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Sending what the client says: an output of the protocol's writers is rendered into a buffer and sent, as often
-- as the buffer is full, and the byte strings that are too long to copy are sent from where they are. An output may
-- have holes, which are bytes that a stream gives: they are sent as the stream gives them.
module Database.PostgreSQL.Internal.Frontend
  ( sendOutput,
    sendStreamed,
    sync,
  )
where

import Control.Arrow (arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (throwIO)
import Data.Bits (shiftR, testBit)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Tuple (swap)
import Database.PostgreSQL.Errors (UsageError (..))
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output, Uncopied (..), WriteResult (..), Writer, outputWriter, runWriter)
import Database.PostgreSQL.Internal.Protocol.Messages (pattern Sync, serialize)
import Database.PostgreSQL.Internal.Carrier (Carrier)
import Foreign.ForeignPtr (withForeignPtr)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes (Effect (..), Link (..), Runtime, Scope (..), view)
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, consume, iterateA, tap)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release)

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
        Hole {} -> ioError (userError "an output with a hole is sent with a stream")
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

-- | What a send that has holes has in hand: the carrier, the loans of the request that the writer's buffer is borrowed from, what
-- the streams' state is taken from, the writer, and what the streams have given.
data Streamed p = Streamed
  { streamCarrier :: Carrier,
    streamLoans :: Loans,
    streamInput :: p,
    streamWriter :: Writer,
    -- | The rest of the piece that a stream gave last, which no hole has taken yet.
    streamCarry :: !ByteString,
    -- | How many bytes the stream of the statement that is being sent has given, and how many its holes have asked for.
    streamGiven :: !Int,
    streamAsked :: !Int
  }

-- | What a writer rendered: the carrier, the pieces to send, and what follows them.
data Showing p = Showing Carrier [ByteString] (Next p)

-- | What follows the pieces: nothing, more of the output, or a hole, with its tag and its length.
data Next p = Over | Resume !(Streamed p) | Holed !(Streamed p) !Int !Int

-- | A hole that is being filled: the state, the tag of the hole, and how many of its bytes are still to come.
data Taking p = Taking !(Streamed p) !Int !Int

-- | The output, rendered and sent as 'sendOutput' does, where a hole takes its bytes from a stream. The arrow gives the next
-- piece of the stream of a number, whose state is taken from the input, or 'Nothing' at its end. A hole takes as many bytes
-- as it says from the stream whose number is its tag halved, and a piece that is longer than what is left of the hole is
-- split, and its rest goes to the next hole. A hole whose tag is odd is the last of its statement that takes from the
-- stream, which then has no more to give, or the send fails with 'StreamLengthError', as it does if a stream ends before
-- its holes are full. No byte that a stream gave beyond its holes is sent.
--
-- The writer renders into a buffer that is borrowed from the loans, for each part of the output that is rendered and sent,
-- and is given back before a stream is pulled, so that a stream may borrow of the loans itself.
sendStreamed :: (Runtime arr) => arr (Int, p) (Maybe ByteString) -> arr (Carrier, Loans, p, Output) ()
sendStreamed pull =
  (\(carrier, loans, input, output) -> Streamed carrier loans input (outputWriter output) BS.empty 0 0)
    ^>> iterateA (burst >>> next')
  where
    burst = scope (streamLoans ^>> borrow) (fst ^>> release) (now renderStreamed >>> shown ^>> consume sendPieces)
    shown (Showing carrier pieces following) = ((carrier, pieces), following)
    sendPieces = somePieces ^>> (returnA ||| sendAll)
    somePieces (carrier, pieces) = if null pieces then Left () else Right (carrier, pieces)
    -- What the send goes on with after a part of the output is sent.
    next' = whatNext ^>> (arr (const (Finish ())) ||| (arr Again ||| (filling >>^ Again)))
    whatNext = \case
      Over -> Left ()
      Resume state -> Right (Left state)
      Holed state tag size -> Right (Right (Taking state tag size))
    filling = (\(Taking state tag size) -> Taking state {streamAsked = streamAsked state + size} tag size) ^>> taking >>> (lastOf ^>> (returnA ||| ending))
    lastOf taken@(Taking state tag _) = if testBit tag 0 then Right taken else Left state
    -- The bytes of the hole are taken one piece after another, until it is full.
    taking = iterateA (whenEmpty ^>> (arr Finish ||| (fetched >>> classified ^>> (now throwIO ||| (arr Again ||| (sentChunk >>^ Again))))))
    whenEmpty taken@(Taking _ _ left) = if left == 0 then Left taken else Right taken
    classified (Taking state tag left, found) = case found of
      Nothing -> Left (StreamLengthError (streamAsked state) (streamGiven state))
      Just piece
        | BS.null piece -> Right (Left (Taking state tag left))
        | otherwise ->
            let (used, rest) = BS.splitAt left piece
             in Right (Right (Taking state {streamCarry = rest, streamGiven = streamGiven state + BS.length used} tag (left - BS.length used), used))
    sentChunk = tap ((\(Taking state _ _, chunk) -> (streamCarrier state, [chunk])) ^>> sendAll) >>^ fst
    -- The end of the stream: it gives no more bytes.
    ending = iterateA (fetched >>> now ended)
    ended (Taking state tag left, found) = case found of
      Nothing -> pure (Finish state {streamCarry = BS.empty, streamGiven = 0, streamAsked = 0})
      Just piece
        | BS.null piece -> pure (Again (Taking state tag left))
        | otherwise -> throwIO (StreamLengthError (streamAsked state) (streamGiven state + BS.length piece))
    -- The rest of the piece that was given before, or else the next piece of the stream.
    fetched = (\taking' -> if carried taking' then Right taking' else Left taking') ^>> (pulled ||| fromCarry)
    carried (Taking state _ _) = not (BS.null (streamCarry state))
    pulled = alongside ((\(Taking state tag _) -> (tag `shiftR` 1, streamInput state)) ^>> pull) >>^ swap
    fromCarry = arr (\(Taking state tag left) -> (Taking state {streamCarry = BS.empty} tag left, Just (streamCarry state)))
    renderStreamed :: (Loan, Streamed p) -> IO (Showing p)
    renderStreamed (loan, state@Streamed {streamCarrier, streamWriter}) =
      withForeignPtr (bufferMemory buffer) (\pointer -> runWriter streamWriter pointer (bufferCapacity buffer) 0) >>= \case
        Finished used -> pure (showing used [] Over)
        NeedSpace used rest -> pure (showing used [] (Resume state {streamWriter = rest}))
        Yield used (UncopiedBytes long) rest -> pure (showing used [long] (Resume state {streamWriter = rest}))
        Hole used tag size rest -> pure (showing used [] (Holed state {streamWriter = rest} tag size))
      where
        buffer = loanBuffer loan
        showing used following = Showing streamCarrier ([view buffer used | used > 0] <> following)
