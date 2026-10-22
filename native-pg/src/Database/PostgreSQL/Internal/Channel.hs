{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | Connection input and output, specialized to its transport link.
module Database.PostgreSQL.Internal.Channel
  ( Channel (..),
    newChannel,
    Unsent,
    OutputBuffer,
    newOutputBuffer,
    outputBufferFrom,
    unsent,
    unsentEmpty,
  )
where

import Control.Exception (throwIO)
import Control.Monad (unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.IORef (IORef, readIORef, writeIORef)
import Data.Word (Word8)
import qualified Network.Streaming.Buffer as Buffer
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Writer, WriteResult (..), outputWriter, runWriter)
import Database.PostgreSQL.Internal.Protocol.Codec
  ( ByteStream (..),
    InputSource (..),
    Output,
    Uncopied (..),
    outputCopied,
    writeOutput,
  )
import Database.PostgreSQL.Transport.Link
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)

-- | A connection's input and output over its link. It's made with the
-- connection, from code specialized to the link, so that using it passes no
-- dictionary, and the rest of the client doesn't know which link it has.
--
-- Callers must ensure exclusive access to the channel and its underlying
-- 'Link', including calls to the 'InputSource' in 'channelInput'. Reading
-- may also send and flush pending output, so reads and writes must not run
-- concurrently. Ownership may pass to another thread between calls.
data Channel = Channel
  { -- | Reads input. While output is unsent, waiting for input also sends it
    -- as the link takes it.
    channelInput :: IORef Unsent -> Int -> InputSource,
    -- | Sends what the link takes without waiting, and gives what's left.
    channelSendSome :: Unsent -> IO Unsent,
    -- | Sends the output, waiting for room as needed.
    channelSend :: Output -> IO (),
    channelSendBuffered :: OutputBuffer -> Output -> IO (),
    channelClose :: IO ()
  }

-- | A channel retains the link; each input operation supplies its own pending
-- output and read size.
newChannel :: Link link => link -> Channel
newChannel link =
  Channel
    { channelInput = linkInputSource link,
      channelSendSome = sendSome link,
      channelSend = sendOutput link,
      channelSendBuffered = sendOutputWith link . Just,
      channelClose = closeLink link
    }
{-# INLINABLE newChannel #-}

-- | Reads from the link into the parser's buffer, which a new allocation
-- makes the given size. While output is unsent, waiting for input also sends
-- it as the link takes it.
linkInputSource :: Link link => link -> IORef Unsent -> Int -> InputSource
linkInputSource link pending readSize =
  InputSource
    { readInputInto = receive,
      inputReadSize = readSize
    }
  where
    receive pointer size =
      receiveReady link pointer size >>= \case
        Just received -> pure received
        Nothing -> do
          left <- readIORef pending
          flushed <- flushReady link
          case left of
            Unsent [] | flushed -> awaitReadable link
            _ ->
              awaitReadableOrWritable link >>= \case
                Readable -> pure ()
                Writable -> writeIORef pending =<< sendSome link left
          receive pointer size
{-# INLINABLE linkInputSource #-}

-- | Sends the output, waiting for the link to become writable as needed.
sendOutput :: Link link => link -> Output -> IO ()
sendOutput link = sendOutputWith link Nothing
{-# INLINABLE sendOutput #-}

sendOutputWith :: Link link => link -> Maybe OutputBuffer -> Output -> IO ()
sendOutputWith link buffer output = go =<< unsent buffer output
  where
    go left =
      sendSome link left >>= \case
        Unsent [] -> do
          flushed <- flushReady link
          unless flushed (awaitWritable link *> go (Unsent []))
        rest -> awaitWritable link *> go rest
{-# INLINABLE sendOutputWith #-}

-- | What's left of an output to send. The server may answer the first
-- messages of an output before it has read the rest, and stop reading until
-- its answers are read. So output can be sent as the answers are read.
newtype Unsent = Unsent [Part]

unsentEmpty :: Unsent -> Bool
unsentEmpty (Unsent parts) = null parts

-- | Bytes, or a stream with how many bytes it has given.
data Part
  = PartBytes ByteString
  | PartStream ByteStream Int
  | PartOutput OutputBuffer Writer
  | PartBorrowedStream ByteStream Int

-- | Memory that outputs are written to, reused once what was written to it
-- is sent.
data OutputBuffer = OutputBuffer (ForeignPtr Word8) Int Bool

newOutputBuffer :: Int -> IO OutputBuffer
newOutputBuffer size = (\memory -> OutputBuffer memory size False) <$> mallocPlainForeignPtrBytes size

-- | The caller must keep the loan exclusive until all output has been sent.
outputBufferFrom :: Buffer.Buffer -> OutputBuffer
outputBufferFrom buffer = OutputBuffer (Buffer.bufferMemory buffer) (Buffer.bufferCapacity buffer) True

-- | All of the output. Caller-supplied buffers are filled incrementally and
-- reused only after their bytes have been sent. Legacy buffers may grow to
-- fit the copied output. Long byte strings and streams retain their own
-- storage until accepted by the link.
unsent :: Maybe OutputBuffer -> Output -> IO Unsent
unsent (Just buffer@(OutputBuffer _ size True)) output
  | size < 4 = ioError (userError "output buffer capacity must be at least four bytes")
  | otherwise = pure (Unsent [PartOutput buffer (outputWriter output)])
unsent reused output = do
  memory <- case reused of
    Just (OutputBuffer memory size _) | outputCopied output <= size -> pure memory
    _ -> mallocPlainForeignPtrBytes (outputCopied output)
  parts <- writeOutput memory output
  pure (Unsent (map part parts))
  where
    part (Left copied) = PartBytes copied
    part (Right (UncopiedBytes value)) = PartBytes value
    part (Right (UncopiedStream source)) = PartStream source 0
    part (Right (UncopiedBorrowedStream source)) = PartBorrowedStream source 0

renderOutput :: OutputBuffer -> Writer -> IO [Part]
renderOutput buffer@(OutputBuffer memory capacity _) writer =
  withForeignPtr memory $ \pointer -> do
    result <- runWriter writer pointer capacity 0
    let copied count parts
          | count > 0 = PartBytes (BSI.BS memory count) : parts
          | otherwise = parts
        resume rest = [PartOutput buffer rest]
        uncopied (UncopiedBytes value) = PartBytes value
        uncopied (UncopiedStream source) = PartStream source 0
        uncopied (UncopiedBorrowedStream source) = PartBorrowedStream source 0
    pure $ case result of
      Finished count -> copied count []
      NeedSpace count rest -> copied count (resume rest)
      Yield count part rest -> copied count (uncopied part : resume rest)

-- | Bytes gathered for one send, and how many pieces at most. A client's
-- stream comes in chunks of up to 16 KiB from warp, and sending each on its
-- own wakes the server once per chunk.
gatherSize, gatherPieces :: Int
gatherSize = 262144
gatherPieces = 16

-- | Sends what the link takes without waiting for room in the socket,
-- pulling streams as it goes, and gives what's left. Each send gathers the
-- parts up front, pulling a stream's chunks, up to 'gatherSize' bytes or
-- 'gatherPieces' pieces. Pulling may wait for the stream's client, so the
-- bytes before a stream are sent before it's pulled: the server starts on
-- its statement, and its statement_timeout, as it would without the stream.
sendSome :: Link link => link -> Unsent -> IO Unsent
sendSome link (Unsent parts) = Unsent <$> go parts
  where
    go remaining = do
      (gathered, rest) <- gather 0 [] remaining
      if null gathered
        then pure rest
        else do
          sent <- sendReady link gathered
          case dropSent sent gathered of
            [] -> go rest
            left -> pure (map PartBytes left <> rest)
    -- the pieces so far, last first, and their size
    gather size pieces remaining
      | size >= gatherSize || length pieces >= gatherPieces = pure (reverse pieces, remaining)
      | otherwise = case remaining of
          [] -> pure (reverse pieces, [])
          -- Rendering the next buffer would overwrite gathered bytes that
          -- a partial send still owns.
          PartOutput _ _ : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartOutput buffer steps : others -> do
            rendered <- renderOutput buffer steps
            gather size pieces (rendered <> others)
          -- The next pull may overwrite the current chunk, including any
          -- suffix a partial send has left in front of this cursor.
          PartBorrowedStream _ _ : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartBorrowedStream source given : others -> do
            chunk <- byteStreamNext source
            let delivered = given + BS.length chunk
            if
              | BS.null chunk -> do
                  unless (given == byteStreamLength source) (throwIO (StreamLengthError (byteStreamLength source) given))
                  gather size pieces others
              | delivered > byteStreamLength source ->
                  throwIO (StreamLengthError (byteStreamLength source) delivered)
              | otherwise -> gather size pieces (PartBytes chunk : PartBorrowedStream source delivered : others)
          PartBytes chunk : others
            | BS.null chunk -> gather size pieces others
            | otherwise ->
                let (prefix, suffix) = BS.splitAt (gatherSize - size) chunk
                    rest = [PartBytes suffix | not (BS.null suffix)] <> others
                 in gather (size + BS.length prefix) (prefix : pieces) rest
          PartStream _ 0 : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartStream source given : others -> do
            chunk <- byteStreamNext source
            let delivered = given + BS.length chunk
            if
              | BS.null chunk -> do
                  unless (given == byteStreamLength source) (throwIO (StreamLengthError (byteStreamLength source) given))
                  gather size pieces others
              | delivered > byteStreamLength source ->
                  throwIO (StreamLengthError (byteStreamLength source) delivered)
              | otherwise -> gather size pieces (PartBytes chunk : PartStream source delivered : others)
    dropSent sent = \case
      chunk : others
        | sent >= BS.length chunk -> dropSent (sent - BS.length chunk) others
        | otherwise -> BS.drop sent chunk : others
      [] -> []
{-# INLINABLE sendSome #-}
