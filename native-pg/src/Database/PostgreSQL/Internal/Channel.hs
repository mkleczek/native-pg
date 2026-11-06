{-# LANGUAGE LambdaCase #-}

-- | Connection input and output, specialized to its transport link.
module Database.PostgreSQL.Internal.Channel
  ( Channel (..),
    newChannel,
    Unsent,
    OutputBuffer,
    newOutputBuffer,
    outputBufferFrom,
    outputBufferPerOutput,
    unsent,
    unsentEmpty,
    discardUnsent,
  )
where

import Control.Exception (bracket)
import Data.IORef (IORef, readIORef, writeIORef)
import Data.Word (Word8)
import Database.PostgreSQL.Internal.Protocol.Codec (InputSource (..), Output)
import Database.PostgreSQL.Transport.Link
import Foreign.Ptr (Ptr)
import Network.Streaming.Sender
  ( OutputBuffer,
    Unsent,
    discardUnsent,
    newOutputBuffer,
    outputBufferFrom,
    outputBufferPerOutput,
    pushOutput,
    sendSome,
    unsent,
    unsentEmpty,
  )

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
    -- | Reads what's ready into the buffer, up to its size, without waiting:
    -- 'Nothing' if nothing is, and 0 at the end of the stream.
    channelReceiveReady :: Ptr Word8 -> Int -> IO (Maybe Int),
    channelClose :: IO ()
  }

-- | A channel retains the link; each input operation supplies its own pending
-- output and read size.
newChannel :: Link link => link -> Channel
newChannel link =
  Channel
    { channelInput = linkInputSource link,
      channelSendSome = fmap fst . sendSome link,
      channelSend = sendOutput link,
      channelSendBuffered = sendOutputWith link . Just,
      channelReceiveReady = receiveReady link,
      channelClose = closeLink link
    }
{-# INLINABLE newChannel #-}

-- | Reads from the link into the parser's buffer, which a new allocation
-- makes the given size. While output is unsent, waiting for input also sends
-- it as the link takes it, and waits for the source of an upload on this
-- thread, so that a delayed upload doesn't stop the answers from being read.
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
          (rest, blocked) <- pushOutput link left
          writeIORef pending rest
          case blocked of
            -- The server may have answered while the rest was sent, so input
            -- is read before waiting for it.
            Done | not (unsentEmpty left) -> pure ()
            _ -> awaitAny link (Interest True blocked)
          -- Input is first, so that an answer is read before more is sent.
          receive pointer size
{-# INLINABLE linkInputSource #-}

-- | Sends the output, waiting for the link to become writable as needed.
sendOutput :: Link link => link -> Output -> IO ()
sendOutput link = sendOutputWith link Nothing
{-# INLINABLE sendOutput #-}

sendOutputWith :: Link link => link -> Maybe OutputBuffer -> Output -> IO ()
sendOutputWith link buffer output = bracket (unsent buffer output) discardUnsent go
  where
    go left =
      pushOutput link left >>= \case
        (_, Done) -> pure ()
        (rest, blocked) -> awaitAny link (Interest False blocked) *> go rest
{-# INLINABLE sendOutputWith #-}
