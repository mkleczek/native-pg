{-# LANGUAGE LambdaCase #-}
-- At -O2, liberate-case copies the receive loop once for each place it calls
-- itself, and one loop is enough. At -O1 the flag does nothing.
{-# OPTIONS_GHC -fno-liberate-case #-}

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
import Network.Streaming.Drive (Polled, Progress (..), drive, poll)
import Network.Streaming.Link (watchInterest)
import Network.Streaming.Watch (Watch)
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
    -- | Reads as 'channelInput' does, but where that waits it gives up, and
    -- says what it would have waited for. It sends output that is unsent, as
    -- far as the link takes it, before it gives up.
    channelPoll :: IORef Unsent -> Ptr Word8 -> Int -> IO (Polled Int),
    -- | Sends output that is unsent, as far as the link takes it, without
    -- reading, for a read that has nowhere to read to. It says what the output
    -- waits for.
    channelPush :: IORef Unsent -> IO Blocked,
    -- | One watch for an interest, which fires when any of it may be retried.
    channelWatch :: Interest -> IO Watch,
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
      channelPoll = \pending pointer size -> poll (receiveStep link pending pointer size) (),
      channelPush = \pending -> do
        (rest, blocked) <- pushOutput link =<< readIORef pending
        writeIORef pending rest
        pure blocked,
      channelWatch = watchInterest link,
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
    { readInputInto = \pointer size -> drive link (receiveStep link pending pointer size) (),
      inputReadSize = readSize
    }
{-# INLINABLE linkInputSource #-}

-- | One try to read: what the link has, or else what's left of the output sent
-- as the link takes it, and what to wait for.
receiveStep :: Link link => link -> IORef Unsent -> Ptr Word8 -> Int -> () -> IO (Progress () Int)
receiveStep link pending pointer size () =
  receiveReady link pointer size >>= \case
    Just received -> pure (Ready received)
    Nothing -> do
      left <- readIORef pending
      (rest, blocked) <- pushOutput link left
      writeIORef pending rest
      pure $ case blocked of
        -- The server may have answered while the rest was sent, so input
        -- is read before waiting for it.
        Done | not (unsentEmpty left) -> Again ()
        -- Input is tried first after the wait, so that an answer is read
        -- before more is sent.
        _ -> Blocked () (Interest True blocked)
{-# INLINE receiveStep #-}

-- | Sends the output, waiting for the link to become writable as needed.
sendOutput :: Link link => link -> Output -> IO ()
sendOutput link = sendOutputWith link Nothing
{-# INLINABLE sendOutput #-}

sendOutputWith :: Link link => link -> Maybe OutputBuffer -> Output -> IO ()
sendOutputWith link buffer output = bracket (unsent buffer output) discardUnsent (drive link step)
  where
    step left =
      pushOutput link left >>= \case
        (_, Done) -> pure (Ready ())
        (rest, blocked) -> pure (Blocked rest (Interest False blocked))
{-# INLINABLE sendOutputWith #-}
