{-# LANGUAGE BangPatterns #-}
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
import Control.Monad (void, (>=>))
import Data.IORef (IORef, readIORef, writeIORef)
import Data.Word (Word8)
import Database.PostgreSQL.Internal.Protocol.Codec (InputSource (..), Output)
import Database.PostgreSQL.Transport.Link
import Foreign.Ptr (Ptr)
import Network.Streaming.Sender
  ( OutputBuffer,
    Pushed (..),
    Resume (..),
    Stalls (..),
    Unsent,
    discardUnsent,
    newOutputBuffer,
    noUnsent,
    outputBufferFrom,
    outputBufferPerOutput,
    pushWith,
    stopping,
    unsent,
    unsentEmpty,
    waitingStalls,
  )
import Network.Streaming.Watch (Watch, anyWatch, awaitFirst)

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
    -- gives a watch for what it would have waited for. It sends output that is
    -- unsent, as far as the link takes it, before it gives up.
    channelPoll :: IORef Unsent -> Ptr Word8 -> Int -> IO (Either (IO Watch) Int),
    -- | Sends output that is unsent, as far as the link takes it, without
    -- reading, for a read that has nowhere to read to. It gives a watch for
    -- what the output waits for, if it does.
    channelPush :: IORef Unsent -> IO (Maybe (IO Watch)),
    channelClose :: IO ()
  }

-- | A channel retains the link; each input operation supplies its own pending
-- output and read size.
newChannel :: Link link => link -> Channel
newChannel link =
  Channel
    { channelInput = linkInputSource link reading,
      channelSendSome =
        pushWith link stopping >=> \case
          Pushed -> pure noUnsent
          Stopped rest _ -> pure rest,
      channelSend = sendOutput link sending,
      channelSendBuffered = sendOutputWith link sending . Just,
      channelReceiveReady = receiveReady link,
      channelPoll = pollStep link,
      channelPush = \pending -> readIORef pending >>= pushPending link pending,
      channelClose = closeLink link
    }
  where
    !reading = readStalls link
    !sending = waitingStalls link
{-# INLINABLE newChannel #-}

-- | The stalls of a read: they stop for the server's input.
readStalls :: Link link => link -> Stalls
readStalls link =
  Stalls
    { stallWritable = (\readable -> if readable then Stop else Again) <$> awaitReadOrWrite link,
      stallSource = \watching -> awaitFirst [(watchReadable link, Stop), (watching, Again)]
    }
{-# NOINLINE readStalls #-}

-- | Reads from the link into the parser's buffer, which a new allocation
-- makes the given size. While output is unsent, waiting for input also sends
-- it as the link takes it, and waits for the source of an upload on this
-- thread, so that a delayed upload doesn't stop the answers from being read.
linkInputSource :: Link link => link -> Stalls -> IORef Unsent -> Int -> InputSource
linkInputSource link stalls pending readSize =
  InputSource
    { readInputInto = receive,
      inputReadSize = readSize
    }
  where
    -- Reads what the link has, or else sends what's left of the output as the
    -- link takes it. With none to send, it waits for input.
    receive pointer size = loop
      where
        loop =
          receiveReady link pointer size >>= \case
            Just received -> pure received
            Nothing -> do
              left <- readIORef pending
              pushWith link stalls left >>= \case
                Pushed
                  | unsentEmpty left -> awaitAny link (Interest True Done) *> loop
                  -- The server may have answered while the rest was sent, so input
                  -- is read before waiting for it.
                  | otherwise -> writeIORef pending noUnsent *> loop
                Stopped rest _ -> writeIORef pending rest *> loop
{-# INLINABLE linkInputSource #-}

-- | The read of 'linkInputSource' that doesn't wait: it gives the watch for what
-- it would have waited for.
pollStep :: Link link => link -> IORef Unsent -> Ptr Word8 -> Int -> IO (Either (IO Watch) Int)
pollStep link pending pointer size = loop
  where
    loop =
      receiveReady link pointer size >>= \case
        Just received -> pure (Right received)
        Nothing -> do
          left <- readIORef pending
          pushPending link pending left >>= \case
            -- The server may have answered while the rest was sent, so input
            -- is read before waiting for it.
            Nothing | not (unsentEmpty left) -> loop
            waiting -> pure (Left (maybe (watchReadable link) (\watch -> anyWatch [watchReadable link, watch]) waiting))
{-# INLINE pollStep #-}

-- | Sends the output that is left without waiting, and gives the watch for what
-- it waits for, if it does.
pushPending :: Link link => link -> IORef Unsent -> Unsent -> IO (Maybe (IO Watch))
pushPending link pending left =
  pushWith link stopping left >>= \case
    Pushed -> Nothing <$ writeIORef pending noUnsent
    Stopped rest watch -> Just watch <$ writeIORef pending rest
{-# INLINABLE pushPending #-}

-- | Sends the output, waiting for the link to become writable as needed.
sendOutput :: Link link => link -> Stalls -> Output -> IO ()
sendOutput link stalls = sendOutputWith link stalls Nothing
{-# INLINABLE sendOutput #-}

sendOutputWith :: Link link => link -> Stalls -> Maybe OutputBuffer -> Output -> IO ()
sendOutputWith link stalls buffer output = bracket (unsent buffer output) discardUnsent (void . pushWith link stalls)
{-# INLINABLE sendOutputWith #-}
