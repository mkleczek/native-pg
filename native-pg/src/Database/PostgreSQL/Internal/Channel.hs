{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TupleSections #-}

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

import Control.Exception (SomeException, bracket, finally, mask, onException, throwIO, try, uninterruptibleMask_)
import Control.Concurrent (ThreadId, forkIOWithUnmask, killThread, myThreadId)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TMVar (TMVar, newEmptyTMVarIO, putTMVar, readTMVar, tryPutTMVar, tryTakeTMVar)
import Network.Streaming (Source, withChunk)
import Network.Streaming.Watch (Watch (..))
import Control.Monad (unless, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Functor (($>))
import GHC.Conc (atomically)
import Data.IORef (IORef, readIORef, writeIORef, newIORef, atomicModifyIORef')
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
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import Foreign.Ptr (Ptr)
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
      channelSendSome = sendSome link,
      channelSend = sendOutput link,
      channelSendBuffered = sendOutputWith link . Just,
      channelReceiveReady = receiveReady link,
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
          if unsentEmpty left && flushed
            then awaitAny link (Interest True Done)
            else do
              case waitingSource left of
                Just pull | flushed -> waitForSourceOrReadable link pull
                _ -> awaitAny link (Interest True OnWritable)
              writeIORef pending =<< sendSome link left
          receive pointer size
{-# INLINABLE linkInputSource #-}

-- | Sends the output, waiting for the link to become writable as needed.
sendOutput :: Link link => link -> Output -> IO ()
sendOutput link = sendOutputWith link Nothing
{-# INLINABLE sendOutput #-}

sendOutputWith :: Link link => link -> Maybe OutputBuffer -> Output -> IO ()
sendOutputWith link buffer output = bracket (unsent buffer output) discardUnsent go
  where
    go left = do
      rest <- sendSome link left
      if unsentEmpty rest
        then do
          flushed <- flushReady link
          unless flushed (awaitAny link (Interest False OnWritable) *> go rest)
        else do
          case waitingSource rest of
            Just pull -> void (atomically (readTMVar (pullEvent pull)))
            Nothing -> awaitAny link (Interest False OnWritable)
          go rest
{-# INLINABLE sendOutputWith #-}

-- | What's left of an output to send. The server may answer the first
-- messages of an output before it has read the rest, and stop reading until
-- its answers are read. So output can be sent as the answers are read.
data Unsent = Unsent [Part] (IORef [(ThreadId, MVar ())])

unsentEmpty :: Unsent -> Bool
unsentEmpty (Unsent parts _) = null parts

-- Cancel and join producers before request storage can be returned. Only
-- producers touch Sources; the caller retains exclusive Link I/O ownership.
discardUnsent :: Unsent -> IO ()
discardUnsent (Unsent _ registry) = uninterruptibleMask_ $ do
  active <- atomicModifyIORef' registry ([],)
  mapM_ (\(thread, finished) -> killThread thread *> readMVar finished) active

data BorrowedPull = BorrowedPull
  { pullEvent :: TMVar (Either SomeException ByteString),
    pullAccepted :: MVar (),
    pullFinished :: MVar (Either SomeException ())
  }

newBorrowedPull :: IORef [(ThreadId, MVar ())] -> Source -> IO BorrowedPull
newBorrowedPull registry source = mask $ \_ -> do
  event <- newEmptyTMVarIO
  accepted <- newEmptyMVar
  finished <- newEmptyMVar
  outcomeVar <- newEmptyMVar
  started <- newEmptyMVar
  let loop = do
        ended <- withChunk source $ \chunk -> do
          atomically (putTMVar event (Right chunk))
          takeMVar accepted
          pure (BS.null chunk)
        unless ended loop
      run unmask = do
        outcome <- try (unmask loop)
        case outcome of
          Left failure -> atomically (tryPutTMVar event (Left failure)) $> ()
          Right () -> pure ()
        putMVar outcomeVar outcome
      done = do
        thread <- myThreadId
        atomicModifyIORef' registry (\entries -> (filter ((/= thread) . fst) entries, ()))
        putMVar finished ()
  thread <- forkIOWithUnmask (\unmask -> (takeMVar started *> run unmask) `finally` done)
  (atomicModifyIORef' registry (\entries -> ((thread, finished) : entries, ())) *> putMVar started ())
    `onException` (killThread thread *> readMVar finished)
  pure (BorrowedPull event accepted outcomeVar)

waitingSource :: Unsent -> Maybe BorrowedPull
waitingSource (Unsent (PartBorrowedCursor pull _ _ : _) _) = Just pull
waitingSource _ = Nothing

-- A delayed HTTP chunk must not stop the database from returning an earlier
-- result. Await readable PG bytes or source readiness in one STM wait, with
-- readable first, on the caller's thread.
waitForSourceOrReadable :: Link link => link -> BorrowedPull -> IO ()
waitForSourceOrReadable link pull =
  awaitAny link (Interest True (OnSource (pure (Watch (void (readTMVar (pullEvent pull))) (pure ())))))

-- | Bytes, or a stream with how many bytes it has given.
data Part
  = PartBytes ByteString
  | PartStream ByteStream {-# UNPACK #-} !Int
  | -- | Output rendering on in the memory from an offset, past the bytes
    -- before the part that was sent from where it is.
    PartOutput (ForeignPtr Word8) {-# UNPACK #-} !Int {-# UNPACK #-} !Int Writer
  | -- | Output that ran out of room, to go on in the memory once it is empty.
    PartOutputEmpty (ForeignPtr Word8) {-# UNPACK #-} !Int Writer
  | PartBorrowedStream {-# UNPACK #-} !Int Source
  | PartBorrowedCursor BorrowedPull {-# UNPACK #-} !Int {-# UNPACK #-} !Int
  | PartBorrowedBytes ByteString BorrowedPull {-# UNPACK #-} !Int {-# UNPACK #-} !Int

-- | Memory that outputs are written to, reused once what was written to it
-- is sent; or new memory for each output.
data OutputBuffer
  = OutputBuffer (ForeignPtr Word8) Int Bool
  | PerOutputBuffer Int

newOutputBuffer :: Int -> IO OutputBuffer
newOutputBuffer size = (\memory -> OutputBuffer memory size False) <$> mallocPlainForeignPtrBytes size

-- | The caller must keep the loan exclusive until all output has been sent.
outputBufferFrom :: Buffer.Buffer -> OutputBuffer
outputBufferFrom buffer = OutputBuffer (Buffer.bufferMemory buffer) (Buffer.bufferCapacity buffer) True

-- | New memory for each output, as large as its copied bytes, or the limit
-- if less, so that an output goes in one send unless it is longer than that.
outputBufferPerOutput :: Int -> OutputBuffer
outputBufferPerOutput = PerOutputBuffer

-- | All of the output. Caller-supplied buffers are filled incrementally and
-- reused only after their bytes have been sent. Per-output buffers are filled
-- in the same way, in memory of their own. Legacy buffers may grow to fit the
-- copied output. Long byte strings and streams retain their own storage until
-- accepted by the link.
unsent :: Maybe OutputBuffer -> Output -> IO Unsent
unsent (Just (OutputBuffer memory size True)) output
  | size < 4 = ioError (userError "output buffer capacity must be at least four bytes")
  | otherwise = Unsent [PartOutput memory size 0 (outputWriter output)] <$> newIORef []
unsent (Just (PerOutputBuffer limit)) output = do
  let size = max 4 (min limit (outputCopied output))
  memory <- mallocPlainForeignPtrBytes size
  Unsent [PartOutput memory size 0 (outputWriter output)] <$> newIORef []
unsent reused output = do
  memory <- case reused of
    Just (OutputBuffer memory size _) | outputCopied output <= size -> pure memory
    _ -> mallocPlainForeignPtrBytes (outputCopied output)
  parts <- writeOutput memory output
  Unsent (map part parts) <$> newIORef []
  where
    part (Left copied) = PartBytes copied
    part (Right (UncopiedBytes value)) = PartBytes value
    part (Right (UncopiedStream source)) = PartStream source 0
    part (Right (UncopiedBorrowedStream count source)) = PartBorrowedStream count source

-- | Renders from the offset in the memory.
renderOutput :: ForeignPtr Word8 -> Int -> Int -> Writer -> IO [Part]
renderOutput memory capacity offset writer =
  withForeignPtr memory $ \pointer -> do
    result <- runWriter writer pointer capacity offset
    let copied count parts
          | count > offset = PartBytes (BSI.BS (plusForeignPtr memory offset) (count - offset)) : parts
          | otherwise = parts
        uncopied (UncopiedBytes value) = PartBytes value
        uncopied (UncopiedStream source) = PartStream source 0
        uncopied (UncopiedBorrowedStream count source) = PartBorrowedStream count source
    pure $ case result of
      Finished count -> copied count []
      NeedSpace count rest -> copied count [PartOutputEmpty memory capacity rest]
      Yield count part rest -> copied count [uncopied part, PartOutput memory capacity count rest]

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
sendSome link (Unsent parts registry) = (`Unsent` registry) <$> go parts
  where
    go (PartBorrowedBytes chunk pull count given : others) = do
      sent <- sendReady link [BS.take gatherSize chunk]
      if sent >= BS.length chunk
        then putMVar (pullAccepted pull) () *> go (PartBorrowedCursor pull count given : others)
        else pure (PartBorrowedBytes (BS.drop sent chunk) pull count given : others)
    go remaining = do
      (gathered, rest) <- gather 0 0 [] remaining
      if null gathered
        then case rest of
          PartBorrowedBytes {} : _ -> go rest
          _ -> pure rest
        else do
          sent <- sendReady link gathered
          case dropSent sent gathered of
            [] -> go rest
            left -> pure (map PartBytes left <> rest)
    -- Count pieces as they are gathered instead of traversing them each time.
    gather !size !pieceCount pieces remaining
      | size >= gatherSize || pieceCount >= gatherPieces = pure (reverse pieces, remaining)
      | otherwise = case remaining of
          [] -> pure (reverse pieces, [])
          -- Rendering in an empty buffer would overwrite gathered bytes that
          -- a partial send still owns.
          PartOutputEmpty {} : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartOutputEmpty memory capacity steps : others -> do
            rendered <- renderOutput memory capacity 0 steps
            gather size pieceCount pieces (rendered <> others)
          -- Past a direct part the output goes on after the gathered bytes.
          -- With none gathered, none are unsent: a partial send leaves its
          -- bytes ahead of the output.
          PartOutput memory capacity offset steps : others -> do
            rendered <- renderOutput memory capacity (if null pieces then 0 else offset) steps
            gather size pieceCount pieces (rendered <> others)
          -- Flush preceding metadata before starting a scoped source.
          PartBorrowedStream _ _ : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartBorrowedStream count source : others -> do
            pull <- newBorrowedPull registry source
            gather size pieceCount pieces (PartBorrowedCursor pull count 0 : others)
          PartBorrowedCursor {} : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartBorrowedCursor pull count given : others ->
            atomically (tryTakeTMVar (pullEvent pull)) >>= \case
              Nothing -> pure (reverse pieces, remaining)
              Just (Left failure) -> throwIO failure
              Just (Right chunk) -> do
                let delivered = given + BS.length chunk
                if
                  | BS.null chunk -> do
                      putMVar (pullAccepted pull) ()
                      readMVar (pullFinished pull) >>= either throwIO pure
                      unless (given == count) (throwIO (StreamLengthError count given))
                      gather size pieceCount pieces others
                  | delivered > count -> throwIO (StreamLengthError count delivered)
                  | otherwise -> pure (reverse pieces, PartBorrowedBytes chunk pull count delivered : others)
          PartBorrowedBytes {} : _ -> pure (reverse pieces, remaining)
          PartBytes chunk : others
            | BS.null chunk -> gather size pieceCount pieces others
            | otherwise ->
                let (prefix, suffix) = BS.splitAt (gatherSize - size) chunk
                    rest = [PartBytes suffix | not (BS.null suffix)] <> others
                 in gather (size + BS.length prefix) (pieceCount + 1) (prefix : pieces) rest
          PartStream _ 0 : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartStream source given : others -> do
            chunk <- byteStreamNext source
            let delivered = given + BS.length chunk
            if
              | BS.null chunk -> do
                  unless (given == byteStreamLength source) (throwIO (StreamLengthError (byteStreamLength source) given))
                  gather size pieceCount pieces others
              | delivered > byteStreamLength source ->
                  throwIO (StreamLengthError (byteStreamLength source) delivered)
              | otherwise -> gather size pieceCount pieces (PartBytes chunk : PartStream source delivered : others)
    dropSent sent = \case
      chunk : others
        | sent >= BS.length chunk -> dropSent (sent - BS.length chunk) others
        | otherwise -> BS.drop sent chunk : others
      [] -> []
{-# INLINABLE sendSome #-}
