{-# LANGUAGE LambdaCase #-}
-- | Integration-only connection and operation ownership. Do not retain a
-- supplied Context or any borrowed slices after its callback returns.
module Database.PostgreSQL.Internal.Context
  ( Context,
    withContext,
    withRequestContext,
    contextConnection,
    contextInput,
    runContextInput,
    runContextBorrowWaiting,
    contextBuffered,
    contextReceiveMore,
    contextWatchStall,
    contextOutput,
    sendContextOutput,
    queueContextOutput,
    contextPending,
    contextReadSize,
    contextScoped,
    newOwnedContext,
    newConnection,
    runContextOperation,
    beginContextOperation,
    endContextOperation,
    openRequestContext,
    closeScopedContext,
    closeAbortedContext,
    ensureContextActive,
    breakConnection,
    Connection (..),
    SessionState (..),
    ConnectionError (..),
  ) where

import Control.Concurrent (ThreadId, myThreadId, killThread)
import Control.Concurrent.MVar (MVar, newMVar, takeMVar, putMVar, readMVar)
import Control.Exception (catch, finally, mask, onException, throwIO, uninterruptibleMask_)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef', atomicModifyIORef')
import Network.Streaming.Buffer (Buffer, bufferCapacity, bufferMemory)
import Network.Streaming.BufferPool (BufferAccess)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import Data.Word (Word8)
import Network.Streaming.Drive (Polled (..))
import Network.Streaming.Link (Interest (..))
import qualified Network.Streaming.Link as Link (Blocked (Done))
import Network.Streaming.Watch (Watch, anyWatch)
import Database.PostgreSQL.Diagnostics (ConnectionError (..))
import Database.PostgreSQL.Internal.Protocol.Codec (Output, Input, InputBuffer, InputError (..), InputSource (..), Poll, Receiver (..), Stall (..), borrowInputWaiting, pollInput, reusableInputBuffer, requestInputBuffer, releaseInputBuffer, unconsumedBytes, unconsumedInput, emptyInputBuffer, runInputUntil, watchInputLoan)
import Database.PostgreSQL.Internal.Protocol.Messages (Notification)
import Database.PostgreSQL.Internal.Channel (Channel (..), OutputBuffer, Unsent, outputBufferFrom, outputBufferPerOutput, unsent, unsentEmpty, newOutputBuffer, discardUnsent)

data Connection = Connection
  { -- | Input and output over the socket, or TLS over it.
    connectionChannel :: Channel,
    connectionReadSize :: Int,
    connectionState :: IORef SessionState,
    -- | Notifications that arrived during operations, newest first, until
    -- 'awaitNotification' takes them.
    connectionNotifications :: IORef [Notification ByteString],
    -- | What a scope read past its last reply, for the next read of the
    -- connection to take before the link's bytes: asynchronous messages,
    -- whole or cut. At most one block, and normally empty.
    connectionCarryOver :: IORef ByteString
  }

-- | Session handles can be copied, so the connection tracks at run time
-- whether an operation is in progress, or the session is no longer usable.
data SessionState
  = Idle
  | Busy
  | -- | An operation failed with an exception, leaving the protocol state
    -- unknown.
    Broken
  | Closed

-- | Builds protocol/link state for a connected, initialized transport.
newConnection :: Channel -> Int -> IO Connection
newConnection channel readSize = do
  state <- newIORef Idle
  notifications <- newIORef []
  carry <- newIORef BS.empty
  pure (Connection channel readSize state notifications carry)

-- | Mutable byte ownership for one operation scope, separate from protocol
-- state. The public module exports only the type.
data Context = Context
  { contextConnection :: Connection,
    contextInput :: IORef InputBuffer,
    contextOutput :: OutputBuffer,
    contextPending :: IORef Unsent,
    contextReadSize :: Int,
    contextScoped :: Bool,
    contextUse :: IORef ContextUse,
    contextFinished :: MVar (),
    -- | What reads that don't wait ask of the link, made once: its closures
    -- read only the connection's and the context's own references, which are
    -- never replaced.
    contextReceiver :: Receiver
  }

data ContextUse = Available | Using ThreadId | Expired

newOwnedContext :: Connection -> IO Context
newOwnedContext connection = do
  input <- newIORef emptyInputBuffer
  pending <- newIORef =<< unsent Nothing mempty
  output <- newOutputBuffer 16384
  use <- newIORef Available
  finished <- newMVar ()
  pure (Context connection input output pending (connectionReadSize connection) False use finished (receiverOf connection pending))

-- | Sends output for this context.
sendContextOutput :: Context -> Output -> IO ()
sendContextOutput context output = do
  ensureContextActive context
  channelSendBuffered (connectionChannel (contextConnection context)) (contextOutput context) output

-- | Starts a batch without waiting for the peer to consume all of it.
-- Reads subsequently advance this context's pending writes.
queueContextOutput :: Context -> Output -> IO ()
queueContextOutput context output = do
  ensureContextActive context
  previous <- readIORef (contextPending context)
  unless (unsentEmpty previous) (throwIO (SessionUnusable "the operation still has pending output"))
  pending <- unsent (Just (contextOutput context)) output
  writeIORef (contextPending context) pending
  remaining <- channelSendSome (connectionChannel (contextConnection context)) pending
  writeIORef (contextPending context) remaining

-- | Runs a concrete parser against this operation's cursor and pending
-- output. The parser consumes no further than the limit, but a read fills
-- the free space of the cursor's buffer, so it may bring in what follows.
runContextInput :: Context -> Int -> Input a -> IO a
runContextInput context limit parse = do
  ensureContextActive context
  input <- readIORef (contextInput context)
  let connection = contextConnection context
      source = carriedInput (connectionCarryOver connection)
        (channelInput (connectionChannel connection) (contextPending context) (contextReadSize context))
  (result, input') <- runInputUntil source limit input parse
    `catch` \(InputError reason) -> throwIO (ProtocolError reason)
  writeIORef (contextInput context) input'
  pure result

-- | Takes the next bytes of a value of this many bytes that lies in the input,
-- as a borrowed chunk, as many as a blocking read takes, of what the input holds
-- and what receives bring. When nothing is ready, it calls the wait with a
-- watch for what the read is stalled on, and tries again, as long as it takes.
-- Pending output is sent as the link takes it, as a read sends it. The wait is
-- the caller's, and it runs with the read in the middle of its work, so it
-- must not touch the connection. An error of the input is an 'InputError',
-- which the caller makes a failure of the protocol.
runContextBorrowWaiting :: Context -> (IO Watch -> IO ()) -> Int -> IO ByteString
runContextBorrowWaiting context wait count = do
  ensureContextActive context
  input <- readIORef (contextInput context)
  (chunk, input') <-
    borrowInputWaiting (contextReceiver context) (wait . contextWatchStall context) (contextReadSize context) count input
  writeIORef (contextInput context) input'
  pure chunk

-- | The bytes that the input holds and nothing has consumed.
contextBuffered :: Context -> IO ByteString
contextBuffered context = unconsumedBytes <$> readIORef (contextInput context)

-- | Receives once into the input, without waiting and without consuming
-- anything.
contextReceiveMore :: Context -> IO Poll
contextReceiveMore context = do
  ensureContextActive context
  input <- readIORef (contextInput context)
  (polled, input') <-
    pollInput (contextReceiver context) (contextReadSize context) input
      `catch` \(InputError reason) -> throwIO (ProtocolError reason)
  writeIORef (contextInput context) input'
  pure polled

-- | What reads that don't wait ask of the link: its receive, after the
-- connection's carried-over bytes, and the push of unsent output.
receiverOf :: Connection -> IORef Unsent -> Receiver
receiverOf connection pending =
  Receiver
    { receiveNow = carriedPoll (connectionCarryOver connection) (channelPoll channel pending),
      receivePush = channelPush channel pending
    }
  where
    channel = connectionChannel connection

-- | One watch for what a read that didn't wait was stalled on, which fires
-- when it may be retried. For a loan it is the input's registration for one,
-- and while output is unsent also what the output waits for, so that it keeps
-- moving. The caller releases it.
contextWatchStall :: Context -> Stall -> IO Watch
contextWatchStall context = \case
  StallOnLink interest -> channelWatch channel interest
  StallOnLoan Link.Done -> loan
  StallOnLoan blocked -> anyWatch [loan, channelWatch channel (Interest False blocked)]
  where
    channel = connectionChannel (contextConnection context)
    loan = readIORef (contextInput context) >>= watchInputLoan

-- | Takes the connection's carried-over bytes before the link's, as
-- 'carriedInput' does.
carriedPoll :: IORef ByteString -> (Ptr Word8 -> Int -> IO (Polled Int)) -> Ptr Word8 -> Int -> IO (Polled Int)
carriedPoll carry poll pointer size = do
  carried <- readIORef carry
  if BS.null carried
    then poll pointer size
    else do
      let (taken, left) = BS.splitAt size carried
      BSU.unsafeUseAsCStringLen taken $ \(origin, count) -> copyBytes pointer (castPtr origin) count
      writeIORef carry left
      pure (Got (BS.length taken))

-- | Reads the connection's carried-over bytes before the link's. A read takes
-- no more than it asks for, and leaves the rest for the next one.
carriedInput :: IORef ByteString -> InputSource -> InputSource
carriedInput carry source =
  source
    { readInputInto = \pointer size -> do
        carried <- readIORef carry
        if BS.null carried
          then readInputInto source pointer size
          else do
            let (taken, left) = BS.splitAt size carried
            BSU.unsafeUseAsCStringLen taken $ \(origin, count) -> copyBytes pointer (castPtr origin) count
            writeIORef carry left
            pure (BS.length taken)
    }

-- | Binds supplied initialized buffers for the callback. Acquires the
-- connection before exposing the Context and cancels/joins any operation
-- still running on another thread before the buffers may be reused.
withContext :: Connection -> Buffer -> Buffer -> (Context -> IO a) -> IO a
withContext connection input output use = do
  when (bufferCapacity input <= 0 || bufferCapacity output <= 0)
    (ioError (userError "buffer capacity must be positive"))
  withForeignPtr (bufferMemory input) $ \inputStart ->
    withForeignPtr (bufferMemory output) $ \outputStart ->
      when (inputStart < outputStart `plusPtr` bufferCapacity output &&
            outputStart < inputStart `plusPtr` bufferCapacity input)
        (throwIO (SessionUnusable "input and output storage overlap"))
  when (bufferCapacity output < 4) (ioError (userError "output buffer capacity must be at least four bytes"))
  withScopedContext connection (pure (reusableInputBuffer input)) (pure (outputBufferFrom output)) (bufferCapacity input) use

-- | Uses one request capability. Body input borrows lazily; output framing
-- is rendered in memory of its own for each output, as large as its copied
-- bytes up to a limit, and streams retain their source storage.
withRequestContext :: Connection -> BufferAccess -> (Context -> IO a) -> IO a
withRequestContext connection access =
  withScopedContext connection (requestInputBuffer access)
    (pure (outputBufferPerOutput requestOutputLimit)) (connectionReadSize connection)

-- | Claims the connection for one request capability, which stays claimed
-- until 'closeScopedContext'. The caller masks the claim and what it does with
-- it.
openRequestContext :: Connection -> BufferAccess -> IO Context
openRequestContext connection access =
  openScopedContext connection (requestInputBuffer access)
    (pure (outputBufferPerOutput requestOutputLimit)) (connectionReadSize connection)

-- | The most memory for one output's framing. A pipeline that copies more
-- takes one more send for each of this many bytes.
requestOutputLimit :: Int
requestOutputLimit = 65536

withScopedContext :: Connection -> IO InputBuffer -> IO OutputBuffer -> Int -> (Context -> IO a) -> IO a
withScopedContext connection makeInput makeOutput readSize use = mask $ \restore -> do
    context <- openScopedContext connection makeInput makeOutput readSize
    result <- restore (use context) `onException` closeScopedContext context True
    closeScopedContext context False
    pure result

openScopedContext :: Connection -> IO InputBuffer -> IO OutputBuffer -> Int -> IO Context
openScopedContext connection makeInput makeOutput readSize = do
    claimConnection connection
    (do
      cursor <- newIORef =<< makeInput
      output <- makeOutput
      pending <- newIORef =<< unsent Nothing mempty
      state <- newIORef Available
      finished <- newMVar ()
      pure (Context connection cursor output pending readSize True state finished (receiverOf connection pending)))
      `onException` writeIORef (connectionState connection) Idle

-- | Gives the connection back: after a failure it is broken. Unsent output
-- breaks it too, and is reported.
closeScopedContext :: Context -> Bool -> IO ()
closeScopedContext context failed = do
    when failed (breakConnection connection)
      `finally` (expireContext context `finally` cleanupContext context)
    unless failed $ do
      pending <- readIORef (contextPending context)
      unless (unsentEmpty pending) $ do
        breakConnection connection
        throwIO (SessionUnusable "operation scope ended with unsent output")
      atomicModifyIORef' (connectionState connection) (\state -> (case state of Busy -> Idle; other -> other, ()))
  where
    connection = contextConnection context

-- | Gives the connection back after a scope aborted, as the operations in the
-- scope left it: broken if one broke it, or if output is unsent, which isn't
-- reported, since the scope's own failure is. The operations decide the
-- connection's fate, as they are the ones that know where in the protocol
-- they stopped; each of them breaks it when it fails unsafely.
closeAbortedContext :: Context -> IO ()
closeAbortedContext context = do
    expireContext context `finally` cleanupContext context
    pending <- readIORef (contextPending context)
    unless (unsentEmpty pending) (breakConnection connection)
    atomicModifyIORef' (connectionState connection) (\state -> (case state of Busy -> Idle; other -> other, ()))
  where
    connection = contextConnection context

cleanupContext :: Context -> IO ()
cleanupContext context = do
  discardUnsent =<< readIORef (contextPending context)
  input <- readIORef (contextInput context)
  carryOver (contextConnection context) input
  releaseInputBuffer input

-- | Keeps the bytes a finished scope read but didn't consume, before its block
-- goes back to its owner. They were read before what the link or an earlier
-- carry-over still holds, which a smaller block may have taken only in part.
-- A scope that failed keeps nothing: its connection is broken.
carryOver :: Connection -> InputBuffer -> IO ()
carryOver connection input = readIORef (connectionState connection) >>= \case
  Busy -> do
    leftover <- unconsumedInput input
    unless (BS.null leftover) $ modifyIORef' (connectionCarryOver connection) (leftover <>)
  _ -> pure ()

expireContext :: Context -> IO ()
expireContext context = do
  owner <- atomicModifyIORef' (contextUse context) $ \case
    Using thread -> (Expired, Just thread)
    _ -> (Expired, Nothing)
  case owner of
    Nothing -> pure ()
    Just thread -> do
      breakConnection (contextConnection context)
      -- A second cancellation must not return the loan while the first
      -- cancellation is still unwinding a child that can touch its buffers.
      uninterruptibleMask_ $ do
        killThread thread
        readMVar (contextFinished context)

ensureContextActive :: Context -> IO ()
ensureContextActive context = readIORef (contextUse context) >>= \case
  Expired -> throwIO (SessionUnusable "the operation context has expired")
  _ -> pure ()

-- | Serializes use within a supplied scope. Owned compatibility contexts
-- acquire connection ownership for each operation instead.
runContextOperation :: Context -> IO a -> IO a
runContextOperation context action = mask $ \restore -> do
  beginContextOperation context
  result <- restore action `onException` endContextOperation context True
  endContextOperation context False
  pure result

-- | Claims the context for the calling thread, which must hold it until
-- 'endContextOperation', with nothing else claiming it meanwhile. The
-- caller masks the claim and what it does with it.
beginContextOperation :: Context -> IO ()
beginContextOperation context = do
  thread <- myThreadId
  failure <- atomicModifyIORef' (contextUse context) $ \case
    Available -> (Using thread, Nothing)
    state@(Using _) -> (state, Just "the operation context is busy")
    Expired -> (Expired, Just "the operation context has expired")
  maybe (pure ()) (throwIO . SessionUnusable) failure
  takeMVar (contextFinished context)
  let connection = contextConnection context
  (if contextScoped context
    then readIORef (connectionState connection) >>= \case
      Busy -> pure ()
      _ -> throwIO (SessionUnusable "the connection is no longer usable")
    else claimConnection connection)
    `onException` finishOperation context

-- | Ends the operation that 'beginContextOperation' began. After a failure
-- the connection is broken, and the context cleaned up.
endContextOperation :: Context -> Bool -> IO ()
endContextOperation context failed = (if failed then failure else success) `finally` finishOperation context
  where
    connection = contextConnection context
    failure = breakConnection connection *> cleanupContext context
    success = unless (contextScoped context) $ do
      previous <- atomicModifyIORef' (connectionState connection)
        (\state -> (case state of Busy -> Idle; other -> other, state))
      case previous of
        Broken -> cleanupContext context
        _ -> pure ()

finishOperation :: Context -> IO ()
finishOperation context = do
  putMVar (contextFinished context) ()
  atomicModifyIORef' (contextUse context) (\state -> (case state of Using _ -> Available; other -> other, ()))

claimConnection :: Connection -> IO ()
claimConnection connection = do
  failure <- atomicModifyIORef' (connectionState connection) $ \case
    Idle -> (Busy, Nothing)
    Busy -> (Busy, Just "the session is busy with another operation")
    Broken -> (Broken, Just "the session broke during an earlier operation")
    Closed -> (Closed, Just "the session is closed")
  maybe (pure ()) (throwIO . SessionUnusable) failure

-- | A late cleanup cannot resurrect a connection already closed by its owner.
breakConnection :: Connection -> IO ()
breakConnection connection = atomicModifyIORef' (connectionState connection) $ \state ->
  (case state of Closed -> Closed; _ -> Broken, ())
