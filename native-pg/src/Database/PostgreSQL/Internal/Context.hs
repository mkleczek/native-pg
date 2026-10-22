{-# LANGUAGE LambdaCase #-}
-- | Integration-only connection and operation ownership. Do not retain a
-- supplied Context or any borrowed slices after its callback returns.
module Database.PostgreSQL.Internal.Context
  ( Context,
    withContext,
    contextConnection,
    contextInput,
    runContextInput,
    contextOutput,
    sendContextOutput,
    queueContextOutput,
    contextPending,
    contextReadSize,
    contextScoped,
    newOwnedContext,
    newConnection,
    runContextOperation,
    ensureContextActive,
    Connection (..),
    SessionState (..),
    ConnectionError (..),
  ) where

import Control.Concurrent (ThreadId, myThreadId, killThread)
import Control.Concurrent.MVar (MVar, newMVar, takeMVar, putMVar, readMVar)
import Control.Exception (Exception (..), catch, finally, mask, onException, throwIO)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import Data.IORef (IORef, newIORef, readIORef, writeIORef, atomicModifyIORef')
import Database.PostgreSQL.Buffer (Buffer, bufferCapacity)
import Database.PostgreSQL.Diagnostics (ServerError (..))
import Database.PostgreSQL.Protocol.Codec (Output, Input, InputBuffer, InputError (..), reusableInputBuffer, emptyInputBuffer, runInputUntil, runInputFramedUntil)
import Database.PostgreSQL.Protocol.Messages (Notification)
import Database.PostgreSQL.Transport (Channel (..), OutputBuffer, Unsent, outputBufferFrom, unsent, unsentEmpty, newOutputBuffer)

data Connection = Connection
  { -- | Input and output over the socket, or TLS over it.
    connectionChannel :: Channel,
    connectionReadSize :: Int,
    -- | Bytes read but not consumed yet; the only buffer the library keeps.
    connectionBuffer :: IORef InputBuffer,
    -- | Output of the running batch that the socket hasn't taken yet. Reads
    -- send it as they wait.
    connectionUnsent :: IORef Unsent,
    -- | Where batches are written. The next batch reuses it, since a batch
    -- has been sent whole by the time its results are read.
    connectionOutput :: OutputBuffer,
    connectionState :: IORef SessionState,
    -- | Notifications that arrived during operations, newest first, until
    -- 'awaitNotification' takes them.
    connectionNotifications :: IORef [Notification ByteString]
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

data ConnectionError
  = -- | The server refused the connection.
    ConnectionRejected ServerError
  | -- | The server ended the session with this error, e.g. because an
    -- administrator terminated it.
    ConnectionTerminated ServerError
  | -- | The server's messages don't follow the protocol.
    ProtocolError String
  | -- | The server asked for an authentication method this client doesn't
    -- implement.
    UnsupportedAuthentication String
  | -- | Authentication failed on the client's side: a password is needed but
    -- missing, or the server's SCRAM signature doesn't match.
    AuthenticationError String
  | -- | The session is busy with another operation, broke in an earlier
    -- one, or has been closed.
    SessionUnusable String
  | -- | A row or a column stream was used out of turn.
    RowMisuse String
  | -- | TLS couldn't be started, e.g. because the server doesn't take it.
    TlsFailure String
  deriving (Eq, Show)

instance Exception ConnectionError where
  displayException failure = case failure of
    ConnectionRejected rejection -> displayException rejection
    ConnectionTerminated termination -> displayException termination
    ProtocolError reason -> "protocol error: " <> reason
    UnsupportedAuthentication method -> "the server asks for authentication with " <> method <> ", which isn't supported"
    AuthenticationError reason -> reason
    SessionUnusable reason -> reason
    RowMisuse reason -> reason
    TlsFailure reason -> reason

-- | Builds protocol/link state for a connected, initialized transport.
newConnection :: Channel -> Int -> IO Connection
newConnection channel readSize = do
  input <- newIORef emptyInputBuffer
  pending <- newIORef =<< unsent Nothing mempty
  output <- newOutputBuffer 16384
  state <- newIORef Idle
  notifications <- newIORef []
  pure (Connection channel readSize input pending output state notifications)

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
    contextFinished :: MVar ()
  }

data ContextUse = Available | Using ThreadId | Expired

newOwnedContext :: Connection -> IO Context
newOwnedContext connection = do
  use <- newIORef Available
  finished <- newMVar ()
  pure (Context connection (connectionBuffer connection) (connectionOutput connection)
    (connectionUnsent connection) (connectionReadSize connection) False use finished)

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
  remaining <- channelSendSome (connectionChannel (contextConnection context)) pending
  writeIORef (contextPending context) remaining

-- | Runs a concrete parser against this operation's cursor and pending
-- output. Supplied scopes never read beyond a known protocol envelope.
runContextInput :: Context -> Int -> Input a -> IO a
runContextInput context limit parse = do
  ensureContextActive context
  input <- readIORef (contextInput context)
  let connection = contextConnection context
      source = channelInput (connectionChannel connection) (contextPending context) (contextReadSize context)
      run = if contextScoped context then runInputFramedUntil else runInputUntil
  (result, input') <- run source limit input parse
    `catch` \(InputError reason) -> throwIO (ProtocolError reason)
  writeIORef (contextInput context) input'
  pure result

-- | Binds supplied initialized buffers for the callback. Acquires the
-- connection before exposing the Context and cancels/joins any operation
-- still running on another thread before the buffers may be reused.
withContext :: Connection -> Buffer -> Buffer -> (Context -> IO a) -> IO a
withContext connection input output use = do
  when (bufferCapacity output < 4) (ioError (userError "output buffer capacity must be at least four bytes"))
  mask $ \restore -> do
    claimConnection connection
    context <- (do
      cursor <- newIORef (reusableInputBuffer input)
      pending <- newIORef =<< unsent Nothing mempty
      state <- newIORef Available
      finished <- newMVar ()
      pure (Context connection cursor (outputBufferFrom output) pending (bufferCapacity input) True state finished))
      `onException` writeIORef (connectionState connection) Idle
    result <- restore (use context) `onException` writeIORef (connectionState connection) Broken
      `finally` expireContext context
    pending <- readIORef (contextPending context)
    unless (unsentEmpty pending) $ do
      writeIORef (connectionState connection) Broken
      throwIO (SessionUnusable "operation scope ended with unsent output")
    atomicModifyIORef' (connectionState connection) (\state -> (case state of Busy -> Idle; other -> other, ()))
    pure result

expireContext :: Context -> IO ()
expireContext context = do
  owner <- atomicModifyIORef' (contextUse context) $ \case
    Using thread -> (Expired, Just thread)
    _ -> (Expired, Nothing)
  case owner of
    Nothing -> pure ()
    Just thread -> do
      writeIORef (connectionState (contextConnection context)) Broken
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
  thread <- myThreadId
  failure <- atomicModifyIORef' (contextUse context) $ \case
    Available -> (Using thread, Nothing)
    state@(Using _) -> (state, Just "the operation context is busy")
    Expired -> (Expired, Just "the operation context has expired")
  maybe (pure ()) (throwIO . SessionUnusable) failure
  takeMVar (contextFinished context)
  let connection = contextConnection context
      done = do
        atomicModifyIORef' (contextUse context) (\state -> (case state of Using _ -> Available; other -> other, ()))
        putMVar (contextFinished context) ()
      run = do
        if contextScoped context
          then readIORef (connectionState connection) >>= \case
            Busy -> pure ()
            _ -> throwIO (SessionUnusable "the connection is no longer usable")
          else claimConnection connection
        result <- restore action `onException` writeIORef (connectionState connection) Broken
        unless (contextScoped context) (writeIORef (connectionState connection) Idle)
        pure result
  run `finally` done

claimConnection :: Connection -> IO ()
claimConnection connection = do
  failure <- atomicModifyIORef' (connectionState connection) $ \case
    Idle -> (Busy, Nothing)
    Busy -> (Busy, Just "the session is busy with another operation")
    Broken -> (Broken, Just "the session broke during an earlier operation")
    Closed -> (Closed, Just "the session is closed")
  maybe (pure ()) (throwIO . SessionUnusable) failure
