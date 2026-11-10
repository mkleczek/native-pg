{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Persistent connection controls and operation-scoped query sessions.
module Database.PostgreSQL.Session
  ( Session, Connection,
    connection, close, isUsable, checkIdle,
    closeStatement, cancel,
  ) where

import Control.Exception (IOException, SomeAsyncException, SomeException, catch, displayException, finally, fromException, mask, mask_, throwIO, try)
import Control.Monad (unless)
import Data.Bool (bool)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Functor (($>))
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (isJust)
import Database.PostgreSQL.Diagnostics (ServerError)
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Internal.Exchange hiding (Session)
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import Database.PostgreSQL.Internal.Startup (withChannel)
import Database.PostgreSQL.Session.Settings (ConnectSettings)
import Database.PostgreSQL.Internal.Protocol.Codec (Alternatives (..), InputSource (..))
import Database.PostgreSQL.Internal.Protocol.Messages
import Database.PostgreSQL.Internal.Channel (Channel (..), unsent)
import Foreign.Marshal.Alloc (allocaBytes)

-- | A session ready for application operations; protocol phases stay private.
-- A borrowed session expires when its buffer callback returns.
type Session = Exchange.Session 'Ready

-- | The persistent connection, including for sessions opened with owned storage.
connection :: Session -> Connection
connection = sessionConnection

-- | Ends an idle or broken connection and closes its socket. The session is
-- unusable from then on, even if closing throws. Further calls do nothing.
-- Throws 'SessionUnusable' if an operation is still running; interrupt it
-- and wait for it to finish before closing.
close :: Connection -> IO ()
close opened = mask $ \restore -> do
  state <- atomicModifyIORef' (connectionState opened) $ \case
    Busy -> (Busy, Busy)
    previous -> (Closed, previous)
  case state of
    Closed -> pure ()
    Busy -> throwIO (SessionUnusable "the session is busy with another operation")
    _ ->
      -- A broken session may have stopped in the middle of a message, and
      -- the server may not be reading, so it isn't told to terminate.
      restore
        ( case state of
            Idle -> channelSend (connectionChannel opened) (serialize Terminate) `catch` \(_ :: IOException) -> pure ()
            _ -> pure ()
        )
        `finally` channelClose (connectionChannel opened)

-- | Whether the connection can start an operation: it hasn't been closed, none
-- is running, and none broke it. An operation that an exception interrupts
-- leaves the session broken, e.g. when its thread is killed.
isUsable :: Connection -> IO Bool
isUsable opened = (Idle ==) <$> readIORef (connectionState opened)

-- | Checks, without waiting, that the server hasn't ended an idle
-- connection. The server sends an idle session nothing, except
-- notifications to one that listens, unless it ends the session, e.g. when it
-- shuts down or an administrator terminates its backend: it then sends the
-- error and closes the connection. So if anything has arrived, or the
-- connection has been closed, it's broken from then on, and the reason is
-- given. Use it on connections that don't listen. A connection that isn't
-- idle isn't read, and why is given.
checkIdle :: Connection -> IO (Either String ())
checkIdle opened = mask_ $ do
  claimed <- atomicModifyIORef' (connectionState opened) $ \case
    Idle -> (Busy, Nothing)
    Busy -> (Busy, Just "the session is busy with another operation")
    Broken -> (Broken, Just "the session broke during an earlier operation")
    Closed -> (Closed, Just "the session is closed")
  case claimed of
    Just reason -> pure (Left reason)
    Nothing ->
      arrived >>= \case
        Right Nothing -> atomicModifyIORef' (connectionState opened) (\state -> (case state of Busy -> Idle; other -> other, Right ()))
        Right (Just 0) -> broken "the server closed the connection"
        Right (Just _) -> broken "the server sent a message to the idle session, as it does before ending it"
        Left failure
          | isJust (fromException @SomeAsyncException failure) -> breakConnection opened *> throwIO failure
          | otherwise -> broken (displayException failure)
  where
    broken reason = breakConnection opened $> Left reason
    -- What a scope read past its last reply has arrived as much as what the
    -- link holds.
    arrived = readIORef (connectionCarryOver opened) >>= \carried ->
      bool (pure (Right (Just (BS.length carried)))) readReady (BS.null carried)
#if defined(mingw32_HOST_OS)
    -- Sockets block on Windows, so reading would wait: only the state is
    -- checked.
    readReady = pure (Right Nothing)
#else
    readReady = try @SomeException (allocaBytes 1 $ \buffer -> channelReceiveReady (connectionChannel opened) buffer 1)
#endif

-- | Closes a named statement. Closing one that doesn't exist isn't an
-- error.
closeStatement :: Session -> ByteString -> IO (Either ServerError (), TransactionStatus)
closeStatement session name = operation session $ do
  sendOutputs session (serialize (Close PreparedStatement name) <> sync)
  closed <- pullEither (continueIn session :: Exchange.Session 'CloseResult) ((\CloseComplete -> Reply ()) :|: sessionTail)
  awaitReady session id closed

-- | Asks the server to cancel what the session with this backend key is
-- running, over a new connection made as the settings ask, TLS included, as
-- libpq's does since PostgreSQL 17. The server doesn't answer: a cancelled
-- statement fails in its own session with SQLSTATE 57014, and a request that
-- comes too late does nothing.
cancel :: ConnectSettings -> BackendKey ByteString -> IO ()
cancel settings key =
  withChannel settings $ \channel _ -> do
    pending <- newIORef =<< unsent Nothing mempty
    ( do
        channelSend channel (serialize (CancelRequest key))
        -- The server closes the connection once it has handled the request.
        let waitForClose = do
              received <- allocaBytes 64 $ \buffer -> readInputInto (channelInput channel pending 64) buffer 64
              unless (received == 0) waitForClose
        waitForClose `catch` \(_ :: IOException) -> pure ()
      )
      `finally` channelClose channel
