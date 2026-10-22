{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Persistent connection controls and operation-scoped query sessions.
module Database.PostgreSQL.Session
  ( Session, Connection,
    connection, close, isUsable,
    closeStatement, cancel,
  ) where

import Control.Exception (IOException, catch, finally, mask, throwIO)
import Control.Monad (unless)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
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
isUsable opened = do
  state <- readIORef (connectionState opened)
  pure $ case state of
    Idle -> True
    _ -> False

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
  withChannel settings $ \opened _ -> do
    pending <- newIORef =<< unsent Nothing mempty
    let channel = opened
    ( do
        channelSend channel (serialize (CancelRequest key))
        -- The server closes the connection once it has handled the request.
        let waitForClose = do
              received <- allocaBytes 64 $ \buffer -> readInputInto (channelInput channel pending 64) buffer 64
              unless (received == 0) waitForClose
        waitForClose `catch` \(_ :: IOException) -> pure ()
      )
      `finally` channelClose channel
