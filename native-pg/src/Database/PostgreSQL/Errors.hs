{-# LANGUAGE LambdaCase #-}

-- | Every error and failure type of the v2 API, in one place. The three kinds of
-- outcome are told apart by their types:
--
-- * Errors the caller decides about are values: a 'QueryError', a 'ConnectError'
--   and a 'PoolError' are the 'Left' of the operation that makes them.
-- * Failures are exceptions: a 'UsageError' is the program's own mistake, and a
--   'ConnectionError' is the connection's end.
-- * A cancellation is neither: it arrives at a wait, and isn't an error here.
module Database.PostgreSQL.Errors
  ( -- * What the server reported
    Diagnostics (..),
    ServerError (..),
    Severity (..),
    isConnectionFatal,

    -- * Errors that are values
    QueryError (..),
    StatementError (..),
    ConnectError (..),
    PoolError (..),

    -- * Failures
    UsageError (..),
    ConnectionError (..),
  )
where

import Control.Exception (Exception (..))
import Database.PostgreSQL.ConnectionString.Types (Host, HostFailure)
import qualified Database.PostgreSQL.ConnectionString.Types as ConnectionString
import Database.PostgreSQL.Diagnostics
  ( Diagnostics (..),
    QueryError (..),
    ServerError (..),
    Severity (..),
    StatementError (..),
    isConnectionFatal,
  )
import Network.Streaming.Pool (PoolError (..))

-- | Connecting failed, in the words of libpq: the server rejected the
-- connection, or the startup failed on the client's side, or, for settings that
-- a connection string made, no host qualified, with each host's failure.
data ConnectError
  = -- | The server refused the connection.
    ConnectionRejected ServerError
  | -- | The server's messages don't follow the protocol.
    ProtocolError String
  | -- | The server asked for an authentication method this client doesn't
    -- implement.
    UnsupportedAuthentication String
  | -- | Authentication failed on the client's side: a password is needed but
    -- missing, or the server's SCRAM signature doesn't match.
    AuthenticationError String
  | -- | TLS couldn't be started, e.g. because the server doesn't take it.
    TlsFailure String
  | -- | No host qualified: each attempt's host and failure, in order.
    ConnectFailure [(Host, HostFailure)]
  deriving (Show)

instance Exception ConnectError where
  displayException = \case
    ConnectionRejected rejection -> displayException rejection
    ProtocolError reason -> "protocol error: " <> reason
    UnsupportedAuthentication method -> "the server asks for authentication with " <> method <> ", which isn't supported"
    AuthenticationError reason -> reason
    TlsFailure reason -> reason
    ConnectFailure failures -> displayException (ConnectionString.ConnectFailure failures)

-- | The program used the API wrongly, or what it read didn't fit. Each is a
-- failure of the step that found it, and the transaction it happened in rolls
-- back.
data UsageError
  = -- | A result was read, or a preparation awaited, while this many earlier
    -- results of the transaction were neither read nor discarded.
    ResultOutOfOrder Int
  | -- | A second part used a connection that one part was using.
    ConnectionBusy
  | -- | A statement was sent after the final pipeline.
    AfterFinal
  | -- | A column of this many bytes doesn't fit the buffer, so it can only be
    -- read as a field.
    ValueTooLarge Int
  | -- | A result had this many rows, which its shape doesn't allow.
    UnexpectedAmountOfRows Int
  | -- | A parameter stream gave a different number of bytes than it declared:
    -- the declared length, and at least as many as were delivered. The message
    -- it belongs to is incomplete, so the connection is broken.
    StreamLengthError Int Int
  deriving (Eq, Show)

instance Exception UsageError where
  displayException = \case
    ResultOutOfOrder unread ->
      "a result was read, or a preparation awaited, while " <> show unread
        <> (if unread == 1 then " earlier result was" else " earlier results were")
        <> " neither read nor discarded"
    ConnectionBusy -> "the connection is busy with another operation"
    AfterFinal -> "a statement followed the final pipeline"
    ValueTooLarge size -> "a value of " <> show size <> " bytes doesn't fit the buffer, and can only be read as a field"
    UnexpectedAmountOfRows count -> "the result had " <> show count <> " rows, which its shape doesn't allow"
    StreamLengthError declared delivered ->
      "a parameter stream declared " <> show declared <> " bytes, and delivered " <> show delivered

-- | The connection ended, and nothing more can be done with it.
data ConnectionError
  = -- | The server ended the session with this error, e.g. because an
    -- administrator terminated it.
    ConnectionTerminated ServerError
  | -- | The connection is unusable: it was closed, or it broke, for the reason
    -- given.
    ConnectionBroken String
  deriving (Eq, Show)

instance Exception ConnectionError where
  displayException = \case
    ConnectionTerminated termination -> displayException termination
    ConnectionBroken reason -> reason
