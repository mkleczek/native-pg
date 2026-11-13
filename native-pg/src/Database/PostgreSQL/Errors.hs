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
import Data.List (intercalate)
import Database.PostgreSQL.ConnectionString.Types (Host (hostName), hostSocketAddress)
import Database.PostgreSQL.Diagnostics
  ( Diagnostics (..),
    QueryError (..),
    ServerError (..),
    Severity (..),
    StatementError (..),
    isConnectionFatal,
  )
import Database.PostgreSQL.Transport (Address (..))
import Foreign.C.Error (Errno (..), eCONNREFUSED, eNOENT)
import GHC.IO.Exception (IOException (..))
import Network.Socket (HostName, PortNumber)
import Network.Streaming.Pool (PoolError (..))

-- | Connecting failed, in the words of libpq: the socket didn't connect, or the
-- server rejected the connection, or the startup failed on the client's side,
-- and each host that was tried has its own reason.
data ConnectError
  = -- | The host name doesn't resolve, for the resolver's reason.
    UnresolvedHost HostName String
  | -- | Connecting to each address the host name resolved to failed: each
    -- address, numeric, and its failure.
    UnreachableHost HostName PortNumber [(String, IOException)]
  | -- | Connecting to the Unix-domain socket failed.
    UnreachableSocket FilePath IOException
  | -- | The server refused the connection.
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
  | -- | The connection broke during the startup.
    Broke String
  | -- | The connect timeout passed first.
    TimedOut
  | -- | The server doesn't have the session attributes or the protocol
    -- version asked for.
    Unsuitable String
  | -- | Checking the session attributes failed.
    CheckFailed ServerError
  | -- | No host qualified: each attempt's host and failure, in order.
    ConnectFailure [(Host, ConnectError)]
  deriving (Show)

instance Exception ConnectError where
  displayException = \case
    UnresolvedHost host reason ->
      "could not translate host name " <> show host <> " to address: " <> reason
    UnreachableHost host port failures ->
      intercalate "\n" $
        [ "connection to server at " <> show host <> numeric address <> ", port " <> show port <> " failed: " <> describe failure tcpHint
        | (address, failure) <- failures
        ]
      where
        numeric address
          | address == host = ""
          | otherwise = " (" <> address <> ")"
    UnreachableSocket path failure ->
      "connection to server on socket " <> show path <> " failed: " <> describe failure socketHint
    ConnectionRejected rejection -> displayException rejection
    ProtocolError reason -> "protocol error: " <> reason
    UnsupportedAuthentication method -> "the server asks for authentication with " <> method <> ", which isn't supported"
    AuthenticationError reason -> reason
    TlsFailure reason -> reason
    Broke reason -> reason
    TimedOut -> "timeout expired"
    Unsuitable reason -> reason
    CheckFailed failure -> displayException failure
    ConnectFailure failures -> intercalate "\n" (map attempt failures)
    where
      describe failure hint = ioe_description failure <> maybe "" (hint . Errno) (ioe_errno failure)
      tcpHint errno
        | errno == eCONNREFUSED = "\n\tIs the server running on that host and accepting TCP/IP connections?"
        | otherwise = ""
      socketHint errno
        | errno == eNOENT || errno == eCONNREFUSED = "\n\tIs the server running locally and accepting connections on that socket?"
        | otherwise = ""
      -- A socket that didn't connect words itself, as libpq does.
      attempt (host, failure) = case failure of
        UnresolvedHost {} -> displayException failure
        UnreachableHost {} -> displayException failure
        UnreachableSocket {} -> displayException failure
        _ -> "connection to server " <> location host <> " failed: " <> displayException failure
      location host = case hostSocketAddress host of
        UnixAddress path -> "on socket " <> show path
        TcpAddress address port
          | address == hostName host -> "at " <> show address <> ", port " <> show port
          | otherwise -> "at " <> show (hostName host) <> " (" <> address <> "), port " <> show port

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
