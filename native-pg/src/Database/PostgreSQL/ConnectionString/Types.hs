{-# LANGUAGE LambdaCase #-}

-- | Connection strings, host settings, and connection attempt failures.
module Database.PostgreSQL.ConnectionString.Types
  ( ConnectionString (..),
    Defaults (..),
    Host (..),
    InvalidConnectionString (..),
    TargetSessionAttrs (..),
    hostConnectSettings,
    hostSocketAddress,
    ConnectFailure (..),
    HostFailure (..),
  ) where

import Control.Applicative ((<|>))
import Control.Exception (Exception (..))
import Data.ByteString (ByteString)
import Data.List (intercalate)
import Data.List.NonEmpty (NonEmpty)
import Database.PostgreSQL.Diagnostics (ConnectionError, ServerError)
import Database.PostgreSQL.Types (ProtocolVersion)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Transport (Address (..), ConnectError, TcpOptions, TlsVersion)
import Network.Socket (PortNumber)

-- | What a connection string asks for, with the environment's defaults.
data ConnectionString = ConnectionString
  { -- | Tried in order.
    connectionHosts :: NonEmpty Host,
    connectionUser :: ByteString,
    connectionPassword :: Maybe ByteString,
    -- | Password-file matches, resolved by 'readConnectionString' separately
    -- for each host. An explicitly supplied password takes priority.
    connectionHostPasswords :: [(Host, ByteString)],
    connectionDatabase :: ByteString,
    -- | Startup parameters: @application_name@, or else
    -- @fallback_application_name@, @options@ and @client_encoding@, where
    -- given.
    connectionParameters :: [(ByteString, ByteString)],
    -- | Seconds each host has to connect, authenticate and qualify.
    connectionTimeout :: Maybe Int,
    connectionTargetSessionAttrs :: TargetSessionAttrs,
    -- | The version asked for, from @max_protocol_version@, and the lowest one
    -- accepted, from @min_protocol_version@.
    connectionProtocolVersion :: ProtocolVersion,
    connectionMinProtocolVersion :: ProtocolVersion,
    connectionTcpOptions :: TcpOptions,
    connectionSslMode :: SslMode,
    -- | The client certificate and its private key, from @sslcert@ and
    -- @sslkey@, or the default files in the defaults' certificate directory,
    -- unless @sslcertmode@ is @disable@.
    connectionSslCertificate :: Maybe (FilePath, FilePath),
    -- | Certificate revocation lists: from @sslcrl@, or, if neither it nor
    -- @sslcrldir@ is given, the default file in the defaults' certificate
    -- directory, and from @sslcrldir@.
    connectionSslRevocationList :: Maybe FilePath,
    connectionSslRevocationListDirectory :: Maybe FilePath,
    -- | The versions of TLS from @ssl_min_protocol_version@, @TLSv1.2@ by
    -- default, to @ssl_max_protocol_version@, that rustls has.
    connectionSslVersions :: [TlsVersion],
    connectionSslServerNameIndication :: Bool,
    -- | From @sslnegotiation@: whether TLS starts without asking the server
    -- first, which needs an sslmode that requires TLS.
    connectionSslDirect :: Bool,
    connectionChannelBinding :: ChannelBindingMode
  }
  deriving (Eq, Show)

-- | A server to try.
data Host = Host
  { -- | As the connection string names it: a host name, an IP address, or the
    -- directory of a Unix-domain socket.
    hostName :: String,
    -- | From @hostaddr@: the IP address to connect to instead of looking up
    -- the name.
    hostAddress :: Maybe String,
    hostPort :: PortNumber
  }
  deriving (Eq, Show)

-- | Which servers qualify, from @target_session_attrs@.
data TargetSessionAttrs
  = AnySession
  | -- | Not in hot standby, and not read-only by default.
    ReadWriteSession
  | ReadOnlySession
  | -- | Not in hot standby.
    PrimarySession
  | StandbySession
  | -- | A standby if a host is one, or else any server.
    PreferStandbySession
  deriving (Eq, Show)

-- | A connection string that can't be used, for the reason given.
newtype InvalidConnectionString = InvalidConnectionString String
  deriving (Show)

instance Exception InvalidConnectionString where
  displayException (InvalidConnectionString reason) = reason

-- | What libpq builds in, given by the client: where to connect when a string
-- gives no host, and where the files libpq looks for by default are.
data Defaults = Defaults
  { -- | A socket directory, or a host name. libpq's is @/tmp@, or the
    -- directory it was built with, and @localhost@ on Windows.
    defaultHost :: String,
    -- | The directory of @root.crt@, @postgresql.crt@, @postgresql.key@ and
    -- @root.crl@, if any: libpq's is @~/.postgresql@.
    defaultCertificateDirectory :: Maybe FilePath
  }
  deriving (Eq, Show)

-- | The settings for connecting to one of the hosts.
hostConnectSettings :: ConnectionString -> Host -> ConnectSettings
hostConnectSettings string host =
  (connectSettings (hostSocketAddress host) (connectionUser string))
    { connectPassword = connectionPassword string <|> lookup host (connectionHostPasswords string),
      connectDatabase = Just (connectionDatabase string),
      connectParameters = connectionParameters string,
      connectProtocolVersion = connectionProtocolVersion string,
      connectTcpOptions = connectionTcpOptions string,
      connectSslMode = connectionSslMode string,
      connectSslCertificate = connectionSslCertificate string,
      connectSslRevocationList = connectionSslRevocationList string,
      connectSslRevocationListDirectory = connectionSslRevocationListDirectory string,
      connectSslVersions = connectionSslVersions string,
      connectSslServerNameIndication = connectionSslServerNameIndication string,
      connectSslDirect = connectionSslDirect string,
      connectChannelBinding = connectionChannelBinding string,
      -- With hostaddr, the address is the IP address the name resolved to.
      connectServerName = Just (hostName host)
    }

-- | Where to connect. A socket directory names the Unix-domain socket for the
-- port in it, and one starting with @\@@ a socket in Linux's abstract
-- namespace.
hostSocketAddress :: Host -> Address
hostSocketAddress host = case (hostAddress host, hostName host) of
  (Just address, _) -> TcpAddress address (hostPort host)
  (Nothing, directory@('/' : _)) -> UnixAddress (socketFile directory)
  (Nothing, '@' : abstract) -> UnixAddress ('\0' : socketFile abstract)
  (Nothing, name) -> TcpAddress name (hostPort host)
  where
    socketFile directory = directory <> "/.s.PGSQL." <> show (hostPort host)

-- | No host qualified: each attempt's host and failure, in order.
-- 'displayException' words it as libpq does.
newtype ConnectFailure = ConnectFailure [(Host, HostFailure)]
  deriving (Show)

-- | Why a host didn't qualify.
data HostFailure
  = -- | The socket didn't connect.
    NotConnected ConnectError
  | -- | The server rejected the connection, or the startup failed.
    NotStarted ConnectionError
  | -- | The connection broke during the startup.
    Broke String
  | -- | The connect timeout passed first.
    TimedOut
  | -- | The server doesn't have the session attributes or the protocol
    -- version asked for.
    Unsuitable String
  | -- | Checking the session attributes failed.
    CheckFailed ServerError
  deriving (Show)

instance Exception ConnectFailure where
  displayException (ConnectFailure failures) = intercalate "\n" (map describe failures)
    where
      describe = \case
        (_, NotConnected failure) -> displayException failure
        (host, failure) -> "connection to server " <> location host <> " failed: " <> reason failure
      reason = \case
        NotConnected failure -> displayException failure
        NotStarted failure -> displayException failure
        Broke why -> why
        TimedOut -> "timeout expired"
        Unsuitable why -> why
        CheckFailed failure -> displayException failure
      location host = case hostSocketAddress host of
        UnixAddress path -> "on socket " <> show path
        TcpAddress address port
          | address == hostName host -> "at " <> show address <> ", port " <> show port
          | otherwise -> "at " <> show (hostName host) <> " (" <> address <> "), port " <> show port
