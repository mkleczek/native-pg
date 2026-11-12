-- | Connection configuration and information reported during startup.
module Database.PostgreSQL.Session.Settings
  ( ConnectSettings (..),
    SslMode (..),
    ChannelBindingMode (..),
    TargetSessionAttrs (..),
    connectSettings,
    ServerInfo (..),
    serverProcessId,
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int32)
import Database.PostgreSQL.Internal.Protocol.Messages.Types (BackendKey, Scalar (..))
import Database.PostgreSQL.Types (ProtocolVersion, protocolVersion3_0)
import Database.PostgreSQL.Transport (Address, RootCertificates, TcpOptions, TlsClient, TlsVersion, defaultTcpOptions)
import Network.Socket (HostName)

data ConnectSettings = ConnectSettings
  { connectAddress :: Address,
    connectUser :: ByteString,
    -- | Used only if the server asks for a password.
    connectPassword :: Maybe ByteString,
    -- | The server uses the user name when this is missing.
    connectDatabase :: Maybe ByteString,
    -- | More startup parameters, e.g. @application_name@ or @options@.
    connectParameters :: [(ByteString, ByteString)],
    -- | Version 3.2, which PostgreSQL 18 supports, has longer cancel keys, but
    -- some connection poolers reject it.
    connectProtocolVersion :: ProtocolVersion,
    -- | The size of a read buffer.
    connectReadSize :: Int,
    connectTcpOptions :: TcpOptions,
    -- | Whether to encrypt with TLS, which isn't used over Unix-domain
    -- sockets, as with libpq.
    connectSslMode :: SslMode,
    -- | The server's name for TLS, if the address's host isn't, e.g. because
    -- it's an IP address the name resolved to.
    connectServerName :: Maybe HostName,
    -- | A client certificate and its private key, in PEM files, used if the
    -- certificate file exists, as with libpq.
    connectSslCertificate :: Maybe (FilePath, FilePath),
    -- | A file of certificate revocation lists, used if it exists, and a
    -- directory of them, named as OpenSSL's c_rehash names them, as with
    -- libpq.
    connectSslRevocationList :: Maybe FilePath,
    connectSslRevocationListDirectory :: Maybe FilePath,
    -- | The versions of TLS the connection may use.
    connectSslVersions :: [TlsVersion],
    -- | Whether to send the server's name with TLS, unless it's an IP
    -- address.
    connectSslServerNameIndication :: Bool,
    -- | Whether to start TLS without asking the server first, as
    -- PostgreSQL 17 takes it.
    connectSslDirect :: Bool,
    connectChannelBinding :: ChannelBindingMode,
    -- | What starts TLS, as libpq's TLS support does: without it a connection
    -- can't encrypt, whatever 'connectSslMode' says.
    connectTlsClient :: Maybe TlsClient,
    -- | The settings of the hosts to try after this one, in order, which a
    -- connection string with several hosts makes. Each has its own address,
    -- password and server name, and none has hosts of its own.
    connectFallbacks :: [ConnectSettings],
    -- | Which of the hosts qualify, and the seconds that each has to connect,
    -- authenticate and qualify.
    connectTargetSessionAttrs :: TargetSessionAttrs,
    connectTimeout :: Maybe Int,
    -- | For tests: what SCRAM's channel binding takes as the TLS end-point
    -- data in place of the server's certificate's: the bytes, or why there are
    -- none, or none. Set through "Database.PostgreSQL.Connection.Internal".
    connectEndPointOverride :: Maybe (Maybe (Either String ByteString))
  }

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

-- | Whether to bind SCRAM to TLS, from libpq's channel_binding.
data ChannelBindingMode
  = ChannelBindingDisable
  | -- | If the server offers it over TLS.
    ChannelBindingPrefer
  | -- | Or else fail, as with libpq: the server is then known to have the
    -- password's secret, and the certificate.
    ChannelBindingRequire
  deriving (Eq, Show)

-- | Whether to encrypt with TLS, from libpq's sslmode.
data SslMode
  = -- | Without TLS.
    SslDisable
  | -- | Without TLS, and if the server rejects the connection, with TLS,
    -- without verifying the server.
    SslAllow
  | -- | With TLS if the server takes it, without verifying the server, and if
    -- it then fails or the server rejects the connection, without TLS.
    SslPrefer
  | -- | With TLS, without verifying the server.
    SslRequire
  | -- | With TLS, verifying the server's certificate against the root
    -- certificates, and its name.
    SslVerifyFull RootCertificates
  deriving (Eq, Show)

-- | No password, database or extra parameters, protocol 3.0, 16 KiB read
-- buffers, TCP keepalives and no TLS, but TLS's versions and server name
-- indication should it be asked for.
connectSettings :: Address -> ByteString -> ConnectSettings
connectSettings address user =
  ConnectSettings
    { connectAddress = address,
      connectUser = user,
      connectPassword = Nothing,
      connectDatabase = Nothing,
      connectParameters = [],
      connectProtocolVersion = protocolVersion3_0,
      connectReadSize = 16384,
      connectTcpOptions = defaultTcpOptions,
      connectSslMode = SslDisable,
      connectServerName = Nothing,
      connectSslCertificate = Nothing,
      connectSslRevocationList = Nothing,
      connectSslRevocationListDirectory = Nothing,
      connectSslVersions = [minBound .. maxBound],
      connectSslServerNameIndication = True,
      connectSslDirect = False,
      connectChannelBinding = ChannelBindingPrefer,
      connectTlsClient = Nothing,
      connectFallbacks = [],
      connectTargetSessionAttrs = AnySession,
      connectTimeout = Nothing,
      connectEndPointOverride = Nothing
    }

data ServerInfo = ServerInfo
  { serverProtocolVersion :: ProtocolVersion,
    -- | The parameters the server reported, e.g. @server_version@, in order.
    serverParameters :: [(ByteString, ByteString)],
    -- | Needed to cancel a query. Some connection poolers don't send it.
    serverBackendKey :: Maybe (BackendKey ByteString)
  }
  deriving (Eq, Show)

-- | The process id of the backend, if the server sent its key.
serverProcessId :: ServerInfo -> Maybe Int32
serverProcessId info = (\(Scalar (Scalar pid, _)) -> pid) <$> serverBackendKey info
