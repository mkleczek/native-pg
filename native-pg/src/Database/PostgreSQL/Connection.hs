-- | Connections, one at a time for a part, whatever the link: a socket or TLS. The
-- steps that connect, close and cancel are stubs, which fail with
-- 'Network.Streaming.NotImplemented.NotImplemented' until the code is there.
module Database.PostgreSQL.Connection
  ( -- * Settings
    ConnectSettings
      ( connectAddress,
        connectUser,
        connectPassword,
        connectDatabase,
        connectParameters,
        connectProtocolVersion,
        connectReadSize,
        connectTcpOptions,
        connectSslMode,
        connectServerName,
        connectSslCertificate,
        connectSslRevocationList,
        connectSslRevocationListDirectory,
        connectSslVersions,
        connectSslServerNameIndication,
        connectSslDirect,
        connectChannelBinding,
        connectTlsClient,
        connectFallbacks,
        connectTargetSessionAttrs,
        connectTimeout
      ),
    connectSettings,
    Address (..),
    SslMode (..),
    ChannelBindingMode (..),
    TargetSessionAttrs (..),
    TlsClient,

    -- * Connections
    Connection,
    connect,
    close,
    ServerInfo (..),
    serverProcessId,
    serverInfo,
    connectedHost,

    -- * Cancelling
    CancelKey,
    cancelKey,
    cancel,
  )
where

import Database.PostgreSQL.Connection.Internal (CancelKey, Connection (..))
import Database.PostgreSQL.ConnectionString.Types (Host)
import Database.PostgreSQL.Errors (ConnectError)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Transport (Address (..), TlsClient)
import Network.Streaming.Classes (Effect, Runtime)
import Network.Streaming.Loans (Loans)
import Network.Streaming.NotImplemented (notImplemented)

-- | Connects to the first host that qualifies, as the settings say: host
-- selection, @connect_timeout@ (a deadline of the arrow), TLS and the
-- authentication, with SCRAM's channel binding, MD5 or a cleartext password.
-- It borrows from the loans only while it sets up.
connect :: (Runtime arr) => ConnectSettings -> arr Loans (Either ConnectError Connection)
connect _ = notImplemented "connect"

-- | Closes the connection, once or again.
close :: (Runtime arr) => arr Connection ()
close = notImplemented "close"

-- | What the server said of itself at startup.
serverInfo :: Connection -> ServerInfo
serverInfo (Connection info _ _) = info

-- | The host that the connection is at.
connectedHost :: Connection -> Host
connectedHost (Connection _ host _) = host

-- | What cancels the statement that the connection runs.
cancelKey :: Connection -> CancelKey
cancelKey (Connection _ _ key) = key

-- | Cancels the statement that the key's connection runs, out of band: over a
-- connection of its own, made as the settings say, TLS included.
cancel :: (Effect arr) => ConnectSettings -> arr CancelKey ()
cancel _ = notImplemented "cancel"
