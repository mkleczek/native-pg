-- | Connections, one at a time for a part, over a socket. The step that cancels is
-- a stub, which fails with 'Network.Streaming.NotImplemented.NotImplemented' until
-- the code is there, and so are TLS, and the hosts after the first.
--
-- A program that is cancelled leaves its connection usable if and only if nothing
-- is owed on it in either direction: no reply is still expected, and nothing is
-- half sent. Otherwise the connection is broken and closed: it is never drained for
-- a client that has gone away.
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

import Control.Arrow (arr, first, (>>>), (>>^), (^>>), (|||))
import Control.Exception (throwIO)
import Control.Monad (unless, when)
import Data.IORef (readIORef)
import Data.List (isPrefixOf)
import Data.Tuple (Solo (MkSolo))
import Database.PostgreSQL.Connection.Internal
import Database.PostgreSQL.ConnectionString.Types (Host (..))
import Database.PostgreSQL.Errors (ConnectError (..), ConnectFailure (..), UsageError (..))
import Database.PostgreSQL.Internal.Socket (connectSocket)
import Database.PostgreSQL.Internal.Startup (startup)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Transport (Address (..), TlsClient)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Classes (Effect (..), Exit (..), Runtime, Scope (..), Within (..), secs)
import Network.Streaming.Loans (Loans, withSetupBuffersOf)
import Network.Streaming.NotImplemented (notImplemented)

-- | Connects to the first host of the settings, as they say: the Unix-domain socket or the numeric address, the
-- startup message and the authentication of the server's asking, with SCRAM-SHA-256, MD5 or a cleartext password, and
-- @connect_timeout@, which is a deadline of the arrow. TLS, host selection and the session attributes are to come: for
-- now a mode that needs TLS fails, and the hosts after the first are not tried. It borrows from the loans only while it
-- sets up.
--
-- It always fails with @ConnectFailure@: an entry for each host it tried, in
-- order, for one host as for many.
connect :: (Runtime arr) => ConnectSettings -> arr Loans (Either ConnectFailure Connection)
connect settings = bounded (connecting settings) >>^ maybe (failing TimedOut) (either failing Right)
  where
    host = hostOf settings
    failing reason = Left (ConnectFailure [(host, reason)])
    bounded = maybe (>>^ Just) (within . secs . fromIntegral) (connectTimeout settings)

-- | The session on a new socket, from the setup buffer of the loans' pool: the one buffer is what the startup renders
-- into and reads in, a message at a time.
connecting :: (Runtime arr) => ConnectSettings -> arr Loans (Either ConnectError Connection)
connecting settings = case encrypting settings of
  Just reason -> arr (const (Left reason))
  Nothing -> (, ()) ^>> withSetupBuffersOf (theBuffer ^>> connected settings)
  where
    -- A pattern takes the fields out; getSolo . fst would leave a thunk.
    theBuffer (MkSolo buffer, _) = buffer

-- | Why the settings can't be served without TLS, if they can't: as libpq built without TLS fails them. A Unix-domain
-- socket isn't encrypted, whatever the mode.
encrypting :: ConnectSettings -> Maybe ConnectError
encrypting settings = case (connectAddress settings, connectSslMode settings) of
  (UnixAddress _, _) -> Nothing
  (_, SslRequire) -> Just (without "require")
  (_, SslVerifyFull _) -> Just (without "verify-full")
  _ -> Nothing
  where
    without mode = TlsFailure ("sslmode value \"" <> mode <> "\" invalid when SSL support is not compiled in")

-- | A socket to the address, which the session is started on.
connected :: (Runtime arr) => ConnectSettings -> arr Buffer (Either ConnectError Connection)
connected settings =
  (connectAddress settings,) ^>> first (connectSocket (connectTcpOptions settings)) >>> withBuffer ^>> (arr Left ||| session)
  where
    withBuffer (reached, buffer) = fmap (,buffer) reached
    -- A pattern takes the fields out; second snd &&& fst would leave a thunk for each.
    startupInput (sock, (_, buffer)) = ((sock, buffer), sock)
    -- The socket is closed unless the session was started: also when the part is cancelled while it starts it.
    session =
      scope
        (arr fst)
        (now (\(sock, exit) -> case exit of Done -> pure (); _ -> Socket.close sock))
        (startupInput ^>> first (startup settings) >>> now (\(started, sock) -> either (\reason -> Left reason <$ Socket.close sock) (fmap Right . made sock) started))
    made sock info = newConnection info (hostOf settings) (CancelKey (serverBackendKey info)) sock

-- | The host that the settings connect to, as a connection string would have named it.
hostOf :: ConnectSettings -> Host
hostOf settings = case connectAddress settings of
  TcpAddress address port -> case connectServerName settings of
    Just name | name /= address -> Host name (Just address) port
    _ -> Host address Nothing port
  UnixAddress ('\0' : name) -> unixHost ('@' : name)
  UnixAddress path -> unixHost path
  where
    -- The directory of the socket, and the port in its file name.
    unixHost path = go [] path
      where
        marker = "/.s.PGSQL."
        go before rest
          | marker `isPrefixOf` rest = Host (reverse before) Nothing (port (drop (length marker) rest))
          | otherwise = case rest of
              [] -> Host path Nothing 5432
              c : others -> go (c : before) others
        port digits = case reads digits of
          [(number, "")] -> number
          _ -> 5432

-- | Closes the connection, once or again. It fails if another part is using it.
close :: (Runtime arr) => arr Connection ()
close =
  now $ \connection -> do
    live <- readIORef (connectionLease connection)
    unless live (throwIO ConnectionReleased)
    busy <- (== Busy) <$> readIORef (sessionStatus (connectionSession connection))
    when busy (throwIO ConnectionBusy)
    breakSession (connectionSession connection) "the connection is closed"

-- | What the server said of itself at startup.
serverInfo :: Connection -> ServerInfo
serverInfo = connectionInfo

-- | The host that the connection is at.
connectedHost :: Connection -> Host
connectedHost = connectionHost

-- | What cancels the statement that the connection runs.
cancelKey :: Connection -> CancelKey
cancelKey = connectionKey

-- | Cancels the statement that the key's connection runs, out of band: over a
-- connection of its own, made as the settings say, TLS included.
cancel :: (Effect arr) => ConnectSettings -> arr CancelKey ()
cancel _ = notImplemented "cancel"
