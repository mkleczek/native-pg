{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Connections, one at a time for a part, over a socket. TLS is a stub: a mode that
-- needs it fails until the code is there.
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
        connectMinProtocolVersion,
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

import Control.Arrow (arr, first, second, (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, fromException, throwIO)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import Data.IORef (readIORef)
import Data.List (isPrefixOf)
import Data.Maybe (fromMaybe)
import Data.Tuple (Solo (MkSolo))
import Database.PostgreSQL.Connection.Internal
import Database.PostgreSQL.ConnectionString.Types (Host (..))
import Database.PostgreSQL.Errors (ConnectError (..), ConnectFailure (..), UsageError (..))
import Database.PostgreSQL.Internal.Frontend (sendOutput)
import Database.PostgreSQL.Internal.Protocol.Messages (BackendKey, pattern CancelRequest, serialize)
import Database.PostgreSQL.Internal.Socket (connectSocket)
import Database.PostgreSQL.Internal.Startup (startup)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Transport (Address (..), TlsClient)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.Classes (Effect (..), Exit (..), Link (receive), Room, Runtime, Scope (..), Within (..), roomOf, secs)
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, done, iterateA)
import Network.Streaming.Loans (Loans, withSetupBuffersOf)
import Network.Streaming.PositiveInt (positiveInt)

-- | Connects to the first of the settings' hosts that qualifies: the settings' own, then the settings of its fallbacks,
-- in order. For each it connects the socket as its address says, the Unix-domain socket or the numeric address, sends
-- the startup message and authenticates as the server asks, with SCRAM-SHA-256, MD5 or a cleartext password, and then
-- checks that the server qualifies: it has the protocol version that @min_protocol_version@ asks for, and the session
-- attributes that @target_session_attrs@ asks for, which @prefer-standby@ asks for in two passes, the hosts that are
-- standbys first and then any. Each host has its own @connect_timeout@, which is a deadline of the arrow. TLS is to
-- come: for now a mode that needs it fails the host. It borrows from the loans only while it sets up a host.
--
-- It always fails with @ConnectFailure@: an entry for each host it tried, in
-- order, for one host as for many. Of the two passes of @prefer-standby@ it gives the second's.
connect :: (Runtime arr) => ConnectSettings -> arr Loans (Either ConnectFailure Connection)
connect settings = case connectTargetSessionAttrs settings of
  PreferStandbySession ->
    alongside (pass StandbySession)
      >>> retry
      ^>> (pass AnySession ||| arr Right)
      >>^ outcome
  target -> pass target >>^ outcome
  where
    pass target = hostsInOrder [host {connectTargetSessionAttrs = target} | host <- settings : connectFallbacks settings]
    -- The loans go to the second pass if the first found no host.
    retry (result, loans) = either (const (Left loans)) Right result
    outcome = either (Left . ConnectFailure) Right

-- | The first of the hosts that connects, or why each of them didn't, in order. The hosts are known when the program is
-- built, so each one's attempt is an arrow of its own, which the one before it runs if it fails.
hostsInOrder :: (Runtime arr) => [ConnectSettings] -> arr Loans (Either [(Host, ConnectError)] Connection)
hostsInOrder = foldr next (arr (const (Left [])))
  where
    next host others =
      alongside (attempt host)
        >>> failed
        ^>> ((second others >>^ continued host) ||| arr Right)
    failed (result, loans) = either (\reason -> Left (reason, loans)) Right result
    continued host (reason, rest) = either (\failures -> Left ((hostOf host, reason) : failures)) Right rest

-- | One host: its session, if it is made within the host's connect timeout.
attempt :: (Runtime arr) => ConnectSettings -> arr Loans (Either ConnectError Connection)
attempt host = bounded (connecting host) >>^ fromMaybe (Left TimedOut)
  where
    bounded = maybe (>>^ Just) (within . secs . fromIntegral) (connectTimeout host)

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
-- connection of its own, made as the settings say, TLS included. It makes one
-- attempt, to the host of the settings, and fails with that attempt's
-- 'ConnectError' itself, also when the settings' @connect_timeout@ passes first.
-- It returns once the server has closed the connection, which it does when it has
-- handled the request: a statement that was cancelled fails in its own session
-- with SQLSTATE 57014, and a request that comes too late does nothing. A server
-- that sent no key, as some connection poolers don't, can't cancel anything: then
-- there is nothing to ask of any server.
--
-- The request is rendered into a small buffer of its own, since the step has no
-- loans.
cancel :: (Runtime arr) => ConnectSettings -> arr CancelKey ()
cancel settings =
  (\(CancelKey key) -> maybe (Left ()) Right key)
    ^>> (done ||| (bounded (requesting settings) >>> now (maybe (throwIO TimedOut) (either throwIO pure))))
  where
    bounded = maybe (>>^ Just) (within . secs . fromIntegral) (connectTimeout settings)

-- | The request on a socket of its own, which is closed when the step ends, however it ends.
requesting :: forall arr. (Runtime arr) => ConnectSettings -> arr (BackendKey ByteString) (Either ConnectError ())
requesting settings = case encrypting settings of
  Just reason -> arr (const (Left reason))
  Nothing ->
    (\key -> (connectAddress settings, key))
      ^>> first (connectSocket (connectTcpOptions settings))
      >>> (\(reached, key) -> fmap (,key) reached)
      ^>> (arr Left ||| asked)
  where
    asked :: arr (Socket.Socket, BackendKey ByteString) (Either ConnectError ())
    asked = scope (arr fst) (now (\(sock, _) -> Socket.close sock)) (now rendered >>> alongside sendOutput >>> waiting ^>> orClosed >>^ Right)
    -- The socket, the room that the reply would be received into, and the request, rendered into the buffer.
    rendered (_, (sock, key)) = (\buffer -> (sock, buffer, serialize (CancelRequest key))) <$> newBuffer (positiveInt @512)
    waiting (_, (sock, buffer, _)) = (sock, roomOf buffer)
    -- The server closes the connection once it has handled the request, and ends the stream; a connection that breaks
    -- first is as good.
    orClosed :: arr (Socket.Socket, Room) ()
    orClosed = tryA untilClosed >>> now (either (\failure -> maybe (throwIO failure) (\(_ :: IOException) -> pure ()) (fromException failure)) pure)
    untilClosed :: arr (Socket.Socket, Room) ()
    untilClosed = iterateA (alongside receive >>^ \(count, link) -> if count == 0 then Finish () else Again link)
