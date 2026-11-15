{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Connections, one at a time for a part, over a socket, or over TLS on one.
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
    connectIn,
    close,
    ServerInfo (..),
    serverProcessId,
    serverInfo,
    connectedHost,

    -- * Cancelling
    CancelKey,
    cancelKey,
    cancel,
    cancelIn,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, second, (>>>), (>>^), (^>>), (|||))
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
import Database.PostgreSQL.Internal.Carrier (Carrier (..), closeCarrier)
import Database.PostgreSQL.Internal.Encryption (NotMade (..), Securing (..), checkedRoots, requested, secured)
import Database.PostgreSQL.Internal.Frontend (sendOutput)
import Database.PostgreSQL.Internal.Protocol.Messages (BackendKey, pattern CancelRequest, serialize)
import Database.PostgreSQL.Internal.Socket (connectSocket)
import Database.PostgreSQL.Internal.Startup (startup)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Transport (Address (..), RootCertificates, ServerVerification (..), TlsClient)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.Classes (Effect (..), Exit (..), Link (receive), Room, Runtime, Scope (..), Within (..), roomOf, secs)
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, done, iterateA)
import Network.Streaming.Loans (Loans, withSetupBuffersOf)
import Network.Streaming.NotImplemented (notImplemented)
import Network.Streaming.PositiveInt (positiveInt)

-- | Connects to the first of the settings' hosts that qualifies: the settings' own, then the settings of its fallbacks,
-- in order. For each it connects the socket as its address says, the Unix-domain socket or the numeric address, TLS as
-- @sslmode@ and @sslnegotiation@ say, sends the startup message and authenticates as the server asks, with SCRAM-SHA-256
-- (bound to the TLS link if the server offers it), MD5 or a cleartext password, and then checks that the server
-- qualifies: it has the protocol version that @min_protocol_version@ asks for, and the session attributes that
-- @target_session_attrs@ asks for, which @prefer-standby@ asks for in two passes, the hosts that are standbys first and
-- then any. Each host has its own @connect_timeout@, which is a deadline of the arrow. It borrows from the loans only
-- while it sets up a host.
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

-- | 'connect' for settings that are the arrow's input, with the loans after them: one program, built once, connects with the
-- settings of each run.
connectIn :: (Runtime arr) => arr (ConnectSettings, Loans) (Either ConnectFailure Connection)
connectIn = notImplemented "connect with its settings as input"

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

-- | Why the settings can't be served without a TLS client, if they can't: as libpq built without TLS fails them. A
-- Unix-domain socket isn't encrypted, whatever the mode.
encrypting :: ConnectSettings -> Maybe ConnectError
encrypting settings = case (connectAddress settings, connectSslMode settings, connectTlsClient settings) of
  (UnixAddress _, _, _) -> Nothing
  (_, _, Just _) -> Nothing
  (_, SslRequire, Nothing) -> Just (without "require")
  (_, SslVerifyFull _, Nothing) -> Just (without "verify-full")
  _ -> Nothing
  where
    without mode = TlsFailure ("sslmode value \"" <> mode <> "\" invalid when SSL support is not compiled in")

-- | A connection to the address, which the session is started on, with TLS or without as @sslmode@ says.
connected :: (Runtime arr) => ConnectSettings -> arr Buffer (Either ConnectError Connection)
connected settings = attempting settings (started settings)

-- | What an attempt makes of the carrier that it got, with the data that SCRAM's channel binding takes and the buffer: the
-- session started on it, or the request that is sent over it.
type Use arr a = arr (Carrier, Maybe (Either String ByteString), Buffer) (Either NotMade a)

-- | The use, made of a connection to the address, with TLS or without as @sslmode@ says. Each attempt is on a new socket and
-- on the same buffer, one after the other:
--
-- * @allow@ connects without TLS first, and with TLS if the server rejected the connection;
-- * @prefer@ connects with TLS first, and without if the server didn't take it, which is no failure, or if it did and then
--   rejected the connection or TLS failed, as libpq does: a host that doesn't qualify, and a failure of the client's own,
--   are reported as they are;
-- * @require@ and @verify-full@ connect with TLS or fail, and @verify-full@ looks for the root certificate file first.
--
-- A Unix-domain socket isn't encrypted, whatever the mode, and without a TLS client @allow@ and @prefer@ connect without.
attempting :: (Runtime arr) => ConnectSettings -> Use arr a -> arr Buffer (Either ConnectError a)
attempting settings use = attempts >>^ either (\(NotMade _ reason) -> Left reason) Right
  where
    attempts = case (connectSslMode settings, connectAddress settings, connectTlsClient settings) of
      (SslAllow, TcpAddress {}, Just client) -> retrying rejected (oneTry settings use Nothing) (secure client NoVerification True)
      (SslPrefer, TcpAddress {}, Just client) -> retrying afterTls (secure client NoVerification False) (oneTry settings use Nothing)
      (SslRequire, TcpAddress {}, Just client) -> secure client NoVerification True
      (SslVerifyFull roots, TcpAddress {}, Just client) -> rootsChecked roots >>> (arr Left ||| secure client (VerifyFull roots) True)
      _ -> oneTry settings use Nothing
    secure client verification required = oneTry settings use (Just (Securing client verification required))
    rejected (NotMade _ failure) = case failure of ConnectionRejected _ -> True; _ -> False
    afterTls (NotMade accepted failure) = accepted && case failure of ConnectionRejected _ -> True; TlsFailure _ -> True; _ -> False

-- | The attempt, and the second one on the same buffer if the first failed as the test says.
retrying ::
  (ArrowChoice arr) =>
  (NotMade -> Bool) ->
  arr Buffer (Either NotMade a) ->
  arr Buffer (Either NotMade a) ->
  arr Buffer (Either NotMade a)
retrying again firstTry secondTry = (alongside firstTry >>^ chosen) >>> (secondTry ||| returnA)
  where
    chosen (result, buffer) = case result of
      Left failed | again failed -> Left buffer
      _ -> Right result

-- | The buffer, or the failure of the root certificate file's not being there, which libpq finds out before it connects.
rootsChecked :: (Runtime arr) => RootCertificates -> arr Buffer (Either NotMade Buffer)
rootsChecked roots = now (\buffer -> maybe (Right buffer) (Left . NotMade False) <$> checkedRoots roots)

-- | One attempt: a new socket, TLS over it as the securing says, if there is one, and the use made of it.
oneTry :: (Runtime arr) => ConnectSettings -> Use arr a -> Maybe Securing -> arr Buffer (Either NotMade a)
oneTry settings use securing =
  (connectAddress settings,) ^>> first (connectSocket (connectTcpOptions settings)) >>> withBuffer ^>> (arr (Left . NotMade False) ||| session)
  where
    withBuffer (reached, buffer) = fmap (,buffer) reached
    -- The socket is closed unless the use was made: also when the part is cancelled while it is.
    session =
      scope
        (arr fst)
        (now (\(sock, exit) -> case exit of Done -> pure (); _ -> Socket.close sock))
        (snd ^>> alongside (maybe onSocket (encrypted settings) securing) >>> (unready ^>> (now closed ||| use)))
    -- A pattern takes the fields out; a selector would leave a thunk for each.
    unready (result, (sock, _)) = either (Left . (,sock)) Right result
    closed (failed, sock) = Left failed <$ Socket.close sock

-- | The session started on the carrier, which is closed if that fails. The failure says whether the server had taken TLS.
started :: (Runtime arr) => ConnectSettings -> Use arr Connection
started settings =
  startupInput ^>> first (startup settings) >>> now (\(info, carrier) -> either (\reason -> Left (NotMade (isSecure carrier) reason) <$ closeCarrier carrier) (fmap Right . made carrier) info)
  where
    startupInput (carrier, binding, buffer) = ((carrier, buffer, binding), carrier)
    made carrier info = newConnection info (hostOf settings) (CancelKey (serverBackendKey info)) carrier
    isSecure = \case Secure _ _ -> True; Plain _ -> False

-- | What an attempt that wants no TLS starts on: the socket itself, without channel binding.
onSocket :: (Arrow arr) => arr (Socket, Buffer) (Either NotMade (Carrier, Maybe (Either String ByteString), Buffer))
onSocket = arr (\(sock, buffer) -> Right (Plain sock, Nothing, buffer))

-- | What an attempt that wants TLS starts on: the server is asked for TLS, unless TLS starts directly, and if it takes it
-- the handshake is done. A server that doesn't take it fails an attempt that requires TLS, and is otherwise talked to
-- without, on the same socket.
encrypted :: (Runtime arr) => ConnectSettings -> Securing -> arr (Socket, Buffer) (Either NotMade (Carrier, Maybe (Either String ByteString), Buffer))
encrypted settings securing@(Securing _ _ required) = (alongside asking >>^ answered) >>> (arr Left ||| (onSocket ||| shaking))
  where
    asking
      | connectSslDirect settings = arr (const (Right True))
      | otherwise = requested
    answered (answer, input) = case answer of
      Left reason -> Left (NotMade False reason)
      Right True -> Right (Right input)
      Right False
        | required -> Left (NotMade False (TlsFailure "server does not support SSL, but SSL was required"))
        | otherwise -> Right (Left input)
    shaking =
      first (secured settings securing)
        >>^ \(result, buffer) -> either (Left . NotMade True) (\(carrier, binding) -> Right (carrier, binding, buffer)) result

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

-- | 'cancel' for settings that are the arrow's input, with the key after them.
cancelIn :: (Runtime arr) => arr (ConnectSettings, CancelKey) ()
cancelIn = notImplemented "cancel with its settings as input"

-- | The request on a connection of its own, with TLS or without as the settings say, which is closed when the step ends,
-- however it ends.
requesting :: forall arr. (Runtime arr) => ConnectSettings -> arr (BackendKey ByteString) (Either ConnectError ())
requesting settings = case encrypting settings of
  Just reason -> arr (const (Left reason))
  Nothing ->
    alongside (now (const (newBuffer (positiveInt @512))))
      >>> first (attempting settings (arr (\(carrier, _, buffer) -> Right (carrier, buffer))))
      >>> (\(reached, key) -> fmap (,key) reached)
      ^>> (arr Left ||| asked)
  where
    asked :: arr ((Carrier, Buffer), BackendKey ByteString) (Either ConnectError ())
    asked = scope (arr (fst . fst)) (now (\(carrier, _) -> closeCarrier carrier)) (rendered ^>> (alongside sendOutput >>> waiting ^>> orClosed >>^ Right))
    -- The carrier, the buffer that the request is rendered into and the reply would be received into, and the request.
    rendered (carrier, ((_, buffer), key)) = (carrier, buffer, serialize (CancelRequest key))
    waiting (_, (carrier, buffer, _)) = (carrier, roomOf buffer)
    -- The server closes the connection once it has handled the request, and ends the stream; a connection that breaks
    -- first is as good.
    orClosed :: arr (Carrier, Room) ()
    orClosed = tryA untilClosed >>> now (either (\failure -> maybe (throwIO failure) (\(_ :: IOException) -> pure ()) (fromException failure)) pure)
    untilClosed :: arr (Carrier, Room) ()
    untilClosed = iterateA (alongside receive >>^ \(count, link) -> if count == 0 then Finish () else Again link)
