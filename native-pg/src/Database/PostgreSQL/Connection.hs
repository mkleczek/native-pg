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
        connectResolver,
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
    prepareTls,
    Resolver,

    -- * Connections
    Connection,
    connect,
    close,
    ServerInfo (..),
    serverProcessId,
    serverInfo,
    connectedHost,
    connectionSlot,

    -- * Cancelling
    CancelKey,
    cancelKey,
    cancel,
  )
where

import Control.Arrow (Arrow, arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, fromException, throwIO)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import Data.IORef (IORef, readIORef)
import Data.List (isPrefixOf)
import Data.Maybe (fromMaybe)
import Data.Tuple (Solo (MkSolo))
import Data.Typeable (Typeable)
import Database.PostgreSQL.Connection.Internal
import Database.PostgreSQL.ConnectionString.Types (Host (..))
import Database.PostgreSQL.Errors (ConnectError (..), ConnectFailure (..), UsageError (..))
import Database.PostgreSQL.Internal.Carrier (Carrier (..), closeCarrier)
import Database.PostgreSQL.Internal.Cares (Resolver)
import Database.PostgreSQL.Internal.Encryption (NotMade (..), Securing (..), prepareTls, requested, secured, unbuilt)
import Database.PostgreSQL.Internal.Frontend (sendOutput)
import Database.PostgreSQL.Internal.Protocol.Messages (BackendKey, pattern CancelRequest, serialize)
import Database.PostgreSQL.Internal.Socket (connectSocket)
import Database.PostgreSQL.Internal.Startup (startup)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Transport (Address (..), RootCertificates, ServerVerification (..), TlsClient, TlsPrepared (..))
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.Classes (Effect (..), Exit (..), Link (receive), Room, Runtime, Scope (..), Within (..), roomOf, secs)
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, done, iterateA)
import Network.Streaming.Loans (Loans, withSetupBuffersOf)
import Network.Streaming.NotImplemented (notImplementedIO)
import Network.Streaming.PositiveInt (positiveInt)

-- | Connects to the first of the settings' hosts that qualifies: the settings' own, then the settings of its fallbacks,
-- in order. For each it connects the socket as its address says, the Unix-domain socket or the address, which a host name
-- is looked up for by the settings' resolver (also for its fallbacks), TLS as
-- @sslmode@ and @sslnegotiation@ say, sends the startup message and authenticates as the server asks, with SCRAM-SHA-256
-- (bound to the TLS link if the server offers it), MD5 or a cleartext password, and then checks that the server
-- qualifies: it has the protocol version that @min_protocol_version@ asks for, and the session attributes that
-- @target_session_attrs@ asks for, which @prefer-standby@ asks for in two passes, the hosts that are standbys first and
-- then any. Each host has its own @connect_timeout@, which is a deadline of the arrow. It borrows from the loans only
-- while it sets up a host.
--
-- The settings are the arrow's input, with the loans after them: one program, built once, connects with the settings of
-- each run, as a tenant's pool does.
--
-- It always fails with @ConnectFailure@: an entry for each host it tried, in
-- order, for one host as for many. Of the two passes of @prefer-standby@ it gives the second's.
connect :: (Runtime arr) => arr (ConnectSettings, Loans) (Either ConnectFailure Connection)
connect = (\(settings, loans) -> (passes settings, loans)) ^>> iterateA pass >>^ either (Left . ConnectFailure) Right
  where
    -- A pass over the hosts, and then the next pass if no host qualified and there is one: the loans go to it.
    pass = alongside (current ^>> hostsInOrder) >>^ concluded
    current (hostLists, loans) = (concat (take 1 hostLists), loans)
    concluded (result, (hostLists, loans)) = case (result, drop 1 hostLists) of
      (Left _, more@(_ : _)) -> Again (more, loans)
      _ -> Finish result

-- | The lists of hosts to try, one after the other: the settings' own and its fallbacks', with the session attributes that
-- they are asked for, and for @prefer-standby@ the hosts that are standbys first, and then any host.
passes :: ConnectSettings -> [[ConnectSettings]]
passes settings = case connectTargetSessionAttrs settings of
  PreferStandbySession -> [hosts StandbySession, hosts AnySession]
  target -> [hosts target]
  where
    hosts target = [host {connectTargetSessionAttrs = target, connectResolver = connectResolver settings} | host <- settings : connectFallbacks settings]

-- | What the loop over the hosts knows: the hosts that are left to try, why each of those tried didn't qualify, the latest
-- first, and the loans.
data Trying = Trying [ConnectSettings] [(Host, ConnectError)] Loans

-- | The first of the hosts that connects, or why each of them didn't, in order. The hosts are the input, so a list of any
-- length is tried by the same arrow, which a loop goes round for each host.
hostsInOrder :: (Runtime arr) => arr ([ConnectSettings], Loans) (Either [(Host, ConnectError)] Connection)
hostsInOrder = (\(hosts, loans) -> Trying hosts [] loans) ^>> iterateA next
  where
    next = untried ^>> (returnA ||| (first attempt >>^ tried))
    -- The next host with the loans, or the end of the hosts, and why none qualified.
    untried (Trying hosts failures loans) = case hosts of
      [] -> Left (Finish (Left (reverse failures)))
      host : others -> Right ((host, loans), (host, Trying others failures loans))
    tried (result, (host, Trying others failures loans)) = case result of
      Left reason -> Again (Trying others ((hostOf host, reason) : failures) loans)
      Right connection -> Finish (Right connection)

-- | One host: its session, if it is made within the host's connect timeout.
attempt :: (Runtime arr) => arr (ConnectSettings, Loans) (Either ConnectError Connection)
attempt = bounded connecting >>^ fromMaybe (Left TimedOut)

-- | The step, with the settings' @connect_timeout@ as its deadline, if they have one: nothing if the deadline passed first.
bounded :: (Runtime arr) => arr (ConnectSettings, a) b -> arr (ConnectSettings, a) (Maybe b)
bounded body = limited ^>> ((body >>^ Just) ||| timeoutA body)
  where
    limited input@(settings, _) = case connectTimeout settings of
      Nothing -> Left input
      Just seconds -> Right (secs (fromIntegral seconds), input)

-- | The session on a new socket, from the setup buffer of the loans' pool: the one buffer is what the startup renders
-- into and reads in, a message at a time.
connecting :: (Runtime arr) => arr (ConnectSettings, Loans) (Either ConnectError Connection)
connecting = servable ^>> (arr Left ||| withSetupBuffersOf (theBuffer ^>> connected))
  where
    servable (settings, loans) = maybe (Right (loans, settings)) Left (encrypting settings)
    -- A pattern takes the fields out; getSolo . fst would leave a thunk.
    theBuffer (MkSolo buffer, (_, settings)) = (settings, buffer)

-- | Why the settings can't be served without a TLS client, if they can't: as libpq built without TLS fails them. A
-- Unix-domain socket isn't encrypted, whatever the mode.
encrypting :: ConnectSettings -> Maybe ConnectError
encrypting settings = case (connectAddress settings, connectSslMode settings, connectTlsClient settings) of
  (UnixAddress _, _, _) -> Nothing
  (_, _, Just _) -> unbuilt settings
  (_, SslRequire, Nothing) -> Just (without "require")
  (_, SslVerifyFull _, Nothing) -> Just (without "verify-full")
  _ -> Nothing
  where
    without mode = TlsFailure ("sslmode value \"" <> mode <> "\" invalid when SSL support is not compiled in")

-- | A connection to the address, which the session is started on, with TLS or without as @sslmode@ says.
connected :: (Runtime arr) => arr (ConnectSettings, Buffer) (Either ConnectError Connection)
connected = attempting started

-- | What an attempt makes of the carrier that it got, with the data that SCRAM's channel binding takes and the buffer, and
-- the settings: the session started on it, or the request that is sent over it.
type Use arr a = arr (ConnectSettings, (Carrier, Maybe (Either String ByteString), Buffer)) (Either NotMade a)

-- | What the attempts of a connection are, as its settings say: the root certificates that are looked for before any is
-- made, how the first attempt is secured (plaintext if it isn't), and when a second is made, and how it is secured.
data Plan = Plan (Maybe RootCertificates) (Maybe Securing) (Maybe Retry)

-- | A second attempt: whether the first's failure makes it, and how it is secured.
data Retry = Retry (NotMade -> Bool) (Maybe Securing)

-- | The attempts, from the settings: a Unix-domain socket isn't encrypted, whatever the mode, and without a TLS client
-- @allow@ and @prefer@ connect without.
--
-- * @allow@ connects without TLS first, and with TLS if the server rejected the connection;
-- * @prefer@ connects with TLS first, and without if the server didn't take it, which is no failure, or if it did and then
--   rejected the connection or TLS failed, as libpq does: a host that doesn't qualify, and a failure of the client's own,
--   are reported as they are;
-- * @require@ and @verify-full@ connect with TLS or fail, and @verify-full@ looks for the root certificate file first.
planOf :: ConnectSettings -> Plan
planOf settings = case (connectSslMode settings, connectAddress settings, connectTlsClient settings) of
  (SslAllow, TcpAddress {}, Just client) -> Plan Nothing Nothing (Just (Retry rejected (secure client NoVerification True)))
  (SslPrefer, TcpAddress {}, Just client) -> Plan Nothing (secure client NoVerification False) (Just (Retry afterTls Nothing))
  (SslRequire, TcpAddress {}, Just client) -> Plan Nothing (secure client NoVerification True) Nothing
  (SslVerifyFull roots, TcpAddress {}, Just client) -> Plan (Just roots) (secure client (VerifyFull roots) True) Nothing
  _ -> Plan Nothing Nothing Nothing
  where
    secure client verification required = Just (Securing client verification required)
    rejected (NotMade _ failure) = case failure of ConnectionRejected _ -> True; _ -> False
    afterTls (NotMade accepted failure) = accepted && case failure of ConnectionRejected _ -> True; TlsFailure _ -> True; _ -> False

-- | The use, made of a connection to the address, with TLS or without as the settings' plan says. Each attempt is on a new
-- socket and on the same buffer, one after the other.
attempting :: (Runtime arr) => Use arr a -> arr (ConnectSettings, Buffer) (Either ConnectError a)
attempting use = planned ^>> rootsChecked >>> (arr Left ||| tries) >>^ either (\(NotMade _ reason) -> Left reason) Right
  where
    planned (settings, buffer) = (settings, planOf settings, buffer)
    tries = (alongside firstTry >>^ chosen) >>> (oneTry use ||| returnA)
    firstTry = (\(settings, Plan _ securing _, buffer) -> (settings, (securing, buffer))) ^>> oneTry use
    -- The second attempt, on the same buffer, if the first failed as the plan says to try again for.
    chosen (result, (settings, Plan _ _ retry, buffer)) = case (result, retry) of
      (Left failed, Just (Retry again securing)) | again failed -> Left (settings, (securing, buffer))
      _ -> Right result

-- | The input, or the failure of the root certificate file's not being there, which libpq finds out before it connects,
-- and which 'prepareTls' found out when the settings were made.
rootsChecked :: (Arrow arr) => arr (ConnectSettings, Plan, Buffer) (Either NotMade (ConnectSettings, Plan, Buffer))
rootsChecked = arr (\input@(settings, Plan roots _ _, _) -> maybe (Right input) (const (maybe (Right input) (Left . NotMade False . TlsFailure) (connectTlsPrepared settings >>= preparedRootsProblem))) roots)

-- | One attempt: a new socket, TLS over it as the securing says, if there is one, and the use made of it.
oneTry :: (Runtime arr) => Use arr a -> arr (ConnectSettings, (Maybe Securing, Buffer)) (Either NotMade a)
oneTry use =
  reaching ^>> first connectSocket >>> withInput ^>> (arr (Left . NotMade False) ||| session)
  where
    reaching (settings, (securing, buffer)) = ((connectResolver settings, (connectTcpOptions settings, connectAddress settings)), (settings, securing, buffer))
    withInput (reached, input) = fmap (,input) reached
    -- The socket is closed unless the use was made: also when the part is cancelled while it is.
    session = scope (arr fst) (now released) (snd ^>> alongside starting >>> (unready ^>> (now closed ||| use)))
    released (sock, exit) = case exit of Done -> pure (); _ -> Socket.close sock
    starting = chosen ^>> (onSocket ||| encrypted)
    chosen (sock, (settings, securing, buffer)) = case securing of
      Nothing -> Left (sock, buffer)
      Just secure -> Right ((settings, secure), (sock, buffer))
    -- A pattern takes the fields out; a selector would leave a thunk for each.
    unready (result, (sock, (settings, _, _))) = either (Left . (,sock)) (Right . (settings,)) result
    closed (failed, sock) = Left failed <$ Socket.close sock

-- | The session started on the carrier, which is closed if that fails. The failure says whether the server had taken TLS.
started :: (Runtime arr) => Use arr Connection
started =
  startupInput ^>> first startup >>> now (\(info, (settings, carrier)) -> either (\reason -> Left (NotMade (isSecure carrier) reason) <$ closeCarrier carrier) (fmap Right . made settings carrier) info)
  where
    startupInput (settings, (carrier, binding, buffer)) = ((settings, (carrier, buffer, binding)), (settings, carrier))
    made settings carrier info = newConnection info (hostOf settings) (CancelKey (serverBackendKey info)) carrier
    isSecure = \case Secure _ _ -> True; Plain _ -> False

-- | What an attempt that wants no TLS starts on: the socket itself, without channel binding.
onSocket :: (Arrow arr) => arr (Socket, Buffer) (Either NotMade (Carrier, Maybe (Either String ByteString), Buffer))
onSocket = arr (\(sock, buffer) -> Right (Plain sock, Nothing, buffer))

-- | What an attempt that wants TLS starts on: the server is asked for TLS, unless TLS starts directly, and if it takes it
-- the handshake is done. A server that doesn't take it fails an attempt that requires TLS, and is otherwise talked to
-- without, on the same socket.
encrypted :: (Runtime arr) => arr ((ConnectSettings, Securing), (Socket, Buffer)) (Either NotMade (Carrier, Maybe (Either String ByteString), Buffer))
encrypted = (alongside asking >>^ answered) >>> (arr Left ||| (onSocket ||| shaking))
  where
    asking = directly ^>> (arr (const (Right True)) ||| (snd ^>> requested))
    directly input@((settings, _), _) = if connectSslDirect settings then Left () else Right input
    answered (answer, input@((_, Securing _ _ required), rest)) = case answer of
      Left reason -> Left (NotMade False reason)
      Right True -> Right (Right input)
      Right False
        | required -> Left (NotMade False (TlsFailure "server does not support SSL, but SSL was required"))
        | otherwise -> Right (Left rest)
    shaking =
      (\((settings, securing), (sock, buffer)) -> (((settings, securing), sock), buffer))
        ^>> first secured
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

-- | The cell that the connection keeps for a program, of the type of its value, which it starts with the value given if the
-- connection has none of that type. It lives as long as the connection does, through every use of it by a pool, and is gone with
-- it: a connection that replaces another has none.
connectionSlot :: (Typeable a) => Connection -> a -> IO (IORef a)
connectionSlot _ _ = notImplementedIO "connectionSlot"

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
-- connection of its own, made as the settings say, TLS included. The settings are
-- the arrow's input, with the key after them, as 'connect''s are. It makes one
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
cancel :: (Runtime arr) => arr (ConnectSettings, CancelKey) ()
cancel =
  (\(settings, CancelKey key) -> maybe (Left ()) (\given -> Right (settings, given)) key)
    ^>> (done ||| (bounded requesting >>> now (maybe (throwIO TimedOut) (either throwIO pure))))

-- | The request on a connection of its own, with TLS or without as the settings say, which is closed when the step ends,
-- however it ends.
requesting :: forall arr. (Runtime arr) => arr (ConnectSettings, BackendKey ByteString) (Either ConnectError ())
requesting = servable ^>> (arr Left ||| made)
  where
    servable input@(settings, _) = maybe (Right input) Left (encrypting settings)
    made =
      alongside (now (const (newBuffer (positiveInt @512))))
        >>> (\(buffer, (settings, key)) -> ((settings, buffer), key))
        ^>> first (attempting (arr (\(_, (carrier, _, buffer)) -> Right (carrier, buffer))))
        >>> (\(reached, key) -> fmap (,key) reached)
        ^>> (arr Left ||| asked)
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
