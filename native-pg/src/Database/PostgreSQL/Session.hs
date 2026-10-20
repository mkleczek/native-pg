{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | The protocol state machine. It is a client of the parser: in each phase it
-- pulls the next message, choosing among the messages that phase accepts.
module Database.PostgreSQL.Session
  ( -- * Phases
    Accepts (..),
    Phase (..),
    Session,

    -- * Connecting
    ConnectSettings (..),
    ConnectionError (..),
    SslMode (..),
    ChannelBindingMode (..),
    ServerError (..),
    ServerInfo (..),
    close,
    connect,
    connectSettings,
    isUsable,

    -- * Statements
    Execution (..),
    Statement (..),
    closeStatement,
    execute,
    prepare,

    -- * Batches
    Batch,
    batched,
    runBatch,

    -- * Cancelling and notifications
    awaitNotification,
    cancel,

    -- * Rows
    Column (..),
    DecodeRow (RowFunction, mapRowFunction),
    Row,
    columnBytes,
    columnStream,
    decodeRow,
    decodeRowStreaming,
    decodeRowStreamingNullable,
    nextColumn,
    rowColumnCount,
  )
where

import Control.Exception (Exception (..), IOException, bracketOnError, catch, finally, mask, onException, throwIO, try)
import Control.Monad (filterM, join, unless, when, (<=<))
import Data.Bifunctor (first)
import Data.Foldable (for_, toList)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Char (isDigit)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int32)
import Data.Kind (Type)
import Data.List (sort)
import Data.Maybe (fromMaybe, isJust)
#if defined(mingw32_HOST_OS)
import System.Directory (doesDirectoryExist, doesFileExist, getPermissions, listDirectory)
#else
import System.Directory (doesDirectoryExist, doesFileExist, listDirectory)
import qualified System.Posix.Files as Posix
import qualified System.Posix.User as Posix
#endif
import Database.PostgreSQL.Decoding (DecodeField (..), DecodingError (..))
import Database.PostgreSQL.Diagnostics (ServerError (..), diagnostics, isConnectionFatal)
import Database.PostgreSQL.Authentication
  ( ChannelBinding (..),
    md5Password,
    newScramNonce,
    scramClientFinal,
    scramClientFirst,
    scramMechanism,
    scramPlusMechanism,
    scramVerifyServerFinal,
    serverEndPoint,
  )
import Database.PostgreSQL.Protocol.Codec
  ( Alternatives (..),
    ByteStream (..),
    Input,
    InputBuffer,
    InputError (..),
    InputSource (..),
    MonadInput (..),
    Output,
    ParseConstructor,
    Parsable (..),
    Serializable (..),
    emptyInputBuffer,
    inputPosition,
    parseChoice,
    readInputSome,
    runInputUntil,
    skipInput,
  )
import Database.PostgreSQL.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Protocol.Messages
import Database.PostgreSQL.Transport
  ( Address (..),
    Channel (..),
    OutputBuffer,
    RootCertificates (..),
    ServerVerification (..),
    TcpOptions,
    TlsClient (..),
    TlsError,
    TlsSettings (..),
    TlsVersion,
    Unsent,
    connectSocket,
    defaultTcpOptions,
    newChannel,
    newOutputBuffer,
    unsent,
  )
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Storable (peek)
import Network.Socket (HostName, Socket)
import qualified Network.Socket as Socket

-- | The phases of a connection, each named after what the client waits for in
-- it.
data Phase
  = -- | The answer to the startup message: an authentication request, or
    -- AuthenticationOk when none is needed.
    Authentication
  | -- | The server's SCRAM challenge, after the client's first SASL message.
    SaslContinue
  | -- | The server's SCRAM signature, after the client's proof.
    SaslFinal
  | -- | AuthenticationOk, after the client's password.
    AuthenticationResult
  | -- | The server's parameters and backend key, until it's ready for
    -- queries.
    Initialization
  | -- | Ready for a query.
    Ready
  | -- | The result of a Parse.
    ParseResult
  | -- | The result of a Bind.
    BindResult
  | -- | The result of a Close.
    CloseResult
  | -- | The parameters' types, which a Describe of a statement answers
    -- first.
    DescribeResult
  | -- | The columns of the statement's rows, or NoData, which follow them.
    RowDescriptionResult
  | -- | The rows of an Execute, until the command completes.
    Rows
  | -- | The ReadyForQuery that answers a Sync.
    SyncResult

-- | The server messages a phase accepts. Handlers for a phase's messages are
-- given in the same order.
class Accepts (phase :: Phase) where
  type Accepted phase :: [Payload]

instance Accepts 'Authentication where
  type
    Accepted 'Authentication =
      '[ 'PayloadField 'AuthenticationOkType,
         'PayloadField 'AuthenticationCleartextPasswordType,
         'PayloadField 'AuthenticationMD5PasswordType,
         'PayloadField 'AuthenticationSASLType,
         'PayloadField 'AuthenticationKerberosV5Type,
         'PayloadField 'AuthenticationGSSType,
         'PayloadField 'AuthenticationSSPIType,
         'PayloadField 'NegotiateProtocolVersionType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'SaslContinue where
  type
    Accepted 'SaslContinue =
      '[ 'PayloadField 'AuthenticationSASLContinueType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'SaslFinal where
  type
    Accepted 'SaslFinal =
      '[ 'PayloadField 'AuthenticationSASLFinalType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'AuthenticationResult where
  type
    Accepted 'AuthenticationResult =
      '[ 'PayloadField 'AuthenticationOkType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'Initialization where
  type
    Accepted 'Initialization =
      '[ 'PayloadField 'ParameterStatusType,
         'PayloadField 'BackendKeyDataType,
         'PayloadField 'ReadyForQueryType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

-- | Between queries the server only sends messages of its own accord.
instance Accepts 'Ready where
  type
    Accepted 'Ready =
      '[ 'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'ParseResult where
  type
    Accepted 'ParseResult =
      '[ 'PayloadField 'ParseCompleteType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'BindResult where
  type
    Accepted 'BindResult =
      '[ 'PayloadField 'BindCompleteType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'CloseResult where
  type
    Accepted 'CloseResult =
      '[ 'PayloadField 'CloseCompleteType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'DescribeResult where
  type
    Accepted 'DescribeResult =
      '[ 'PayloadField 'ParameterDescriptionType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'RowDescriptionResult where
  type
    Accepted 'RowDescriptionResult =
      '[ 'PayloadField 'RowDescriptionType,
         'PayloadField 'NoDataType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

-- | Rows are read only up to their headers; their columns are pulled after.
instance Accepts 'Rows where
  type
    Accepted 'Rows =
      '[ 'PayloadField 'DataRowHeaderType,
         'PayloadField 'CommandCompleteType,
         'PayloadField 'EmptyQueryResponseType,
         'PayloadField 'PortalSuspendedType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

-- | An error here comes from the Sync itself, e.g. a deferred constraint
-- failing as the implicit transaction commits.
instance Accepts 'SyncResult where
  type
    Accepted 'SyncResult =
      '[ 'PayloadField 'ReadyForQueryType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

data Connection = Connection
  { -- | Input and output over the socket, or TLS over it.
    connectionChannel :: Channel,
    -- | Bytes read but not consumed yet; the only buffer the library keeps.
    connectionBuffer :: IORef InputBuffer,
    -- | Output of the running batch that the socket hasn't taken yet. Reads
    -- send it as they wait.
    connectionUnsent :: IORef Unsent,
    -- | Where batches are written. The next batch reuses it, since a batch
    -- has been sent whole by the time its results are read.
    connectionOutput :: OutputBuffer,
    connectionState :: IORef SessionState,
    -- | Notifications that arrived during operations, newest first, until
    -- 'awaitNotification' takes them.
    connectionNotifications :: IORef [Notification ByteString]
  }

-- | Session handles can be copied, so the connection tracks at run time
-- whether an operation is in progress, or the session is no longer usable.
data SessionState
  = Idle
  | Busy
  | -- | An operation failed with an exception, leaving the protocol state
    -- unknown.
    Broken
  | Closed

-- | A connection in a known phase.
newtype Session (phase :: Phase) = Session Connection

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
    -- | What starts TLS: native-pg-tls's, which this library doesn't depend
    -- on. Without it, a connection that requires TLS fails.
    connectTlsClient :: Maybe TlsClient,
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
    connectChannelBinding :: ChannelBindingMode
  }

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
      connectTlsClient = Nothing,
      connectSslCertificate = Nothing,
      connectSslRevocationList = Nothing,
      connectSslRevocationListDirectory = Nothing,
      connectSslVersions = [minBound .. maxBound],
      connectSslServerNameIndication = True,
      connectSslDirect = False,
      connectChannelBinding = ChannelBindingPrefer
    }

data ServerInfo = ServerInfo
  { serverProtocolVersion :: ProtocolVersion,
    -- | The parameters the server reported, e.g. @server_version@, in order.
    serverParameters :: [(ByteString, ByteString)],
    -- | Needed to cancel a query. Some connection poolers don't send it.
    serverBackendKey :: Maybe (BackendKey ByteString)
  }
  deriving (Eq, Show)

data ConnectionError
  = -- | The server refused the connection.
    ConnectionRejected ServerError
  | -- | The server ended the session with this error, e.g. because an
    -- administrator terminated it.
    ConnectionTerminated ServerError
  | -- | The server's messages don't follow the protocol.
    ProtocolError String
  | -- | The server asked for an authentication method this client doesn't
    -- implement.
    UnsupportedAuthentication String
  | -- | Authentication failed on the client's side: a password is needed but
    -- missing, or the server's SCRAM signature doesn't match.
    AuthenticationError String
  | -- | The session is busy with another operation, broke in an earlier
    -- one, or has been closed.
    SessionUnusable String
  | -- | A row or a column stream was used out of turn.
    RowMisuse String
  | -- | TLS couldn't be started, e.g. because the server doesn't take it.
    TlsFailure String
  deriving (Eq, Show)

instance Exception ConnectionError where
  displayException failure = case failure of
    ConnectionRejected rejection -> displayException rejection
    ConnectionTerminated termination -> displayException termination
    ProtocolError reason -> "protocol error: " <> reason
    UnsupportedAuthentication method -> "the server asks for authentication with " <> method <> ", which isn't supported"
    AuthenticationError reason -> reason
    SessionUnusable reason -> reason
    RowMisuse reason -> reason
    TlsFailure reason -> reason

-- | The size of the buffer a connection writes its batches to. A batch that
-- copies more bytes gets a buffer of its own.
outputBufferSize :: Int
outputBufferSize = 16384

-- | Connects, authenticates and waits until the server is ready for a query.
-- Throws 'Database.PostgreSQL.Transport.ConnectError' when the socket doesn't
-- connect, and 'ConnectionError' when the server rejects the connection.
connect :: ConnectSettings -> IO (Session 'Ready, ServerInfo)
connect settings = withChannel settings start
  where
    -- Given how to make the channel, once there's somewhere for its unsent
    -- output.
    start channel = do
      buffer <- newIORef emptyInputBuffer
      pending <- newIORef =<< unsent Nothing mempty
      output <- newOutputBuffer outputBufferSize
      state <- newIORef Idle
      notifications <- newIORef []
      let session = Session (Connection (channel pending (connectReadSize settings)) buffer pending output state notifications)
      send @'StartupMessageType session (startupMessage settings)
      (version, initializing) <- authenticate settings (connectProtocolVersion settings) session
      initialize version initializing

-- | Connects a socket to the server, starts TLS over it as the settings ask,
-- and runs the action with how to make the channel over it. The socket is
-- closed if the action fails.
withChannel :: ConnectSettings -> ((IORef Unsent -> Int -> Channel) -> IO a) -> IO a
withChannel settings use = case (connectSslMode settings, connectAddress settings, connectTlsClient settings) of
  (SslDisable, _, _) -> attempt Nothing
  (_, UnixAddress _, _) -> attempt Nothing
  -- As libpq without TLS does.
  (SslAllow, _, Nothing) -> attempt Nothing
  (SslPrefer, _, Nothing) -> attempt Nothing
  (mode, _, Nothing) -> throwIO (TlsFailure ("sslmode value \"" <> sslModeName mode <> "\" invalid when SSL support is not compiled in"))
  (SslAllow, TcpAddress {}, Just client) ->
    attempt Nothing `catch` \failure -> case failure of
      ConnectionRejected _ -> attempt (Just (client, NoVerification, True))
      _ -> throwIO failure
  (SslPrefer, TcpAddress {}, Just client) ->
    attempt (Just (client, NoVerification, False)) `catch` \(FailedWithTls _) -> attempt Nothing
  (SslRequire, TcpAddress {}, Just client) -> attempt (Just (client, NoVerification, True))
  (SslVerifyFull roots, TcpAddress {}, Just client) -> do
    case roots of
      RootCertificateFile file -> do
        exists <- doesFileExist file
        unless exists . throwIO . TlsFailure $
          "root certificate file \""
            <> file
            <> "\" does not exist\nEither provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode to disable server certificate verification."
      SystemRootCertificates -> pure ()
    attempt (Just (client, VerifyFull roots, True))
  where
    sslModeName mode = case mode of
      SslDisable -> "disable"
      SslAllow -> "allow"
      SslPrefer -> "prefer"
      SslRequire -> "require"
      SslVerifyFull _ -> "verify-full"
    -- Over a new socket, with TLS if a client, the verification, and whether
    -- it's required are given. A failure after TLS started, when it isn't
    -- required, is thrown as 'FailedWithTls'.
    attempt tls =
      bracketOnError (connectSocket (connectTcpOptions settings) (connectAddress settings)) Socket.close $ \sock ->
        case (tls, connectAddress settings) of
          (Just (client, verification, required), TcpAddress host _) -> do
            accepted <- if connectSslDirect settings then pure True else requestTls sock
            case accepted of
              False
                | required -> throwIO (TlsFailure "server does not support SSL, but SSL was required")
                | otherwise -> use (newChannel sock)
              True -> do
                let started = do
                      certificate <- clientCertificate
                      lists <- revocationLists
                      use
                        =<< startTlsChannel
                          client
                          ( TlsSettings
                              verification
                              certificate
                              lists
                              (connectSslVersions settings)
                              (connectSslServerNameIndication settings)
                              (connectSslDirect settings)
                          )
                          (fromMaybe host (connectServerName settings))
                          sock
                          `catch` \(failure :: TlsError) -> throwIO (TlsFailure (displayException failure))
                if required then started else started `catch` (throwIO . FailedWithTls)
          _ -> use (newChannel sock)
    -- The client certificate, if its file exists, with its private key,
    -- which has to exist and be readable only by its owner, as with libpq.
    clientCertificate = case connectSslCertificate settings of
      Nothing -> pure Nothing
      Just (chain, key) -> do
        present <- doesFileExist chain
        if not present
          then pure Nothing
          else do
            usable <- try (privateKeyFile key)
            case usable of
              Left (_ :: IOException) -> throwIO (TlsFailure ("certificate present, but not private key file \"" <> key <> "\""))
              Right (Just problem) -> throwIO (TlsFailure ("private key file \"" <> key <> "\" " <> problem))
              Right Nothing -> pure (Just (chain, key))
    revocationLists = do
      file <- filterM doesFileExist (toList (connectSslRevocationList settings))
      directory <- case connectSslRevocationListDirectory settings of
        Nothing -> pure []
        Just directory -> do
          exists <- doesDirectoryExist directory
          if exists
            then map ((directory <> "/") <>) . sort . filter rehashed <$> listDirectory directory
            else pure []
      pure (file <> directory)
    -- c_rehash's names of revocation lists: their issuer's hash, .r and a
    -- number.
    rehashed name = case break (== '.') name of
      (_, '.' : 'r' : number) -> not (null number) && all isDigit number
      _ -> False

-- | What's wrong with a private key file, as libpq checks it, if anything.
privateKeyFile :: FilePath -> IO (Maybe String)
#if defined(mingw32_HOST_OS)
privateKeyFile key = Nothing <$ getPermissions key
#else
privateKeyFile key = do
  status <- Posix.getFileStatus key
  user <- Posix.getEffectiveUserID
  let mode = Posix.fileMode status
      open bits = Posix.intersectFileModes mode bits /= Posix.nullFileMode
      groupOrWorld = foldr Posix.unionFileModes Posix.nullFileMode [Posix.groupModes, Posix.otherModes]
      groupWriteOrWorld = foldr Posix.unionFileModes Posix.nullFileMode [Posix.groupWriteMode, Posix.groupExecuteMode, Posix.otherModes]
  pure $
    if
      | not (Posix.isRegularFile status) -> Just "is not a regular file"
      | Posix.fileOwner status == user && open groupOrWorld -> Just tooOpen
      | Posix.fileOwner status == 0 && open groupWriteOrWorld -> Just tooOpen
      | otherwise -> Nothing
  where
    tooOpen = "has group or world access; file must have permissions u=rw (0600) or less if owned by the current user, or permissions u=rw,g=r (0640) or less if owned by root"
#endif

-- | A connection with TLS that wasn't required failed, so it's tried without.
newtype FailedWithTls = FailedWithTls ConnectionError
  deriving (Show)

instance Exception FailedWithTls

-- | Asks the server for TLS, and reads its answer, a single byte, so that no
-- unencrypted byte after it is taken for TLS's: whether it takes TLS.
requestTls :: Socket -> IO Bool
requestTls sock = do
  pending <- newIORef =<< unsent Nothing mempty
  let channel = newChannel sock pending 1
  channelSend channel (serialize @SSLRequest @'SSLRequestType SSLRequest)
  answer <- allocaBytes 1 $ \byte -> do
    received <- readInputInto (channelInput channel) byte 1
    if received == 0 then pure Nothing else Just <$> peek byte
  case answer of
    Just 83 -> pure True
    Just 78 -> pure False
    -- The server isn't authenticated yet, so its error isn't read.
    Just 69 -> throwIO (TlsFailure "server sent an error response during SSL exchange")
    Just other -> throwIO (TlsFailure ("received invalid response to SSL negotiation: " <> [toEnum (fromIntegral other)]))
    Nothing -> throwIO (TlsFailure "server closed the connection unexpectedly")

-- | Ends an idle or broken session and closes its socket. The session is
-- unusable from then on, even if closing throws. Further calls do nothing.
-- Throws 'SessionUnusable' if an operation is still running; interrupt it
-- and wait for it to finish before closing.
close :: Session phase -> IO ()
close session@(Session connection) = mask $ \restore -> do
  state <- atomicModifyIORef' (connectionState connection) $ \case
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
            Idle -> send @'TerminateType session Terminate `catch` \(_ :: IOException) -> pure ()
            _ -> pure ()
        )
        `finally` channelClose (connectionChannel connection)

-- | Whether the session can run an operation: it hasn't been closed, none
-- is running, and none broke it. An operation that an exception interrupts
-- leaves the session broken, e.g. when its thread is killed.
isUsable :: Session phase -> IO Bool
isUsable (Session connection) = do
  state <- readIORef (connectionState connection)
  pure $ case state of
    Idle -> True
    _ -> False

startupMessage :: ConnectSettings -> StartupMessage ByteString
startupMessage settings =
  StartupMessage (connectProtocolVersion settings) $
    map Scalar $
      ("user", connectUser settings)
        : maybe [] (\database -> [("database", database)]) (connectDatabase settings)
        <> connectParameters settings

data AuthenticationRequest
  = Authenticated
  | CleartextPasswordRequested
  | Md5PasswordRequested ByteString
  | SaslRequested [ByteString]
  | UnsupportedMethodRequested String
  | ProtocolVersionNegotiated ProtocolVersion

authenticate :: ConnectSettings -> ProtocolVersion -> Session 'Authentication -> IO (ProtocolVersion, Session 'Initialization)
authenticate settings version session = do
  request <-
    pull session $
      (\AuthenticationOk -> Reply Authenticated)
        :|: (\AuthenticationCleartextPassword -> Reply CleartextPasswordRequested)
        :|: (\(AuthenticationMD5Password salt) -> Reply (Md5PasswordRequested salt))
        :|: (\(AuthenticationSASL mechanisms) -> Reply (SaslRequested mechanisms))
        :|: (\AuthenticationKerberosV5 -> Reply (UnsupportedMethodRequested "Kerberos V5"))
        :|: (\AuthenticationGSS -> Reply (UnsupportedMethodRequested "GSSAPI"))
        :|: (\AuthenticationSSPI -> Reply (UnsupportedMethodRequested "SSPI"))
        :|: (\(NegotiateProtocolVersion negotiated (_ :: [ByteString])) -> Reply (ProtocolVersionNegotiated negotiated))
        :|: startupTail
  case request of
    Authenticated
      | required -> throwIO (AuthenticationError "channel binding required, but server authenticated client without channel binding")
      | otherwise -> pure (version, continueIn session)
    ProtocolVersionNegotiated negotiated -> authenticate settings negotiated session
    SaslRequested mechanisms
      | Just certificate <- channelServerCertificate channel,
        connectChannelBinding settings /= ChannelBindingDisable,
        scramPlusMechanism `elem` mechanisms -> do
          password <- requirePassword
          endPoint <- either (throwIO . AuthenticationError) pure (serverEndPoint certificate)
          (version,) <$> authenticateScram scramPlusMechanism (BoundToServerEndPoint endPoint) password session
      | required ->
          throwIO (AuthenticationError "channel binding is required, but server did not offer an authentication method that supports channel binding")
      | scramMechanism `elem` mechanisms -> do
          password <- requirePassword
          let binding
                | isJust (channelServerCertificate channel) && connectChannelBinding settings /= ChannelBindingDisable = ChannelBindingUnused
                | otherwise = WithoutChannelBinding
          (version,) <$> authenticateScram scramMechanism binding password session
      | otherwise -> throwIO (UnsupportedAuthentication ("SASL with " <> show mechanisms))
    _ | required -> throwIO (AuthenticationError "channel binding required but not supported by server's authentication request")
    CleartextPasswordRequested -> do
      password <- requirePassword
      send @'PasswordMessageType session (PasswordMessage password)
      (version,) <$> awaitAuthenticationOk (continueIn session)
    Md5PasswordRequested salt -> do
      password <- requirePassword
      send @'PasswordMessageType session (PasswordMessage (md5Password (connectUser settings) password salt))
      (version,) <$> awaitAuthenticationOk (continueIn session)
    UnsupportedMethodRequested method -> throwIO (UnsupportedAuthentication method)
  where
    Session connection = session
    channel = connectionChannel connection
    required = connectChannelBinding settings == ChannelBindingRequire
    requirePassword =
      maybe (throwIO (AuthenticationError "the server asks for a password, but none is set")) pure (connectPassword settings)

-- | With the mechanism, and its channel binding.
authenticateScram :: ByteString -> ChannelBinding -> ByteString -> Session 'Authentication -> IO (Session 'Initialization)
authenticateScram mechanism binding password session = do
  (clientFirst, client) <- scramClientFirst binding "" <$> newScramNonce
  send @'SASLInitialResponseType session (SASLInitialResponse mechanism (Just clientFirst))
  let continuing = continueIn session :: Session 'SaslContinue
  serverFirst <-
    pull continuing $
      (\(AuthenticationSASLContinue challenge) -> Reply challenge) :|: startupTail
  (clientFinal, serverSignature) <- either (throwIO . AuthenticationError) pure (scramClientFinal password client serverFirst)
  send @'SASLResponseType continuing (SASLResponse clientFinal)
  let finishing = continueIn continuing :: Session 'SaslFinal
  serverFinal <-
    pull finishing $
      (\(AuthenticationSASLFinal outcome) -> Reply outcome) :|: startupTail
  either (throwIO . AuthenticationError) pure (scramVerifyServerFinal serverSignature serverFinal)
  awaitAuthenticationOk (continueIn finishing)

awaitAuthenticationOk :: Session 'AuthenticationResult -> IO (Session 'Initialization)
awaitAuthenticationOk session = do
  pull session $
    (\AuthenticationOk -> Reply ()) :|: startupTail
  pure (continueIn session)

data InitializationMessage
  = ParameterReported ByteString ByteString
  | BackendKeyReported (BackendKey ByteString)
  | Initialized

-- | Values kept for the connection's lifetime are copied, so they don't keep
-- read buffers alive.
initialize :: ProtocolVersion -> Session 'Initialization -> IO (Session 'Ready, ServerInfo)
initialize version session = go [] Nothing
  where
    go parameters backendKey = do
      message <-
        pull session $
          (\(ParameterStatus (Scalar (name, value))) -> Reply (ParameterReported (BS.copy name) (BS.copy value)))
            :|: (\(BackendKeyData (Scalar (processId, Scalar secret))) -> Reply (BackendKeyReported (Scalar (processId, Scalar (BS.copy secret)))))
            :|: (\(ReadyForQuery _) -> Reply Initialized)
            :|: startupTail
      case message of
        ParameterReported name value -> go ((name, value) : parameters) backendKey
        BackendKeyReported key -> go parameters (Just key)
        Initialized -> pure (continueIn session, ServerInfo version (reverse parameters) backendKey)

-- | A statement to run.
data Statement
  = -- | SQL parsed for this execution only, as the unnamed statement, with its
    -- parameters' types. 'Nothing' lets the server infer a type.
    Unprepared ByteString [OptionalOid]
  | -- | A statement prepared earlier under this name.
    Prepared ByteString
  | -- | SQL parsed and prepared under the name, with its parameters' types,
    -- and run. The action runs once the server has parsed it, e.g. to
    -- remember that the statement is prepared, even if running it fails.
    Preparing ByteString ByteString [OptionalOid] (IO ())

data Execution = Execution
  { executionStatement :: Statement,
    -- | No formats means text for all parameters, one means that format for
    -- all of them, and more give each parameter its own.
    executionParameterFormats :: [FormatCode],
    executionParameters :: [Maybe Parameter],
    -- | Formats for the result columns, in the same way.
    executionResultFormats :: [FormatCode]
  }

-- | Prepares a statement under a name, with its parameters' types. 'Nothing'
-- lets the server infer a type.
prepare :: Session 'Ready -> ByteString -> ByteString -> [OptionalOid] -> IO (Either ServerError (), TransactionStatus)
prepare session name query parameterTypes = operation session $ do
  sendOutputs session (serialize @(Parse ByteString) @'ParseType (Parse name query parameterTypes) <> sync)
  parsed <- pullEither (continueIn session :: Session 'ParseResult) ((\ParseComplete -> Reply ()) :|: sessionTail)
  awaitReady session id parsed

-- | Closes a named statement. Closing one that doesn't exist isn't an
-- error.
closeStatement :: Session 'Ready -> ByteString -> IO (Either ServerError (), TransactionStatus)
closeStatement session name = operation session $ do
  sendOutputs session (serialize @(Close ByteString) @'CloseType (Close PreparedStatement name) <> sync)
  closed <- pullEither (continueIn session :: Session 'CloseResult) ((\CloseComplete -> Reply ()) :|: sessionTail)
  awaitReady session id closed

-- | Runs a statement and folds its rows; the step pulls each row's columns.
-- Returns the fold's result and the command tag, e.g. @SELECT 3@, or the
-- server's error, with the transaction status after the statement.
--
-- If the step throws, the session is broken.
execute :: Session 'Ready -> Execution -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString), TransactionStatus)
execute session execution step initial = runBatch session id (batched id execution step initial)

-- | Statements sent together, before one Sync, whose results are read in
-- order. After a statement fails on the server, the server skips the
-- statements after it, so their results aren't read. A server error is
-- given as an @e@.
data Batch e a = Batch Output (Session 'Ready -> IO (Either e a))

instance Functor (Batch e) where
  fmap f (Batch output reading) = Batch output (fmap (fmap f) . reading)

instance Applicative (Batch e) where
  pure value = Batch mempty (\_ -> pure (Right value))
  Batch left readLeft <*> Batch right readRight =
    Batch (left <> right) $ \session ->
      readLeft session >>= either (pure . Left) (\f -> fmap f <$> readRight session)

-- | A statement of a batch, whose rows are folded as 'execute' folds them,
-- and whose server error is given by the function.
batched :: (ServerError -> e) -> Execution -> (acc -> Row -> IO acc) -> acc -> Batch e (acc, ByteString)
batched failed execution step initial =
  Batch (executionMessages execution) (\session -> first failed <$> readExecution session execution step initial)

-- | Sends the batch's statements and a Sync, and reads their results, with
-- the transaction status after them. An error the server reports after the
-- statements replaces the result, given by the function.
--
-- If a step throws, the session is broken.
runBatch :: Session 'Ready -> (ServerError -> e) -> Batch e a -> IO (Either e a, TransactionStatus)
runBatch session@(Session connection) failed (Batch output reading) = operation session $ do
  -- The server may answer the first statements before it has read the last,
  -- and stop reading until its answers are read. So what the socket doesn't
  -- take at once is sent as the answers are waited for.
  writeIORef (connectionUnsent connection) =<< channelSendSome (connectionChannel connection) =<< unsent (Just (connectionOutput connection)) (output <> sync)
  result <- reading session
  awaitReady session failed result

-- | Reads the results of a statement that has been sent.
readExecution :: Session 'Ready -> Execution -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString))
readExecution session execution step initial = do
  parsed <- case executionStatement execution of
    Unprepared _ _ -> parseComplete
    Prepared _
      | streamsParameter execution -> described
      | otherwise -> pure (Right ())
    Preparing _ _ _ onParsed -> do
      parsed <- parseComplete
      for_ parsed (const onParsed)
      pure parsed
  case parsed of
    Left failure -> pure (Left failure)
    Right () -> do
      bound <- pullEither (continueIn session :: Session 'BindResult) ((\BindComplete -> Reply ()) :|: sessionTail)
      case bound of
        Left failure -> pure (Left failure)
        Right () -> readRows (continueIn session) step initial
  where
    parseComplete = pullEither (continueIn session :: Session 'ParseResult) ((\ParseComplete -> Reply ()) :|: sessionTail)
    described = do
      parameters <- pullEither (continueIn session :: Session 'DescribeResult) ((\(ParameterDescription _) -> Reply ()) :|: sessionTail)
      case parameters of
        Left failure -> pure (Left failure)
        Right () -> pullEither (continueIn session :: Session 'RowDescriptionResult) ((\(_ :: RowDescription ByteString) -> Reply ()) :|: (\NoData -> Reply ()) :|: sessionTail)

-- | Waits for a notification on a channel the session listens on (with
-- @LISTEN@). Notifications that arrived during earlier operations come
-- first, oldest first. If the server ends the session meanwhile, e.g.
-- because an administrator terminated it, this throws
-- 'ConnectionTerminated'.
awaitNotification :: Session 'Ready -> IO (Notification ByteString)
awaitNotification session@(Session connection) = operation session $ do
  kept <- atomicModifyIORef' (connectionNotifications connection) takeOldest
  maybe waitForOne pure kept
  where
    takeOldest [] = ([], Nothing)
    takeOldest kept = (init kept, Just (last kept))
    waitForOne = do
      reply <-
        pullEither session $
          onError
            :|: onNotice
            :|: onParameterStatus
            :|: (\(NotificationResponse notification) -> Reply (copyNotification notification))
            :|: NoAlternatives
      either (\failure -> throwIO (ProtocolError ("an error while idle: " <> show failure))) pure reply

-- | Asks the server to cancel what the session with this backend key is
-- running, over a new connection made as the settings ask, TLS included, as
-- libpq's does since PostgreSQL 17. The server doesn't answer: a cancelled
-- statement fails in its own session with SQLSTATE 57014, and a request that
-- comes too late does nothing.
cancel :: ConnectSettings -> BackendKey ByteString -> IO ()
cancel settings key =
  withChannel settings $ \opened -> do
    pending <- newIORef =<< unsent Nothing mempty
    let channel = opened pending 64
    ( do
        channelSend channel (serialize @(CancelRequest ByteString) @'CancelRequestType (CancelRequest key))
        -- The server closes the connection once it has handled the request.
        let waitForClose = do
              received <- allocaBytes 64 $ \buffer -> readInputInto (channelInput channel) buffer 64
              unless (received == 0) waitForClose
        waitForClose `catch` \(_ :: IOException) -> pure ()
      )
      `finally` channelClose channel

-- | Parse (unless the statement is prepared already), Bind and Execute. A
-- prepared statement is described instead if a parameter streams: the server
-- starts @statement_timeout@ at a Parse or a Describe, and reads a Bind whole
-- before it runs it, so a slow stream counts either way.
executionMessages :: Execution -> Output
executionMessages execution =
  preamble
    <> serialize @(Bind Parameter) @'BindType
      ( Bind
          BindBody
            { bindPortalName = "",
              bindStatementName = statementName,
              bindParameterFormats = executionParameterFormats execution,
              bindParameterValues = executionParameters execution,
              bindResultFormats = executionResultFormats execution
            }
      )
    <> serialize @(Execute ByteString) @'ExecuteType (Execute "" 0)
  where
    (preamble, statementName) = case executionStatement execution of
      Unprepared query parameterTypes -> (serialize @(Parse ByteString) @'ParseType (Parse "" query parameterTypes), "")
      Prepared name
        | streamsParameter execution -> (serialize @(Describe ByteString) @'DescribeType (Describe PreparedStatement name), name)
        | otherwise -> (mempty, name)
      Preparing name query parameterTypes _ -> (serialize @(Parse ByteString) @'ParseType (Parse name query parameterTypes), name)

-- | Whether a parameter of the execution streams.
streamsParameter :: Execution -> Bool
streamsParameter = any streamed . executionParameters
  where
    streamed (Just (ParameterStream _)) = True
    streamed _ = False

sync :: Output
sync = serialize @Sync @'SyncType Sync

data RowsMessage
  = RowStarted Int32 Int
  | Completed ByteString

readRows :: Session 'Rows -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString))
readRows session@(Session connection) step = go
  where
    go acc = do
      message <-
        pullEither session $
          (\(DataRowHeader rowLength columnCount) -> Reply (RowStarted rowLength (fromIntegral columnCount)))
            :|: (\(CommandComplete tag) -> Reply (Completed (BS.copy tag)))
            :|: (\EmptyQueryResponse -> Reply (Completed ""))
            :|: (\PortalSuspended -> Reply (Completed ""))
            :|: sessionTail
      case message of
        Left failure -> pure (Left failure)
        Right (Completed tag) -> pure (Right (acc, tag))
        Right (RowStarted rowLength columnCount) -> do
          row <- startRow connection rowLength columnCount
          acc' <- step acc row
          finishRow row
          go acc'

-- | Pulls the ReadyForQuery that ends a Sync. An error before it, e.g. a
-- deferred constraint failing at the implicit commit, replaces a successful
-- result, given by the function.
awaitReady :: Session phase -> (ServerError -> e) -> Either e a -> IO (Either e a, TransactionStatus)
awaitReady session failed result = do
  let syncing = continueIn session :: Session 'SyncResult
  reply <-
    receive syncing $
      (\(ReadyForQuery status) -> Reply status) :|: sessionTail
  case reply of
    Reply status -> pure (result, status)
    Skip -> awaitReady session failed result
    Notified notification -> do
      keepNotification session notification
      awaitReady session failed result
    Rejected fields -> do
      failure <- sessionError fields
      awaitReady session failed (result >>= const (Left (failed failure)))

-- | A row, whose columns are pulled in order. It's valid only during the step
-- it's given to.
data Row = Row
  { rowConnection :: Connection,
    rowColumnCount :: Int,
    -- | The position where the row ends.
    rowEnd :: Int,
    rowCursor :: IORef RowCursor
  }

data RowCursor = RowCursor
  { cursorActive :: !Bool,
    -- | How many columns were pulled.
    cursorColumn :: !Int,
    -- | Unread bytes of the current column's value.
    cursorValueLeft :: !Int,
    -- | Whether the current column's value is being read already.
    cursorValueTaken :: !Bool
  }

data Column
  = NullColumn
  | -- | A value of this many bytes, to take with 'columnBytes' or
    -- 'columnStream'.
    Column Int
  deriving (Eq, Show)

startRow :: Connection -> Int32 -> Int -> IO Row
startRow connection rowLength columnCount = do
  unless (rowLength >= 6 && columnCount >= 0) $
    throwIO (ProtocolError ("invalid DataRow length " <> show rowLength <> " or column count " <> show columnCount))
  position <- inputPosition <$> readIORef (connectionBuffer connection)
  cursor <- newIORef (RowCursor True 0 0 False)
  pure (Row connection columnCount (position + fromIntegral rowLength - 6) cursor)

-- | Skips what the step left unread, so the next message can be read.
finishRow :: Row -> IO ()
finishRow row = do
  writeIORef (rowCursor row) (RowCursor False 0 0 True)
  position <- inputPosition <$> readIORef (connectionBuffer (rowConnection row))
  inRow row (skipInput (rowEnd row - position))

-- | Pulls the next column, skipping what's left unread of the current one.
-- 'Nothing' after the last column.
nextColumn :: Row -> IO (Maybe Column)
nextColumn row = do
  cursor <- activeCursor row
  when (cursorValueLeft cursor > 0) $ inRow row (skipInput (cursorValueLeft cursor))
  if cursorColumn cursor == rowColumnCount row
    then do
      writeIORef (rowCursor row) cursor {cursorValueLeft = 0, cursorValueTaken = True}
      pure Nothing
    else do
      size <- inRow row (parser @Int32 @('FixedBytesAtom 4))
      let column = cursorColumn cursor + 1
      case compare size (-1) of
        EQ -> do
          writeIORef (rowCursor row) (RowCursor True column 0 True)
          pure (Just NullColumn)
        GT -> do
          writeIORef (rowCursor row) (RowCursor True column (fromIntegral size) False)
          pure (Just (Column (fromIntegral size)))
        LT -> throwIO (ProtocolError ("invalid column length " <> show size))

-- | The current column's value, whole. It is a slice of the read buffer, or
-- for a value larger than the buffer's free space, a new allocation.
columnBytes :: Row -> IO ByteString
columnBytes row = do
  cursor <- untakenCursor row
  value <- inRow row (readInputExactly (cursorValueLeft cursor))
  writeIORef (rowCursor row) cursor {cursorValueLeft = 0, cursorValueTaken = True}
  pure value

-- | The current column's value as chunks that are slices of the read buffer,
-- each at most the read size. The stream is valid until the next column is
-- pulled.
columnStream :: Row -> IO ByteStream
columnStream row = do
  cursor <- untakenCursor row
  writeIORef (rowCursor row) cursor {cursorValueTaken = True}
  pure
    ByteStream
      { byteStreamLength = cursorValueLeft cursor,
        byteStreamNext = do
          current <- activeCursor row
          unless (cursorColumn current == cursorColumn cursor) $
            throwIO (RowMisuse "a column stream was read after the next column was pulled")
          if cursorValueLeft current == 0
            then pure BS.empty
            else do
              chunk <- inRow row (readInputSome (cursorValueLeft current))
              writeIORef (rowCursor row) current {cursorValueLeft = cursorValueLeft current - BS.length chunk}
              pure chunk
      }

-- | A row's columns, described by the Haskell types they decode to.
class DecodeRow (columns :: [Type]) where
  -- | A function from the columns' values, in order, to @final@.
  type RowFunction columns (final :: Type) :: Type

  -- | How many columns there are.
  rowArity :: Int

  -- | Decodes the columns, numbered from the given one, and applies the
  -- function to them.
  applyColumns :: Int -> RowFunction columns final -> Input (Either DecodingError final)

  -- | Applies a function to what the row function finally gives, e.g. to
  -- make a pure constructor of the columns' values give an action.
  mapRowFunction :: (final -> final') -> RowFunction columns final -> RowFunction columns final'

instance DecodeRow '[] where
  type RowFunction '[] final = final
  rowArity = 0
  {-# INLINE rowArity #-}
  applyColumns _ final = pure (Right final)
  {-# INLINE applyColumns #-}
  mapRowFunction f = f
  {-# INLINE mapRowFunction #-}

instance (DecodeField a, DecodeRow columns) => DecodeRow (a ': columns) where
  type RowFunction (a ': columns) final = a -> RowFunction columns final
  rowArity = 1 + rowArity @columns
  {-# INLINE rowArity #-}
  mapRowFunction f function = mapRowFunction @columns f . function
  {-# INLINE mapRowFunction #-}
  applyColumns column function = do
    value <- parser @(Maybe ByteString) @'MaybeBytes32Atom
    case decodeField value of
      Left message -> pure (Left (ColumnDecodingFailed column message))
      Right decoded -> applyColumns @columns (column + 1) (function decoded)
  {-# INLINE applyColumns #-}

-- | Decodes the row's columns, as listed by their types, e.g.
-- @decodeRow \@'[Int32, Text] row (\\count name -> ...)@, and runs the
-- action the function returns. The row has to have exactly these columns,
-- and none of them may have been pulled. The columns are decoded in one pass
-- over the read buffer, and decoded byte strings are slices of it.
decodeRow :: forall columns r. DecodeRow columns => Row -> RowFunction columns (IO r) -> IO (Either DecodingError r)
decodeRow row function = do
  started <- startDecoding row (rowArity @columns)
  case started of
    Left failure -> pure (Left failure)
    Right () -> do
      decoded <- inRow row (applyColumns @columns 1 function)
      writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True)
      either (pure . Left) (fmap Right) decoded
-- Inlined so that each call site unrolls the decoding of its own columns.
{-# INLINE decodeRow #-}

-- | Like 'decodeRow', but the row has one more column, which is streamed:
-- the function gets it last, as a 'ByteStream' that is valid during the
-- action.
decodeRowStreaming :: forall columns r. DecodeRow columns => Row -> RowFunction columns (ByteStream -> IO r) -> IO (Either DecodingError r)
decodeRowStreaming row function =
  join <$> decodeRowStreamingNullable @columns row (mapRowFunction @columns @(ByteStream -> IO r) @(Maybe ByteStream -> IO (Either DecodingError r)) streamOrFail function)
  where
    streamOrFail continue = maybe (pure (Left (ColumnDecodingFailed (rowArity @columns + 1) "unexpected NULL in a streamed column"))) (fmap Right . continue)
{-# INLINE decodeRowStreaming #-}

-- | Like 'decodeRowStreaming', but the streamed column may be NULL, which the
-- function gets as 'Nothing'.
decodeRowStreamingNullable :: forall columns r. DecodeRow columns => Row -> RowFunction columns (Maybe ByteStream -> IO r) -> IO (Either DecodingError r)
decodeRowStreamingNullable row function = do
  let streamed = rowArity @columns + 1
  started <- startDecoding row streamed
  case started of
    Left failure -> pure (Left failure)
    Right () -> do
      decoded <- inRow row $ do
        applied <- applyColumns @columns 1 function
        case applied of
          Left failure -> pure (Left failure)
          Right continue -> do
            size <- parser @Int32 @('FixedBytesAtom 4)
            pure (Right (fromIntegral size, continue))
      case decoded of
        Left failure -> do
          writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True)
          pure (Left failure)
        Right (size, continue)
          | size < 0 -> do
              writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True)
              Right <$> continue Nothing
          | otherwise -> do
              writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) size False)
              stream <- columnStream row
              Right <$> continue (Just stream)
{-# INLINE decodeRowStreamingNullable #-}

-- | Checks that no column of the row has been pulled yet, and that it has
-- the expected number of columns.
startDecoding :: Row -> Int -> IO (Either DecodingError ())
startDecoding row expected = do
  cursor <- activeCursor row
  unless (cursorColumn cursor == 0) $
    throwIO (RowMisuse "a row was decoded after some of its columns were pulled")
  pure $
    if expected == rowColumnCount row
      then Right ()
      else Left (ColumnCountMismatch expected (rowColumnCount row))

activeCursor :: Row -> IO RowCursor
activeCursor row = do
  cursor <- readIORef (rowCursor row)
  unless (cursorActive cursor) $ throwIO (RowMisuse "a row was used after its step returned")
  pure cursor

untakenCursor :: Row -> IO RowCursor
untakenCursor row = do
  cursor <- activeCursor row
  when (cursorValueTaken cursor) $
    throwIO (RowMisuse "a column value was read twice, or before its column was pulled")
  pure cursor

inRow :: Row -> Input a -> IO a
inRow row = runOn (rowConnection row) (rowEnd row)

-- | What a handler makes of a message.
data Reply a
  = Reply a
  | -- | Keep pulling.
    Skip
  | -- | Keep the notification for 'awaitNotification', and keep pulling.
    Notified (Notification ByteString)
  | Rejected [ErrorNoticeField ByteString]

-- | Handlers for the messages any phase of startup accepts last.
startupTail :: Alternatives '[ErrorResponse ByteString -> Reply a, NoticeResponse ByteString -> Reply a] (Reply a)
startupTail = onError :|: onNotice :|: NoAlternatives

-- | Handlers for the messages any phase after startup accepts last. Notices
-- and reports of parameters and notifications are skipped for now.
sessionTail ::
  Alternatives
    '[ ErrorResponse ByteString -> Reply a,
       NoticeResponse ByteString -> Reply a,
       ParameterStatus ByteString -> Reply a,
       NotificationResponse ByteString -> Reply a
     ]
    (Reply a)
sessionTail = onError :|: onNotice :|: onParameterStatus :|: (\(NotificationResponse notification) -> Notified notification) :|: NoAlternatives

onError :: ErrorResponse ByteString -> Reply a
onError (ErrorResponse fields) = Rejected fields

onNotice :: NoticeResponse ByteString -> Reply a
onNotice _ = Skip

onParameterStatus :: ParameterStatus ByteString -> Reply a
onParameterStatus _ = Skip

-- | Decodes an error's fields, which are copied out of the read buffer.
serverError :: [ErrorNoticeField ByteString] -> IO ServerError
serverError = either (throwIO . ProtocolError) (pure . ServerError) . diagnostics

-- | An error during a session. One after which the server ends the session
-- is thrown as 'ConnectionTerminated', leaving the session broken.
sessionError :: [ErrorNoticeField ByteString] -> IO ServerError
sessionError fields = do
  failure <- serverError fields
  when (isConnectionFatal failure) $ throwIO (ConnectionTerminated failure)
  pure failure

-- | Pulls messages until one gives a reply or an error. Skipped messages are
-- pulled past.
pullReply ::
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions (Reply a)) (Reply a) =>
  Session phase ->
  Alternatives functions (Reply a) ->
  IO (Either [ErrorNoticeField ByteString] a)
pullReply session handlers = do
  reply <- receive session handlers
  case reply of
    Reply a -> pure (Right a)
    Skip -> pullReply session handlers
    Notified notification -> do
      keepNotification session notification
      pullReply session handlers
    Rejected fields -> pure (Left fields)

-- | Keeps a notification, copied out of the read buffer, for
-- 'awaitNotification'.
keepNotification :: Session phase -> Notification ByteString -> IO ()
keepNotification (Session connection) notification =
  atomicModifyIORef' (connectionNotifications connection) (\kept -> (copyNotification notification : kept, ()))

copyNotification :: Notification ByteString -> Notification ByteString
copyNotification (Notification processId channel payload) = Notification processId (BS.copy channel) (BS.copy payload)

-- | Pulls a reply during a session; see 'sessionError'.
pullEither ::
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions (Reply a)) (Reply a) =>
  Session phase ->
  Alternatives functions (Reply a) ->
  IO (Either ServerError a)
pullEither session handlers =
  pullReply session handlers >>= either (fmap Left . sessionError) (pure . Right)

-- | Pulls a reply during startup, when any error is thrown as
-- 'ConnectionRejected'.
pull ::
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions (Reply a)) (Reply a) =>
  Session phase ->
  Alternatives functions (Reply a) ->
  IO a
pull session handlers =
  pullReply session handlers >>= either (throwIO . ConnectionRejected <=< serverError) pure

-- | Pulls the next message, which has to be one the phase accepts, and passes
-- it to its handler.
receive ::
  forall phase functions r.
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions r) r =>
  Session phase ->
  Alternatives functions r ->
  IO r
receive (Session connection) handlers = runOn connection maxBound (parseChoice @(Accepted phase) handlers)

-- | Runs a parser on the connection's unconsumed bytes, reading no further
-- than the given position.
runOn :: Connection -> Int -> Input a -> IO a
runOn connection limit parse = do
  buffer <- readIORef (connectionBuffer connection)
  (result, buffer') <-
    runInputUntil (channelInput (connectionChannel connection)) limit buffer parse
      `catch` \(InputError message) -> throwIO (ProtocolError message)
  writeIORef (connectionBuffer connection) buffer'
  pure result

-- | Runs an operation on an idle session. The session is busy meanwhile, and
-- broken if the operation fails with an exception.
operation :: Session 'Ready -> IO a -> IO a
operation (Session connection) action = mask $ \restore -> do
  acquired <- atomicModifyIORef' (connectionState connection) $ \case
    Idle -> (Busy, Nothing)
    Busy -> (Busy, Just "the session is busy with another operation")
    Broken -> (Broken, Just "the session broke during an earlier operation")
    Closed -> (Closed, Just "the session is closed")
  maybe (pure ()) (throwIO . SessionUnusable) acquired
  result <- restore action `onException` writeIORef (connectionState connection) Broken
  writeIORef (connectionState connection) Idle
  pure result

send :: forall s a phase. Serializable a s => Session phase -> a -> IO ()
send session message = sendOutputs session (serialize @a @s message)

sendOutputs :: Session phase -> Output -> IO ()
sendOutputs (Session connection) = channelSend (connectionChannel connection)

-- | The session in the phase the protocol continues in.
continueIn :: Session phase -> Session next
continueIn (Session connection) = Session connection
