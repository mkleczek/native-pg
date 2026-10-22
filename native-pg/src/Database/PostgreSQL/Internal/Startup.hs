{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Private socket negotiation, authentication and startup exchange.
module Database.PostgreSQL.Internal.Startup
  ( withChannel, startSession, connectWithBuffers,
  ) where

import Control.Exception (Exception (..), IOException, bracketOnError, catch, throwIO, try)
import Control.Monad (filterM, unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Char (isDigit)
import Data.Foldable (toList)
import Data.IORef (newIORef)
import Data.List (sort)
import Data.Maybe (fromMaybe, isJust)
#if defined(mingw32_HOST_OS)
import System.Directory (doesDirectoryExist, doesFileExist, getPermissions, listDirectory)
#else
import System.Directory (doesDirectoryExist, doesFileExist, listDirectory)
import qualified System.Posix.Files as Posix
import qualified System.Posix.User as Posix
#endif
import Database.PostgreSQL.Session.Settings
import Network.Streaming.Resources.Internal (Buffer)
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Internal.Exchange
import Database.PostgreSQL.Internal.Authentication
  ( ChannelBinding (..), md5Password, newScramNonce, scramClientFinal,
    scramClientFirst, scramMechanism, scramPlusMechanism, scramVerifyServerFinal,
  )
import Database.PostgreSQL.Internal.Protocol.Codec (Alternatives (..), InputSource (..), Serializable (..))
import Database.PostgreSQL.Internal.Protocol.Messages
import Database.PostgreSQL.Transport
  ( Address (..), newChannel, RootCertificates (..), ServerVerification (..),
    TlsChannel (..), TlsClient (..), TlsError, TlsSettings (..),
  )
import Database.PostgreSQL.Internal.Channel (Channel (..), unsent)
import Database.PostgreSQL.Internal.Socket (connectSocket)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Storable (peek)
import Network.Socket (Socket)
import qualified Network.Socket as Socket

-- | Startup/authentication using caller-supplied initialized workspace.
-- Integration-only callers reserve separate setup capacity for this scope.
connectWithBuffers :: Buffer -> Buffer -> ConnectSettings -> IO (Connection, ServerInfo)
connectWithBuffers input output settings = withChannel settings $ \channel binding -> do
  connection <- newConnection channel (connectReadSize settings)
  withContext connection input output $ \context -> do
    (_, info) <- startSession settings binding context
    pure (connection, info)

startSession :: ConnectSettings -> Maybe (Either String ByteString) -> Context -> IO (Session 'Ready, ServerInfo)
startSession settings binding context = do
  let session = Session context
  send @'StartupMessageType session (startupMessage settings)
  (version, initializing) <- authenticate settings binding (connectProtocolVersion settings) session
  initialize version initializing

-- | Connects a socket to the server, starts TLS over it as the settings ask,
-- and runs the action with its channel builder and optional channel-binding
-- result. The socket is closed if the action fails.
withChannel :: ConnectSettings -> (Channel -> Maybe (Either String ByteString) -> IO a) -> IO a
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
                | otherwise -> use (newChannel sock) Nothing
              True -> do
                let started = do
                      certificate <- clientCertificate
                      lists <- revocationLists
                      opened <-
                        startTlsChannel
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
                      use (tlsChannel opened) (tlsServerEndPoint opened)
                if required then started else started `catch` (throwIO . FailedWithTls)
          _ -> use (newChannel sock) Nothing
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
  let channel = newChannel sock
  channelSend channel (serialize @SSLRequest @'SSLRequestType SSLRequest)
  answer <- allocaBytes 1 $ \byte -> do
    received <- readInputInto (channelInput channel pending 1) byte 1
    if received == 0 then pure Nothing else Just <$> peek byte
  case answer of
    Just 83 -> pure True
    Just 78 -> pure False
    -- The server isn't authenticated yet, so its error isn't read.
    Just 69 -> throwIO (TlsFailure "server sent an error response during SSL exchange")
    Just other -> throwIO (TlsFailure ("received invalid response to SSL negotiation: " <> [toEnum (fromIntegral other)]))
    Nothing -> throwIO (TlsFailure "server closed the connection unexpectedly")

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

authenticate :: ConnectSettings -> Maybe (Either String ByteString) -> ProtocolVersion -> Session 'Authentication -> IO (ProtocolVersion, Session 'Initialization)
authenticate settings bindingData version session = do
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
    ProtocolVersionNegotiated negotiated -> authenticate settings bindingData negotiated session
    SaslRequested mechanisms
      | Just binding <- bindingData,
        connectChannelBinding settings /= ChannelBindingDisable,
        scramPlusMechanism `elem` mechanisms -> do
          password <- requirePassword
          endPoint <- either (throwIO . AuthenticationError) pure binding
          (version,) <$> authenticateScram scramPlusMechanism (BoundToServerEndPoint endPoint) password session
      | required ->
          throwIO (AuthenticationError "channel binding is required, but server did not offer an authentication method that supports channel binding")
      | scramMechanism `elem` mechanisms -> do
          password <- requirePassword
          let binding
                | isJust bindingData && connectChannelBinding settings /= ChannelBindingDisable = ChannelBindingUnused
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
