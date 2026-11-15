{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Encrypting a connection as @sslmode@ and @sslnegotiation@ say, over a socket that is connected: asking the server
-- for TLS, and the handshake, with the client's certificate, the root certificates and the revocation lists, as libpq
-- checks them. The waits are the arrow's: the handshake blocks on the socket as a link does.
--
-- The files that TLS needs are looked at when the settings are made, by 'prepareTls', which builds the TLS configuration of
-- each host: the client certificate and its key, the revocation lists, the root certificates. A connect reads none of
-- them, and makes a connection from the configuration.
module Database.PostgreSQL.Internal.Encryption
  ( Securing (..),
    NotMade (..),
    requested,
    secured,
    prepareTls,
    unbuilt,
    rootsProblem,
  )
where

import Control.Arrow (first, (>>>), (^>>), (|||))
import Control.Exception (Exception (..), Handler (..), IOException, SomeException, catches, fromException, throwIO, try)
import Control.Monad (filterM)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Char (isDigit)
import Data.Foldable (toList)
import Data.List (sort)
import Data.Maybe (fromMaybe)
import Database.PostgreSQL.Errors (ConnectError (..))
import Database.PostgreSQL.Internal.Carrier (Carrier (..), newGuard)
import Database.PostgreSQL.Internal.Frontend (sendOutput)
import Database.PostgreSQL.Internal.Protocol.Messages (pattern SSLRequest, serialize)
import Database.PostgreSQL.Session.Settings (ConnectSettings (..), SslMode (..))
import Database.PostgreSQL.Transport (Address (..), RootCertificates (..), ServerVerification (..), TlsClient (..), TlsError (..), TlsLink (..), TlsPrepared (..), TlsSettings (..), TlsStarter (..), TlsWanted (..), shake)
import GHC.IO.Exception (IOException (..))
import Network.Socket (Socket)
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Classes (Effect (..), Link (..), Room (..), Runtime, Scope (..), view)
import Network.Streaming.Combinators (tap)
import System.Directory (doesDirectoryExist, doesFileExist, listDirectory)
#if defined(mingw32_HOST_OS)
import System.Directory (getPermissions)
#else
import qualified System.Posix.Files as Posix
import qualified System.Posix.User as Posix
#endif

-- | What an attempt does about TLS: the client, how it verifies the server, and whether the attempt fails if the server
-- doesn't take TLS.
data Securing = Securing !TlsClient !ServerVerification !Bool

-- | Why an attempt failed, and whether the server had taken TLS by then, as sslmode=prefer tries again without it if it
-- had.
data NotMade = NotMade !Bool ConnectError

-- | Asks the server for TLS, and reads its answer, which is one byte, so that no unencrypted byte after it is taken for
-- TLS's: whether it takes TLS. The question and the answer are in the buffer.
requested :: (Runtime arr) => arr (Socket, Buffer) (Either ConnectError Bool)
requested = tryA asking >>> now (either (fmap Left . classified) pure)
  where
    asking =
      (\(sock, buffer) -> (Plain sock, buffer))
        ^>> tap ((\(carrier, buffer) -> (carrier, buffer, serialize SSLRequest)) ^>> sendOutput)
        >>> (\(carrier, buffer) -> ((carrier, Room buffer 0 1), buffer))
        ^>> first receive
        >>> now (\(count, buffer) -> pure (answer count (view buffer 1)))
    answer :: Int -> ByteString -> Either ConnectError Bool
    answer count byte
      | count == 0 = Left (TlsFailure "server closed the connection unexpectedly")
      | otherwise = case BS.head byte of
          83 -> Right True
          78 -> Right False
          -- The server isn't authenticated yet, so its error isn't read.
          69 -> Left (TlsFailure "server sent an error response during SSL exchange")
          other -> Left (TlsFailure ("received invalid response to SSL negotiation: " <> [toEnum (fromIntegral other)]))

-- | Starts TLS over the socket, from the configuration that was built for the settings, and shakes hands: the carrier, and
-- the data that SCRAM's channel binding takes, or why none, or why TLS couldn't be started, which includes why the
-- configuration couldn't be built. No file is read.
secured :: (Runtime arr) => arr ((ConnectSettings, Securing), Socket) (Either ConnectError (Carrier, Maybe (Either String ByteString)))
secured = now opening >>> (now (pure . Left) ||| shaking)
  where
    opening ((settings, Securing _ verification _), sock) = guarded $ case connectTlsPrepared settings of
      Just prepared | preparedFor prepared == wantedBy settings verification -> Right . (,settings) <$> either throwIO (\starter -> startTls starter sock) (preparedStarter prepared)
      _ -> pure (Left notBuilt)
    -- The link is ended if the handshake failed, which closes the socket too.
    shaking =
      (\(link, settings) -> (link, (link, settings)))
        ^>> first (tryA shake)
        >>> now
          ( \(shaken, (link, settings)) -> case shaken of
              Right binding -> (\guard -> Right (Secure link guard, fromMaybe binding (connectEndPointOverride settings))) <$> newGuard
              Left failure -> Left <$> (linkEnd link *> classified failure)
          )

-- | The verification that a mode asks of a connection with TLS, or none for a mode that has no TLS.
verificationOf :: SslMode -> Maybe ServerVerification
verificationOf = \case
  SslDisable -> Nothing
  SslVerifyFull roots -> Just (VerifyFull roots)
  _ -> Just NoVerification

-- | What the settings say that a configuration of TLS is for, as the connection uses it.
wantedBy :: ConnectSettings -> ServerVerification -> TlsWanted
wantedBy settings verification =
  TlsWanted
    { wantedVerification = verification,
      wantedCertificate = connectSslCertificate settings,
      wantedRevocationList = connectSslRevocationList settings,
      wantedRevocationListDirectory = connectSslRevocationListDirectory settings,
      wantedVersions = connectSslVersions settings,
      wantedServerNameIndication = connectSslServerNameIndication settings,
      wantedDirect = connectSslDirect settings,
      wantedServerName = case connectAddress settings of
        TcpAddress host _ -> fromMaybe host (connectServerName settings)
        UnixAddress path -> path
    }

-- | The failure of a connect whose settings ask for TLS, and whose TLS configuration wasn't built, or was built for other
-- settings.
notBuilt :: ConnectError
notBuilt = TlsFailure "the TLS configuration of these settings was not built: prepare them with prepareTls before connecting"

-- | Why the settings can't make a connection with TLS, though they ask for TLS and have a client: its configuration wasn't built for them,
-- which is checked before anything is connected, so that no mode, @prefer@ included, goes on without it.
unbuilt :: ConnectSettings -> Maybe ConnectError
unbuilt settings = case (connectAddress settings, verificationOf (connectSslMode settings)) of
  (TcpAddress {}, Just verification)
    | maybe True ((/= wantedBy settings verification) . preparedFor) (connectTlsPrepared settings) -> Just notBuilt
  _ -> Nothing

-- | The settings, with the TLS configuration of each of their hosts built: the settings' own and their fallbacks'. It reads
-- the files that the configuration needs, which a connect never does, so a program calls it where it makes the settings, in
-- IO. A host that can't use TLS, because it has no TLS client, no TCP address or a mode without TLS, gets none. A
-- configuration that can't be built is kept with the settings, and fails a connect that gets as far as TLS, as one that fails
-- at the connect always did.
prepareTls :: ConnectSettings -> IO ConnectSettings
prepareTls settings = do
  prepared <- preparedHost settings
  fallbacks <- traverse preparedHost (connectFallbacks settings)
  pure prepared {connectFallbacks = fallbacks}

preparedHost :: ConnectSettings -> IO ConnectSettings
preparedHost settings = case (connectAddress settings, connectTlsClient settings, verificationOf (connectSslMode settings)) of
  (TcpAddress {}, Just client, Just verification) -> do
    problem <- rootsProblem verification
    starter <- captured (build client verification)
    pure settings {connectTlsPrepared = Just (TlsPrepared (wantedBy settings verification) problem starter)}
  _ -> pure settings {connectTlsPrepared = Nothing}
  where
    build client verification = do
      certificate <- clientCertificate settings
      lists <- revocationLists settings
      buildTls client (tlsSettings verification certificate lists) (wantedServerName (wantedBy settings verification))
    tlsSettings verification certificate lists =
      TlsSettings
        verification
        certificate
        lists
        (connectSslVersions settings)
        (connectSslServerNameIndication settings)
        (connectSslDirect settings)

-- | The action's result, or the failure that says why TLS couldn't be started, as the connect would have thrown it: a
-- connection's own, a TLS failure and a socket's. What else failed is thrown on.
captured :: IO a -> IO (Either SomeException a)
captured action =
  (Right <$> action)
    `catches` [ Handler (\connecting -> pure (Left (toException (connecting :: ConnectError)))),
                Handler (\tls -> pure (Left (toException (tls :: TlsError)))),
                Handler (\stream -> pure (Left (toException (stream :: IOException))))
              ]

-- | The failure of what started TLS or shook hands, as the connection's: a connection's own, a TLS failure, and a
-- socket's. What else failed is thrown on.
classified :: SomeException -> IO ConnectError
classified failure
  | Just connecting <- fromException failure = pure connecting
  | Just tls <- fromException failure = pure (TlsFailure (displayException (tls :: TlsError)))
  | Just stream <- fromException failure = pure (Broke (ioe_description stream))
  | otherwise = throwIO failure

-- | The action, with the failures that say why TLS couldn't be started as values.
guarded :: IO (Either ConnectError a) -> IO (Either ConnectError a)
guarded action =
  action
    `catches` [ Handler (\connecting -> pure (Left (connecting :: ConnectError))),
                Handler (\tls -> pure (Left (TlsFailure (displayException (tls :: TlsError))))),
                Handler (\stream -> pure (Left (Broke (ioe_description (stream :: IOException)))))
              ]

-- | What libpq says of a root certificate file that doesn't exist, which it checks before it connects: the words, if there is
-- a failure.
rootsProblem :: ServerVerification -> IO (Maybe String)
rootsProblem = \case
  NoVerification -> pure Nothing
  VerifyFull SystemRootCertificates -> pure Nothing
  VerifyFull (RootCertificateFile file) -> do
    exists <- doesFileExist file
    pure $
      if exists
        then Nothing
        else
          Just $
            "root certificate file \""
              <> file
              <> "\" does not exist\nEither provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode to disable server certificate verification."

-- | The client certificate, if its file exists, with its private key, which has to exist and be readable only by its
-- owner, as with libpq.
clientCertificate :: ConnectSettings -> IO (Maybe (FilePath, FilePath))
clientCertificate settings = case connectSslCertificate settings of
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

-- | The revocation lists: the file, if it exists, and the files of the directory that are named as OpenSSL's c_rehash names
-- them, in order, if the directory exists.
revocationLists :: ConnectSettings -> IO [FilePath]
revocationLists settings = do
  file <- filterM doesFileExist (toList (connectSslRevocationList settings))
  directory <- case connectSslRevocationListDirectory settings of
    Nothing -> pure []
    Just directory -> do
      exists <- doesDirectoryExist directory
      if exists
        then map ((directory <> "/") <>) . sort . filter rehashed <$> listDirectory directory
        else pure []
  pure (file <> directory)
  where
    -- c_rehash's names of revocation lists: their issuer's hash, .r and a number.
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
