{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | TLS over a connected socket, through rustls-ffi: the only part of the
-- client in C. Only the handshake waits for the socket. Reads and writes take
-- what's ready, and give 'Nothing', or less than they're given, when it isn't,
-- so the transport waits on the socket as it does without TLS.
module Database.PostgreSQL.Tls
  ( -- * For native-pg's connections
    tlsClient,

    -- * Configurations
    TlsConfig,
    newTlsConfig,

    -- * Connections
    TlsConnection,
    startTls,
    tlsSocket,
    receiveTls,
    sendTls,
    flushTls,
    closeTls,

    -- * Connections rustls made elsewhere, e.g. a test's server
    RustlsConnection,
    newTlsConnection,
    handshake,
  )
where

import Control.Concurrent (threadWaitRead, threadWaitWrite)
import Control.Concurrent.MVar (MVar, modifyMVar, newMVar)
import Control.Exception (IOException, bracket, catch, finally, onException, throwIO)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.Foldable (for_)
import Data.Word (Word16, Word32, Word8)
import Database.PostgreSQL.Transport (Link (..), RootCertificates (..), ServerVerification (..), TlsClient (..), TlsError (..), TlsSettings (..), TlsVersion (..), newChannel)
import Foreign.C.ConstPtr (ConstPtr (..))
import Foreign.C.Error (Errno (..), eAGAIN, eINTR, eWOULDBLOCK, errnoToIOError)
import Foreign.C.String (CString, peekCStringLen, withCString)
import Foreign.C.Types (CBool (..), CChar, CInt (..), CSize (..), CUInt (..))
import Foreign.ForeignPtr (FinalizerPtr, ForeignPtr, finalizeForeignPtr, newForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca, allocaBytes)
import Foreign.Marshal.Array (withArrayLen)
import Foreign.Marshal.Utils (fromBool, with)
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable (Storable, peek)
import Network.Socket (Socket, close, withFdSocket)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Types (Fd (..))

-- | A version's number in TLS.
version :: TlsVersion -> Word16
version = \case
  Tls12 -> 0x0303
  Tls13 -> 0x0304

-- | Starts TLS for native-pg's connections, with a new configuration each
-- time, so that changed certificate files count, as with libpq.
tlsClient :: TlsClient
tlsClient = TlsClient $ \settings serverName sock -> do
  config <- newTlsConfig settings
  newChannel <$> startTls config serverName sock

-- | A configuration for TLS connections, which any number of them share.
newtype TlsConfig = TlsConfig (ForeignPtr ClientConfig)

-- | A configuration with the settings. Its connections offer ALPN's
-- @postgresql@. Throws 'TlsError' if a certificate file can't be used.
newTlsConfig :: TlsSettings -> IO TlsConfig
newTlsConfig settings = do
  builder <-
    -- The default provider, rustls-ffi's from its build, made the process's.
    bracket (built "rustls has no crypto provider" c_crypto_provider_builder_new_from_default) c_crypto_provider_builder_free $ \providerBuilder ->
      bracket (built "could not make the crypto provider" (c_crypto_provider_builder_build providerBuilder)) c_crypto_provider_free $ \provider ->
        withArrayLen (map version (tlsVersions settings)) $ \count versions ->
          built "could not configure TLS's versions" (c_client_config_builder_new_custom provider versions (fromIntegral count))
  ( do
      c_client_config_builder_set_enable_sni builder (fromBool (tlsServerNameIndication settings))
      case tlsVerification settings of
        NoVerification -> check "could not configure the certificate verifier" =<< c_accept_any_certificate builder
        VerifyFull (RootCertificateFile roots) -> withVerifier roots (c_client_config_builder_set_server_verifier builder)
        VerifyFull SystemRootCertificates
          | null (tlsRevocationLists settings) -> c_client_config_builder_set_server_verifier builder . ConstPtr =<< systemVerifier
          | otherwise -> throwIO (TlsError "revocation lists with the system's root certificates aren't supported")
      for_ (tlsClientCertificate settings) $ \(chainFile, keyFile) -> do
        chain <- BS.readFile chainFile
        key <- BS.readFile keyFile
        certified <-
          BS.unsafeUseAsCStringLen chain $ \(chainBytes, chainLength) ->
            BS.unsafeUseAsCStringLen key $ \(keyBytes, keyLength) ->
              built
                ("could not load client certificate file " <> show chainFile <> " with its key")
                (c_certified_key_build (castPtr chainBytes) (fromIntegral chainLength) (castPtr keyBytes) (fromIntegral keyLength))
        -- The configuration keeps a reference of its own to the key.
        with certified (\keys -> check "could not use the client certificate" =<< c_client_config_builder_set_certified_key builder (castPtr keys) 1)
          `finally` c_certified_key_free certified
      check "could not configure ALPN" =<< c_offer_postgresql builder
    )
    `onException` c_client_config_builder_free builder
  -- Building consumes the builder, whether it succeeds or not.
  config <- built "could not create the SSL configuration" (c_client_config_builder_build builder)
  TlsConfig <$> newForeignPtr p_client_config_free (unConstPtr config)
  where
    withVerifier roots useVerifier =
      bracket c_root_cert_store_builder_new c_root_cert_store_builder_free $ \storeBuilder -> do
        check ("could not read root certificate file " <> show roots)
          =<< withCString roots (\path -> c_root_cert_store_builder_load_roots_from_file storeBuilder path (CBool 1))
        bracket (built "could not create the root certificate store" (c_root_cert_store_builder_build storeBuilder)) c_root_cert_store_free $ \store ->
          bracket (c_web_pki_server_cert_verifier_builder_new store) c_web_pki_server_cert_verifier_builder_free $ \verifierBuilder -> do
            for_ (tlsRevocationLists settings) $ \file -> do
              lists <- BS.readFile file
              BS.unsafeUseAsCStringLen lists $ \(bytes, len) ->
                check ("could not load revocation list file " <> show file)
                  =<< c_web_pki_server_cert_verifier_builder_add_crl verifierBuilder (castPtr bytes) (fromIntegral len)
            -- The configuration keeps a reference of its own to the verifier.
            bracket
              (built "could not create the certificate verifier" (c_web_pki_server_cert_verifier_builder_build verifierBuilder))
              c_server_cert_verifier_free
              (useVerifier . ConstPtr)

-- | The verifier against the system's root certificates, the same each time,
-- since it loads all of them, and never freed.
systemVerifier :: IO (Ptr ServerCertVerifier)
systemVerifier = modifyMVar systemVerifierMade $ \made -> case made of
  Just verifier -> pure (made, verifier)
  Nothing -> do
    verifier <- built "could not load the system's root certificates" c_platform_server_cert_verifier
    pure (Just verifier, verifier)

systemVerifierMade :: MVar (Maybe (Ptr ServerCertVerifier))
systemVerifierMade = unsafePerformIO (newMVar Nothing)
{-# NOINLINE systemVerifierMade #-}

-- | A TLS connection over a socket.
data TlsConnection = TlsConnection
  { -- | The socket it reads and writes.
    tlsSocket :: Socket,
    tlsConnection :: ForeignPtr RustlsConnection
  }

instance Link TlsConnection where
  linkSocket = tlsSocket
  {-# INLINE linkSocket #-}
  receiveReady = receiveTls
  {-# INLINE receiveReady #-}
  sendReady = sendTls
  {-# INLINE sendReady #-}
  flushReady = flushTls
  {-# INLINE flushReady #-}
  closeLink = closeTls
  {-# INLINE closeLink #-}

-- | Starts TLS over the socket, as a client of the named server, and waits
-- for the handshake to finish. The name is a host name, which the server's
-- certificate is checked against, or an IP address. Throws 'TlsError' if the
-- handshake fails.
startTls :: TlsConfig -> String -> Socket -> IO TlsConnection
startTls (TlsConfig config) serverName sock = do
  connection <- withForeignPtr config $ \configPointer ->
    withCString serverName $ \name ->
      built ("invalid server name " <> show serverName) (c_client_connection_new (ConstPtr configPointer) name)
  tls <- newTlsConnection sock connection
  handshake tls
  pure tls

-- | Takes over a connection rustls made, to read and write it over the
-- socket. It's freed when it's closed, or when it's no longer used.
newTlsConnection :: Socket -> Ptr RustlsConnection -> IO TlsConnection
newTlsConnection sock connection = do
  -- Records wait in the connection until the socket takes them, up to this
  -- many bytes.
  c_connection_set_buffer_limit connection 65536
  TlsConnection sock <$> newForeignPtr p_connection_free connection

-- | Sends and receives the handshake's messages until it's done.
handshake :: TlsConnection -> IO ()
handshake tls = do
  flushed <- flushTls tls
  handshaking <- withConnection tls c_connection_is_handshaking
  if
    | not flushed -> waitFor threadWaitWrite *> handshake tls
    | handshaking == CBool 0 -> pure ()
    | otherwise ->
        readRecords tls >>= \case
          Read -> handshake tls
          WouldBlock -> waitFor threadWaitRead *> handshake tls
          Ended -> throwIO (TlsError "the connection closed during the handshake")
  where
    waitFor wait = withFdSocket (tlsSocket tls) (wait . Fd)

-- | Reads what's ready into the buffer, up to its size, without waiting:
-- 'Nothing' if nothing is, and 0 at the end of the stream.
receiveTls :: TlsConnection -> Ptr Word8 -> Int -> IO (Maybe Int)
receiveTls tls pointer size = go False
  where
    -- rustls asks for initialized memory, for reasons internal to Rust, but
    -- only writes to it.
    go ended = do
      (result, received) <- withConnection tls $ \connection ->
        alloca $ \out -> (,) <$> c_connection_read connection pointer (fromIntegral size) out <*> peek out
      if
        | result == resultOk -> pure (Just (fromIntegral received))
        | result == resultUnexpectedEof -> pure (Just 0)
        | result == resultPlaintextEmpty && ended -> pure (Just 0)
        | result == resultPlaintextEmpty ->
            readRecords tls >>= \case
              Read -> go False
              WouldBlock -> pure Nothing
              Ended -> go True
        | otherwise -> throwIO . TlsError =<< describe result

-- | Encrypts what it can of the pieces, and writes what the socket takes,
-- without waiting. Gives how many bytes of the pieces it took: those not
-- written yet wait in the connection, for 'flushTls'.
sendTls :: TlsConnection -> [ByteString] -> IO Int
sendTls tls = go 0
  where
    go taken pieces = do
      (accepted, rest) <- encrypt 0 pieces
      flushed <- flushTls tls
      if flushed && accepted > 0 && not (null rest)
        then go (taken + accepted) rest
        else pure (taken + accepted)
    encrypt accepted = \case
      [] -> pure (accepted, [])
      piece : others -> do
        written <- withConnection tls $ \connection ->
          BS.unsafeUseAsCStringLen piece $ \(bytes, len) ->
            alloca $ \out -> do
              result <- c_connection_write connection (castPtr bytes) (fromIntegral len) out
              unless (result == resultOk) (throwIO . TlsError =<< describe result)
              fromIntegral <$> peek out
        if written < BS.length piece
          then pure (accepted + written, BS.drop written piece : others)
          else encrypt (accepted + written) others

-- | Writes what the socket takes of the records waiting in the connection,
-- without waiting. True if none is left.
flushTls :: TlsConnection -> IO Bool
flushTls tls = do
  waiting <- withConnection tls c_connection_wants_write
  if waiting == CBool 0
    then pure True
    else do
      errno <- withConnection tls $ \connection ->
        withFdSocket (tlsSocket tls) $ \fd ->
          alloca (c_write connection fd)
      if
        | errno == 0 || Errno errno == eINTR -> flushTls tls
        | Errno errno == eAGAIN || Errno errno == eWOULDBLOCK -> pure False
        | otherwise -> throwIO (errnoToIOError "writev" (Errno errno) Nothing Nothing)

-- | Tells the server the connection ends, if the socket takes it at once,
-- closes the socket, and frees the connection. It mustn't run while another
-- thread uses the connection.
closeTls :: TlsConnection -> IO ()
closeTls tls =
  ( (withConnection tls c_connection_send_close_notify *> void (flushTls tls))
      `catch` \(_ :: IOException) -> pure ()
  )
    `finally` close (tlsSocket tls)
    `finally` finalizeForeignPtr (tlsConnection tls)

-- | What reading records did.
data Records = Read | WouldBlock | Ended

-- | Reads what the socket has into the connection, and decrypts it. A record
-- that fails, or a certificate the verifier rejects, throws 'TlsError',
-- after the alert rustls sends for it if the socket takes it at once.
readRecords :: TlsConnection -> IO Records
readRecords tls = do
  (errno, received) <- withConnection tls $ \connection ->
    withFdSocket (tlsSocket tls) $ \fd ->
      alloca $ \out -> (,) <$> c_read connection fd out <*> peek out
  if
    | errno == 0 && received == 0 -> pure Ended
    | errno == 0 -> do
        result <- withConnection tls c_connection_process_new_packets
        unless (result == resultOk) $ do
          void (flushTls tls) `catch` \(_ :: IOException) -> pure ()
          throwIO . TlsError =<< describe result
        pure Read
    | Errno errno == eINTR -> readRecords tls
    | Errno errno == eAGAIN || Errno errno == eWOULDBLOCK -> pure WouldBlock
    | otherwise -> throwIO (errnoToIOError "recv" (Errno errno) Nothing Nothing)

withConnection :: TlsConnection -> (Ptr RustlsConnection -> IO a) -> IO a
withConnection = withForeignPtr . tlsConnection

-- | What a function stores where its last argument points, once it
-- succeeds. Throws 'TlsError' otherwise.
built :: Storable a => String -> (Ptr () -> IO Word32) -> IO a
built context build = alloca $ \out -> do
  check context =<< build (castPtr out)
  peek out

-- | Throws 'TlsError' unless the result is a success.
check :: String -> Word32 -> IO ()
check context result =
  when (result /= resultOk) $
    throwIO . TlsError . ((context <> ": ") <>) =<< describe result

-- | rustls's description of a result.
describe :: Word32 -> IO String
describe result =
  allocaBytes 512 $ \buffer ->
    alloca $ \out -> do
      c_error (fromIntegral result) buffer 512 out
      len <- peek out
      peekCStringLen (buffer, fromIntegral len)

resultOk, resultUnexpectedEof, resultPlaintextEmpty :: Word32
resultOk = 7000
resultUnexpectedEof = 7010
resultPlaintextEmpty = 7011

data ClientConfigBuilder

data CryptoProvider

data CryptoProviderBuilder

data CertifiedKey

data ClientConfig

data RootCertStoreBuilder

data RootCertStore

data ServerCertVerifierBuilder

data ServerCertVerifier

-- | rustls's state of a connection.
data RustlsConnection

foreign import capi unsafe "rustls.h rustls_crypto_provider_builder_new_from_default"
  c_crypto_provider_builder_new_from_default :: Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_crypto_provider_builder_build"
  c_crypto_provider_builder_build :: Ptr CryptoProviderBuilder -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_crypto_provider_builder_free"
  c_crypto_provider_builder_free :: Ptr CryptoProviderBuilder -> IO ()

foreign import capi unsafe "rustls.h rustls_crypto_provider_free"
  c_crypto_provider_free :: ConstPtr CryptoProvider -> IO ()

foreign import capi unsafe "rustls.h rustls_client_config_builder_new_custom"
  c_client_config_builder_new_custom :: ConstPtr CryptoProvider -> Ptr Word16 -> CSize -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_client_config_builder_set_enable_sni"
  c_client_config_builder_set_enable_sni :: Ptr ClientConfigBuilder -> CBool -> IO ()

foreign import capi unsafe "rustls.h rustls_client_config_builder_free"
  c_client_config_builder_free :: Ptr ClientConfigBuilder -> IO ()

foreign import capi unsafe "rustls.h rustls_client_config_builder_set_server_verifier"
  c_client_config_builder_set_server_verifier :: Ptr ClientConfigBuilder -> ConstPtr ServerCertVerifier -> IO ()

foreign import capi unsafe "rustls.h rustls_certified_key_build"
  c_certified_key_build :: Ptr Word8 -> CSize -> Ptr Word8 -> CSize -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_certified_key_free"
  c_certified_key_free :: ConstPtr CertifiedKey -> IO ()

foreign import capi unsafe "rustls.h rustls_client_config_builder_set_certified_key"
  c_client_config_builder_set_certified_key :: Ptr ClientConfigBuilder -> Ptr () -> CSize -> IO Word32

foreign import capi unsafe "rustls.h rustls_client_config_builder_build"
  c_client_config_builder_build :: Ptr ClientConfigBuilder -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h &rustls_client_config_free"
  p_client_config_free :: FinalizerPtr ClientConfig

foreign import capi unsafe "rustls.h rustls_root_cert_store_builder_new"
  c_root_cert_store_builder_new :: IO (Ptr RootCertStoreBuilder)

foreign import capi unsafe "rustls.h rustls_root_cert_store_builder_load_roots_from_file"
  c_root_cert_store_builder_load_roots_from_file :: Ptr RootCertStoreBuilder -> CString -> CBool -> IO Word32

foreign import capi unsafe "rustls.h rustls_root_cert_store_builder_build"
  c_root_cert_store_builder_build :: Ptr RootCertStoreBuilder -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_root_cert_store_builder_free"
  c_root_cert_store_builder_free :: Ptr RootCertStoreBuilder -> IO ()

foreign import capi unsafe "rustls.h rustls_root_cert_store_free"
  c_root_cert_store_free :: ConstPtr RootCertStore -> IO ()

foreign import capi unsafe "rustls.h rustls_web_pki_server_cert_verifier_builder_new"
  c_web_pki_server_cert_verifier_builder_new :: ConstPtr RootCertStore -> IO (Ptr ServerCertVerifierBuilder)

foreign import capi unsafe "rustls.h rustls_web_pki_server_cert_verifier_builder_add_crl"
  c_web_pki_server_cert_verifier_builder_add_crl :: Ptr ServerCertVerifierBuilder -> Ptr Word8 -> CSize -> IO Word32

foreign import capi unsafe "rustls.h rustls_web_pki_server_cert_verifier_builder_build"
  c_web_pki_server_cert_verifier_builder_build :: Ptr ServerCertVerifierBuilder -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_web_pki_server_cert_verifier_builder_free"
  c_web_pki_server_cert_verifier_builder_free :: Ptr ServerCertVerifierBuilder -> IO ()

foreign import capi unsafe "rustls.h rustls_platform_server_cert_verifier"
  c_platform_server_cert_verifier :: Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_server_cert_verifier_free"
  c_server_cert_verifier_free :: Ptr ServerCertVerifier -> IO ()

foreign import capi unsafe "rustls.h rustls_client_connection_new"
  c_client_connection_new :: ConstPtr ClientConfig -> CString -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_connection_set_buffer_limit"
  c_connection_set_buffer_limit :: Ptr RustlsConnection -> CSize -> IO ()

foreign import capi unsafe "rustls.h rustls_connection_is_handshaking"
  c_connection_is_handshaking :: Ptr RustlsConnection -> IO CBool

foreign import capi unsafe "rustls.h rustls_connection_wants_write"
  c_connection_wants_write :: Ptr RustlsConnection -> IO CBool

foreign import capi unsafe "rustls.h rustls_connection_process_new_packets"
  c_connection_process_new_packets :: Ptr RustlsConnection -> IO Word32

foreign import capi unsafe "rustls.h rustls_connection_read"
  c_connection_read :: Ptr RustlsConnection -> Ptr Word8 -> CSize -> Ptr CSize -> IO Word32

foreign import capi unsafe "rustls.h rustls_connection_write"
  c_connection_write :: Ptr RustlsConnection -> Ptr Word8 -> CSize -> Ptr CSize -> IO Word32

foreign import capi unsafe "rustls.h rustls_connection_send_close_notify"
  c_connection_send_close_notify :: Ptr RustlsConnection -> IO ()

foreign import capi unsafe "rustls.h &rustls_connection_free"
  p_connection_free :: FinalizerPtr RustlsConnection

foreign import capi unsafe "rustls.h rustls_error"
  c_error :: CUInt -> Ptr CChar -> CSize -> Ptr CSize -> IO ()

foreign import capi unsafe "native-pg-tls.h native_pg_tls_accept_any_certificate"
  c_accept_any_certificate :: Ptr ClientConfigBuilder -> IO Word32

foreign import capi unsafe "native-pg-tls.h native_pg_tls_offer_postgresql"
  c_offer_postgresql :: Ptr ClientConfigBuilder -> IO Word32

#if defined(mingw32_HOST_OS)
-- Sockets block on Windows.
foreign import capi safe "native-pg-tls.h native_pg_tls_read"
  c_read :: Ptr RustlsConnection -> CInt -> Ptr CSize -> IO CInt

foreign import capi safe "native-pg-tls.h native_pg_tls_write"
  c_write :: Ptr RustlsConnection -> CInt -> Ptr CSize -> IO CInt
#else
foreign import capi unsafe "native-pg-tls.h native_pg_tls_read"
  c_read :: Ptr RustlsConnection -> CInt -> Ptr CSize -> IO CInt

foreign import capi unsafe "native-pg-tls.h native_pg_tls_write"
  c_write :: Ptr RustlsConnection -> CInt -> Ptr CSize -> IO CInt
#endif
