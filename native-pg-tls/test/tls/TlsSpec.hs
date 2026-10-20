{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE LambdaCase #-}

-- | TLS connections against a server in the same process, over a pair of
-- Unix-domain sockets. The server is rustls too, through its server API, and
-- its side of a connection runs on the same functions as the client's.
-- Each side has exclusive access to its own TLS connection.
module TlsSpec (spec) where

import Control.Concurrent.Async (concurrently)
import Control.Exception (bracket, displayException, throwIO, try)
import Control.Monad (replicateM_, unless, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.Word (Word32, Word8)
import Database.PostgreSQL.Tls.Internal
import Database.PostgreSQL.Transport (Link (..), RootCertificates (..), ServerVerification (..), TlsChannel (..), TlsClient (..), TlsError (..), TlsSettings (..))
import Foreign.C.Types (CSize (..))
import Foreign.Marshal.Alloc (alloca, allocaBytes)
import Foreign.Marshal.Array (withArray)
import Foreign.Marshal.Utils (fillBytes)
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable (peek)
import qualified Network.Socket as Socket
import Skeletest
import qualified Skeletest.Predicate as P
import System.Mem (performMajorGC)

spec :: Spec
spec = do
  describe "the handshake" $ do
    it "verifies the server's certificate against the roots, and its name" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost"
        void $ concurrently (startTls config sock) (handshake server)

    it "keeps the server name alive while waiting for the server" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost"
        let serve = do
              -- The client has built its native connection and is now waiting
              -- for the server. The constructor's temporary storage is gone.
              awaitReadable server
              reclaimTemporaryBuffers
              handshake server
        void $ concurrently (startTls config sock) serve

    it "connects to any server without verification" $
      withServer "server-other-name" $ \sock server -> do
        config <- newTlsConfig (settings NoVerification) "localhost"
        void $ concurrently (startTls config sock) (handshake server)

    it "refuses a certificate that no root signed" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "other-ca.crt")))) "localhost"
        refusal config sock server `shouldReturn` "SSL error: invalid peer certificate: UnknownIssuer"

    it "refuses a certificate for another name" $
      withServer "server-other-name" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost"
        refusal config sock server `shouldReturn` "SSL error: invalid peer certificate: NotValidForName"

  describe "TLS setup" $
    it "supplies the server-end-point hash separately from the channel" $
      withServer "server" $ \sock server -> do
        (opened, ()) <-
          concurrently
            (startTlsChannel tlsClient (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost" sock)
            (handshake server)
        let expected = BS.pack
              [ 0xff, 0xc8, 0xc8, 0xc2, 0x06, 0xfe, 0xe8, 0x34,
                0xbd, 0xe2, 0x7d, 0x21, 0x19, 0x94, 0xf9, 0xa8,
                0xb5, 0xbc, 0x44, 0x9c, 0x0e, 0xcd, 0xdb, 0x35,
                0xdf, 0x38, 0x75, 0x72, 0xb0, 0x29, 0x51, 0x88
              ]
        tlsServerEndPoint opened `shouldBe` Just (Right expected)

  describe "a connection" $ do
    it "carries more bytes both ways than the sockets hold" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost"
        let payload = BS.pack (take 1000000 (cycle [0 .. 250]))
            client = do
              tls <- startTls config sock
              sendAll tls [BS.take 300000 payload, BS.drop 300000 payload]
              receiveExactly tls (BS.length payload)
            echo = do
              handshake server
              sendAll server . pure =<< receiveExactly server (BS.length payload)
        fst <$> concurrently client echo `shouldReturn` payload

    it "keeps the server name alive for tickets after setup returns" $
      withServer "server" $ \sock server -> do
        -- Only the returned connection remains live during subsequent I/O.
        (client, ()) <-
          concurrently
            ( do
                config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost"
                startTls config sock
            )
            (handshake server)
        reclaimTemporaryBuffers
        let payload = BS.replicate 32768 42
        -- TLS 1.3's session tickets precede these application bytes.
        snd <$> concurrently (sendAll server [payload]) (receiveExactly client (BS.length payload))
          `shouldReturn` payload
        closeLink client

    it "ends the server's stream when it closes" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost"
        (client, ()) <- concurrently (startTls config sock) (handshake server)
        closeLink client
        receiveExactly server 1 `shouldReturn` BS.empty

  describe "a configuration" $ do
    it "keeps cached server names alive after a connection closes" $ do
      config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt")))) "localhost"
      replicateM_ 2 $ do
        withServer "server" $ \sock server -> do
          (client, ()) <- concurrently (startTls config sock) (handshake server)
          snd <$> concurrently (sendAll server [BS.singleton 42]) (receiveExactly client 1)
            `shouldReturn` BS.singleton 42
          closeLink client
        reclaimTemporaryBuffers

    it "needs its root certificate file" $
      newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "missing.crt")))) "localhost"
        `shouldSatisfy` P.throws (P.con (TlsError (P.hasInfix "could not read root certificate file")))

-- | Reclaim temporary pinned buffers and exercise reuse of their blocks.
-- Small allocations matter here: a single large allocation uses different
-- storage from the CString passed to rustls's connection constructor.
reclaimTemporaryBuffers :: IO ()
reclaimTemporaryBuffers = replicateM_ 4 $ do
  performMajorGC
  replicateM_ 1024 (allocaBytes 1024 $ \buffer -> fillBytes buffer 120 1024)

-- | Sends the pieces, waiting for room in the socket as needed.
sendAll :: TlsConnection -> [ByteString] -> IO ()
sendAll tls pieces = do
  taken <- sendReady tls pieces
  flushed <- flushReady tls
  let rest = dropBytes taken pieces
  unless (null rest && flushed) $ do
    awaitWritable tls
    sendAll tls rest
  where
    dropBytes taken = \case
      piece : others
        | taken >= BS.length piece -> dropBytes (taken - BS.length piece) others
        | otherwise -> BS.drop taken piece : others
      [] -> []

-- | Reads the given number of bytes, waiting for them as needed, or fewer at
-- the end of the stream.
receiveExactly :: TlsConnection -> Int -> IO ByteString
receiveExactly tls count = BS.concat . reverse <$> go 0 []
  where
    go got chunks
      | got >= count = pure chunks
      | otherwise =
          allocaBytes 16384 $ \buffer ->
            receiveReady tls buffer 16384 >>= \case
              Just 0 -> pure chunks
              Just received -> do
                chunk <- BS.packCStringLen (castPtr buffer, received)
                go (got + received) (chunk : chunks)
              Nothing -> do
                awaitReadable tls
                go got chunks

-- | The settings with the verification, and rustls's defaults otherwise.
settings :: ServerVerification -> TlsSettings
settings verification = TlsSettings verification Nothing [] [minBound .. maxBound] True False

certificate :: FilePath -> FilePath
certificate name = "test/certificates/" <> name

-- | How the client's handshake with the server fails.
refusal :: TlsConfig -> Socket.Socket -> TlsConnection -> IO String
refusal config sock server = do
  (client, _) <- concurrently (try (startTls config sock)) (try @TlsError (handshake server))
  either (pure . displayException @TlsError) (\_ -> fail "the handshake succeeded") client

-- | A connected pair of sockets: the client's, and the server's TLS
-- connection with the named certificate, before its handshake.
withServer :: String -> (Socket.Socket -> TlsConnection -> IO a) -> IO a
withServer name action =
  bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) closeBoth $ \(clientSocket, serverSocket) -> do
    chain <- BS.readFile (certificate (name <> ".crt"))
    key <- BS.readFile (certificate (name <> ".key"))
    server <- newTlsConnection serverSocket =<< serverConnection chain key
    action clientSocket server
  where
    closeBoth (a, b) = Socket.close a *> Socket.close b

-- | A server's connection with the certificate chain and key.
serverConnection :: ByteString -> ByteString -> IO (Ptr RustlsConnection)
serverConnection chain key = do
  certified :: Ptr CertifiedKey <-
    BS.unsafeUseAsCStringLen chain $ \(chainBytes, chainLength) ->
      BS.unsafeUseAsCStringLen key $ \(keyBytes, keyLength) ->
        built (c_certified_key_build (castPtr chainBytes) (fromIntegral chainLength) (castPtr keyBytes) (fromIntegral keyLength))
  builder <- c_server_config_builder_new
  withArray [certified] $ \keys -> succeeds (c_server_config_builder_set_certified_keys builder (castPtr keys) 1)
  config <- built (c_server_config_builder_build builder)
  built (c_server_connection_new config)
  where
    built :: (Ptr () -> IO Word32) -> IO (Ptr a)
    built function = alloca $ \out -> succeeds (function (castPtr out)) *> peek out
    succeeds function = do
      result <- function
      unless (result == 7000) (throwIO (TlsError ("the test server's configuration failed with " <> show result)))

data CertifiedKey

data ServerConfigBuilder

data ServerConfig

foreign import capi unsafe "rustls.h rustls_certified_key_build"
  c_certified_key_build :: Ptr Word8 -> CSize -> Ptr Word8 -> CSize -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_server_config_builder_new"
  c_server_config_builder_new :: IO (Ptr ServerConfigBuilder)

foreign import capi unsafe "rustls.h rustls_server_config_builder_set_certified_keys"
  c_server_config_builder_set_certified_keys :: Ptr ServerConfigBuilder -> Ptr () -> CSize -> IO Word32

foreign import capi unsafe "rustls.h rustls_server_config_builder_build"
  c_server_config_builder_build :: Ptr ServerConfigBuilder -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_server_connection_new"
  c_server_connection_new :: Ptr ServerConfig -> Ptr () -> IO Word32
