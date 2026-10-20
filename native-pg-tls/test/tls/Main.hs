{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE LambdaCase #-}

-- | TLS connections against a server in the same process, over a pair of
-- Unix-domain sockets. The server is rustls too, through its server API, and
-- its side of a connection runs on the same functions as the client's.
-- Each side has exclusive access to its own TLS connection.
module Main (main) where

import Control.Concurrent (threadWaitRead, threadWaitWrite)
import Control.Concurrent.Async (concurrently)
import Control.Exception (bracket, displayException, throwIO, try)
import Control.Monad (unless, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.List (isInfixOf)
import Data.Word (Word32, Word8)
import Database.PostgreSQL.Tls.Internal
import Database.PostgreSQL.Transport (Link (..), RootCertificates (..), ServerVerification (..), TlsError (..), TlsSettings (..))
import Foreign.C.Types (CSize (..))
import Foreign.Marshal.Alloc (alloca, allocaBytes)
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable (peek)
import qualified Network.Socket as Socket
import System.Posix.Types (Fd (..))
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "the handshake" $ do
    it "verifies the server's certificate against the roots, and its name" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt"))))
        void $ concurrently (startTls config "localhost" sock) (handshake server)

    it "connects to any server without verification" $
      withServer "server-other-name" $ \sock server -> do
        config <- newTlsConfig (settings NoVerification)
        void $ concurrently (startTls config "localhost" sock) (handshake server)

    it "refuses a certificate that no root signed" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "other-ca.crt"))))
        refusal config sock server `shouldReturn` "SSL error: invalid peer certificate: UnknownIssuer"

    it "refuses a certificate for another name" $
      withServer "server-other-name" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt"))))
        refusal config sock server `shouldReturn` "SSL error: invalid peer certificate: NotValidForName"

  describe "a connection" $ do
    it "carries more bytes both ways than the sockets hold" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt"))))
        let payload = BS.pack (take 1000000 (cycle [0 .. 250]))
            client = do
              tls <- startTls config "localhost" sock
              sendAll tls [BS.take 300000 payload, BS.drop 300000 payload]
              receiveExactly tls (BS.length payload)
            echo = do
              handshake server
              sendAll server . pure =<< receiveExactly server (BS.length payload)
        fst <$> concurrently client echo `shouldReturn` payload

    it "ends the server's stream when it closes" $
      withServer "server" $ \sock server -> do
        config <- newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "ca.crt"))))
        (client, ()) <- concurrently (startTls config "localhost" sock) (handshake server)
        closeLink client
        receiveExactly server 1 `shouldReturn` BS.empty

  describe "a configuration" $
    it "needs its root certificate file" $
      newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "missing.crt"))))
        `shouldThrow` \(TlsError reason) -> "could not read root certificate file" `isInfixOf` reason

-- | Sends the pieces, waiting for room in the socket as needed.
sendAll :: TlsConnection -> [ByteString] -> IO ()
sendAll tls pieces = do
  taken <- sendReady tls pieces
  flushed <- flushReady tls
  let rest = dropBytes taken pieces
  unless (null rest && flushed) $ do
    Socket.withFdSocket (linkSocket tls) (threadWaitWrite . Fd)
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
                Socket.withFdSocket (linkSocket tls) (threadWaitRead . Fd)
                go got chunks

-- | The settings with the verification, and rustls's defaults otherwise.
settings :: ServerVerification -> TlsSettings
settings verification = TlsSettings verification Nothing [] [minBound .. maxBound] True False

certificate :: FilePath -> FilePath
certificate name = "test/certificates/" <> name

-- | How the client's handshake with the server fails.
refusal :: TlsConfig -> Socket.Socket -> TlsConnection -> IO String
refusal config sock server = do
  (client, _) <- concurrently (try (startTls config "localhost" sock)) (try @TlsError (handshake server))
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
