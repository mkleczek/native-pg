{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}
module StreamingSetupSpec (spec) where

import Control.Concurrent.Resource.Waiting qualified as Waiting
import Control.Concurrent (forkFinally, killThread)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar)
import Control.Exception (bracket, try)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty (..), toList)
import Data.Maybe (isJust)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString as Bytes
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy
import qualified Database.PostgreSQL.ConnectionString as CS
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Internal.Execution as PG
import qualified Database.PostgreSQL.Internal.Row as PG
import qualified Database.PostgreSQL.Session.Settings as PG
import qualified Database.PostgreSQL.Diagnostics as PG
import qualified Database.PostgreSQL.Streaming as Streaming
import Database.PostgreSQL.Tls (tlsClient)
import Database.PostgreSQL.Types (TransactionStatus (..))
import Database.PostgreSQL.Streaming (ByteStream (..))
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import Database.PostgreSQL.Transport (Address (..))
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources (ResourceSettings (..), withRequestBuffers)
import qualified Network.Streaming.Resources as Resources
import Network.Streaming.Buffer (Buffer, bufferCapacity, bufferMemory)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Skeletest
import System.FilePath (takeDirectory)
import System.Timeout (timeout)

spec :: FilePath -> Socket.PortNumber -> Spec
spec socketPath port = describe "pooled connection setup" $ do
  let local user password = (PG.connectSettings (UnixAddress socketPath) user)
        { PG.connectPassword = password, PG.connectDatabase = Just "postgres" }
      parse extra = either failTest pure $
        CS.parseConnectionString (CS.Defaults "/tmp" Nothing) (const Nothing) (Just "postgres")
          ("host=" <> BS.pack (takeDirectory socketPath) <> " port=" <> BS.pack (show port) <> " dbname=postgres user=postgres " <> extra)
      closeSelected (connection, _, _) = PG.close connection
      encrypted connecting = connecting { PG.connectTlsClient = Just tlsClient }

  it "authenticates with tiny setup buffers while body storage is busy" $
    withTestResources settings $ \resources -> Resources.withBuffers @(Buffer, Buffer) resources $ \_ ->
      bracket (Streaming.connect resources (local "native_pg_scram" (Just "scram secret"))) (PG.close . fst) $ \(connection, info) -> do
        PG.isUsable connection `shouldReturn` True
        isJust (lookup "server_version" (PG.serverParameters info)) `shouldBe` True
        timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

  it "authenticates SCRAM over TLS with required binding and tiny setup buffers" $
    withTestResources settings $ \resources -> Resources.withBuffers @(Buffer, Buffer) resources $ \_ -> do
      let connecting = encrypted (PG.connectSettings (TcpAddress "127.0.0.1" port) "native_pg_scram")
            { PG.connectDatabase = Just "postgres", PG.connectPassword = Just "scram secret"
            , PG.connectSslMode = PG.SslRequire, PG.connectChannelBinding = PG.ChannelBindingRequire }
      bracket (Streaming.connect resources connecting) (PG.close . fst) $ \(connection, _) -> do
        PG.isUsable connection `shouldReturn` True
        timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

  xfail "Request-context scaffold: shared operation implementation follows" $ it "streams repeated operations after setup storage has been overwritten and loaned again" $
    withTestResources settings $ \resources ->
      bracket (Streaming.connect resources ((local "postgres" Nothing) { PG.connectParameters = [("application_name", "pooled-setup")] })) (PG.close . fst) $ \(connection, info) ->
        Resources.withSetupBuffers @(Buffer, Buffer) resources $ \(input, output) -> do
          traverse_ (\buffer -> withForeignPtr (bufferMemory buffer) $ \pointer ->
            fillBytes pointer 0xaa (bufferCapacity buffer)) [input, output]
          lookup "application_name" (PG.serverParameters info) `shouldBe` Just "pooled-setup"
          let query = PG.Execution (PG.Unprepared "SELECT repeat('x', 100000)" []) [] [] []
          traverse_ (\_ -> withRequestBuffers resources (\access -> Streaming.withOperation access connection (\operation ->
            PG.execute operation query countField 0)
              `shouldReturn` (Right (100000, "SELECT 1"), TxIdle))) [1 :: Int, 2]

  it "returns the complete setup pair after authentication fails" $
    withTestResources settings $ \resources -> do
      result <- try @PG.ConnectionError (Streaming.connect resources (local "native_pg_scram" (Just "wrong")))
      case result of
        Left (PG.ConnectionRejected _) -> pure ()
        Left failure -> failTest (show failure)
        Right (connection, _) -> PG.close connection >> failTest "wrong password was accepted"
      timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

  it "closes an unsuitable TLS host and tries the next host using the same setup pair" $
    withTestResources settings $ \resources -> do
      string <- parse "sslmode=require target_session_attrs=read-write"
      let adjusted = string { CS.connectionHosts = CS.Host "127.0.0.1" Nothing port :| toList (CS.connectionHosts string) }
          adjust connecting = encrypted $ case PG.connectAddress connecting of
            TcpAddress {} -> connecting { PG.connectParameters = ("options", "-c default_transaction_read_only=on") : PG.connectParameters connecting }
            _ -> connecting
      bracket (Streaming.connectFirst resources adjust adjusted) closeSelected $ \(connection, _, host) -> do
        CS.hostName host `shouldBe` takeDirectory socketPath
        PG.isUsable connection `shouldReturn` True
        timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

  it "retries primary hosts when prefer-standby finds no standby" $
    withTestResources settings $ \resources -> do
      string <- parse "target_session_attrs=prefer-standby"
      bracket (Streaming.connectFirst resources id string) closeSelected $ \(connection, _, host) -> do
        CS.hostName host `shouldBe` takeDirectory socketPath
        PG.isUsable connection `shouldReturn` True
      timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

  it "reports failed hosts in order and returns setup storage" $
    withTestResources settings $ \resources -> do
      string <- parse ""
      let missing = [CS.Host "/missing-native-pg-first" Nothing port, CS.Host "/missing-native-pg-second" Nothing port]
          adjusted = string { CS.connectionHosts = CS.Host "/missing-native-pg-first" Nothing port :| drop 1 missing }
      result <- try @CS.ConnectFailure (Streaming.connectFirst resources id adjusted)
      case result of
        Left (CS.ConnectFailure failures) -> map fst failures `shouldBe` missing
        Right connected -> closeSelected connected >> failTest "missing hosts connected"
      timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

  it "uses setup storage for fallback qualification queries when startup omits attributes" $
    withTestResources settings $ \resources -> Resources.withBuffers @(Buffer, Buffer) resources $ \_ ->
      withOlderServer $ \scriptedPort -> do
        string <- either failTest pure $
          CS.parseConnectionString (CS.Defaults "/tmp" Nothing) (const Nothing) (Just "postgres")
            ("host=127.0.0.1 user=postgres dbname=postgres sslmode=disable target_session_attrs=read-write port=" <> BS.pack (show scriptedPort))
        bracket (Streaming.connectFirst resources id string) closeSelected $ \(connection, info, _) -> do
          PG.serverParameters info `shouldBe` []
          PG.isUsable connection `shouldReturn` True
          timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

  it "returns setup storage when a silent startup is cancelled" $
    withTestResources settings $ \resources ->
      bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \listener -> do
        Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
        Socket.listen listener 1
        silentPort <- Socket.socketPort listener
        outcome <- timeout 100000 (Streaming.connect resources (PG.connectSettings (TcpAddress "127.0.0.1" silentPort) "postgres"))
        traverse_ (PG.close . fst) outcome
        isJust outcome `shouldBe` False
        timeout 1000000 (Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure (2 :: Int))) `shouldReturn` Just 2

settings :: ResourceSettings
settings = ResourceSettings (positiveInt @48) (positiveInt @32) (positiveInt @16) (positiveInt @1)

-- A pre-14 startup omits the reported attributes and must answer a SHOW query.
withOlderServer :: (Socket.PortNumber -> IO a) -> IO a
withOlderServer action =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \listener -> do
    Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen listener 1
    port <- Socket.socketPort listener
    done <- newEmptyMVar
    bracket (forkFinally (serve listener) (putMVar done))
      (\thread -> killThread thread >> readMVar done >> pure ()) $ \_ -> do
        result <- timeout 5000000 (action port) >>= maybe (failTest "scripted connection timed out") pure
        outcome <- timeout 1000000 (readMVar done)
        case outcome of
          Just (Right ()) -> pure result
          Just (Left failure) -> failTest (show failure)
          Nothing -> failTest "scripted server did not finish"
  where
    serve listener = bracket (fst <$> Socket.accept listener) Socket.close $ \socket -> do
      size <- messageSize <$> receiveExactly socket 4
      _ <- receiveExactly socket (size - 4)
      SocketBytes.sendAll socket (frame 'R' (bytes (Builder.word32BE 0)) <> frame 'Z' "I")
      frontendUntil socket 'S'
      let row = bytes $ Builder.word16BE 1 <> Builder.word32BE 3 <> Builder.byteString "off"
      SocketBytes.sendAll socket $
        frame '1' "" <> frame '2' ""
          <> frame 'D' row <> frame 'C' "SHOW\0" <> frame 'Z' "I"
      frontendUntil socket 'X'
    frontendUntil socket wanted = do
      kind <- receiveExactly socket 1
      size <- messageSize <$> receiveExactly socket 4
      _ <- receiveExactly socket (size - 4)
      if kind == BS.singleton wanted then pure () else frontendUntil socket wanted
    frame kind payload = BS.singleton kind <> bytes (Builder.word32BE (fromIntegral (BS.length payload + 4))) <> payload
    bytes = Lazy.toStrict . Builder.toLazyByteString
    messageSize = Bytes.foldl' (\total byte -> total * 256 + fromIntegral byte) 0

receiveExactly :: Socket.Socket -> Int -> IO BS.ByteString
receiveExactly socket = go []
  where
    go chunks 0 = pure (BS.concat (reverse chunks))
    go chunks remaining = do
      chunk <- SocketBytes.recv socket remaining
      if BS.null chunk then fail "scripted peer closed early"
        else go (chunk : chunks) (remaining - BS.length chunk)

countField :: Int -> PG.Row -> IO Int
countField count row = PG.nextColumn row >>= \case
  Just (PG.Column _) -> PG.columnStream row >>= drain count
  _ -> failTest "expected one non-null streamed field"
  where
    drain total stream = do
      chunk <- byteStreamNext stream
      if BS.null chunk then pure total else do
        BS.all (== 'x') chunk `shouldBe` True
        drain (total + BS.length chunk) stream

withTestResources :: ResourceSettings -> (Resources.Resources -> IO a) -> IO a
withTestResources settings action = Waiting.withWaiting $ \waiting ->
  Resources.withResources waiting settings action
