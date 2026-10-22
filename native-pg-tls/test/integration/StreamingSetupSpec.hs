{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}
module StreamingSetupSpec (spec) where

import Control.Exception (bracket, try)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty (..), toList)
import Data.Maybe (isJust)
import qualified Data.ByteString.Char8 as BS
import qualified Database.PostgreSQL.ConnectionString as CS
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Streaming as Streaming
import Database.PostgreSQL.Tls (tlsClient)
import Database.PostgreSQL.Transport (Address (..))
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources (ResourceSettings (..), withResources)
import qualified Network.Streaming.Resources.Internal as Resources
import qualified Network.Socket as Socket
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
      closeSelected (connection, _, _) = PG.closeConnection connection
      encrypted connecting = connecting { PG.connectTlsClient = Just tlsClient }

  it "authenticates with tiny setup buffers while body storage is busy" $
    withResources settings $ \resources -> Resources.withBuffers resources (positiveInt @2) $ \_ ->
      bracket (Streaming.connect resources (local "native_pg_scram" (Just "scram secret"))) (PG.closeConnection . fst) $ \(connection, info) -> do
        PG.isConnectionUsable connection `shouldReturn` True
        isJust (lookup "server_version" (PG.serverParameters info)) `shouldBe` True
        timeout 1000000 (Resources.withSetupBuffers resources (positiveInt @2) (pure . length)) `shouldReturn` Just 2

  it "returns the complete setup pair after authentication fails" $
    withResources settings $ \resources -> do
      result <- try @PG.ConnectionError (Streaming.connect resources (local "native_pg_scram" (Just "wrong")))
      case result of
        Left (PG.ConnectionRejected _) -> pure ()
        Left failure -> failTest (show failure)
        Right (connection, _) -> PG.closeConnection connection >> failTest "wrong password was accepted"
      timeout 1000000 (Resources.withSetupBuffers resources (positiveInt @2) (pure . length)) `shouldReturn` Just 2

  xfail "pooled host qualification is pending" $ it "closes an unsuitable TLS host and tries the next host using the same setup pair" $
    withResources settings $ \resources -> do
      string <- parse "sslmode=require target_session_attrs=read-write"
      let adjusted = string { CS.connectionHosts = CS.Host "127.0.0.1" Nothing port :| toList (CS.connectionHosts string) }
          adjust connecting = encrypted $ case PG.connectAddress connecting of
            TcpAddress {} -> connecting { PG.connectParameters = ("options", "-c default_transaction_read_only=on") : PG.connectParameters connecting }
            _ -> connecting
      bracket (Streaming.connectFirst resources adjust adjusted) closeSelected $ \(connection, _, host) -> do
        CS.hostName host `shouldBe` takeDirectory socketPath
        PG.isConnectionUsable connection `shouldReturn` True
        timeout 1000000 (Resources.withSetupBuffers resources (positiveInt @2) (pure . length)) `shouldReturn` Just 2

  xfail "pooled standby fallback is pending" $ it "retries primary hosts when prefer-standby finds no standby" $
    withResources settings $ \resources -> do
      string <- parse "target_session_attrs=prefer-standby"
      bracket (Streaming.connectFirst resources id string) closeSelected $ \(connection, _, host) -> do
        CS.hostName host `shouldBe` takeDirectory socketPath
        PG.isConnectionUsable connection `shouldReturn` True
      timeout 1000000 (Resources.withSetupBuffers resources (positiveInt @2) (pure . length)) `shouldReturn` Just 2

  xfail "pooled host failures are pending" $ it "reports failed hosts in order and returns setup storage" $
    withResources settings $ \resources -> do
      string <- parse ""
      let missing = [CS.Host "/missing-native-pg-first" Nothing port, CS.Host "/missing-native-pg-second" Nothing port]
          adjusted = string { CS.connectionHosts = CS.Host "/missing-native-pg-first" Nothing port :| drop 1 missing }
      result <- try @CS.ConnectFailure (Streaming.connectFirst resources id adjusted)
      case result of
        Left (CS.ConnectFailure failures) -> map fst failures `shouldBe` missing
        Right connected -> closeSelected connected >> failTest "missing hosts connected"
      timeout 1000000 (Resources.withSetupBuffers resources (positiveInt @2) (pure . length)) `shouldReturn` Just 2

  it "returns setup storage when a silent startup is cancelled" $
    withResources settings $ \resources ->
      bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \listener -> do
        Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
        Socket.listen listener 1
        silentPort <- Socket.socketPort listener
        outcome <- timeout 100000 (Streaming.connect resources (PG.connectSettings (TcpAddress "127.0.0.1" silentPort) "postgres"))
        traverse_ (PG.closeConnection . fst) outcome
        isJust outcome `shouldBe` False
        timeout 1000000 (Resources.withSetupBuffers resources (positiveInt @2) (pure . length)) `shouldReturn` Just 2

settings :: ResourceSettings
settings = ResourceSettings (positiveInt @32) (positiveInt @32) (positiveInt @16)
