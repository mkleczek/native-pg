{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}
module OperationContextSpec (spec) where

import Control.Exception (bracket)
import qualified Data.ByteString as BS
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Database.PostgreSQL.Diagnostics (Diagnostics (..))
import qualified Database.PostgreSQL.Streaming as Streaming
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources (ResourceSettings (..), withResources)
import qualified Network.Streaming.Resources.Internal as Resources
import qualified Database.PostgreSQL.Session as PG
import Database.PostgreSQL.Transport (Address (..))
import Database.PostgreSQL.Tls (tlsClient)
import qualified Database.PostgreSQL.Transaction as Tx
import Network.Socket (PortNumber)
import Skeletest hiding (context)

spec :: PortNumber -> Spec
spec port = describe "explicit contexts over PostgreSQL TLS" $ do
  it "authenticates and streams with four-byte workspace and larger owned metadata" $
    withConnection $ \connection input output -> do
      total <- newIORef 0
      let statement = Tx.Statement "SELECT 42::bigint, repeat('h', 96), repeat('x', 256)" (const [])
            (Tx.singleRow (Tx.scopedRow $ \current ->
              Tx.decodeScopedStreamingNullable @'[Int64, BS.ByteString] current $ \count headers stream -> do
                maybe (failTest "unexpected NULL") (\source -> Tx.streamChunks source (modifyIORef' total . (+) . BS.length)) stream
                pure (count, headers))) False
      result <- Streaming.withBuffers input output connection $ \context ->
        Tx.transactionNoRetryIn Tx.ReadCommitted Tx.Read (Tx.statement () statement) context
      result `shouldBe` Right (42, BS.replicate 96 104)
      readIORef total `shouldReturn` 256
      PG.isConnectionUsable connection `shouldReturn` True

  it "reports deferred COMMIT failure after a borrowed body was delivered" $
    withConnection $ \connection input output -> do
      delivered <- newIORef False
      let statement = Tx.Statement "SELECT repeat('x', 256)" (const [])
            (Tx.singleRow (Tx.scopedRow $ \current ->
              Tx.decodeScopedStreamingNullable @'[] current $ \stream -> do
                maybe (failTest "unexpected NULL") (\source -> Tx.streamChunks source (const (pure ()))) stream
                writeIORef delivered True)) False
          transaction = do
            Tx.sql "CREATE TEMP TABLE context_parent (id int PRIMARY KEY)"
            Tx.sql "CREATE TEMP TABLE context_child (id int REFERENCES context_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            Tx.sql "INSERT INTO context_child VALUES (1)"
            Tx.statement () statement
      result <- Streaming.withBuffers input output connection $ \context ->
        Tx.transactionNoRetryIn Tx.ReadCommitted Tx.Write transaction context
      readIORef delivered `shouldReturn` True
      case result of
        Left (Tx.QueryError query (Tx.ServerFailed (PG.ServerError fields))) -> do
          query `shouldBe` "COMMIT"
          diagnosticCode fields `shouldBe` "23503"
        _ -> failTest "expected the deferred foreign-key failure from COMMIT"
  where
    settings = (PG.connectSettings (TcpAddress "127.0.0.1" port) "native_pg_scram")
      { PG.connectPassword = Just "scram secret",
        PG.connectDatabase = Just "postgres",
        PG.connectSslMode = PG.SslRequire,
        PG.connectTlsClient = Just tlsClient
      }
    withConnection use =
      withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @4)) $ \resources ->
        bracket (Streaming.connect resources settings) (PG.closeConnection . fst) $ \(connection, _) ->
          Resources.withBuffers resources (positiveInt @2) $ \buffers -> case buffers of
            [input, output] -> use connection input output
            _ -> failTest "expected an input/output buffer pair"
