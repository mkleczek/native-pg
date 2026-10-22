{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}
module OperationContextSpec (spec) where

import Control.Exception (bracket, try)
import Control.Monad (unless)
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
import qualified Database.PostgreSQL.Transaction.Pull as Pull
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
  xfail "pull transaction execution is implemented in the following API implementation" $ it "pulls metadata and a borrowed body and commits inside its callback" $
    withConnection $ \connection input output -> do
      total <- newIORef 0
      let statement = Pull.Statement "SELECT 42::bigint, repeat('x', 256)" (const [])
            (Pull.singleRow (Pull.streamingRow @'[Int64] PullBody)) False
      Streaming.withBuffers input output connection $ \context ->
        Pull.withTransaction Pull.ReadCommitted Pull.Read (PG.sessionFromContext context) $ \run -> do
          cursor <- run (Pull.pipeline (Pull.statement () statement))
          result <- run (Pull.nextRow cursor)
          case result of
            Just (PullBody count (Just field)) -> do
              count `shouldBe` 42
              Pull.fieldLength field `shouldBe` 256
              drain (run (Pull.nextChunk field)) (modifyIORef' total . (+) . BS.length)
            _ -> failTest "expected the streamed row"
          run Pull.finishTransaction
          readIORef total `shouldReturn` 256
      PG.isConnectionUsable connection `shouldReturn` True

  xfail "pull transaction execution is implemented in the following API implementation" $ it "rolls back a pull COMMIT failure and reuses the session" $
    withConnection $ \connection input output -> do
      delivered <- newIORef False
      let statement = Pull.Statement "SELECT 42::bigint, repeat('x', 256)" (const [])
            (Pull.singleRow (Pull.streamingRow @'[Int64] PullBody)) False
      result <- Streaming.withBuffers input output connection $ \context ->
        try @PG.QueryError $ Pull.withTransaction Pull.ReadCommitted Pull.Write (PG.sessionFromContext context) $ \run -> do
          run (Pull.sql "CREATE TEMP TABLE pull_parent (id int PRIMARY KEY)")
          run (Pull.sql "CREATE TEMP TABLE pull_child (id int REFERENCES pull_parent(id) DEFERRABLE INITIALLY DEFERRED)")
          run (Pull.sql "INSERT INTO pull_child VALUES (1)")
          cursor <- run (Pull.pipeline (Pull.statement () statement))
          row <- run (Pull.nextRow cursor)
          case row of
            Just (PullBody _ (Just field)) -> drain (run (Pull.nextChunk field)) (const (pure ()))
            _ -> failTest "expected the streamed row"
          writeIORef delivered True
          run Pull.finishTransaction
      readIORef delivered `shouldReturn` True
      case result of
        Left (PG.QueryError "COMMIT" (PG.ServerFailed (PG.ServerError fields))) ->
          diagnosticCode fields `shouldBe` "23503"
        _ -> failTest "expected the deferred foreign-key failure from COMMIT"
      PG.isConnectionUsable connection `shouldReturn` True
      let reused = Tx.Statement "SELECT 42::bigint" (const []) (Tx.singleRow (Tx.row @'[Int64] id)) False
      Streaming.withBuffers input output connection
        (Tx.transactionNoRetryIn Tx.ReadCommitted Tx.Read (Tx.statement () reused)) `shouldReturn` Right 42
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
          Resources.withBuffers @(Resources.Buffer, Resources.Buffer) resources $ \(input, output) -> use connection input output

data PullBody tx = PullBody Int64 (Maybe (Pull.Field tx))

drain :: IO BS.ByteString -> (BS.ByteString -> IO ()) -> IO ()
drain source use = do
  chunk <- source
  unless (BS.null chunk) (use chunk *> drain source use)
