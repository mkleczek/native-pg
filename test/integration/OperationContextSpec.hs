{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}
module OperationContextSpec (spec) where

import Control.Monad.IO.Unlift (withRunInIO)
import AcquisitionTestSupport (getAcquisition)
import Control.Exception (bracket, try)
import Control.Monad (unless)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import Network.Streaming (Source (..))
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import Database.PostgreSQL.Transaction (Param (..))
import Database.PostgreSQL.Types (FormatCode (..))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Database.PostgreSQL.Diagnostics (Diagnostics (..))
import qualified Database.PostgreSQL.Streaming as Streaming
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool (PoolSettings (..))
import qualified Network.Streaming.BufferPool as BufferPool
import Network.Streaming.Buffer (bufferMemory, bufferCapacity)
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Session.Settings as PG
import qualified Database.PostgreSQL.Diagnostics as PG
import Database.PostgreSQL.Transport (Address (..))
import Database.PostgreSQL.Tls (tlsClient)
import qualified Database.PostgreSQL.Transaction as Tx
import Data.Functor.Const (Const (..))
import Network.Socket (PortNumber)
import Skeletest hiding (context)

spec :: PortNumber -> Spec
spec port = describe "explicit contexts over PostgreSQL TLS" $ do
  it "authenticates and streams with four-byte workspace and larger owned metadata" $
    withConnection $ \connection input output -> do
      total <- newIORef 0
      let statement = Tx.Statement "SELECT 42::bigint, repeat('h', 96), repeat('x', 256)" (const [])
            (Tx.singleRow (Tx.streamingRow @'[Int64, BS.ByteString] Headers)) False
      result <- Streaming.withBuffers input output connection $ \session ->
        Tx.runTransaction Tx.ReadCommitted Tx.Read session $ withRunInIO $ \run -> do
          cursor <- run (Tx.pipeline (Tx.statement () statement))
          current <- run (Tx.nextRow cursor)
          value <- case current of
            Just (Headers count headers (Just field)) -> do
              drain (run (Tx.nextChunk field)) (modifyIORef' total . (+) . BS.length)
              pure (count, headers)
            _ -> failTest "unexpected NULL"
          run Tx.finishTransaction
          pure value
      result `shouldBe` (42, BS.replicate 96 104)
      readIORef total `shouldReturn` 256
      PG.isUsable connection `shouldReturn` True

  xfail "Request-context scaffold: shared operation implementation follows" $ it "shares one primary across a borrowed upload and streamed TLS response" $
    withTestResources (PoolSettings
      { bufferSize = positiveInt @256
      , ordinaryBufferCount = positiveInt @1
      , reserveBufferCount = positiveInt @1
      , setupBufferCount = positiveInt @2
      }) $ \resources ->
      bracket (Streaming.connect resources settings) (PG.close . fst) $ \(connection, _) ->
        BufferPool.withRequestBuffers resources $ \access -> do
          remaining <- newIORef 16385
          total <- newIORef 0
          let source = Source $ \consume -> do
                left <- readIORef remaining
                if left == 0 then consume BS.empty else bracket (BufferPool.borrowBuffer access) BufferPool.releaseBuffer $ \loan -> do
                  let buffer = BufferPool.loanBuffer loan
                      count = min left (bufferCapacity buffer)
                  withForeignPtr (bufferMemory buffer) $ \pointer -> fillBytes pointer 120 count
                  result <- consume (BSI.BS (bufferMemory buffer) count)
                  writeIORef remaining (left - count)
                  pure result
              statement = Tx.Statement "SELECT $1::bytea"
                (const [Param (Just 17) BinaryFormat (Just (ParameterBorrowedStream 16385 source))])
                (Tx.singleRow (Tx.streamingRow @'[] Body)) False
          Streaming.withOperation access connection $ \session ->
            Tx.runTransaction Tx.ReadCommitted Tx.Read session $ withRunInIO $ \run -> do
              cursor <- run (Tx.pipeline (Tx.statement () statement))
              current <- run (Tx.nextRow cursor)
              case current of
                Just (Body (Just field)) -> drain (run (Tx.nextChunk field)) $ \chunk -> do
                  BS.all (== 120) chunk `shouldBe` True
                  modifyIORef' total (+ BS.length chunk)
                _ -> failTest "unexpected NULL"
              run Tx.finishTransaction
          readIORef total `shouldReturn` 16385
          readIORef remaining `shouldReturn` 0
          PG.isUsable connection `shouldReturn` True

  it "reports deferred COMMIT failure after a borrowed body was delivered" $
    withConnection $ \connection input output -> do
      delivered <- newIORef False
      let statement = Tx.Statement "SELECT repeat('x', 256)" (const [])
            (Tx.singleRow (Tx.streamingRow @'[] Body)) False
      result <- Streaming.withBuffers input output connection $ \session ->
        try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Write session $ withRunInIO $ \run -> do
          run (Tx.sql "CREATE TEMP TABLE context_parent (id int PRIMARY KEY)")
          run (Tx.sql "CREATE TEMP TABLE context_child (id int REFERENCES context_parent(id) DEFERRABLE INITIALLY DEFERRED)")
          run (Tx.sql "INSERT INTO context_child VALUES (1)")
          cursor <- run (Tx.pipeline (Tx.statement () statement))
          current <- run (Tx.nextRow cursor)
          case current of
            Just (Body (Just field)) -> drain (run (Tx.nextChunk field)) (const (pure ()))
            _ -> failTest "unexpected NULL"
          writeIORef delivered True
          run Tx.finishTransaction
      readIORef delivered `shouldReturn` True
      case result of
        Left (Tx.QueryError query (Tx.ServerFailed (PG.ServerError fields))) -> do
          query `shouldBe` "COMMIT"
          diagnosticCode fields `shouldBe` "23503"
        _ -> failTest "expected the deferred foreign-key failure from COMMIT"
  it "pulls metadata and a borrowed body and commits inside its callback" $
    withConnection $ \connection input output -> do
      total <- newIORef 0
      let statement = Tx.Statement "SELECT 42::bigint, repeat('x', 256)" (const [])
            (Tx.singleRow (Tx.streamingRow @'[Int64] PullBody)) False
      Streaming.withBuffers input output connection $ \session ->
        Tx.runTransaction Tx.ReadCommitted Tx.Read session $ withRunInIO $ \run -> do
          cursor <- run (Tx.pipeline (Tx.statement () statement))
          result <- run (Tx.nextRow cursor)
          case result of
            Just (PullBody count (Just field)) -> do
              count `shouldBe` 42
              Tx.fieldLength field `shouldBe` 256
              drain (run (Tx.nextChunk field)) (modifyIORef' total . (+) . BS.length)
            _ -> failTest "expected the streamed row"
          run Tx.finishTransaction
          readIORef total `shouldReturn` 256
      PG.isUsable connection `shouldReturn` True

  it "rolls back a pull COMMIT failure and reuses the session" $
    withConnection $ \connection input output -> do
      delivered <- newIORef False
      let statement = Tx.Statement "SELECT 42::bigint, repeat('x', 256)" (const [])
            (Tx.singleRow (Tx.streamingRow @'[Int64] PullBody)) False
      result <- Streaming.withBuffers input output connection $ \session ->
        try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Write session $ withRunInIO $ \run -> do
          run (Tx.sql "CREATE TEMP TABLE pull_parent (id int PRIMARY KEY)")
          run (Tx.sql "CREATE TEMP TABLE pull_child (id int REFERENCES pull_parent(id) DEFERRABLE INITIALLY DEFERRED)")
          run (Tx.sql "INSERT INTO pull_child VALUES (1)")
          cursor <- run (Tx.pipeline (Tx.statement () statement))
          row <- run (Tx.nextRow cursor)
          case row of
            Just (PullBody _ (Just field)) -> drain (run (Tx.nextChunk field)) (const (pure ()))
            _ -> failTest "expected the streamed row"
          writeIORef delivered True
          run Tx.finishTransaction
      readIORef delivered `shouldReturn` True
      case result of
        Left (PG.QueryError "COMMIT" (PG.ServerFailed (PG.ServerError fields))) ->
          diagnosticCode fields `shouldBe` "23503"
        _ -> failTest "expected the deferred foreign-key failure from COMMIT"
      PG.isUsable connection `shouldReturn` True
      let reused = Tx.Statement "SELECT 42::bigint" (const []) (Tx.singleRow (Tx.row @'[Int64] Const)) False
      Streaming.withBuffers input output connection (\session ->
        Tx.runTransaction Tx.ReadCommitted Tx.Read session $ withRunInIO $ \run -> do
          value <- run (Tx.pipeline (Tx.statement () reused) >>= Tx.singleRowValue)
          run Tx.finishTransaction
          pure value) `shouldReturn` 42
  where
    settings = (PG.connectSettings (TcpAddress "127.0.0.1" port) "native_pg_scram")
      { PG.connectPassword = Just "scram secret",
        PG.connectDatabase = Just "postgres",
        PG.connectSslMode = PG.SslRequire,
        PG.connectTlsClient = Just tlsClient
      }
    withConnection use =
      withTestResources (PoolSettings
      { bufferSize = positiveInt @4
      , ordinaryBufferCount = positiveInt @2
      , reserveBufferCount = positiveInt @1
      , setupBufferCount = positiveInt @2
      }) $ \resources ->
        bracket (Streaming.connect resources settings) (PG.close . fst) $ \(connection, _) ->
          BufferPool.withRequestBuffers resources $ \access ->
            bracket (BufferPool.borrowBuffer access) BufferPool.releaseBuffer $ \input ->
              bracket (BufferPool.borrowBuffer access) BufferPool.releaseBuffer $ \output ->
                use connection (BufferPool.loanBuffer input) (BufferPool.loanBuffer output)

withTestResources :: PoolSettings -> (BufferPool.BufferPool -> IO a) -> IO a
withTestResources settings action = getAcquisition >>= \waiting ->
  BufferPool.newBufferPool waiting settings >>= action

newtype Body tx = Body (Maybe (Tx.Field tx))
data Headers tx = Headers Int64 BS.ByteString (Maybe (Tx.Field tx))
data PullBody tx = PullBody Int64 (Maybe (Tx.Field tx))

drain :: IO BS.ByteString -> (BS.ByteString -> IO ()) -> IO ()
drain source use = do
  chunk <- source
  unless (BS.null chunk) (use chunk *> drain source use)
