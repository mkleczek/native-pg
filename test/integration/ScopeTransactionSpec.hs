{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | Operations and transactions in a scope, against a real server and a pool
-- of real connections.
module ScopeTransactionSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (ErrorCall (..), SomeException, evaluate, finally, throwIO, try)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString as BS
import Data.Bifunctor (first)
import Data.Either (isLeft, isRight)
import Data.Functor ((<&>))
import Data.Functor.Const (Const (..))
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import qualified Data.ByteString.Char8 as BC
import Data.Text (Text, pack)
import qualified Database.PostgreSQL.Diagnostics as PG
import Database.PostgreSQL.Pool (Config (..), Checkout)
import qualified Database.PostgreSQL.Pool as Pool
import Database.PostgreSQL.Pool.Observation
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Session.Settings as PG
import qualified Database.PostgreSQL.Streaming as Streaming
import Database.PostgreSQL.Transport (Address (..))
import Database.PostgreSQL.Tls (tlsClient)
import qualified Database.PostgreSQL.Transaction as Tx
import GHC.Clock (getMonotonicTime)
import Network.Socket (PortNumber)
import Network.Streaming.BufferPool (PoolSettings (..))
import qualified Network.Streaming.BufferPool as BufferPool
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Source (WaitingSource (..))
import Network.Streaming.Watch (awaitWatch)
import Network.Streaming.Scope (withScope)
import Skeletest hiding (context)
import System.Timeout (timeout)

spec :: PortNumber -> Spec
spec port = describe "operations and transactions in a scope" $ do
  describe "when the scope completes" $ do
    it "commits the transaction, and gives the connection back as succeeded" $ withEnv port 1 $ \env -> do
      run env "CREATE TEMP TABLE scope_commit (id int)"
      inRequest env Tx.Write $ \_ tx -> Tx.runIn tx (Tx.sql "INSERT INTO scope_commit VALUES (1)")
      lastNames env 2 `shouldReturn` ["in use", "ready: succeeded"]
      count env "scope_commit" `shouldReturn` 1

    it "sends nothing for a transaction that did nothing" $ withEnv port 1 $ \env -> do
      inRequest env Tx.Read $ \_ _ -> pure ()
      lastNames env 2 `shouldReturn` ["in use", "ready: succeeded"]
      (PG.isUsable =<< connectionOf env) `shouldReturn` True

    it "is done with a transaction that finished itself" $ withEnv port 1 $ \env -> do
      run env "CREATE TEMP TABLE scope_finished (id int)"
      inRequest env Tx.Write $ \_ tx -> do
        Tx.runIn tx (Tx.sql "INSERT INTO scope_finished VALUES (1)" *> Tx.finishTransaction)
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      count env "scope_finished" `shouldReturn` 1

    it "drains a field that was not read, and keeps the connection" $ withEnv port 1 $ \env -> do
      inRequest env Tx.Read $ \_ tx -> Tx.runIn tx $ do
        field <- firstField =<< Tx.pipeline (Tx.statement () (bodyOf 5000))
        liftIO (Tx.fieldLength field `shouldBe` 5000)
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      opened env `shouldReturn` 1

    it "rolls back a transaction whose failure the code handled, without failing the scope" $ withEnv port 1 $ \env -> do
      run env "CREATE TEMP TABLE scope_handled (id int)"
      inRequest env Tx.Write $ \_ tx -> do
        Tx.runIn tx (Tx.sql "INSERT INTO scope_handled VALUES (1)")
        failure <- try @Tx.QueryError (Tx.runIn tx (Tx.pipeline (Tx.statement () divisionByZero) >>= Tx.singleRowValue))
        isLeft failure `shouldBe` True
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      opened env `shouldReturn` 1
      count env "scope_handled" `shouldReturn` 0

    -- The failure comes after the row of the field, and nothing read it before the scope
    -- closed, so the ending is what finds it, with the ReadyForQuery still unread.
    it "rolls back when the ending finds a failure after a field was read to its end, and keeps the connection" $ withEnv port 1 $ \env -> withEnv port 1 $ \admin -> do
      pid <- backendPid env
      kept <- newIORef Nothing
      result <- try @Tx.QueryError $ inRequest env Tx.Read $ \checkout tx -> do
        writeIORef kept (Just (Pool.checkoutConnection checkout))
        Tx.runIn tx $ do
          field <- firstField =<< Tx.pipeline (Tx.statement () failingAfterFirstRow)
          drain (Tx.nextChunk field) (const (pure ()))
      case result of
        Left (Tx.QueryError _ (Tx.ServerFailed (PG.ServerError fields))) -> PG.diagnosticCode fields `shouldBe` "22012"
        _ -> failTest "expected the division by zero of the second row"
      lastNames env 1 `shouldReturn` ["ready: failed"]
      stateOf admin pid `shouldReturn` Just "idle"
      Just connection <- readIORef kept
      PG.checkIdle connection `shouldReturn` Right ()
      opened env `shouldReturn` 1
      backendPid env `shouldReturn` pid

    -- The server's reply to COMMIT is the failure, and the status that follows it is
    -- idle: there is nothing to roll back, but the ReadyForQuery has to be read.
    it "keeps the connection when the ending's COMMIT fails on a deferred constraint, as the scope closes" $ withEnv port 1 $ \env -> do
      pid <- backendPid env
      kept <- newIORef Nothing
      result <- try @Tx.QueryError $ inRequest env Tx.Write $ \checkout tx -> do
        writeIORef kept (Just (Pool.checkoutConnection checkout))
        Tx.runIn tx $ do
          Tx.sql "CREATE TEMP TABLE closing_parent (id int PRIMARY KEY)"
          Tx.sql "CREATE TEMP TABLE closing_child (id int REFERENCES closing_parent(id) DEFERRABLE INITIALLY DEFERRED)"
          Tx.sql "INSERT INTO closing_child VALUES (1)"
      case result of
        Left (Tx.QueryError _ (Tx.ServerFailed (PG.ServerError fields))) -> PG.diagnosticCode fields `shouldBe` "23503"
        _ -> failTest "expected the deferred foreign-key failure from COMMIT"
      lastNames env 1 `shouldReturn` ["ready: failed"]
      Just connection <- readIORef kept
      PG.checkIdle connection `shouldReturn` Right ()
      opened env `shouldReturn` 1
      backendPid env `shouldReturn` pid

    -- The release runs masked, but a wait for the server is interruptible: the
    -- thread of a request that times out is interrupted in it.
    it "breaks the connection when the thread is interrupted while the ending drains, without waiting for the rest" $ withEnv port 1 $ \env -> do
      bodyDone <- newIORef False
      started <- getMonotonicTime
      outcome <- timeout 2_000_000 $ inRequest env Tx.Read $ \_ tx -> do
        Tx.runIn tx $ do
          field <- firstField =<< Tx.pipeline (Tx.statement () sleepingAtTheEnd)
          drain (Tx.nextChunk field) (const (pure ()))
        writeIORef bodyDone True
      -- the scope was closing, and not the body running, when the timeout came
      readIORef bodyDone `shouldReturn` True
      outcome `shouldBe` Nothing
      elapsed <- subtract started <$> getMonotonicTime
      (elapsed < 10) `shouldBe` True
      brokenAfter env

  describe "when the scope aborts" $ do
    it "rolls the transaction back, and gives the connection back as failed" $ withEnv port 1 $ \env -> do
      run env "CREATE TEMP TABLE scope_rollback (id int)"
      result <- try @ErrorCall $ inRequest env Tx.Write $ \_ tx -> do
        Tx.runIn tx (Tx.sql "INSERT INTO scope_rollback VALUES (1)")
        throwIO (ErrorCall "the request failed") :: IO ()
      result `shouldBe` Left (ErrorCall "the request failed")
      lastNames env 1 `shouldReturn` ["ready: failed"]
      opened env `shouldReturn` 1
      count env "scope_rollback" `shouldReturn` 0

    it "rolls back after a query error, and keeps the connection" $ withEnv port 1 $ \env -> do
      run env "CREATE TEMP TABLE scope_error (id int)"
      result <- try @Tx.QueryError $ inRequest env Tx.Write $ \_ tx -> do
        Tx.runIn tx (Tx.sql "INSERT INTO scope_error VALUES (1)")
        Tx.runIn tx (Tx.pipeline (Tx.statement () divisionByZero) >>= Tx.singleRowValue)
      isLeft result `shouldBe` True
      lastNames env 1 `shouldReturn` ["ready: failed"]
      opened env `shouldReturn` 1
      count env "scope_error" `shouldReturn` 0

    it "rolls back when the failure comes after a field was read to its end" $ withEnv port 1 $ \env -> do
      result <- try @ErrorCall $ inRequest env Tx.Read $ \_ tx -> do
        Tx.runIn tx $ do
          field <- firstField =<< Tx.pipeline (Tx.statement () (bodyOf 5000))
          drain (Tx.nextChunk field) (const (pure ()))
        throwIO (ErrorCall "the request failed") :: IO ()
      result `shouldBe` Left (ErrorCall "the request failed")
      lastNames env 1 `shouldReturn` ["ready: failed"]
      opened env `shouldReturn` 1

    it "breaks the connection when a field was left in the middle of its stream" $ withEnv port 1 $ \env -> do
      result <- try @ErrorCall $ inRequest env Tx.Read $ \_ tx -> do
        Tx.runIn tx $ do
          field <- firstField =<< Tx.pipeline (Tx.statement () (bodyOf 50000))
          chunk <- Tx.nextChunk field
          liftIO ((BS.length chunk < 50000) `shouldBe` True)
        throwIO (ErrorCall "the client went away") :: IO ()
      result `shouldBe` Left (ErrorCall "the client went away")
      brokenAfter env

    it "breaks the connection when the thread is interrupted while a result is read" $ withEnv port 1 $ \env -> do
      started <- newEmptyMVar
      finished <- newEmptyMVar
      worker <- forkIO $
        try @SomeException
          ( inRequest env Tx.Read $ \_ tx -> Tx.runIn tx $ do
              cursor <- Tx.pipeline (Tx.statement () sleeping)
              liftIO (putMVar started ())
              void (Tx.singleRowValue cursor)
          )
          >>= putMVar finished
      takeMVar started
      threadDelay 100_000
      killThread worker
      timeout 5_000_000 (takeMVar finished) >>= \outcome -> fmap (either (const "interrupted") (const "returned")) outcome `shouldBe` (Just "interrupted" :: Maybe String)
      brokenAfter env

    it "breaks the connection when the thread is interrupted between operations, and doesn't wait for the answer" $ withEnv port 1 $ \env -> do
      started <- newEmptyMVar
      finished <- newEmptyMVar
      worker <- forkIO $
        try @SomeException
          ( inRequest env Tx.Read $ \_ tx -> do
              -- Sent, and not read
              Tx.runIn tx (void (Tx.pipeline (Tx.statement () sleeping)))
              putMVar started ()
              threadDelay 30_000_000
          )
          >>= putMVar finished
      takeMVar started
      killThread worker
      timeout 5_000_000 (takeMVar finished) >>= \outcome -> fmap (either (const "interrupted") (const "returned")) outcome `shouldBe` (Just "interrupted" :: Maybe String)
      brokenAfter env

  describe "a field as a pull source" $ do
    it "pulls a field larger than the buffers to its end, exactly" $ withEnv port 1 $ \env -> do
      collected <- inRequest env Tx.Read $ \_ tx -> do
        field <- Tx.runIn tx (firstField =<< Tx.pipeline (Tx.statement () (numbersOf 20000)))
        pullAll (Tx.fieldSource field)
      collected `shouldBe` numbers 20000
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      opened env `shouldReturn` 1

    it "gives up the connection of a request that stopped pulling in the middle" $ withEnv port 1 $ \env -> do
      result <- try @ErrorCall $ inRequest env Tx.Read $ \_ tx -> do
        field <- Tx.runIn tx (firstField =<< Tx.pipeline (Tx.statement () (numbersOf 20000)))
        _ <- pullWaiting (Tx.fieldSource field) awaitWatch
        throwIO (ErrorCall "the client went away") :: IO ()
      result `shouldBe` Left (ErrorCall "the client went away")
      brokenAfter env

  describe "the ending's reply" $ do
    it "gives COMMIT's success once the body was pulled, and the scope has nothing left to do" $ withEnv port 1 $ \env -> do
      run env "CREATE TEMP TABLE reply_commit (id int)"
      outcome <- inRequest env Tx.Write $ \_ tx -> do
        Tx.runIn tx (Tx.sql "INSERT INTO reply_commit VALUES (1)")
        field <- Tx.runIn tx (firstField =<< Tx.finalPipeline (Tx.statement () (numbersOf 3000)))
        _ <- pullAll (Tx.fieldSource field)
        awaitEnding tx
      isRight outcome `shouldBe` True
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      count env "reply_commit" `shouldReturn` 1

    it "gives the failure of a deferred constraint at COMMIT as a value, after the body was pulled" $ withEnv port 1 $ \env -> do
      outcome <- inRequest env Tx.Write $ \_ tx -> do
        Tx.runIn tx (Tx.sql "CREATE TEMP TABLE reply_parent (id int PRIMARY KEY)")
        Tx.runIn tx (Tx.sql "CREATE TEMP TABLE reply_child (id int REFERENCES reply_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        Tx.runIn tx (Tx.sql "INSERT INTO reply_child VALUES (1)")
        field <- Tx.runIn tx (firstField =<< Tx.finalPipeline (Tx.statement () (numbersOf 3000)))
        delivered <- pullAll (Tx.fieldSource field)
        delivered `shouldBe` numbers 3000
        awaitEnding tx
      case outcome of
        Left (Tx.QueryError "COMMIT" (Tx.ServerFailed (PG.ServerError fields))) -> PG.diagnosticCode fields `shouldBe` "23503"
        _ -> failTest "expected the deferred foreign-key failure from COMMIT"
      -- The failure was seen, and the connection is fine
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      opened env `shouldReturn` 1

  describe "use after the scope" $ do
    it "rejects a transaction run from another thread" $ withEnv port 1 $ \env -> do
      inRequest env Tx.Read $ \_ tx -> do
        outcome <- newEmptyMVar
        _ <- forkIO (try @PG.ConnectionError (Tx.runIn tx (Tx.sql "SELECT 1")) >>= putMVar outcome)
        takeMVar outcome >>= \case
          Left (PG.SessionUnusable _) -> pure ()
          _ -> failTest "another thread ran the transaction"

    it "rejects the session of a scope that has closed" $ withEnv port 1 $ \env -> do
      kept <- newIORef Nothing
      BufferPool.withRequestBuffers (envResources env) $ \access -> withScope $ \scope -> do
        Right checkout <- Pool.checkoutIn scope (envPool env) preflight
        session <- Streaming.operationIn scope access (Pool.checkoutConnection checkout)
        writeIORef kept (Just session)
      Just session <- readIORef kept
      result <- try @PG.ConnectionError (Tx.runTransaction Tx.ReadCommitted Tx.Read session (Tx.sql "SELECT 1"))
      case result of
        Left (PG.SessionUnusable _) -> pure ()
        _ -> failTest "the session of a closed scope ran a transaction"

-- | A pool of real connections over the request buffers.
data Env = Env
  { envResources :: BufferPool.BufferPool,
    envPool :: Pool.Pool PG.Connection String,
    envEvents :: IORef [String]
  }

withEnv :: PortNumber -> Int -> (Env -> IO a) -> IO a
withEnv port size use = do
  waiting <- getAcquisition
  resources <- BufferPool.newBufferPool waiting PoolSettings
    { bufferSize = positiveInt @1024,
      ordinaryBufferCount = positiveInt @4,
      reserveBufferCount = positiveInt @2,
      setupBufferCount = positiveInt @2
    }
  events <- newIORef []
  let connect = try @SomeException (fst <$> Streaming.connect resources (settings port)) <&> first show
  pool <- Pool.newPool waiting Config
    { configSize = size,
      configAcquisitionTimeout = 10,
      configMaxLifetime = 3600,
      configMaxIdletime = 3600,
      configConnect = connect,
      configDescribeFailure = Just . pack,
      configClose = PG.close,
      configUsableAfterException = PG.isUsable,
      configObserve = \(ConnectionObservation _ status) -> atomicModifyIORef' events (\names -> (names <> [statusName status], ()))
    }
  use (Env resources pool events) `finally` Pool.destroy pool

settings :: PortNumber -> PG.ConnectSettings
settings port = (PG.connectSettings (TcpAddress "127.0.0.1" port) "native_pg_scram")
  { PG.connectPassword = Just "scram secret",
    PG.connectDatabase = Just "postgres",
    PG.connectSslMode = PG.SslRequire,
    PG.connectTlsClient = Just tlsClient
  }

preflight :: PG.Connection -> IO (Either (Maybe Text) ())
preflight = fmap (first (Just . pack)) . PG.checkIdle

-- | Runs the body in a request: a checkout of a connection, an operation on it
-- and a transaction, all in one scope.
inRequest :: Env -> Tx.Mode -> (forall s. Checkout s PG.Connection -> Tx.Tx s -> IO a) -> IO a
inRequest env mode body =
  BufferPool.withRequestBuffers (envResources env) $ \access -> withScope $ \scope -> do
    checked <- Pool.checkoutIn scope (envPool env) preflight
    checkout <- either (\failure -> failTest ("no connection: " <> show failure)) pure checked
    session <- Streaming.operationIn scope access (Pool.checkoutConnection checkout)
    tx <- Tx.transactionIn scope Tx.ReadCommitted mode session
    body checkout tx

-- | Pulls the source to its end, with the blocking pull, copying each chunk.
pullAll :: WaitingSource source => source -> IO BS.ByteString
pullAll source = go []
  where
    go chunks = pullWaiting source awaitWatch >>= \case
      Nothing -> pure (BS.concat (reverse chunks))
      Just chunk -> evaluate (BS.copy chunk) >>= \copied -> go (copied : chunks)

-- | Asks for the ending's reply, and waits for what it is blocked on until it
-- has it, but not for ever: a reply that is not given, or a watch that fires
-- without one, fails the test instead of keeping it waiting.
awaitEnding :: Tx.Tx s -> IO (Either Tx.QueryError ())
awaitEnding tx = timeout 5_000_000 loop >>= maybe (failTest "the ending's reply was not given in time") pure
  where
    loop = Tx.endingWaiting tx awaitWatch

-- | The numbers from 1 to this one, separated by commas: a field of that size.
numbers :: Int -> BS.ByteString
numbers n = BC.intercalate "," (map (BC.pack . show) [1 .. n])

numbersOf :: Int -> Tx.Statement () Body
numbersOf n = Tx.Statement ("SELECT string_agg(n::text, ',') FROM generate_series(1, " <> BC.pack (show n) <> ") n") (const [])
  (Tx.singleRow (Tx.streamingRow @'[] Body))

-- | The field of the first row.
firstField :: Tx.Result tx Body -> Tx.Transaction tx (Tx.Field tx)
firstField cursor = Tx.nextRow cursor >>= \case
  Just (Body (Just field)) -> pure field
  _ -> liftIO (failTest "the row has no field")

-- | Runs a statement in a request of its own.
run :: Env -> BS.ByteString -> IO ()
run env sql = inRequest env Tx.Write $ \_ tx -> Tx.runIn tx (Tx.sql sql)

count :: Env -> BS.ByteString -> IO Int64
count env table = inRequest env Tx.Read $ \_ tx -> Tx.runIn tx $
  Tx.pipeline (Tx.statement () (Tx.Statement ("SELECT count(*) FROM " <> table) (const []) (Tx.singleRow (Tx.row @'[Int64] Const))))
    >>= Tx.singleRowValue

-- | The connection that the pool has idle, taken in a request.
connectionOf :: Env -> IO PG.Connection
connectionOf env = inRequest env Tx.Read $ \checkout _ -> pure (Pool.checkoutConnection checkout)

-- | The backend of the connection that a request runs on.
backendPid :: Env -> IO Int64
backendPid env = inRequest env Tx.Read $ \_ tx ->
  Tx.runIn tx $
    Tx.pipeline (Tx.statement () (Tx.Statement "SELECT pg_backend_pid()::bigint" (const []) (Tx.singleRow (Tx.row @'[Int64] Const))))
      >>= Tx.singleRowValue

-- | What the server says a backend is doing, as pg_stat_activity does.
stateOf :: Env -> Int64 -> IO (Maybe Text)
stateOf env pid = inRequest env Tx.Read $ \_ tx ->
  Tx.runIn tx $
    Tx.pipeline (Tx.statement () (Tx.Statement ("SELECT state FROM pg_stat_activity WHERE pid = " <> BC.pack (show pid)) (const []) (Tx.rowMaybe (Tx.row @'[Text] Const))))
      >>= Tx.rowMaybeValue

-- | The connection was closed, and the pool opens a new one for the next request.
brokenAfter :: Env -> IO ()
brokenAfter env = do
  fmap (take 10) <$> lastNames env 1 `shouldReturn` ["terminated"]
  opened env `shouldReturn` 1
  count env "pg_class" `shouldSatisfyValue` (> 0)
  opened env `shouldReturn` 2
  where
    shouldSatisfyValue action predicate = action >>= (`shouldBe` True) . predicate

-- | How many connections the pool opened.
opened :: Env -> IO Int
opened env = length . filter (== "connecting") <$> readIORef (envEvents env)

-- | The last events of the pool, by name.
lastNames :: Env -> Int -> IO [String]
lastNames env n = reverse . take n . reverse <$> readIORef (envEvents env)

statusName :: ConnectionStatus -> String
statusName = \case
  ConnectingConnectionStatus -> "connecting"
  ReadyForUseConnectionStatus EstablishedConnectionReadyForUseReason -> "ready: established"
  ReadyForUseConnectionStatus (SessionFailedConnectionReadyForUseReason _) -> "ready: failed"
  ReadyForUseConnectionStatus SessionSucceededConnectionReadyForUseReason -> "ready: succeeded"
  InUseConnectionStatus -> "in use"
  TerminatedConnectionStatus AgingConnectionTerminationReason -> "terminated: aging"
  TerminatedConnectionStatus IdlenessConnectionTerminationReason -> "terminated: idleness"
  TerminatedConnectionStatus (NetworkErrorConnectionTerminationReason details) -> "terminated: network error " <> show details
  TerminatedConnectionStatus ReleaseConnectionTerminationReason -> "terminated: release"

newtype Body tx = Body (Maybe (Tx.Field tx))

-- | A statement with a field of this many bytes.
bodyOf :: Int -> Tx.Statement () Body
bodyOf size = Tx.Statement ("SELECT repeat('x', " <> BC.pack (show size) <> ")") (const [])
  (Tx.singleRow (Tx.streamingRow @'[] Body))

divisionByZero :: Tx.Statement () (Const Int64)
divisionByZero = Tx.Statement "SELECT 1 / 0" (const []) (Tx.singleRow (Tx.row @'[Int64] Const))

-- | A field of the first row, and then the server's failure, which comes with the row after it.
failingAfterFirstRow :: Tx.Statement () Body
failingAfterFirstRow =
  Tx.Statement
    "SELECT (1 / (2 - s))::text FROM generate_series(1, 2) s"
    (const [])
    (Tx.streamingRow @'[] Body)

-- | Enough rows for the server to send most of them, and then a wait of 30 seconds before the last.
sleepingAtTheEnd :: Tx.Statement () Body
sleepingAtTheEnd =
  Tx.Statement
    "SELECT (CASE WHEN s < 3000 THEN 1 ELSE (SELECT 1 FROM pg_sleep(30)) END)::text FROM generate_series(1, 3000) s"
    (const [])
    (Tx.streamingRow @'[] Body)

sleeping :: Tx.Statement () (Const Int64)
sleeping = Tx.Statement "SELECT 1 FROM pg_sleep(30)" (const []) (Tx.singleRow (Tx.row @'[Int64] Const))

drain :: Tx.Transaction tx BS.ByteString -> (BS.ByteString -> Tx.Transaction tx ()) -> Tx.Transaction tx ()
drain source use = do
  chunk <- source
  if BS.null chunk then pure () else use chunk *> drain source use
