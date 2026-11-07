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
import Network.Socket (PortNumber)
import Network.Streaming.BufferPool (PoolSettings (..))
import qualified Network.Streaming.BufferPool as BufferPool
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Source (Source, pull)
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
        isLeft' failure `shouldBe` True
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      opened env `shouldReturn` 1
      count env "scope_handled" `shouldReturn` 0

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
      isLeft' result `shouldBe` True
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
      timeout 5_000_000 (takeMVar finished) >>= \outcome -> fmap (either (const "interrupted") (const "returned")) outcome `shouldBe` Just "interrupted"
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
      timeout 5_000_000 (takeMVar finished) >>= \outcome -> fmap (either (const "interrupted") (const "returned")) outcome `shouldBe` Just "interrupted"
      brokenAfter env

  describe "a field as a pull source" $ do
    xfail "fieldSource: it is not implemented yet" $ it "pulls a field larger than the buffers to its end, exactly" $ withEnv port 1 $ \env -> do
      collected <- inRequest env Tx.Read $ \_ tx -> do
        field <- Tx.runIn tx (firstField =<< Tx.pipeline (Tx.statement () (numbersOf 20000)))
        pullAll (Tx.fieldSource field)
      collected `shouldBe` numbers 20000
      lastNames env 1 `shouldReturn` ["ready: succeeded"]
      opened env `shouldReturn` 1

    xfail "fieldSource: it is not implemented yet" $ it "gives up the connection of a request that stopped pulling in the middle" $ withEnv port 1 $ \env -> do
      result <- try @ErrorCall $ inRequest env Tx.Read $ \_ tx -> do
        field <- Tx.runIn tx (firstField =<< Tx.pipeline (Tx.statement () (numbersOf 20000)))
        _ <- pull (Tx.fieldSource field)
        throwIO (ErrorCall "the client went away") :: IO ()
      result `shouldBe` Left (ErrorCall "the client went away")
      brokenAfter env

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
  let connect = try @SomeException (fst <$> Streaming.connect resources (settings port)) >>= pure . first show
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
pullAll :: Source source => source -> IO BS.ByteString
pullAll source = go []
  where
    go chunks = pull source >>= \case
      Nothing -> pure (BS.concat (reverse chunks))
      Just chunk -> evaluate (BS.copy chunk) >>= \copied -> go (copied : chunks)

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

-- | The connection was closed, and the pool opens a new one for the next request.
brokenAfter :: Env -> IO ()
brokenAfter env = do
  fmap (take 10) <$> lastNames env 1 `shouldReturn` ["terminated"]
  opened env `shouldReturn` 1
  count env "pg_class" `shouldSatisfyValue` (> 0)
  opened env `shouldReturn` 2
  where
    shouldSatisfyValue action predicate = action >>= \value -> predicate value `shouldBe` True

-- | How many connections the pool opened.
opened :: Env -> IO Int
opened env = length . filter (== "connecting") <$> readIORef (envEvents env)

-- | The last events of the pool, by name.
lastNames :: Env -> Int -> IO [String]
lastNames env n = (reverse . take n . reverse) <$> readIORef (envEvents env)

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

sleeping :: Tx.Statement () (Const Int64)
sleeping = Tx.Statement "SELECT 1 FROM pg_sleep(30)" (const []) (Tx.singleRow (Tx.row @'[Int64] Const))

isLeft' :: Either a b -> Bool
isLeft' = either (const True) (const False)

drain :: Tx.Transaction tx BS.ByteString -> (BS.ByteString -> Tx.Transaction tx ()) -> Tx.Transaction tx ()
drain source use = do
  chunk <- source
  if BS.null chunk then pure () else use chunk *> drain source use
