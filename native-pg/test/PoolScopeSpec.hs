{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Tests of the pool's checkouts in a scope.
module PoolScopeSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent (forkIO, threadDelay, throwTo)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar, tryReadMVar)
import Control.Exception (ErrorCall (..), SomeException, throwIO, toException, try)
import Control.Monad (void)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Text (pack)
import Data.UUID (UUID)
import Network.Streaming.Scope (withScope)
import Skeletest
import System.Timeout (timeout)

import Database.PostgreSQL.Pool
import Database.PostgreSQL.Pool.Observation

-- | The exception that ended a scope.
thrown :: IO () -> IO (Either ErrorCall ())
thrown = try

spec :: Spec
spec = describe "checking out in a scope" $ do
  describe "giving the connection back" $ do
    xfail "checkoutIn: it is not implemented yet" $ it "gives it back as succeeded when the scope completes, and reuses it" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      withScope $ \scope -> do
        checked <- checkoutIn scope pool (const (pure (Right ())))
        fmap checkoutConnection checked `shouldBe` Right 1
      names harness `shouldReturn` ["connecting", "ready: established", "in use", "ready: succeeded"]
      withScope $ \scope -> do
        checked <- checkoutIn scope pool (const (pure (Right ())))
        fmap checkoutConnection checked `shouldBe` Right 1
      readIORef (harnessOpened harness) `shouldReturn` 1

    xfail "checkoutIn: it is not implemented yet" $ it "keeps the connection until the scope closes, and counts it in use" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      during <- withScope $ \scope -> do
        _ <- checkoutIn scope pool (const (pure (Right ())))
        stats pool
      during `shouldBe` Stats {statsIdle = 0, statsInUse = 1, statsOpening = 0}
      stats pool `shouldReturn` Stats {statsIdle = 1, statsInUse = 0, statsOpening = 0}

    xfail "checkoutIn: it is not implemented yet" $ it "gives the connection to a use that waits once the scope closes, and not before" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness) {configAcquisitionTimeout = 10}
      holding <- newEmptyMVar
      closing <- newEmptyMVar
      _ <- forkIO $ withScope $ \scope -> do
        _ <- checkoutIn scope pool (const (pure (Right ())))
        putMVar holding ()
        takeMVar closing
      takeMVar holding
      later <- newEmptyMVar
      _ <- forkIO (use pool (\connection -> pure (Succeeded, connection)) >>= putMVar later)
      threadDelay 100_000
      tryReadMVar later `shouldReturn` Nothing
      putMVar closing ()
      timeout 2_000_000 (takeMVar later) `shouldReturn` Just (Right 1)

    xfail "checkoutIn: it is not implemented yet" $ it "gives it back as failed when the scope aborts and the connection is usable" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      result <- thrown $ withScope $ \scope -> do
        _ <- checkoutIn scope pool (const (pure (Right ())))
        throwIO (ErrorCall "the scope failed")
      result `shouldBe` Left (ErrorCall "the scope failed")
      names harness `shouldReturn` ["connecting", "ready: established", "in use", "ready: failed"]
      withScope $ \scope -> do
        checked <- checkoutIn scope pool (const (pure (Right ())))
        fmap checkoutConnection checked `shouldBe` Right 1

    xfail "checkoutIn: it is not implemented yet" $ it "closes it when the scope aborts and the connection isn't usable" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness) {configUsableAfterException = const (pure False)}
      result <- thrown $ withScope $ \scope -> do
        _ <- checkoutIn scope pool (const (pure (Right ())))
        throwIO (ErrorCall "the scope failed")
      result `shouldBe` Left (ErrorCall "the scope failed")
      -- With the exception's details, which include its backtrace
      fmap (take 48) <$> lastName harness `shouldReturn` Just "terminated: network error Just \"the scope failed"
      readIORef (harnessClosed harness) `shouldReturn` [1]
      withScope $ \scope -> do
        checked <- checkoutIn scope pool (const (pure (Right ())))
        fmap checkoutConnection checked `shouldBe` Right 2

    xfail "checkoutIn: it is not implemented yet" $ it "gives it back when the scope's thread is interrupted" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      holding <- newEmptyMVar
      finished <- newEmptyMVar
      worker <- forkIO $
        try @SomeException
          ( withScope $ \scope -> do
              _ <- checkoutIn scope pool (const (pure (Right ())))
              putMVar holding ()
              threadDelay 10_000_000
          )
          >>= putMVar finished
      takeMVar holding
      throwTo worker (ErrorCall "interrupted")
      timeout 2_000_000 (takeMVar finished) >>= \outcome ->
        fmap (either show (const "returned")) outcome `shouldBe` Just "interrupted"
      lastName harness `shouldReturn` Just "ready: failed"
      stats pool `shouldReturn` Stats {statsIdle = 1, statsInUse = 0, statsOpening = 0}

    xfail "checkoutIn: it is not implemented yet" $ it "gives back the connections of several checkouts, last in, first out" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness) {configSize = 2}
      withScope $ \scope -> do
        first <- checkoutIn scope pool (const (pure (Right ())))
        second <- checkoutIn scope pool (const (pure (Right ())))
        (fmap checkoutConnection first, fmap checkoutConnection second) `shouldBe` (Right 1, Right 2)
      observed <- readIORef (harnessEvents harness)
      let ofStatus wanted = [connection | (connection, status) <- observed, status == wanted]
      reverse (ofStatus "ready: succeeded") `shouldBe` ofStatus "ready: established"

  describe "deciding how the connection goes back" $ do
    xfail "checkoutIn: it is not implemented yet" $ it "closes a connection that was marked broken though the scope completed, with the details" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      withScope $ \scope -> do
        Right checkout <- checkoutIn scope pool (const (pure (Right ())))
        markBroken checkout (Just "lost")
      lastName harness `shouldReturn` Just "terminated: network error Just \"lost\""
      readIORef (harnessClosed harness) `shouldReturn` [1]

    xfail "checkoutIn: it is not implemented yet" $ it "gives back as failed a connection that was marked failed though the scope completed" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      withScope $ \scope -> do
        Right checkout <- checkoutIn scope pool (const (pure (Right ())))
        markFailed checkout (toException (ErrorCall "the use failed"))
      lastName harness `shouldReturn` Just "ready: failed"
      readIORef (harnessClosed harness) `shouldReturn` []

    xfail "checkoutIn: it is not implemented yet" $ it "keeps what the use decided when the scope aborts and the connection isn't usable" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness) {configUsableAfterException = const (pure False)}
      void . thrown $ withScope $ \scope -> do
        Right checkout <- checkoutIn scope pool (const (pure (Right ())))
        markFailed checkout (toException (ErrorCall "the use failed"))
        throwIO (ErrorCall "the scope failed")
      lastName harness `shouldReturn` Just "ready: failed"

    xfail "checkoutIn: it is not implemented yet" $ it "lets a mark of broken override a mark of failed, and not the other way round" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness) {configSize = 2}
      withScope $ \scope -> do
        Right first <- checkoutIn scope pool (const (pure (Right ())))
        markFailed first (toException (ErrorCall "failed"))
        markBroken first (Just "first")
        Right second <- checkoutIn scope pool (const (pure (Right ())))
        markBroken second (Just "second")
        markFailed second (toException (ErrorCall "failed"))
        markBroken second (Just "again")
      readIORef (harnessClosed harness) `shouldReturn` [2, 1]
      observed <- names harness
      filter (\name -> take 10 name == "terminated") observed
        `shouldBe` ["terminated: network error Just \"second\"", "terminated: network error Just \"first\""]

  describe "not getting a connection" $ do
    xfail "checkoutIn: it is not implemented yet" $ it "gives the timeout as a value when the pool stays full, and keeps what it holds" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness) {configAcquisitionTimeout = 0.1}
      withScope $ \scope -> do
        _ <- checkoutIn scope pool (const (pure (Right ())))
        withScope $ \inner -> do
          checked <- checkoutIn inner pool (const (pure (Right ())))
          either Just (const Nothing) checked `shouldBe` Just AcquisitionTimeout
        stats pool `shouldReturn` Stats {statsIdle = 0, statsInUse = 1, statsOpening = 0}

    xfail "checkoutIn: it is not implemented yet" $ it "gives a failure to connect as a value, and frees the slot" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness) {configConnect = pure (Left "refused")}
      withScope $ \scope -> do
        checked <- checkoutIn scope pool (const (pure (Right ())))
        either Just (const Nothing) checked `shouldBe` Just (ConnectionFailed "refused")
      stats pool `shouldReturn` Stats {statsIdle = 0, statsInUse = 0, statsOpening = 0}

    xfail "checkoutIn: it is not implemented yet" $ it "gives a pool that was destroyed as a value" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      destroy pool
      withScope $ \scope -> do
        checked <- checkoutIn scope pool (const (pure (Right ())))
        either Just (const Nothing) checked `shouldBe` Just PoolClosed

    xfail "checkoutIn: it is not implemented yet" $ it "retires a connection that fails the check, and tries again under the same deadline" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      checks <- newIORef (0 :: Int)
      let stale connection = do
            checked <- atomicModifyIORef' checks (\n -> (n + 1, n))
            pure (if checked == 0 && connection == 1 then Left (Just "stale") else Right ())
      -- The first connection goes idle, and fails the check the next time
      withScope $ \scope -> void (checkoutIn scope pool (const (pure (Right ()))))
      withScope $ \scope -> do
        checked <- checkoutIn scope pool stale
        fmap checkoutConnection checked `shouldBe` Right 2
      readIORef (harnessClosed harness) `shouldReturn` [1]
      readIORef checks `shouldReturn` 2

    xfail "checkoutIn: it is not implemented yet" $ it "gives up a connection whose check throws, and propagates the exception" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      result <- thrown $ withScope $ \scope -> void (checkoutIn scope pool (const (throwIO (ErrorCall "check failed"))))
      result `shouldBe` Left (ErrorCall "check failed")
      readIORef (harnessClosed harness) `shouldReturn` [1]
      stats pool `shouldReturn` Stats {statsIdle = 0, statsInUse = 0, statsOpening = 0}

-- | Counts the connections opened, and records the closed ones and the events.
data Harness = Harness
  { harnessOpened :: IORef Int
  , harnessClosed :: IORef [Int]
  , harnessEvents :: IORef [(UUID, String)]
  }

newHarness :: IO Harness
newHarness = Harness <$> newIORef 0 <*> newIORef [] <*> newIORef []

-- | The events, by name, in order.
names :: Harness -> IO [String]
names harness = fmap snd <$> readIORef (harnessEvents harness)

lastName :: Harness -> IO (Maybe String)
lastName harness = (\observed -> if null observed then Nothing else Just (last observed)) <$> names harness

-- | One connection, opened as 1, 2 and so on, with long timeouts.
config :: Harness -> Config Int String
config harness =
  Config
    { configSize = 1
    , configAcquisitionTimeout = 1
    , configMaxLifetime = 3600
    , configMaxIdletime = 3600
    , configConnect = Right <$> atomicModifyIORef' (harnessOpened harness) (\opened -> (opened + 1, opened + 1))
    , configDescribeFailure = Just . pack
    , configClose = append (harnessClosed harness)
    , configUsableAfterException = const (pure True)
    , configObserve = \(ConnectionObservation connectionId status) -> append (harnessEvents harness) (connectionId, statusName status)
    }
  where
    append ref value = atomicModifyIORef' ref (\values -> (values <> [value], ()))

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
