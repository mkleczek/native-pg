{-# LANGUAGE LambdaCase #-}

-- | Tests of the pool, with numbers for connections.
module PoolSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar, tryReadMVar)
import Control.Exception (ErrorCall (..), throwIO, toException)
import Control.Monad (replicateM_, void, when)
import Data.Functor (($>))
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List (nub)
import Data.Maybe (isNothing, listToMaybe)
import Data.Text (pack)
import Data.UUID (UUID)
import Skeletest
import Skeletest.Predicate qualified as P

import Database.PostgreSQL.Pool
import Database.PostgreSQL.Pool.Observation

spec :: Spec
spec = do
  describe "use" $ do
    it "opens a connection, and reuses it" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      useOnce pool `shouldReturn` Right 1
      useOnce pool `shouldReturn` Right 1
      readIORef (harnessOpened harness) `shouldReturn` 1

    it "makes a use wait while the pool is full, and gives it the next free connection" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      (connection, finish) <- hold pool
      result <- newEmptyMVar
      _ <- forkIO (useOnce pool >>= putMVar result)
      threadDelay 100_000
      tryReadMVar result `shouldReturn` Nothing
      finish
      takeMVar result `shouldReturn` Right connection
      readIORef (harnessOpened harness) `shouldReturn` 1

    it "gives a freed connection to a waiting use rather than a later one" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      order <- newIORef []
      taken <- newEmptyMVar
      done <- newEmptyMVar
      finished <- newEmptyMVar
      let record name = void $ use pool (\connection -> atomicModifyIORef' order (\o -> (o <> [name], ())) $> (Succeeded, connection))
      -- Uses the pool again as soon as it gives the connection back, before
      -- the waiting use can run
      _ <- forkIO $ do
        _ <- use pool (\connection -> putMVar taken () *> takeMVar done $> (Succeeded, connection))
        record "later"
        putMVar finished ()
      takeMVar taken
      _ <- forkIO (record "waiting" *> putMVar finished ())
      threadDelay 20_000
      putMVar done ()
      replicateM_ 2 (takeMVar finished)
      readIORef order `shouldReturn` ["waiting", "later"]

    it "loses neither a connection nor a slot when a waiting use is killed" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      (_, finish) <- hold pool
      waiter <- forkIO (void (useOnce pool))
      threadDelay 20_000
      killThread waiter
      finish
      useOnce pool `shouldReturn` Right 1

    it "times out waiting for a connection" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness){configAcquisitionTimeout = 0.1}
      (_, finish) <- hold pool
      useOnce pool `shouldReturn` Left AcquisitionTimeout
      finish

    it "gives the slot back when opening a connection fails" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      failing <- newIORef True
      let connect =
            readIORef failing >>= \case
              True -> writeIORef failing False $> Left "the server is down"
              False -> configConnect (config harness)
      pool <- newPool waiting (config harness){configConnect = connect}
      useOnce pool `shouldReturn` Left (ConnectionFailed "the server is down")
      useOnce pool `shouldReturn` Right 1
      take 2 . fmap snd <$> readIORef (harnessEvents harness)
        `shouldReturn` ["connecting", "terminated: network error Just \"the server is down\""]

    it "keeps startup and callback work outside the admission timeout" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
        { configAcquisitionTimeout = 0.01
        , configConnect = threadDelay 30_000 *> configConnect (config harness)
        }
      use pool (\connection -> threadDelay 30_000 $> (Succeeded, connection)) `shouldReturn` Right 1
      useOnce pool `shouldReturn` Right 1
      release pool

    it "returns ownership once when a return observer throws" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      failReturn <- newIORef True
      let observe observation@(ConnectionObservation _ status) = do
            configObserve (config harness) observation
            case status of
              ReadyForUseConnectionStatus SessionSucceededConnectionReadyForUseReason -> do
                failing <- atomicModifyIORef' failReturn (False,)
                when failing (throwIO (ErrorCall "observer failed"))
              _ -> pure ()
      pool <- newPool waiting (config harness){configObserve = observe}
      useOnce pool `shouldSatisfy` P.throws (P.eq (ErrorCall "observer failed"))
      (_, finish) <- hold pool
      unavailable <- atomically (tryReserveReady pool)
      isNothing unavailable `shouldBe` True
      finish
      useOnce pool `shouldReturn` Right 1
      readIORef (harnessOpened harness) `shouldReturn` 1
      release pool
      readIORef (harnessClosed harness) `shouldReturn` [1]

    it "closes a connection that a use broke" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      use pool (\connection -> pure (Broke (Just (pack "lost")), connection)) `shouldReturn` Right 1
      readIORef (harnessClosed harness) `shouldReturn` [1]
      useOnce pool `shouldReturn` Right 2

    it "keeps a connection after a use that failed" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      use pool (\connection -> pure (Failed (toException (ErrorCall "no rows")), connection)) `shouldReturn` Right 1
      useOnce pool `shouldReturn` Right 1
      readIORef (harnessClosed harness) `shouldReturn` []

    it "replaces a connection past its maximum lifetime" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness){configMaxLifetime = 0.1}
      useOnce pool `shouldReturn` Right 1
      threadDelay 200_000
      useOnce pool `shouldReturn` Right 2
      readIORef (harnessClosed harness) `shouldReturn` [1]
      events <- fmap snd <$> readIORef (harnessEvents harness)
      events `shouldSatisfy` P.hasInfix ["terminated: aging"]

    it "replaces a connection that was idle too long when it's taken" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness){configMaxIdletime = 0.1}
      useOnce pool `shouldReturn` Right 1
      threadDelay 200_000
      useOnce pool `shouldReturn` Right 2
      events <- fmap snd <$> readIORef (harnessEvents harness)
      events `shouldSatisfy` P.hasInfix ["terminated: idleness"]

    it "closes idle connections in the background" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness){configMaxIdletime = 0.3}
      useOnce pool `shouldReturn` Right 1
      threadDelay 1_600_000
      readIORef (harnessClosed harness) `shouldReturn` [1]

    it "gives a connection back after a use that threw, if it's still usable" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      use pool (\_ -> throwIO (ErrorCall "boom") :: IO (Outcome, ())) `shouldSatisfy` P.throws (P.eq (ErrorCall "boom"))
      useOnce pool `shouldReturn` Right 1
      events <- fmap snd <$> readIORef (harnessEvents harness)
      events `shouldSatisfy` P.hasInfix ["in use", "ready: failed"]

    it "closes a connection after a use that threw, if it's not usable" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness){configUsableAfterException = const (pure False)}
      use pool (\_ -> throwIO (ErrorCall "boom") :: IO (Outcome, ())) `shouldSatisfy` P.throws (P.eq (ErrorCall "boom"))
      readIORef (harnessClosed harness) `shouldReturn` [1]
      useOnce pool `shouldReturn` Right 2

    it "loses neither the connection nor its slot when a use is killed" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness){configUsableAfterException = const (pure False)}
      started <- newEmptyMVar
      user <- forkIO . void . use pool $ \connection -> putMVar started () *> threadDelay 10_000_000 $> (Succeeded, connection)
      takeMVar started
      killThread user
      useOnce pool `shouldReturn` Right 2
      readIORef (harnessClosed harness) `shouldReturn` [1]

    it "loses no slot when opening a connection is killed" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      started <- newEmptyMVar
      first <- newIORef True
      let connect =
            readIORef first >>= \case
              True -> writeIORef first False *> putMVar started () *> threadDelay 10_000_000 $> Left "never"
              False -> configConnect (config harness)
      pool <- newPool waiting (config harness){configConnect = connect}
      user <- forkIO (void (useOnce pool))
      takeMVar started
      killThread user
      useOnce pool `shouldReturn` Right 1

  describe "release" $
    it "closes the idle connections, and the ones in use when they're done" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness){configSize = 2}
      (_, finish) <- hold pool
      useOnce pool `shouldReturn` Right 2
      release pool
      readIORef (harnessClosed harness) `shouldReturn` [2]
      finish
      readIORef (harnessClosed harness) `shouldReturn` [2, 1]
      -- Closed, and not reported ready again afterwards.
      fmap snd . listToMaybe . reverse <$> readIORef (harnessEvents harness) `shouldReturn` Just "terminated: release"
      useOnce pool `shouldReturn` Right 3

  describe "observations" $
    it "follow a connection from opening to its use" $ getAcquisition >>= \waiting -> do
      harness <- newHarness
      pool <- newPool waiting (config harness)
      useOnce pool `shouldReturn` Right 1
      events <- readIORef (harnessEvents harness)
      snd <$> events `shouldBe` ["connecting", "ready: established", "in use", "ready: succeeded"]
      -- All of one connection.
      length (nub (fst <$> events)) `shouldBe` 1

-- | Counts the connections opened, and records the closed ones and the events.
data Harness = Harness
  { harnessOpened :: IORef Int
  , harnessClosed :: IORef [Int]
  , harnessEvents :: IORef [(UUID, String)]
  }

newHarness :: IO Harness
newHarness = Harness <$> newIORef 0 <*> newIORef [] <*> newIORef []

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

-- | A use that succeeds, giving its connection.
useOnce :: Pool Int String -> IO (Either (UsageError String) Int)
useOnce pool = use pool (\connection -> pure (Succeeded, connection))

-- | Starts a use that holds its connection, and waits until it has one. Gives
-- the connection, and an action that ends the use and waits for it.
hold :: Pool Int String -> IO (Int, IO ())
hold pool = do
  taken <- newEmptyMVar
  done <- newEmptyMVar
  finished <- newEmptyMVar
  _ <- forkIO $ do
    _ <- use pool (\connection -> putMVar taken connection *> takeMVar done $> (Succeeded, ()))
    putMVar finished ()
  connection <- takeMVar taken
  pure (connection, putMVar done () *> takeMVar finished)
