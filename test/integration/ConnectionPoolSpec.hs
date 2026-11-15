{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | The connection pool against a real server: how a checkout ends, and what the pool does with its connections as it
-- closes and as they age, and what it tells of them. Every case is run by each arrow.
--
-- A case that the v2 code can't pass yet is marked with what it needs ('needs'): the marks come off as the part that they
-- wait for lands.
module ConnectionPoolSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (forM_, void, when)
import qualified Data.ByteString.Char8 as BC
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List (nub)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Word (Word64)
import Database.PostgreSQL.Errors
import Database.PostgreSQL.Pool (ConnectionPool, ConnectionPoolSettings (..), ConnectionReadyForUseReason (..), ConnectionStatus (..), ConnectionTerminationReason (..), Observation (..), connectionPoolSettings, withConnection, withConnectionPool)
import qualified Database.PostgreSQL.Transaction as Tx
import GHC.Clock (getMonotonicTime)
import GHC.Stats (allocated_bytes, getRTSStats)
import Network.Socket (PortNumber)
import Network.Streaming.BufferPool (BufferPoolSettings (..))
import Network.Streaming.PositiveInt (positiveInt)
import Skeletest
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)
import System.Mem (performMinorGC)
import TestPrograms

spec :: FilePath -> PortNumber -> Spec
spec socketPath _port = describe "the connection pool" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let settings = unixSettings socketPath "postgres" Nothing
      -- The pool of this size, for the use, with the buffers that a request runs in, and the way to run a request.
      pooled :: (ConnectionPoolSettings -> ConnectionPoolSettings) -> (Env -> IO a) -> IO a
      pooled change use = do
        buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
        withConnectionPool (change (connectionPoolSettings 1 settings)) $ \pool -> use (Env runner buffers pool)
      -- What the server says a backend is doing, asked on a connection of its own: no row once the backend is gone.
      stateOf :: Int64 -> IO (Either QueryError [Text])
      stateOf pid =
        ordinary runner (session settings (query (plain ("SELECT state FROM pg_stat_activity WHERE pid = " <> BC.pack (show pid)) single)))
      gone :: Int64 -> IO Bool
      gone pid = (== Right []) <$> stateOf pid

  -- How a checkout ends: by the deadline of the pool, or of the arrow that runs it, or by the pool's closing.
  describe "a checkout" $ do
    it "gives CheckoutTimedOut once the pool's timeout has passed, and not before" $
      pooled (\s -> s {poolCheckoutTimeout = Just (millis 100)}) $ \env -> do
        -- The only connection is held by the outer request, so the inner one waits for it.
        started <- getMonotonicTime
        onEnv env (withConnection (envPool env) (snd ^>> withConnection (envPool env) done)) `shouldReturn` Right (Left CheckoutTimedOut)
        ended <- getMonotonicTime
        (ended - started >= 0.1) `shouldBe` True
        (ended - started < 5) `shouldBe` True

    it "gives up its place when the deadline of an enclosing 'within' cancels it, and keeps the connection for the next" $
      pooled id $ \env -> do
        pid <- pidOf env
        onEnv env (withConnection (envPool env) (snd ^>> withinFor (millis 50) (withConnection (envPool env) done)))
          `shouldReturn` Right Nothing
        pidOf env `shouldReturn` pid

    it "gives each of several waiting checkouts its deadline, and serves the one that comes after them" $
      pooled (\s -> s {poolCheckoutTimeout = Just (millis 100)}) $ \env -> do
        pid <- pidOf env
        let waiting :: (Runtime arr) => arr Loans (Either PoolError ())
            waiting = withConnection (envPool env) done
        onEnv env (withConnection (envPool env) (snd ^>> steps [waiting `yields` Left CheckoutTimedOut, waiting `yields` Left CheckoutTimedOut]))
          `shouldReturn` Right ()
        pidOf env `shouldReturn` pid

    it "gives PoolClosed once the pool is closed, though a request still holds its connection" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      (pool, holder) <- withConnectionPool (connectionPoolSettings 1 settings) $ \pool -> (pool,) <$> startHolding (Env runner buffers pool)
      -- The pool's bracket has ended while the request holds the only connection, so the checkout doesn't wait for it.
      runOn runner buffers (withConnection pool done) `shouldReturn` Left PoolClosed
      finishHolding holder

  -- What a request pays the pool when its connection is free: the bytes that a request allocates for the checkout, the
  -- use of the connection and the release, with a body that does nothing. The ceiling is what each arrow allocated when it
  -- was set: a change that makes a request dearer says so here, as one that makes it cheaper takes the ceiling down. With
  -- @POOL_COST_REPORT@ set, the case says what it measured, on stderr.
  describe "a request that finds its connection free" $
    it "allocates no more than it did" $
      pooled id $ \env -> do
        void (pidOf env)
        bytes <- perRequest env
        lookupEnv "POOL_COST_REPORT" >>= \asked -> when (asked /= Nothing) (hPutStrLn stderr ("pool-cost " <> name <> " | withConnection | " <> show (round bytes :: Int)))
        let ceiling' = case name of
              "threads" -> 2560
              "delimited" -> 2080
              _ -> 3395
        when (bytes > ceiling' + 24) $
          failTest ("a request allocates " <> show (round bytes :: Int) <> " bytes, where the ceiling is " <> show (round ceiling' :: Int))

  -- What the pool does with its connections as it closes: those that are free close at once, and one that a request still
  -- has closes as the request gives it back.
  describe "closing" $ do
    it "closes the connections that are free" $ do
      pid <- pooled id pidOf
      eventually 5 (gone pid) `shouldReturn` True

    it "closes the connection that a request still has, as the request gives it back" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      (pool, holder) <- withConnectionPool (connectionPoolSettings 1 settings) $ \pool -> (pool,) <$> startHolding (Env runner buffers pool)
      -- The pool's bracket has ended while the request holds the only connection: it is still open.
      stateOf (holderBackend holder) `shouldReturn` Right ["idle"]
      finishHolding holder
      eventually 5 (gone (holderBackend holder)) `shouldReturn` True
      -- The pool is used last: while it is reachable, its connection is not closed by the collection of its socket.
      runOn runner buffers (withConnection pool done) `shouldReturn` Left PoolClosed

  -- What the pool does with its connections as they age: it closes a connection when it is older than the pool's lifetime, and
  -- when nobody has used it for the pool's idle time, and never one that a request has.
  describe "maintenance" $ do
    needs "pool maintenance" $ it "closes a connection that is older than the pool's lifetime, and makes another for the next request" $
      pooled (\s -> s {poolLifetime = Just (millis 600)}) $ \env -> do
        pid <- pidOf env
        -- Within its lifetime the connection is the same.
        pidOf env `shouldReturn` pid
        -- Once it is older, the pool has closed it, though nothing used it.
        eventually 6 (gone pid) `shouldReturn` True
        pid' <- pidOf env
        (pid' /= pid) `shouldBe` True

    needs "pool maintenance" $ it "closes a connection that nobody has used for the pool's idle time, and not one that is used" $
      pooled (\s -> s {poolIdleTime = Just (millis 600)}) $ \env -> do
        pid <- pidOf env
        -- Used every 150 milliseconds for longer than the idle time, the connection stays.
        forM_ [1 .. 8 :: Int] $ \_ -> threadDelay 150000 *> (pidOf env `shouldReturn` pid)
        -- Not used for the idle time, it is closed.
        eventually 6 (gone pid) `shouldReturn` True
        pid' <- pidOf env
        (pid' /= pid) `shouldBe` True

    needs "pool maintenance" $ it "leaves a connection to the request that has it, though it is older than the lifetime, and closes it once it is given back" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      withConnectionPool ((connectionPoolSettings 1 settings) {poolLifetime = Just (millis 300)}) $ \pool -> do
        holder <- startHolding (Env runner buffers pool)
        threadDelay 900000
        stateOf (holderBackend holder) `shouldReturn` Right ["idle"]
        finishHolding holder
        eventually 6 (gone (holderBackend holder)) `shouldReturn` True

    it "leaves the connections alone when the pool has neither a lifetime nor an idle time" $
      pooled id $ \env -> do
        pid <- pidOf env
        threadDelay 1200000
        pidOf env `shouldReturn` pid

  -- What the pool tells its observer, as v1 told it: a connection is made, ready, in use, ready again or terminated.
  describe "the observer" $ do
    let observed :: (ConnectionPoolSettings -> ConnectionPoolSettings) -> IO (IORef [Observation], ConnectionPoolSettings -> ConnectionPoolSettings)
        observed change = do
          events <- newIORef []
          pure (events, \s -> (change s) {poolObserver = Just (\event -> atomicModifyIORef' events (\told -> (event : told, ())))})
        eventsOf events = reverse <$> readIORef events
        said events = map describeEvent <$> eventsOf events

    needs "the pool's observer" $ it "tells of a connection that is made, used and given back, and of its closing with the pool" $ do
      (events, change) <- observed id
      pooled change $ \env -> void (pidOf env *> pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "ready: succeeded", "in use", "ready: succeeded", "terminated: release"]
      map connectionOf <$> eventsOf events >>= \ids -> length (nub ids) `shouldBe` 1

    needs "the pool's observer" $ it "tells of a use that failed without breaking its connection" $ do
      (events, change) <- observed id
      pooled change $ \env -> do
        onEnv env (withConnection (envPool env) (now (const (throwIO (userError "the use failed")))) `failsAs` \failure -> failure `shouldBe` userError "the use failed")
          `shouldReturn` ()
        void (pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "ready: failed", "in use", "ready: succeeded", "terminated: release"]

    needs "the pool's observer" $ it "tells of a connection that a use broke, and of the one that replaces it" $ do
      (events, change) <- observed id
      pooled change $ \env -> do
        onEnv env (withinFor (millis 100) (withConnection (envPool env) (committed Tx.Read (Tx.statement sleeping) Tx.single))) `shouldReturn` Nothing
        void (pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "terminated: network error", "connecting", "ready: established", "in use", "ready: succeeded", "terminated: release"]

    needs "the pool's observer" $ it "tells of a connection that the server ended, and of the one that replaces it" $ do
      (events, change) <- observed id
      pooled change $ \env -> do
        pid <- pidOf env
        ordinary runner (session settings (query (plain ("SELECT pg_terminate_backend(" <> BC.pack (show pid) <> ")") single))) `shouldReturn` Right [True]
        eventually 6 (gone pid) `shouldReturn` True
        void (pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "ready: succeeded", "terminated: network error", "connecting", "ready: established", "in use", "ready: succeeded", "terminated: release"]

    needs "the pool's observer" $ it "tells of a connection that can't be made" $ do
      (events, change) <- observed id
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      withConnectionPool (change (connectionPoolSettings 1 (unixSettings (socketPath <> "-missing") "postgres" Nothing))) $ \pool ->
        runOn runner buffers (withConnection pool done `failsAs` \(ConnectFailure failures) -> length failures `shouldBe` 1) `shouldReturn` ()
      said events `shouldReturn` ["connecting", "terminated: network error"]

    needs "pool maintenance" $ it "tells of a connection that reached its lifetime, and of one that nobody used for the idle time" $ do
      (aging, changeAging) <- observed (\s -> s {poolLifetime = Just (millis 500)})
      pooled changeAging $ \env -> do
        pid <- pidOf env
        eventually 6 (gone pid) `shouldReturn` True
      said aging `shouldReturn` ["connecting", "ready: established", "in use", "ready: succeeded", "terminated: aging"]
      (idling, changeIdling) <- observed (\s -> s {poolIdleTime = Just (millis 500)})
      pooled changeIdling $ \env -> do
        pid <- pidOf env
        eventually 6 (gone pid) `shouldReturn` True
      said idling `shouldReturn` ["connecting", "ready: established", "in use", "ready: succeeded", "terminated: idleness"]

-- * The pool of the cases

-- | The pool of real connections over the request buffers, and the way to run a program over them.
data Env = Env
  { envRunner :: Runner,
    envBuffers :: BufferPool,
    envPool :: ConnectionPool
  }

-- | The program in one request of its own, on the runner of the environment.
onEnv :: Env -> Program a -> IO a
onEnv env = runOn (envRunner env) (envBuffers env)

-- | The backend of the connection that a request runs on.
pidOf :: Env -> IO Int64
pidOf env = onEnv env (withConnection (envPool env) backend) >>= either (\failure -> failTest ("no connection: " <> show failure)) (either (\failure -> failTest ("no backend: " <> show failure)) pure)

-- | The backend of the connection that a request runs on, as a step on it.
backend :: (Runtime arr) => OnConn arr (Either QueryError Int64)
backend = committed Tx.Read (Tx.statement (plain "SELECT pg_backend_pid()::bigint" single)) Tx.single

-- * A request that outlives its pool's bracket

-- | A request on a thread of its own, which holds the pool's connection until it is told to give it back.
data Holder = Holder
  { -- | The backend of the connection that it holds.
    holderBackend :: Int64,
    holderGoOn :: MVar (),
    holderEnded :: MVar (Either SomeException ())
  }

-- | Starts the request, and gives once it holds the connection.
startHolding :: Env -> IO Holder
startHolding env = do
  held <- newEmptyMVar
  goOn <- newEmptyMVar
  ended <- newEmptyMVar
  let holdIt :: (Runtime arr) => OnConn arr ()
      holdIt = backend >>> now (either (throwIO . userError . show) (\pid -> putMVar held pid *> takeMVar goOn))
  _ <- forkIO (try (onEnv env (withConnection (envPool env) holdIt >>> now (either throwIO pure))) >>= putMVar ended)
  pid <- takeMVar held
  pure (Holder pid goOn ended)

-- | Tells the request to give the connection back, and waits until it has ended.
finishHolding :: Holder -> IO ()
finishHolding holder = putMVar (holderGoOn holder) () *> takeMVar (holderEnded holder) >>= either throwIO pure


-- | Looks until the check is true, for at most this many seconds.
eventually :: Double -> IO Bool -> IO Bool
eventually seconds check = do
  started <- getMonotonicTime
  let go =
        check >>= \ok ->
          if ok
            then pure True
            else getMonotonicTime >>= \time -> if time - started > seconds then pure False else threadDelay 20000 *> go
  go

-- | A statement that runs for 30 seconds.
sleeping :: (Runtime arr) => Tx.Statement arr () Int64
sleeping = plain "SELECT 1::int8 FROM pg_sleep(30)" single

-- | An event as the cases compare it: what it says of the connection, and not which connection.
describeEvent :: Observation -> String
describeEvent (ConnectionObservation _ status) = case status of
  ConnectingConnectionStatus -> "connecting"
  ReadyForUseConnectionStatus EstablishedConnectionReadyForUseReason -> "ready: established"
  ReadyForUseConnectionStatus (SessionFailedConnectionReadyForUseReason _) -> "ready: failed"
  ReadyForUseConnectionStatus SessionSucceededConnectionReadyForUseReason -> "ready: succeeded"
  InUseConnectionStatus -> "in use"
  TerminatedConnectionStatus AgingConnectionTerminationReason -> "terminated: aging"
  TerminatedConnectionStatus IdlenessConnectionTerminationReason -> "terminated: idleness"
  TerminatedConnectionStatus (NetworkErrorConnectionTerminationReason _) -> "terminated: network error"
  TerminatedConnectionStatus ReleaseConnectionTerminationReason -> "terminated: release"

-- | Which connection an event is of.
connectionOf :: Observation -> UUID
connectionOf (ConnectionObservation identifier _) = identifier

-- | The bytes that a request allocates for the pool: the difference between a long run and a short one, after a run that
-- loads the code.
perRequest :: Env -> IO Double
perRequest env = do
  run 1000
  few <- allocatedBy (run 5000)
  many <- allocatedBy (run 25000)
  pure ((fromIntegral many - fromIntegral few) / 20000)
  where
    run :: Int -> IO ()
    run count = onEnv env (timesWith count (tap (withConnection (envPool env) done >>> done)) >>> done)

-- | The bytes that the action allocated: the runtime counts a nursery when it is collected, so a collection is made on both
-- sides.
allocatedBy :: IO () -> IO Word64
allocatedBy action = do
  performMinorGC
  before <- allocated_bytes <$> getRTSStats
  action
  performMinorGC
  allocated <- allocated_bytes <$> getRTSStats
  pure (allocated - before)
