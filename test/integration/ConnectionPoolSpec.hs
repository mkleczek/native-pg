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

import Control.Arrow (arr, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkIO, rtsSupportsBoundThreads, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (forM_, replicateM, replicateM_, void, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BC
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Data.Int (Int32, Int64)
import Data.List (nub)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Word (Word64)
import Database.PostgreSQL.Connection (Address (..), ConnectSettings (..), connectSettings, connectionSlot)
import Database.PostgreSQL.Errors
import Database.PostgreSQL.Pool (ConnectionPool, ConnectionPoolSettings (..), ConnectionReadyForUseReason (..), ConnectionStatus (..), ConnectionTerminationReason (..), Observation (..), PoolStats (..), closeConnectionPool, connectionPoolSettings, connectionPoolStats, lookAt, maintain, newConnectionPool, newMaintenance, withConnection)
import qualified Database.PostgreSQL.Transaction as Tx
import GHC.Clock (getMonotonicTime)
import GHC.Stats (allocated_bytes, gc, gcdetails_live_bytes, getRTSStats)
import Network.Socket (PortNumber, Socket)
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.BufferPool (BufferPoolSettings (..))
import Network.Streaming.PositiveInt (positiveInt)
import Skeletest
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO (hPutStrLn, stderr)
import System.Mem (performMajorGC, performMinorGC)
import System.Timeout (timeout)
import TestPools
import TestPrograms
import TestServers

spec :: FilePath -> PortNumber -> Spec
spec socketPath _port = describe "the connection pool" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let settings = unixSettings socketPath "postgres" Nothing
      -- The pool of this size, for the use, with the buffers that a request runs in, and the way to run a request.
      pooled :: (ConnectionPoolSettings -> ConnectionPoolSettings) -> (Env -> IO a) -> IO a
      pooled change use = do
        buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
        withPool runner buffers (change (connectionPoolSettings 1 settings)) $ \pool -> use (Env runner buffers pool)
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
        onEnv env (viaPool (envPool env) (snd ^>> viaPool (envPool env) done)) `shouldReturn` Right (Left CheckoutTimedOut)
        ended <- getMonotonicTime
        (ended - started >= 0.1) `shouldBe` True
        (ended - started < 5) `shouldBe` True

    it "gives up its place when the deadline of an enclosing 'within' cancels it, and keeps the connection for the next" $
      pooled id $ \env -> do
        pid <- pidOf env
        onEnv env (viaPool (envPool env) (snd ^>> withinFor (millis 50) (viaPool (envPool env) done)))
          `shouldReturn` Right Nothing
        pidOf env `shouldReturn` pid

    it "gives each of several waiting checkouts its deadline, and serves the one that comes after them" $
      pooled (\s -> s {poolCheckoutTimeout = Just (millis 100)}) $ \env -> do
        pid <- pidOf env
        let waiting :: (Runtime arr) => arr Loans (Either PoolError ())
            waiting = viaPool (envPool env) done
        onEnv env (viaPool (envPool env) (snd ^>> steps [waiting `yields` Left CheckoutTimedOut, waiting `yields` Left CheckoutTimedOut]))
          `shouldReturn` Right ()
        pidOf env `shouldReturn` pid

    it "gives PoolClosed once the pool is closed, though a request still holds its connection" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      (pool, holder) <- withPool runner buffers (connectionPoolSettings 1 settings) $ \pool -> (pool,) <$> startHolding (Env runner buffers pool)
      -- The pool's bracket has ended while the request holds the only connection, so the checkout doesn't wait for it.
      runOn runner buffers (viaPool pool done) `shouldReturn` Left PoolClosed
      finishHolding holder

  -- The pool is the input of withConnection: one program, built once, serves the pool of each run, with that pool's own
  -- connections and checkout timeout.
  describe "a pool that is the arrow's input" $ do
    let applicationPool application = connectionPoolSettings 1 settings {connectParameters = [("application_name", application)]}
        applicationOf :: (Runtime arr) => arr (ConnectionPool, Loans) (Either PoolError (Either QueryError [Text]))
        applicationOf = withConnection (query (plain "SELECT current_setting('application_name')" single))
        -- The pool's only connection is held by the outer request, so the inner one waits for it, for the pool's timeout.
        holdingThenAsking :: (Runtime arr) => arr (ConnectionPool, Loans) (Either PoolError (Maybe (Either PoolError ())))
        holdingThenAsking = (\(pool, loans) -> (pool, (pool, loans))) ^>> withConnection (snd ^>> withinFor (millis 400) (withConnection done))
        buffersOf = newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
    it "lends each pool of a run its own connections, in one program" $ do
      buffers <- buffersOf
      withPool runner buffers (applicationPool "one") $ \one -> withPool runner buffers (applicationPool "two") $ \two ->
        runOn runner buffers (across applicationOf [one, two, one])
          `shouldReturn` [Right (Right ["one"]), Right (Right ["two"]), Right (Right ["one"])]
    it "takes the checkout timeout from the pool of each run" $ do
      buffers <- buffersOf
      withPool runner buffers ((applicationPool "one") {poolCheckoutTimeout = Just (millis 100)}) $ \limited -> withPool runner buffers (applicationPool "two") $ \unlimited ->
        runOn runner buffers (across holdingThenAsking [limited, unlimited])
          `shouldReturn` [Right (Just (Left CheckoutTimedOut)), Right Nothing]
    it "gives PoolClosed for a pool that is closed, and serves the pool that isn't, in the same program" $ do
      buffers <- buffersOf
      shut <- withPool runner buffers (applicationPool "one") pure
      withPool runner buffers (applicationPool "two") $ \open ->
        runOn runner buffers (across applicationOf [shut, open, shut])
          `shouldReturn` [Left PoolClosed, Right (Right ["two"]), Left PoolClosed]

  -- Pools are values that are made and closed without waiting, and their maintenance is an arrow that a program runs: the cases
  -- that follow are those of closing and ageing, for pools made as values, with 'maintain' run as a part.
  describe "a pool that is a value" $ do
    let buffersOf = newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
        lasting change = withPool' (change (connectionPoolSettings 1 settings))
        withPool' settings' use = buffersOf >>= \buffers -> withPool runner buffers settings' (\pool -> use (Env runner buffers pool))
    it "makes no connection when it is made, and gives PoolClosed once it is closed, as often as it is closed" $ do
      buffers <- buffersOf
      maintenance <- newMaintenance
      pool <- newConnectionPool maintenance (connectionPoolSettings 2 settings)
      counts pool `shouldReturn` (0, 0, 0)
      void (pidOf (Env runner buffers pool))
      counts pool `shouldReturn` (1, 0, 0)
      closeConnectionPool pool
      closeConnectionPool pool
      runOn runner buffers (viaPool pool done) `shouldReturn` Left PoolClosed

    it "closes the connections that are free as it is closed, and the one that a request has as it gives it back, without waiting" $ do
      buffers <- buffersOf
      maintenance <- newMaintenance
      pool <- newConnectionPool maintenance (connectionPoolSettings 2 settings)
      let env = Env runner buffers pool
      held <- startHolding env
      freePid <- pidOf env
      started <- getMonotonicTime
      closeConnectionPool pool
      took <- subtract started <$> getMonotonicTime
      (took < 0.1) `shouldBe` True
      eventually 5 (gone freePid) `shouldReturn` True
      stateOf (holderBackend held) `shouldReturn` Right ["idle"]
      finishHolding held
      eventually 5 (gone (holderBackend held)) `shouldReturn` True

    it "closes a connection that is older than the pool's lifetime, and makes another for the next request" $
      lasting (\s -> s {poolLifetime = Just (millis 600)}) $ \env -> do
        pid <- pidOf env
        pidOf env `shouldReturn` pid
        eventually 6 (gone pid) `shouldReturn` True
        pid' <- pidOf env
        (pid' /= pid) `shouldBe` True

    it "closes a connection that nobody has used for the pool's idle time, and not one that is used" $
      lasting (\s -> s {poolIdleTime = Just (millis 600)}) $ \env -> do
        pid <- pidOf env
        forM_ [1 .. 8 :: Int] $ \_ -> threadDelay 150000 *> (pidOf env `shouldReturn` pid)
        eventually 6 (gone pid) `shouldReturn` True

    it "looks after every pool that is registered with the maintenance, with one part" $ do
      buffers <- buffersOf
      maintenance <- newMaintenance
      aging <- newConnectionPool maintenance ((connectionPoolSettings 1 settings) {poolLifetime = Just (millis 500)})
      idling <- newConnectionPool maintenance ((connectionPoolSettings 1 settings) {poolIdleTime = Just (millis 500)})
      _ <- forkIO (void (try @SomeException (runOn runner buffers (const maintenance ^>> maintain))))
      agingPid <- pidOf (Env runner buffers aging)
      idlingPid <- pidOf (Env runner buffers idling)
      eventually 6 ((&&) <$> gone agingPid <*> gone idlingPid) `shouldReturn` True
      closeConnectionPool aging
      closeConnectionPool idling

    it "ends where it sleeps when it is cancelled, whatever pools are registered" $ do
      buffers <- buffersOf
      maintenance <- newMaintenance
      pool <- newConnectionPool maintenance ((connectionPoolSettings 1 settings) {poolIdleTime = Just (secs 60)})
      started <- getMonotonicTime
      runOn runner buffers (race (const maintenance ^>> maintain) (done >>> sleep (millis 100))) `shouldReturn` Right ()
      ended <- getMonotonicTime
      (ended - started < 5) `shouldBe` True
      closeConnectionPool pool

  -- What the pool does past its size, as v1's overflow policy did: a request that finds every connection in use waits for one
  -- to come back, and after the overflow delay opens one more, up to the overflow. That connection is closed as its request ends.
  describe "overflow" $ do
    let bounded :: Env -> Program a -> IO (Maybe a)
        bounded env program = onEnv env (withinFor (secs 5) program)
        backendOf :: Env -> IO Int64
        backendOf env =
          bounded env (viaPool (envPool env) backend) >>= \case
            Just (Right (Right pid)) -> pure pid
            other -> failTest ("no connection: " <> show other)
    it "opens a connection past the pool's size only after the overflow delay, and closes it as its request ends" $
      pooled (\s -> s {poolMaxOverflow = 1, poolOverflowDelay = millis 300}) $ \env -> do
        holder <- startHolding env
        started <- getMonotonicTime
        pid <- backendOf env
        ended <- getMonotonicTime
        (ended - started >= 0.25) `shouldBe` True
        (pid /= holderBackend holder) `shouldBe` True
        eventually 5 (gone pid) `shouldReturn` True
        stateOf (holderBackend holder) `shouldReturn` Right ["idle"]
        finishHolding holder

    -- A connection that is ready is the first that is wanted: a request that waits for the delay of an overflow connection has
    -- the one that comes back, and no other is opened.
    it "gives a connection that comes back during the delay to the request that waits, and opens none" $
      pooled (\s -> s {poolMaxOverflow = 1, poolOverflowDelay = secs 5}) $ \env -> do
        holder <- startHolding env
        answer <- newEmptyMVar
        _ <- forkIO (try (onEnv env (viaPool (envPool env) backend)) >>= putMVar answer)
        threadDelay 200000
        started <- getMonotonicTime
        finishHolding holder
        result <- takeMVar answer
        ended <- getMonotonicTime
        case result of
          Right (Right (Right pid)) -> pid `shouldBe` holderBackend holder
          other -> failTest ("no connection: " <> show (other :: Either SomeException (Either PoolError (Either QueryError Int64))))
        (ended - started < 2) `shouldBe` True
        -- The only connection that the pool has is the holder's: none was opened for the delay that ran out meanwhile.
        counts (envPool env) `shouldReturn` (1, 0, 0)

    it "keeps to the size and the overflow: a request past both waits for a connection to come back, and gives up at the pool's timeout" $
      pooled (\s -> s {poolMaxOverflow = 1, poolOverflowDelay = millis 50, poolCheckoutTimeout = Just (millis 600)}) $ \env -> do
        base <- startHolding env
        extra <- startHolding env
        bounded env (viaPool (envPool env) done) `shouldReturn` Just (Left CheckoutTimedOut)
        finishHolding extra
        finishHolding base

    it "serves the requests that wait in the order they came, and leaves no place for one that gave up" $
      pooled (\s -> s {poolMaxOverflow = 1, poolOverflowDelay = millis 150}) $ \env -> do
        base <- startHolding env
        order <- newIORef []
        finished <- newEmptyMVar
        let arrives :: String -> IO ()
            arrives label = void (forkIO (onEnv env (viaPool (envPool env) (now (const (atomicModifyIORef' order (\told -> (label : told, ())))))) *> putMVar finished ()))
        -- The first gives up before its delay is over.
        gaveUp <- newEmptyMVar
        _ <- forkIO (onEnv env (withinFor (millis 100) (viaPool (envPool env) done)) >>= putMVar gaveUp)
        threadDelay 50000
        arrives "first"
        threadDelay 50000
        arrives "second"
        takeMVar gaveUp `shouldReturn` Nothing
        timeout 5000000 (replicateM_ 2 (takeMVar finished)) `shouldReturn` Just ()
        reverse <$> readIORef order `shouldReturn` ["first", "second"]
        finishHolding base

  -- What the pool counts: the connections that are idle, in use, and being made.
  describe "stats" $ do
    it "counts the connections that are idle, that a request has, and that are being made" $
      pooled (\s -> s {poolSize = 2}) $ \env -> do
        counts (envPool env) `shouldReturn` (0, 0, 0)
        void (pidOf env)
        counts (envPool env) `shouldReturn` (1, 0, 0)
        first <- startHolding env
        counts (envPool env) `shouldReturn` (0, 1, 0)
        second <- startHolding env
        counts (envPool env) `shouldReturn` (0, 2, 0)
        finishHolding first
        finishHolding second
        counts (envPool env) `shouldReturn` (2, 0, 0)

    it "counts the connection that a request is making as opening" $
      withSilentServer $ \silent -> do
        buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
        withPool runner buffers (connectionPoolSettings 1 (connectSettings (UnixAddress (silent </> ".s.PGSQL.5432")) "postgres")) $ \pool -> do
          _ <- forkIO (void (runOn runner buffers (withinFor (secs 2) (viaPool pool done))))
          eventually 3 ((== (0, 0, 1)) <$> counts pool) `shouldReturn` True
          -- The request gave up: its slot is empty and free again.
          eventually 5 ((== (0, 0, 0)) <$> counts pool) `shouldReturn` True

    -- The pool keeps a soft limit, which its size is until the server refuses a connection for its connection limit, and the count of
    -- the refusals: a program that reports on the pool, as PostgREST's metrics do, reads both with the counts of the connections.
    it "reports the size of the pool as its soft limit, and no refusals, while the server has refused nothing" $
      pooled (\s -> s {poolSize = 4, poolMaxOverflow = 1}) $ \env -> do
        void (pidOf env)
        stats <- connectionPoolStats (envPool env)
        (statsSoftLimit stats, statsRefusals stats) `shouldBe` (4, 0)

  -- What a program keeps for a connection, as a program that prepares statements keeps the names that its connection has, lives as
  -- long as the connection does: through every use of it by a pool, and not beyond.
  describe "the slot of a connection" $ do
    let terminated pid = ordinary runner (session settings (query (plain ("SELECT pg_terminate_backend(" <> BC.pack (show pid) <> ")") single))) `shouldReturn` Right [True]
    it "starts with the value that it is given on a new connection, and is the connection's own" $
      ordinary
        runner
        ( session settings $
            alsoOn settings $
              steps
                [ onFirst seen `yields` 0,
                  onFirst raised `yields` 1,
                  onSecond seen `yields` 0,
                  onSecond raised `yields` 1,
                  onFirst raised `yields` 2
                ]
        )
        `shouldReturn` ()
    it "keeps a cell for each type of value" $
      ordinary runner (session settings (steps [raised `yields` 1, noted `yields` "none", noted `yields` "none!", raised `yields` 2]))
        `shouldReturn` ()
    it "survives the checkouts of the same pooled connection" $
      pooled id $ \env -> do
        pid <- pidOf env
        onEnv env (viaPool (envPool env) raised) `shouldReturn` Right 1
        onEnv env (viaPool (envPool env) raised) `shouldReturn` Right 2
        pidOf env `shouldReturn` pid
    it "is gone when the pool replaces the connection, which starts with the value that it is given" $
      pooled id $ \env -> do
        pid <- pidOf env
        onEnv env (viaPool (envPool env) raised) `shouldReturn` Right 1
        terminated pid
        eventually 6 (gone pid) `shouldReturn` True
        onEnv env (viaPool (envPool env) (backend &&& seen)) >>= \case
          Right (Right pid', value) -> do
            (pid' /= pid) `shouldBe` True
            value `shouldBe` 0
          other -> failTest ("the replacing connection failed: " <> show other)
    it "is not reached through a handle that was given up" $
      pooled id $ \env -> do
        leaked <- onEnv env (viaPool (envPool env) (arr fst)) >>= either (\failure -> failTest ("no connection: " <> show failure)) pure
        (either Just (const Nothing) <$> try @UsageError (connectionSlot leaked (0 :: Int))) `shouldReturn` Just ConnectionReleased
    it "keeps the names that a program has prepared on a connection, so that it prepares once for each connection" $
      pooled id $ \env -> do
        pid <- pidOf env
        let asked = onEnv env (viaPool (envPool env) slotted)
        -- The first use prepares the statement, the second executes what the connection has, and the replacing connection has none.
        asked `shouldReturn` Right (Right 2)
        asked `shouldReturn` Right (Right 2)
        terminated pid
        eventually 6 (gone pid) `shouldReturn` True
        asked `shouldReturn` Right (Right 2)

  -- A server that refuses a connection for its connection limit, which v1's overflow policy took for a reason to wait and try again:
  -- the request that is making the connection waits for the overflow delay, and goes on. A pool without overflow reports it.
  describe "a connection that the server refuses for its limit" $ do
    let refusal :: Socket -> IO ()
        refusal client = receiveStartup client *> SocketBytes.sendAll client (frame 'E' "SFATAL\0VFATAL\0C53300\0Msorry, too many clients already\0\0")
        rejected :: Socket -> IO ()
        rejected client = receiveStartup client *> SocketBytes.sendAll client (frame 'E' "SFATAL\0VFATAL\0C28P01\0Mpassword authentication failed\0\0")
        accepting :: Socket -> IO ()
        accepting client = receiveStartup client *> SocketBytes.sendAll client (frame 'R' (int32 0) <> frame 'Z' "I") *> drain client
        counting :: IORef Int -> (Socket -> IO ()) -> Socket -> IO ()
        counting attempts script client = atomicModifyIORef' attempts (\n -> (n + 1, ())) *> script client
        poolTo port change = change (connectionPoolSettings 1 (connectSettings (TcpAddress "127.0.0.1" port) "postgres"))
        buffersOf = newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
    it "is tried again after the overflow delay, and the request gets its connection" $ do
      attempts <- newIORef 0
      buffers <- buffersOf
      withScripted [counting attempts refusal, counting attempts accepting] $ \port ->
        withPool runner buffers (poolTo port (\s -> s {poolMaxOverflow = 1, poolOverflowDelay = millis 150})) $ \pool -> do
          started <- getMonotonicTime
          runOn runner buffers (viaPool pool done) `shouldReturn` Right ()
          ended <- getMonotonicTime
          (ended - started >= 0.12) `shouldBe` True
          readIORef attempts `shouldReturn` 2

    it "is tried again until the deadline of the arrow that runs the request, and leaves its slot free" $ do
      attempts <- newIORef 0
      buffers <- buffersOf
      withScripted (repeat (counting attempts refusal)) $ \port ->
        withPool runner buffers (poolTo port (\s -> s {poolMaxOverflow = 1, poolOverflowDelay = millis 60})) $ \pool -> do
          runOn runner buffers (withinFor (millis 500) (viaPool pool done)) `shouldReturn` Nothing
          tried <- readIORef attempts
          (tried >= 3 && tried < 12) `shouldBe` True
          counts pool `shouldReturn` (0, 0, 0)

    -- What the pool did before it had overflow, and still does when it has none, or when the failure is another.
    it "is reported by a pool that has no overflow" $ do
      attempts <- newIORef 0
      buffers <- buffersOf
      withScripted [counting attempts refusal, counting attempts accepting] $ \port ->
        withPool runner buffers (poolTo port id) $ \pool -> do
          runOn runner buffers (viaPool pool done `failsAs` \(ConnectFailure failures) -> length failures `shouldBe` 1) `shouldReturn` ()
          readIORef attempts `shouldReturn` 1

    it "is the only failure that is tried again: another is reported at once" $ do
      attempts <- newIORef 0
      buffers <- buffersOf
      withScripted [counting attempts rejected, counting attempts accepting] $ \port ->
        withPool runner buffers (poolTo port (\s -> s {poolMaxOverflow = 1, poolOverflowDelay = millis 50})) $ \pool -> do
          runOn runner buffers (viaPool pool done `failsAs` \(ConnectFailure failures) -> length failures `shouldBe` 1) `shouldReturn` ()
          readIORef attempts `shouldReturn` 1

  -- What the stats say of the refusals: how many the server made, and the soft limit that they cut, to 70% of what it was for each.
  describe "the refusals that the stats report" $ do
    let refusal :: Socket -> IO ()
        refusal client = receiveStartup client *> SocketBytes.sendAll client (frame 'E' "SFATAL\0VFATAL\0C53300\0Msorry, too many clients already\0\0")
        accepting :: Socket -> IO ()
        accepting client = receiveStartup client *> SocketBytes.sendAll client (frame 'R' (int32 0) <> frame 'Z' "I") *> drain client
        poolTo port change = change (connectionPoolSettings 1 (connectSettings (TcpAddress "127.0.0.1" port) "postgres"))
        buffersOf = newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
        limits pool = (\stats -> (statsRefusals stats, statsSoftLimit stats)) <$> connectionPoolStats pool
    it "counts each refusal that a pool with overflow tries again, and cuts its soft limit by 30% for each" $ do
      buffers <- buffersOf
      withScripted [refusal, refusal, accepting] $ \port ->
        withPool runner buffers (poolTo port (\s -> s {poolSize = 10, poolMaxOverflow = 1, poolOverflowDelay = millis 60})) $ \pool -> do
          limits pool `shouldReturn` (0, 10)
          runOn runner buffers (viaPool pool done) `shouldReturn` Right ()
          -- 70% of 10 is 7, and 70% of 7 is 4.
          limits pool `shouldReturn` (2, 4)
    it "counts the refusal that a pool without overflow reports, and keeps its soft limit" $ do
      buffers <- buffersOf
      withScripted [refusal, accepting] $ \port ->
        withPool runner buffers (poolTo port (\s -> s {poolSize = 5})) $ \pool -> do
          runOn runner buffers (viaPool pool done `failsAs` \(ConnectFailure failures) -> length failures `shouldBe` 1) `shouldReturn` ()
          limits pool `shouldReturn` (1, 5)

  -- What a request pays the pool when its connection is free: the bytes that a request allocates for the checkout, the
  -- use of the connection and the release, with a body that does nothing. The ceiling is what each arrow allocated when it
  -- was set, in the threaded build; the non-threaded run time allocates a few dozen bytes more for the same code, which it is
  -- allowed. A change that makes a request dearer says so here, as one that makes it cheaper takes the ceiling down. With
  -- @POOL_COST_REPORT@ set, the case says what it measured, on stderr.
  describe "a request that finds its connection free" $ do
    let costs :: String -> (ConnectionPoolSettings -> ConnectionPoolSettings) -> Double -> IO ()
        costs label change ceiling' = pooled change $ \env -> do
          void (pidOf env)
          bytes <- perRequest env
          lookupEnv "POOL_COST_REPORT" >>= \asked -> when (asked /= Nothing) (hPutStrLn stderr ("pool-cost " <> name <> " | " <> label <> " | " <> show (round bytes :: Int)))
          when (bytes > ceiling' + 24 + (if rtsSupportsBoundThreads then 0 else 48)) $
            failTest ("a request allocates " <> show (round bytes :: Int) <> " bytes, where the ceiling is " <> show (round ceiling' :: Int))
    it "allocates no more than it did" $
      costs "withConnection" id $ case name of
        "threads" -> 2376
        "delimited" -> 1965
        _ -> 3140
    it "allocates no more than it did when the pool has a checkout timeout" $
      costs "withConnection, with a timeout" (\s -> s {poolCheckoutTimeout = Just (secs 3600)}) $ case name of
        "threads" -> 2376
        "delimited" -> 1980
        _ -> 3140
    it "allocates no more than it did when the pool has an observer" $
      costs "withConnection, observed" (\s -> s {poolObserver = Just (const (pure ()))}) $ case name of
        "threads" -> 2464
        "delimited" -> 2070
        _ -> 3225

  -- What the pool does with its connections as it closes: those that are free close at once, and one that a request still
  -- has closes as the request gives it back.
  describe "closing" $ do
    it "closes the connections that are free" $ do
      pid <- pooled id pidOf
      eventually 5 (gone pid) `shouldReturn` True

    it "closes the connection that a request still has, as the request gives it back" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      (pool, holder) <- withPool runner buffers (connectionPoolSettings 1 settings) $ \pool -> (pool,) <$> startHolding (Env runner buffers pool)
      -- The pool's bracket has ended while the request holds the only connection: it is still open.
      stateOf (holderBackend holder) `shouldReturn` Right ["idle"]
      finishHolding holder
      eventually 5 (gone (holderBackend holder)) `shouldReturn` True
      -- The pool is used last: while it is reachable, its connection is not closed by the collection of its socket.
      runOn runner buffers (viaPool pool done) `shouldReturn` Left PoolClosed

  -- A statement that a request left running, by being cancelled or by missing its deadline, runs on in the server until it ends, since
  -- the pool only closes its connection. The pool has the maintenance send the cancel request, so that the interrupted request does
  -- not wait for it and a server that doesn't answer can't hold it for longer than the pool's cancel timeout.
  describe "a statement that is interrupted" $ do
    let sleepingOn :: (Runtime arr) => ConnectionPool -> arr Loans (Either PoolError (Either QueryError Int64))
        sleepingOn pool = viaPool pool (committed Tx.Read (Tx.statement sleeping) Tx.single)
    it "is cancelled on the server when the deadline of its request passes, and its backend is gone within a second" $
      pooled id $ \env -> do
        pid <- pidOf env
        started <- getMonotonicTime
        (() <$) <$> onEnv env (withinFor (millis 100) (sleepingOn (envPool env))) `shouldReturn` Nothing
        ended <- getMonotonicTime
        -- The request ended at its deadline, and did not wait for the cancel.
        (ended - started < 1) `shouldBe` True
        eventually 1 (gone pid) `shouldReturn` True
    it "is cancelled on the server when the part that runs it is cancelled from outside" $
      pooled id $ \env -> do
        pid <- pidOf env
        (either (const ()) (const ()) <$> onEnv env (race (sleepingOn (envPool env)) (after (millis 100) done))) `shouldReturn` ()
        eventually 1 (gone pid) `shouldReturn` True
    it "does not reach the statement of the request that comes next, which has a connection of its own" $
      pooled id $ \env -> do
        pid <- pidOf env
        (() <$) <$> onEnv env (withinFor (millis 100) (sleepingOn (envPool env))) `shouldReturn` Nothing
        later <- onEnv env (viaPool (envPool env) (committed Tx.Read (Tx.statement (plain "SELECT 1::int8 FROM pg_sleep(0.6)" (single @_ @Int64))) Tx.single))
        either (const Nothing) (either (const Nothing) Just) later `shouldBe` Just (1 :: Int64)
        eventually 1 (gone pid) `shouldReturn` True
    it "is given the pool's cancel timeout, and a server that does not answer the cancel delays neither the request nor the next cancel" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      firstCancel <- newEmptyMVar
      secondCancel <- newEmptyMVar
      let serving :: Socket -> IO ()
          serving client = receiveStartup client *> SocketBytes.sendAll client (frame 'R' (int32 0) <> frame 'K' (int32 7 <> int32 9) <> frame 'Z' "I") *> drain client
          silent :: MVar () -> Socket -> IO ()
          silent signal client = receiveStartup client *> putMVar signal () *> drain client
          interrupted pool = (() <$) <$> runOn runner buffers (withinFor (millis 100) (viaPool pool (committed Tx.Read (Tx.statement (plain "SELECT 1::int8" (single @_ @Int64))) Tx.single)))
      withScripted [serving, silent firstCancel, serving, silent secondCancel] $ \port ->
        withPool runner buffers ((connectionPoolSettings 1 (connectSettings (TcpAddress "127.0.0.1" port) "postgres")) {poolCancelTimeout = millis 300}) $ \pool -> do
          started <- getMonotonicTime
          interrupted pool `shouldReturn` Nothing
          ended <- getMonotonicTime
          (ended - started < 1) `shouldBe` True
          timeout 3000000 (takeMVar firstCancel) `shouldReturn` Just ()
          -- The first cancel waits for a server that says nothing, until the cancel timeout: the second goes on after it.
          interrupted pool `shouldReturn` Nothing
          timeout 3000000 (takeMVar secondCancel) `shouldReturn` Just ()

  -- What the pool does with its connections as they age: it closes a connection when it is older than the pool's lifetime, and
  -- when nobody has used it for the pool's idle time, and never one that a request has.
  describe "maintenance" $ do
    it "closes a connection that is older than the pool's lifetime, and makes another for the next request" $
      pooled (\s -> s {poolLifetime = Just (millis 600)}) $ \env -> do
        pid <- pidOf env
        -- Within its lifetime the connection is the same.
        pidOf env `shouldReturn` pid
        -- Once it is older, the pool has closed it, though nothing used it.
        eventually 6 (gone pid) `shouldReturn` True
        pid' <- pidOf env
        (pid' /= pid) `shouldBe` True

    it "closes a connection that nobody has used for the pool's idle time, and not one that is used" $
      pooled (\s -> s {poolIdleTime = Just (millis 600)}) $ \env -> do
        pid <- pidOf env
        -- Used every 150 milliseconds for longer than the idle time, the connection stays.
        forM_ [1 .. 8 :: Int] $ \_ -> threadDelay 150000 *> (pidOf env `shouldReturn` pid)
        -- Not used for the idle time, it is closed.
        eventually 6 (gone pid) `shouldReturn` True
        pid' <- pidOf env
        (pid' /= pid) `shouldBe` True

    it "leaves a connection to the request that has it, though it is older than the lifetime, and closes it once it is given back" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      withPool runner buffers ((connectionPoolSettings 1 settings) {poolLifetime = Just (millis 300)}) $ \pool -> do
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

  -- What a pool costs by the connections that it has, and not by the connections that it may have: one look at its connections, which the
  -- maintenance makes for each pool every second, and the memory of a pool before it has a connection. A pool of 2,000 slots with two
  -- connections costs what a pool of 20 slots with the same two does. With @POOL_COST_REPORT@ set, each case says what it measured, on stderr.
  describe "the cost of a pool by its connections, and not by its size" $ do
    let reported :: String -> String -> IO ()
        reported label figures = lookupEnv "POOL_COST_REPORT" >>= \asked -> when (asked /= Nothing) (hPutStrLn stderr ("pool-size " <> name <> " | " <> label <> " | " <> figures))
        -- What one look allocates, in bytes, and takes, in microseconds, at a pool of this size that has two connections, both free: the
        -- pool has a lifetime that none has reached, so that every look goes through its connections.
        lookCost :: Int -> IO (Double, Double)
        lookCost size = do
          buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
          maintenance <- newMaintenance
          pool <- newConnectionPool maintenance ((connectionPoolSettings size settings) {poolLifetime = Just (secs 3600)})
          let env = Env runner buffers pool
          one <- startHolding env
          two <- startHolding env
          finishHolding one
          finishHolding two
          counts pool `shouldReturn` (2, 0, 0)
          instant <- monotonicNanoseconds
          let looks = 200 :: Int
          started <- getMonotonicTime
          bytes <- allocatedBy (replicateM_ looks (lookAt pool instant))
          ended <- getMonotonicTime
          counts pool `shouldReturn` (2, 0, 0)
          closeConnectionPool pool
          pure (fromIntegral bytes / fromIntegral looks, (ended - started) * 1e6 / fromIntegral looks)
        -- What a pool of this size holds before it has a connection, in bytes: the live bytes of the heap with 500 of them, less without.
        heldBy :: Int -> IO Double
        heldBy size = do
          maintenance <- newMaintenance
          without <- liveBytes
          pools <- replicateM 500 (newConnectionPool maintenance ((connectionPoolSettings size settings) {poolLifetime = Just (secs 1800)}))
          holding <- liveBytes
          mapM_ closeConnectionPool pools
          pure (fromIntegral holding / 500 - fromIntegral without / 500)
    needs "a look that visits the connections that a pool has" $
      it "allocates for a pool of 2,000 slots no more than for a pool of 20 slots, with two connections in each" $ do
        (smallBytes, smallMicros) <- lookCost 20
        (largeBytes, largeMicros) <- lookCost 2000
        reported "a look" (show (round smallBytes :: Int) <> " bytes, " <> show (round smallMicros :: Int) <> " us at 20 | " <> show (round largeBytes :: Int) <> " bytes, " <> show (round largeMicros :: Int) <> " us at 2000")
        when (largeBytes > smallBytes * 1.05 + 64) $
          failTest ("a look allocates " <> show (round largeBytes :: Int) <> " bytes at 2,000 slots, and " <> show (round smallBytes :: Int) <> " at 20")
    needs "a pool whose memory goes by its connections" $
      it "holds for a pool of 2,000 slots, before it has a connection, no more than for a pool of 20 slots" $ do
        small <- heldBy 20
        large <- heldBy 2000
        reported "an empty pool" (show (round small :: Int) <> " bytes at 20 | " <> show (round large :: Int) <> " bytes at 2000")
        when (large > small * 1.1 + 1024) $
          failTest ("an empty pool holds " <> show (round large :: Int) <> " bytes at 2,000 slots, and " <> show (round small :: Int) <> " at 20")

  -- What the pool tells its observer, as v1 told it: a connection is made, ready, in use, ready again or terminated.
  describe "the observer" $ do
    let observed :: (ConnectionPoolSettings -> ConnectionPoolSettings) -> IO (IORef [Observation], ConnectionPoolSettings -> ConnectionPoolSettings)
        observed change = do
          events <- newIORef []
          pure (events, \s -> (change s) {poolObserver = Just (\event -> atomicModifyIORef' events (\told -> (event : told, ())))})
        eventsOf events = reverse <$> readIORef events
        said events = map describeEvent <$> eventsOf events

    it "tells of a connection that is made, used and given back, and of its closing with the pool" $ do
      (events, change) <- observed id
      pooled change $ \env -> void (pidOf env *> pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "ready: succeeded", "in use", "ready: succeeded", "terminated: release"]
      map connectionOf <$> eventsOf events >>= \ids -> length (nub ids) `shouldBe` 1

    it "tells of a use that failed without breaking its connection" $ do
      (events, change) <- observed id
      pooled change $ \env -> do
        onEnv env (viaPool (envPool env) (now (const (throwIO (userError "the use failed")))) `failsAs` \failure -> failure `shouldBe` userError "the use failed")
          `shouldReturn` ()
        void (pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "ready: failed", "in use", "ready: succeeded", "terminated: release"]

    it "tells of a connection that a use broke, and of the one that replaces it" $ do
      (events, change) <- observed id
      pooled change $ \env -> do
        onEnv env (withinFor (millis 100) (viaPool (envPool env) (committed Tx.Read (Tx.statement sleeping) Tx.single))) `shouldReturn` Nothing
        void (pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "terminated: network error", "connecting", "ready: established", "in use", "ready: succeeded", "terminated: release"]

    it "tells of a connection that the server ended, and of the one that replaces it" $ do
      (events, change) <- observed id
      pooled change $ \env -> do
        pid <- pidOf env
        ordinary runner (session settings (query (plain ("SELECT pg_terminate_backend(" <> BC.pack (show pid) <> ")") single))) `shouldReturn` Right [True]
        eventually 6 (gone pid) `shouldReturn` True
        void (pidOf env)
      said events
        `shouldReturn` ["connecting", "ready: established", "in use", "ready: succeeded", "terminated: network error", "connecting", "ready: established", "in use", "ready: succeeded", "terminated: release"]

    it "tells of a connection that can't be made" $ do
      (events, change) <- observed id
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      withPool runner buffers (change (connectionPoolSettings 1 (unixSettings (socketPath <> "-missing") "postgres" Nothing))) $ \pool ->
        runOn runner buffers (viaPool pool done `failsAs` \(ConnectFailure failures) -> length failures `shouldBe` 1) `shouldReturn` ()
      said events `shouldReturn` ["connecting", "terminated: network error"]

    it "tells of a connection that reached its lifetime, and of one that nobody used for the idle time" $ do
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
pidOf env = onEnv env (viaPool (envPool env) backend) >>= either (\failure -> failTest ("no connection: " <> show failure)) (either (\failure -> failTest ("no backend: " <> show failure)) pure)

-- | The count that the slot of the connection keeps, which starts at zero.
seen :: (Runtime arr) => OnConn arr Int
seen = now (\(connection, _) -> connectionSlot connection (0 :: Int) >>= readIORef)

-- | The count in the slot of the connection, raised by one: what it is then.
raised :: (Runtime arr) => OnConn arr Int
raised = now (\(connection, _) -> connectionSlot connection (0 :: Int) >>= \slot -> atomicModifyIORef' slot (\n -> (n + 1, n + 1)))

-- | The text that the slot of the connection keeps, which starts as none: what it was, and then it is told with a mark.
noted :: (Runtime arr) => OnConn arr Text
noted = now (\(connection, _) -> connectionSlot connection ("none" :: Text) >>= \slot -> atomicModifyIORef' slot (\text -> (text <> "!", text)))

-- | The names that a program has prepared on a connection, as a program keeps them in the slot of the connection.
newtype Prepared = Prepared [ByteString]

-- | A request that prepares its statement on a connection that doesn't have it, and remembers that the connection has it, and else
-- executes what the connection has.
slotted :: (Runtime arr) => OnConn arr (Either QueryError Int32)
slotted = alongside known >>> (\(has, input) -> if has then Left input else Right input) ^>> (executing ||| (preparing &&& remember >>^ fst))
  where
    statement = valued "SELECT $1::int + 1" [Just "1"] (single @_ @Int32)
    slot connection = connectionSlot connection (Prepared [])
    known = now (\(connection, _) -> slot connection >>= fmap (\(Prepared names) -> "slotted" `elem` names) . readIORef)
    remember = now (\(connection, _) -> slot connection >>= \cell -> modifyIORef' cell (\(Prepared names) -> Prepared ("slotted" : names)))
    preparing = committed Tx.Read ((,) <$> Tx.prepare "slotted" statement <*> Tx.executePrepared "slotted" statement) (\(preparation, result) -> Tx.prepared preparation >>> Tx.single result)
    executing = committed Tx.Read (Tx.executePrepared "slotted" statement) Tx.single

-- | How many connections the pool has that are idle, that a request has, and that a request is making.
counts :: ConnectionPool -> IO (Int, Int, Int)
counts pool = (\stats -> (statsIdle stats, statsInUse stats, statsOpening stats)) <$> connectionPoolStats pool

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
  _ <- forkIO (try (onEnv env (viaPool (envPool env) holdIt >>> now (either throwIO pure))) >>= putMVar ended)
  pid <- timeout 5000000 (takeMVar held) >>= maybe (failTest "the request did not get its connection in five seconds") pure
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
    run count = onEnv env (timesWith count (tap (viaPool (envPool env) done >>> done)) >>> done)

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

-- | The bytes of the heap that are live after a major collection.
liveBytes :: IO Word64
liveBytes = performMajorGC *> (gcdetails_live_bytes . gc <$> getRTSStats)
