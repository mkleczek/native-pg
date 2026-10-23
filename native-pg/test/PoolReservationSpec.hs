module PoolReservationSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent.Resource.Acquisition qualified as Acquire
import Control.Concurrent (forkFinally, forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (atomically, newTVarIO, orElse, readTVar, retry, writeTVar)
import Control.Exception (ErrorCall (..), SomeException, throwIO, try, finally)
import Control.Monad (void)
import Data.Either (isLeft)
import Data.Functor (($>))
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (isNothing)
import Data.Text qualified as Text
import Database.PostgreSQL.Pool
import GHC.Clock (getMonotonicTimeNSec)
import System.Timeout (timeout)
import Skeletest

spec :: Spec
spec = readySpec *> openingSpec *> shutdownSpec *> policySpec

readySpec :: Spec
readySpec = describe "ready connection reservations" $ do
  it "takes only ready connections and returns one exactly once" $ getAcquisition >>= \waiting -> do
    (pool, opened, _) <- harness waiting id
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    useOnce pool `shouldReturn` Right 1
    reservation <- ready pool
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    validateReservation reservation `shouldReturn` True
    useReservation reservation (\connection -> pure (Succeeded, connection)) `shouldReturn` 1
    finishReservation reservation Succeeded
    validateReservation reservation `shouldReturn` False
    rejected <- try @ReservationError (useReservation reservation (\_ -> pure (Succeeded, ())))
    rejected `shouldBe` Left ReservationFinished
    useOnce pool `shouldReturn` Right 1
    readIORef opened `shouldReturn` 1
    release pool

  it "rolls back the ready claim when another STM resource cannot be acquired" $ getAcquisition >>= \waiting -> do
    (pool, _, _) <- harness waiting id
    void (useOnce pool)
    permit <- newTVarIO False
    combined <- atomically $ (do
      reservation <- tryReserveReady pool
      available <- readTVar permit
      if available then pure reservation else retry) `orElse` pure Nothing
    isNothing combined `shouldBe` True
    atomically (writeTVar permit True)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    finishReservation reservation Succeeded
    release pool

  it "validates idleness at the time of use and frees invalid capacity once" $ getAcquisition >>= \waiting -> do
    (pool, _, closed) <- harness waiting (\config -> config{configMaxIdletime = 0.01})
    void (useOnce pool)
    reservation <- ready pool
    threadDelay 30_000
    validateReservation reservation `shouldReturn` False
    validateReservation reservation `shouldReturn` False
    finishReservation reservation Succeeded
    readIORef closed `shouldReturn` [1]
    useOnce pool `shouldReturn` Right 2
    release pool

  it "invalidates a tentative claim across pool release" $ getAcquisition >>= \waiting -> do
    (pool, _, closed) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    release pool
    validateReservation reservation `shouldReturn` False
    finishReservation reservation Succeeded
    readIORef closed `shouldReturn` [1]
    useOnce pool `shouldReturn` Right 2
    release pool

  it "keeps validated ownership through release and closes at finish" $ getAcquisition >>= \waiting -> do
    (pool, _, closed) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    release pool
    validateReservation reservation `shouldReturn` True
    finishReservation reservation Succeeded
    finishReservation reservation Succeeded
    readIORef closed `shouldReturn` [1]
    useOnce pool `shouldReturn` Right 2
    release pool

  it "returns an exception-usable connection and rethrows the action exception" $ getAcquisition >>= \waiting -> do
    (pool, _, closed) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    result <- try @ErrorCall (useReservation reservation (\_ -> throwIO (ErrorCall "action failed")) :: IO ())
    result `shouldBe` Left (ErrorCall "action failed")
    readIORef closed `shouldReturn` []
    useOnce pool `shouldReturn` Right 1
    release pool

  it "frees capacity even when closing a broken reservation throws" $ getAcquisition >>= \waiting -> do
    (pool, opened, _) <- harness waiting (\config -> config{configClose = const (throwIO (ErrorCall "close failed"))})
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    void (try @SomeException (finishReservation reservation (Broke Nothing)))
    finishReservation reservation Succeeded
    useOnce pool `shouldReturn` Right 2
    readIORef opened `shouldReturn` 2
    void (try @SomeException (release pool))

  it "hands the connection to an existing use waiter before a new reservation" $ getAcquisition >>= \waiting -> do
    (pool, _, _) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    entered <- newEmptyMVar
    done <- newEmptyMVar
    result <- newEmptyMVar
    _ <- forkIO $ use pool (\connection -> putMVar entered () *> takeMVar done $> (Succeeded, connection)) >>= putMVar result
    threadDelay 20_000
    finishReservation reservation Succeeded
    takeMVar entered
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    putMVar done ()
    takeMVar result `shouldReturn` Right 1
    release pool

ready :: Pool Int String -> IO (Reservation Int String)
ready pool = atomically (tryReserveReady pool) >>= maybe (fail "expected a ready reservation") pure

useOnce :: Pool Int String -> IO (Either (UsageError String) Int)
useOnce pool = use pool (\connection -> pure (Succeeded, connection))

harness :: Acquire.Context -> (Config Int String -> Config Int String) -> IO (Pool Int String, IORef Int, IORef [Int])
harness waiting = policyHarness waiting defaultPolicy

policyHarness :: Acquire.Context -> Policy String -> (Config Int String -> Config Int String) -> IO (Pool Int String, IORef Int, IORef [Int])
policyHarness waiting policy configure = do
  opened <- newIORef 0
  closed <- newIORef []
  pool <- newPoolWithPolicy waiting policy . configure $ Config
    { configSize = 1
    , configAcquisitionTimeout = 1
    , configMaxLifetime = 3600
    , configMaxIdletime = 3600
    , configConnect = Right <$> atomicModifyIORef' opened (\count -> (count + 1, count + 1))
    , configDescribeFailure = Just . Text.pack
    , configClose = \connection -> atomicModifyIORef' closed (\connections -> (connections <> [connection], ()))
    , configUsableAfterException = const (pure True)
    , configObserve = const (pure ())
    }
  pure (pool, opened, closed)

opening :: Pool Int String -> IO (Opening Int String)
opening pool = atomically (tryReserveOpening pool) >>= maybe (fail "expected an opening slot") pure

openingSpec :: Spec
openingSpec = describe "independent connection startup" $ do
  it "opens a ready connection without occupying a request reservation" $ getAcquisition >>= \waiting -> do
    (pool, opened, _) <- harness waiting id
    startup <- opening pool
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    prepareOpening startup `shouldReturn` Right ()
    rejected <- try @OpeningError (prepareOpening startup)
    rejected `shouldBe` Left OpeningFinished
    abandonOpening startup
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    useReservation reservation (\connection -> pure (Succeeded, connection)) `shouldReturn` 1
    readIORef opened `shouldReturn` 1
    release pool

  it "abandons an unused opening token exactly once" $ getAcquisition >>= \waiting -> do
    (pool, _, _) <- harness waiting id
    first <- opening pool
    abandonOpening first
    abandonOpening first
    second <- opening pool
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    rejected <- try @OpeningError (prepareOpening first)
    rejected `shouldBe` Left OpeningFinished
    prepareOpening second `shouldReturn` Right ()
    useOnce pool `shouldReturn` Right 1
    release pool

  it "returns capacity after a reported startup failure" $ getAcquisition >>= \waiting -> do
    failNext <- newIORef True
    (pool, _, _) <- harness waiting (\config -> config{configConnect = do
      failing <- atomicModifyIORef' failNext (False,)
      if failing then pure (Left "offline") else configConnect config})
    first <- opening pool
    prepareOpening first `shouldReturn` Left (ConnectionFailed "offline")
    second <- opening pool
    prepareOpening second `shouldReturn` Right ()
    useOnce pool `shouldReturn` Right 1
    release pool

  it "returns capacity when a connecting thread is cancelled" $ getAcquisition >>= \waiting -> do
    started <- newEmptyMVar
    gate <- newEmptyMVar
    finished <- newEmptyMVar
    (pool, _, _) <- harness waiting (\config -> config{configConnect = putMVar started () *> takeMVar gate *> configConnect config})
    startup <- opening pool
    thread <- forkFinally (prepareOpening startup) (putMVar finished)
    takeMVar started
    killThread thread
    isLeft <$> takeMVar finished `shouldReturn` True
    abandonOpening startup
    replacement <- opening pool
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    abandonOpening replacement
    release pool

  it "closes a connection opened across release instead of recycling its old generation" $ getAcquisition >>= \waiting -> do
    started <- newEmptyMVar
    gate <- newEmptyMVar
    finished <- newEmptyMVar
    (pool, _, closed) <- harness waiting (\config -> config{configConnect = putMVar started () *> takeMVar gate *> configConnect config})
    startup <- opening pool
    _ <- forkIO (prepareOpening startup >>= putMVar finished)
    takeMVar started
    release pool
    putMVar gate ()
    takeMVar finished `shouldReturn` Right ()
    readIORef closed `shouldReturn` [1]
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    replacement <- opening pool
    abandonOpening replacement
    release pool

  it "returns a startup connection to an existing use waiter" $ getAcquisition >>= \waiting -> do
    (pool, _, _) <- harness waiting id
    startup <- opening pool
    entered <- newEmptyMVar
    done <- newEmptyMVar
    result <- newEmptyMVar
    _ <- forkIO $ use pool (\connection -> putMVar entered () *> takeMVar done $> (Succeeded, connection)) >>= putMVar result
    threadDelay 20_000
    prepareOpening startup `shouldReturn` Right ()
    takeMVar entered
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    putMVar done ()
    takeMVar result `shouldReturn` Right 1
    release pool


shutdownSpec :: Spec
shutdownSpec = describe "permanent pool shutdown" $ do
  it "rejects waiting and new uses, retires owned connections, and stays closed after release" $ getAcquisition >>= \waiting -> do
    (pool, opened, closed) <- harness waiting id
    void (useOnce pool)
    owned <- ready pool
    validateReservation owned `shouldReturn` True
    results <- newEmptyMVar
    waiter <- forkIO (useOnce pool >>= putMVar results)
    threadDelay 20_000
    (do
      destroy pool
      timeout 200_000 (takeMVar results) `shouldReturn` Just (Left PoolClosed)
      useOnce pool `shouldReturn` Left PoolClosed
      release pool
      useOnce pool `shouldReturn` Left PoolClosed
      useReservation owned (\n -> pure (Succeeded, n)) `shouldReturn` 1
      finishReservation owned Succeeded
      destroy pool
      readIORef closed `shouldReturn` [1]
      readIORef opened `shouldReturn` 1
      stats pool `shouldReturn` Stats 0 0 0
      try @PoolClosedError (atomically (void $ tryReserveReady pool)) `shouldReturn` Left PoolClosedError
      try @PoolClosedError (atomically (void $ tryReserveOpening pool)) `shouldReturn` Left PoolClosedError)
      `finally` killThread waiter

  it "invalidates tentative reservations and unused opening slots" $ getAcquisition >>= \waiting -> do
    (pool, opened, closed) <- harness waiting (\cfg -> cfg{configSize = 2})
    void (useOnce pool)
    owned <- ready pool
    startup <- opening pool
    stats pool `shouldReturn` Stats 0 0 1
    destroy pool
    validateReservation owned `shouldReturn` False
    finishReservation owned Succeeded
    prepareOpening startup `shouldReturn` Left PoolClosed
    abandonOpening startup
    stats pool `shouldReturn` Stats 0 0 0
    readIORef opened `shouldReturn` 1
    readIORef closed `shouldReturn` [1]

  it "closes a connection whose startup crosses shutdown without running a callback" $ getAcquisition >>= \waiting -> do
    started <- newEmptyMVar
    gate <- newEmptyMVar
    finished <- newEmptyMVar
    called <- newIORef False
    (pool, _, closed) <- harness waiting (\cfg -> cfg{configConnect = putMVar started () *> takeMVar gate *> configConnect cfg})
    owner <- forkIO $ use pool (\n -> atomicModifyIORef' called (True,) $> (Succeeded, n)) >>= putMVar finished
    takeMVar started
    (do
      stats pool `shouldReturn` Stats 0 0 1
      destroy pool
      putMVar gate ()
      timeout 200_000 (takeMVar finished) `shouldReturn` Just (Left PoolClosed)
      readIORef called `shouldReturn` False
      readIORef closed `shouldReturn` [1]
      stats pool `shouldReturn` Stats 0 0 0)
      `finally` killThread owner

policySpec :: Spec
policySpec = describe "connection policy" $ do
  it "delays overflow startup, reports active counts, and closes overflow on return" $ getAcquisition >>= \waiting -> do
    policy <- overflowPolicy 1 0.08 (== "connection limit")
    (pool, opened, closed) <- policyHarness waiting policy (\cfg -> cfg{configSize = 2})
    held <- newEmptyMVar
    gate <- newEmptyMVar
    done <- newEmptyMVar
    owner <- forkIO $ use pool (\n -> putMVar held () *> takeMVar gate $> (Succeeded, n)) >>= putMVar done
    takeMVar held
    (do
      stats pool `shouldReturn` Stats 0 1 0
      before <- getMonotonicTimeNSec
      useOnce pool `shouldReturn` Right 2
      after <- getMonotonicTimeNSec
      (after - before >= 60_000_000) `shouldBe` True
      readIORef opened `shouldReturn` 2
      readIORef closed `shouldReturn` [2]
      putMVar gate ()
      takeMVar done `shouldReturn` Right 1
      stats pool `shouldReturn` Stats 1 0 0
      poolPolicyStats pool `shouldReturn` [(Text.pack "soft_limit", 1), (Text.pack "refusals", 0)]
      destroy pool)
      `finally` killThread owner

  it "wakes a queued use after refusal backoff without reporting a startup error" $ getAcquisition >>= \waiting -> do
    attempts <- newIORef (0 :: Int)
    policy <- overflowPolicy 2 0.04 (== "connection limit")
    (pool, _, _) <- policyHarness waiting policy (\cfg -> cfg{configSize = 3, configConnect = do
      attempt <- atomicModifyIORef' attempts (\n -> (n + 1, n))
      pure $ if attempt == 0 then Left "connection limit" else Right 7})
    timeout 400_000 (useOnce pool) `shouldReturn` Just (Right 7)
    readIORef attempts `shouldReturn` 2
    poolPolicyStats pool `shouldReturn` [(Text.pack "soft_limit", 1), (Text.pack "refusals", 1)]
    destroy pool

  it "uses a returned connection immediately while overflow startup is delayed" $ getAcquisition >>= \waiting -> do
    policy <- overflowPolicy 1 0.3 (const False)
    (pool, opened, _) <- policyHarness waiting policy (\cfg -> cfg{configSize = 2})
    void (useOnce pool)
    owned <- ready pool
    validateReservation owned `shouldReturn` True
    result <- newEmptyMVar
    waiter <- forkIO (useOnce pool >>= putMVar result)
    threadDelay 20_000
    (do
      finishReservation owned Succeeded
      timeout 100_000 (takeMVar result) `shouldReturn` Just (Right 1)
      readIORef opened `shouldReturn` 1
      destroy pool)
      `finally` killThread waiter

  it "keeps refusal retries inside the original acquisition deadline" $ getAcquisition >>= \waiting -> do
    policy <- overflowPolicy 1 0.02 (== "connection limit")
    attempts <- newIORef (0 :: Int)
    (pool, _, _) <- policyHarness waiting policy (\cfg -> cfg
      { configAcquisitionTimeout = 0.09
      , configConnect = atomicModifyIORef' attempts (\n -> (n + 1, ())) $> Left "connection limit"
      })
    timeout 300_000 (useOnce pool) `shouldReturn` Just (Left AcquisitionTimeout)
    attempted <- readIORef attempts
    (attempted >= 2 && attempted < 10) `shouldBe` True
    stats pool `shouldReturn` Stats 0 0 0
    destroy pool

  it "reports unrelated startup failures immediately without counting refusals" $ getAcquisition >>= \waiting -> do
    policy <- overflowPolicy 1 0.05 (== "connection limit")
    (pool, _, _) <- policyHarness waiting policy (\cfg -> cfg{configConnect = pure $ Left "authentication failed"})
    timeout 100_000 (useOnce pool) `shouldReturn` Just (Left $ ConnectionFailed "authentication failed")
    poolPolicyStats pool `shouldReturn` [(Text.pack "soft_limit", 1), (Text.pack "refusals", 0)]
    stats pool `shouldReturn` Stats 0 0 0
    destroy pool

  it "keeps repeated stale-connection replacements inside one checkout deadline" $ getAcquisition >>= \waiting -> do
    (pool, opened, closed) <- harness waiting (\cfg -> cfg{configAcquisitionTimeout = 0.07})
    called <- newIORef False
    let stale _ = threadDelay 20_000 $> Left Nothing
        action n = atomicModifyIORef' called (True,) $> (Succeeded, n)
    before <- getMonotonicTimeNSec
    timeout 250_000 (useChecked pool stale action) `shouldReturn` Just (Left AcquisitionTimeout)
    after <- getMonotonicTimeNSec
    (after - before < 200_000_000) `shouldBe` True
    attempts <- readIORef opened
    (attempts >= 2 && attempts < 10) `shouldBe` True
    length <$> readIORef closed `shouldReturn` attempts
    readIORef called `shouldReturn` False
    stats pool `shouldReturn` Stats 0 0 0
    destroy pool

  it "reclaims a connection when its checkout validation throws" $ getAcquisition >>= \waiting -> do
    (pool, _, closed) <- harness waiting id
    result <- try @ErrorCall $ useChecked pool (const $ throwIO $ ErrorCall "validation failed") (\n -> pure (Succeeded, n))
    result `shouldBe` Left (ErrorCall "validation failed")
    readIORef closed `shouldReturn` [1]
    useOnce pool `shouldReturn` Right 2
    destroy pool

  it "reuses the most recently returned connection so other idle connections can expire" $ getAcquisition >>= \waiting -> do
    (pool, _, closed) <- harness waiting (\cfg -> cfg{configSize = 2})
    void (useOnce pool)
    first <- ready pool
    validateReservation first `shouldReturn` True
    startup <- opening pool
    prepareOpening startup `shouldReturn` Right ()
    second <- ready pool
    validateReservation second `shouldReturn` True
    finishReservation first Succeeded
    finishReservation second Succeeded
    useOnce pool `shouldReturn` Right 2
    useOnce pool `shouldReturn` Right 2
    destroy pool
    readIORef closed `shouldReturn` [2, 1]

  it "preserves FIFO after cancelling the first delayed overflow waiter" $ getAcquisition >>= \waiting -> do
    policy <- overflowPolicy 1 0.08 (const False)
    (pool, _, _) <- policyHarness waiting policy (\cfg -> cfg{configSize = 2})
    held <- newEmptyMVar
    gate <- newEmptyMVar
    ownerDone <- newEmptyMVar
    owner <- forkIO $ use pool (\n -> putMVar held () *> takeMVar gate $> (Succeeded, n)) >>= putMVar ownerDone
    takeMVar held
    firstDone <- newEmptyMVar
    first <- forkFinally (useOnce pool) (putMVar firstDone)
    threadDelay 15_000
    secondEntered <- newEmptyMVar
    secondGate <- newEmptyMVar
    secondDone <- newEmptyMVar
    second <- forkIO $ use pool (\n -> putMVar secondEntered n *> takeMVar secondGate $> (Succeeded, n)) >>= putMVar secondDone
    threadDelay 15_000
    thirdDone <- newEmptyMVar
    third <- forkIO (useOnce pool >>= putMVar thirdDone)
    (do
      killThread first
      isLeft <$> takeMVar firstDone `shouldReturn` True
      timeout 300_000 (takeMVar secondEntered) `shouldReturn` Just 2
      timeout 20_000 (takeMVar thirdDone) `shouldReturn` Nothing
      putMVar secondGate ()
      takeMVar secondDone `shouldReturn` Right 2
      timeout 300_000 (takeMVar thirdDone) `shouldReturn` Just (Right 3)
      putMVar gate ()
      takeMVar ownerDone `shouldReturn` Right 1
      destroy pool)
      `finally` (killThread first >> killThread second >> killThread third >> killThread owner)
