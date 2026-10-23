{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DataKinds #-}
module PoolAdmissionSpec (spec) where

import Control.Concurrent (ThreadId, forkFinally, killThread, yield)
import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent.Resource.Acquisition qualified as Acquire
import Control.Concurrent.STM (retry)
import Control.Concurrent.MVar
import Control.Exception (SomeException, bracket, finally, try)
import Control.Monad (replicateM, void)
import Data.Either (isLeft)
import Data.IORef
import Data.Maybe (isNothing)
import Database.PostgreSQL.Pool qualified as Pool
import GHC.Conc (ThreadStatus (..), BlockReason (..), threadStatus)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool qualified as BufferPool
import Skeletest
import System.Mem (performGC)
import System.Timeout (timeout)
import System.Mem.Weak (deRefWeak, mkWeakPtr)

spec :: Spec
spec = describe "independent request and connection admission" $ do
  it "detaches immediate opening and idle grants before later FIFO admission" $
    getAcquisition >>= \waiting -> do
      pool <- newShared waiting
      timeout 200_000 (useOnce pool) `shouldReturn` Just (Right 1)
      timeout 200_000 (useOnce pool) `shouldReturn` Just (Right 1)
      held <- newEmptyMVar
      hold <- newEmptyMVar
      holderDone <- newEmptyMVar
      holder <- forkFinally
        (Pool.use pool $ \connection -> putMVar held () >> takeMVar hold >> pure (Pool.Succeeded, connection))
        (putMVar holderDone)
      takeMVar held
      firstEntered <- newEmptyMVar
      releaseFirst <- newEmptyMVar
      firstDone <- newEmptyMVar
      first <- forkFinally
        (Pool.use pool $ \connection -> putMVar firstEntered () >> takeMVar releaseFirst >> pure (Pool.Succeeded, connection))
        (putMVar firstDone)
      awaitBlocked first
      secondDone <- newEmptyMVar
      second <- forkFinally (useOnce pool) (putMVar secondDone)
      (do
        awaitBlocked second
        putMVar hold ()
        timeout 200_000 (takeMVar firstEntered) `shouldReturn` Just ()
        isNothing <$> tryReadMVar secondDone `shouldReturn` True
        putMVar releaseFirst ()
        awaitSuccessful firstDone
        awaitSuccessful secondDone)
        `finally` (killThread first >> killThread second >> killThread holder)
      timeout 200_000 (useOnce pool) `shouldReturn` Just (Right 1)
      Pool.release pool

  it "gives a freed opening slot to a queued use before a new arrival" $
    getAcquisition >>= \waiting -> do
      attempts <- newIORef (0 :: Int)
      starting <- newEmptyMVar
      failStartup <- newEmptyMVar
      let connect = atomicModifyIORef' attempts (\n -> (n + 1, n)) >>= \case
            0 -> putMVar starting () >> takeMVar failStartup >> pure (Left "startup failed")
            _ -> pure (Right 1)
      pool <- Pool.newPool waiting (sharedConfig connect)
      failed <- newEmptyMVar
      laterDone <- newEmptyMVar
      starter <- forkFinally
        (do
          useOnce pool >>= putMVar failed
          useOnce pool)
        (putMVar laterDone)
      takeMVar starting
      firstEntered <- newEmptyMVar
      releaseFirst <- newEmptyMVar
      firstDone <- newEmptyMVar
      first <- forkFinally
        (Pool.use pool $ \connection -> putMVar firstEntered () >> takeMVar releaseFirst >> pure (Pool.Succeeded, connection))
        (putMVar firstDone)
      (do
        awaitBlocked first
        putMVar failStartup ()
        timeout 200_000 (takeMVar failed) `shouldReturn` Just (Left (Pool.ConnectionFailed "startup failed"))
        timeout 200_000 (takeMVar firstEntered) `shouldReturn` Just ()
        isNothing <$> tryReadMVar laterDone `shouldReturn` True
        putMVar releaseFirst ()
        awaitSuccessful firstDone
        awaitSuccessful laterDone
        readIORef attempts `shouldReturn` 2)
        `finally` (killThread first >> killThread starter)
      Pool.release pool

  it "gives each pool its own FIFO while sharing the acquisition context" $
    getAcquisition >>= \waiting -> do
      busy <- newShared waiting
      ready <- newShared waiting
      held <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      _ <- forkFinally
        (Pool.use busy $ \connection -> putMVar held () >> takeMVar hold >> pure (Pool.Succeeded, connection))
        (putMVar done)
      takeMVar held
      queued <- newEmptyMVar
      worker <- forkFinally (useOnce busy) (putMVar queued)
      (do
        awaitBlocked worker
        timeout 200_000 (useOnce ready) `shouldReturn` Just (Right 1)
        putMVar hold ()
        awaitSuccessful queued)
        `finally` (killThread worker >> tryPutMVar hold () >> readMVar done)
      Pool.release busy
      Pool.release ready

  it "removes a cancelled FIFO head without stranding its successor" $
    getAcquisition >>= \waiting -> do
      pool <- newShared waiting
      held <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      _ <- forkFinally
        (Pool.use pool $ \connection -> putMVar held () >> takeMVar hold >> pure (Pool.Succeeded, connection))
        (putMVar done)
      takeMVar held
      cancelled <- newEmptyMVar
      first <- forkFinally (useOnce pool) (putMVar cancelled)
      awaitBlocked first
      next <- newEmptyMVar
      second <- forkFinally (useOnce pool) (putMVar next)
      (do
        awaitBlocked second
        killThread first
        fmap isLeft <$> timeout 200_000 (takeMVar cancelled) `shouldReturn` Just True
        putMVar hold ()
        awaitSuccessful next)
        `finally` (killThread first >> killThread second >> tryPutMVar hold () >> readMVar done)
      useOnce pool `shouldReturn` Right 1
      Pool.release pool

  it "collects an idle pool while its shared acquisition context remains usable" $
    getAcquisition >>= \waiting -> do
      weak <- do
        pool <- newShared waiting
        useOnce pool `shouldReturn` Right 1
        mkWeakPtr pool Nothing
      let collect = do
            performGC
            deRefWeak weak >>= \case
              Nothing -> pure ()
              Just _ -> yield >> collect
      timeout 1_000_000 collect `shouldReturn` Just ()
      assertAcquisitionUsable waiting

  it "cancels queued application checkouts before cleanup and keeps the context usable" $ do
    waiting <- getAcquisition
    pool <- newShared waiting
    held <- newEmptyMVar
    hold <- newEmptyMVar
    done <- newEmptyMVar
    _ <- forkFinally
      (Pool.use pool $ \connection -> putMVar held () >> takeMVar hold >> pure (Pool.Succeeded, connection))
      (putMVar done)
    takeMVar held
    queued <- replicateM 3 $ do
      stopped <- newEmptyMVar
      worker <- forkFinally (useOnce pool) (putMVar stopped)
      awaitBlocked worker
      pure (worker, stopped)
    (do
      mapM_ (killThread . fst) queued
      mapM_ (\(_, stopped) -> fmap isLeft <$> timeout 200_000 (readMVar stopped) `shouldReturn` Just True) queued
      putMVar hold ()
      timeout 200_000 (readMVar done) >>= \case
        Just (Right (Right 1)) -> pure ()
        _ -> failTest "active checkout did not drain before cleanup"
      Pool.release pool
      useOnce pool `shouldReturn` Right 2
      Pool.release pool
      sibling <- newShared waiting
      useOnce sibling `shouldReturn` Right 1
      Pool.release sibling)
      `finally` (mapM_ (killThread . fst) queued >> tryPutMVar hold () >> readMVar done)

  it "keeps the acquisition context usable after a constructor rejects its settings" $
    getAcquisition >>= \waiting -> do
      rejected <- try @IOError $ Pool.newPool waiting (sharedConfig (pure (Right 1))) {Pool.configSize = 0}
      isLeft rejected `shouldBe` True
      assertAcquisitionUsable waiting

  it "keeps the acquisition context usable after releasing a pool" $
    getAcquisition >>= \waiting -> do
      pool <- newShared waiting
      useOnce pool `shouldReturn` Right 1
      Pool.release pool
      assertAcquisitionUsable waiting

  it "serves connection checkout while an older request waits for its primary" $
    withTestResources settings $ \waiting resources -> do
      pool <- newShared waiting
      BufferPool.withRequestBuffers resources $ \_ -> do
        stopped <- newEmptyMVar
        worker <- forkFinally
          (BufferPool.withRequestBuffers resources $ const $ useOnce pool)
          (putMVar stopped)
        (do
          awaitBlocked worker
          isNothing <$> tryReadMVar stopped `shouldReturn` True
          timeout 200_000 (useOnce pool) `shouldReturn` Just (Right 1))
          `finally` (killThread worker >> takeMVar stopped)
      Pool.release pool

  it "keeps a saturated connection pool from blocking another admitted request" $
    withTestResources settings {BufferPool.ordinaryBufferCount = positiveInt @2} $ \waiting resources -> do
      busy <- newShared waiting
      ready <- newShared waiting
      taken <- newEmptyMVar
      unblock <- newEmptyMVar
      done <- newEmptyMVar
      holder <- forkFinally
        (Pool.use busy $ \connection -> putMVar taken () >> takeMVar unblock >> pure (Pool.Succeeded, connection))
        (putMVar done)
      takeMVar taken
      BufferPool.withRequestBuffers resources $ \_ -> do
        stopped <- newEmptyMVar
        worker <- forkFinally (useOnce busy) (putMVar stopped)
        (do
          awaitBlocked worker
          BufferPool.withRequestBuffers resources $ \_ ->
            timeout 200_000 (useOnce ready) `shouldReturn` Just (Right 1))
          `finally` (killThread worker >> takeMVar stopped)
      putMVar unblock ()
      _ <- takeMVar done
      killThread holder
      Pool.release busy
      Pool.release ready

  it "returns the database connection while response storage remains borrowed" $
    withTestResources settings $ \waiting resources -> do
      pool <- newShared waiting
      BufferPool.withRequestBuffers resources $ \access -> bracket (BufferPool.borrowBuffer access) BufferPool.releaseBuffer $ \_ -> do
        useOnce pool `shouldReturn` Right 1
        useOnce pool `shouldReturn` Right 1
        timeout 20_000 (BufferPool.withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
      Pool.release pool

  it "keeps a request's eventual pool alive while primary admission is queued" $
    withTestResources settings $ \waiting resources ->
      BufferPool.withRequestBuffers resources $ \_ ->
        bracket (startQueuedRequest waiting resources)
          (\(worker, stopped) -> killThread worker >> readMVar stopped)
          (\(worker, stopped) -> do
            awaitBlocked worker
            performGC
            performGC
            isNothing <$> tryReadMVar stopped `shouldReturn` True)

  it "keeps a queued connection pool alive after its request primary becomes available" $
    withTestResources settings $ \waiting resources -> do
      queued <- BufferPool.withRequestBuffers resources $ \_ -> do
        queued@(worker, stopped) <- startQueuedRequest waiting resources
        awaitBlocked worker
        performGC
        performGC
        isNothing <$> tryReadMVar stopped `shouldReturn` True
        pure queued
      let (worker, stopped) = queued
      (timeout 200_000 (readMVar stopped) >>= \case
        Just (Right (Right 1)) -> pure ()
        _ -> failTest "queued request lost its eventual pool during collection")
        `finally` (killThread worker >> readMVar stopped)

  it "returns the request primary when cancellation interrupts connection checkout" $
    withTestResources settings $ \waiting resources -> do
      pool <- newShared waiting
      taken <- newEmptyMVar
      hold <- newEmptyMVar
      heldDone <- newEmptyMVar
      _ <- forkFinally
        (Pool.use pool $ \connection -> putMVar taken () >> takeMVar hold >> pure (Pool.Succeeded, connection))
        (putMVar heldDone)
      takeMVar taken
      done <- newEmptyMVar
      worker <- forkFinally
        (BufferPool.withRequestBuffers resources $ \access -> bracket (BufferPool.borrowBuffer access) BufferPool.releaseBuffer $ const $ useOnce pool)
        (putMVar done)
      awaitBlocked worker
      killThread worker
      fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just True
      timeout 200_000 (BufferPool.withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()
      putMVar hold ()
      _ <- takeMVar heldDone
      useOnce pool `shouldReturn` Right 1
      Pool.release pool

settings :: BufferPool.PoolSettings
settings = BufferPool.PoolSettings
  { BufferPool.bufferSize = positiveInt @16
  , BufferPool.ordinaryBufferCount = positiveInt @1
  , BufferPool.reserveBufferCount = positiveInt @1
  , BufferPool.setupBufferCount = positiveInt @2
  }

newShared :: Acquire.Context -> IO (Pool.Pool Int String)
newShared waiting = do
  opened <- newIORef 0
  Pool.newPool waiting (sharedConfig (Right <$> atomicModifyIORef' opened (\n -> (n + 1, n + 1))))

sharedConfig :: IO (Either String Int) -> Pool.Config Int String
sharedConfig connect = Pool.Config
    { Pool.configSize = 1
    , Pool.configAcquisitionTimeout = 1
    , Pool.configMaxLifetime = 3600
    , Pool.configMaxIdletime = 3600
    , Pool.configConnect = connect
    , Pool.configDescribeFailure = const Nothing
    , Pool.configClose = const (pure ())
    , Pool.configUsableAfterException = const (pure True)
    , Pool.configObserve = const (pure ())
    }

useOnce :: Pool.Pool Int String -> IO (Either (Pool.UsageError String) Int)
useOnce pool = Pool.use pool (\connection -> pure (Pool.Succeeded, connection))

-- Only the worker retains the pool while primary admission waits.
startQueuedRequest :: Acquire.Context -> BufferPool.BufferPool -> IO (ThreadId, MVar (Either SomeException (Either (Pool.UsageError String) Int)))
startQueuedRequest waiting resources = do
  pool <- newShared waiting
  useOnce pool `shouldReturn` Right 1
  stopped <- newEmptyMVar
  worker <- forkFinally
    (BufferPool.withRequestBuffers resources (const (useOnce pool)))
    (putMVar stopped)
  pure (worker, stopped)

awaitBlocked :: ThreadId -> IO ()
awaitBlocked worker = timeout 1_000_000 loop >>= (`shouldBe` Just ())
  where
    loop = threadStatus worker >>= \case
      ThreadBlocked BlockedOnSTM -> pure ()
      ThreadBlocked BlockedOnMVar -> pure ()
      ThreadFinished -> pure ()
      ThreadDied -> pure ()
      _ -> yield >> loop

awaitSuccessful :: MVar (Either SomeException (Either (Pool.UsageError String) Int)) -> IO ()
awaitSuccessful done = timeout 200_000 (takeMVar done) >>= \case
  Just (Right (Right 1)) -> pure ()
  _ -> failTest "FIFO successor did not receive its connection"

withTestResources :: BufferPool.PoolSettings -> (Acquire.Context -> BufferPool.BufferPool -> IO a) -> IO a
withTestResources settings action = getAcquisition >>= \waiting ->
  BufferPool.newBufferPool waiting settings >>= action waiting

assertAcquisitionUsable :: Acquire.Context -> IO ()
assertAcquisitionUsable waiting =
  Acquire.acquire waiting Acquire.NoDeadline retry
    (\waiter -> void (Acquire.tryGrant waiter (pure ())))
    (const (pure ())) (const (pure ())) `shouldReturn` Just ()
