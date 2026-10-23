{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DataKinds #-}
module PoolAdmissionSpec (spec) where

import Control.Concurrent (ThreadId, forkFinally, killThread, yield)
import Control.Concurrent.Resource.Waiting qualified as Waiting
import Control.Concurrent.MVar
import Control.Exception (SomeException, bracket, finally, try)
import Control.Monad (replicateM)
import Data.Either (isLeft)
import Data.IORef
import Data.Maybe (isNothing)
import Database.PostgreSQL.Pool qualified as Pool
import GHC.Conc (ThreadStatus (..), BlockReason (..), threadStatus)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources qualified as Resources
import Skeletest
import System.Mem (performGC)
import System.Timeout (timeout)
import System.Mem.Weak (deRefWeak, mkWeakPtr)

spec :: Spec
spec = describe "independent request and connection admission" $ do
  it "detaches immediate opening and idle grants before later FIFO admission" $
    Waiting.withWaiting $ \waiting -> do
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

  it "gives each pool its own FIFO while sharing deadline workers" $
    Waiting.withWaiting $ \waiting -> do
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
    Waiting.withWaiting $ \waiting -> do
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

  it "collects an idle pool while its shared deadline service remains open" $
    Waiting.withWaiting $ \waiting -> do
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
      assertWaitingOpen waiting

  xfail "Shared Waiting constructor scaffold: shared closure and validation follow in the next change" $ it "rejects every queued checkout when the shared deadline service closes" $ do
    waiting <- Waiting.newWaiting
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
      Waiting.closeWaiting waiting
      mapM_ (\(_, stopped) -> timeout 200_000 (takeMVar stopped) >>= \result -> case result of
        Just (Right (Left Pool.AcquisitionTimeout)) -> pure ()
        _ -> failTest "closing shared deadlines did not reject a queued checkout") queued)
      `finally` (mapM_ (killThread . fst) queued >> tryPutMVar hold () >> readMVar done >> Waiting.closeWaiting waiting)
    Pool.release pool

  xfail "Shared Waiting constructor scaffold: shared closure and validation follow in the next change" $ it "keeps caller deadlines open after a constructor rejects its settings" $
    Waiting.withWaiting $ \waiting -> do
      rejected <- try @IOError $ Pool.newPool waiting (sharedConfig (pure (Right 1))) {Pool.configSize = 0}
      isLeft rejected `shouldBe` True
      assertWaitingOpen waiting

  it "keeps caller deadlines open after releasing a pool" $
    Waiting.withWaiting $ \waiting -> do
      pool <- newShared waiting
      useOnce pool `shouldReturn` Right 1
      Pool.release pool
      assertWaitingOpen waiting

  it "serves connection checkout while an older request waits for its primary" $
    withTestResources settings $ \waiting resources -> do
      pool <- newShared waiting
      Resources.withRequestBuffers resources $ \_ -> do
        stopped <- newEmptyMVar
        worker <- forkFinally
          (Resources.withRequestBuffers resources $ const $ useOnce pool)
          (putMVar stopped)
        (do
          awaitBlocked worker
          isNothing <$> tryReadMVar stopped `shouldReturn` True
          timeout 200_000 (useOnce pool) `shouldReturn` Just (Right 1))
          `finally` (killThread worker >> takeMVar stopped)
      Pool.release pool

  it "keeps a saturated connection pool from blocking another admitted request" $
    withTestResources settings {Resources.streamMemoryBytes = positiveInt @48} $ \waiting resources -> do
      busy <- newShared waiting
      ready <- newShared waiting
      taken <- newEmptyMVar
      unblock <- newEmptyMVar
      done <- newEmptyMVar
      holder <- forkFinally
        (Pool.use busy $ \connection -> putMVar taken () >> takeMVar unblock >> pure (Pool.Succeeded, connection))
        (putMVar done)
      takeMVar taken
      Resources.withRequestBuffers resources $ \_ -> do
        stopped <- newEmptyMVar
        worker <- forkFinally (useOnce busy) (putMVar stopped)
        (do
          awaitBlocked worker
          Resources.withRequestBuffers resources $ \_ ->
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
      Resources.withRequestBuffers resources $ \access -> Resources.withBuffer access $ \_ -> do
        useOnce pool `shouldReturn` Right 1
        useOnce pool `shouldReturn` Right 1
        timeout 20_000 (Resources.withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
      Pool.release pool

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
        (Resources.withRequestBuffers resources $ \access -> Resources.withBuffer access $ const $ useOnce pool)
        (putMVar done)
      awaitBlocked worker
      killThread worker
      fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just True
      timeout 200_000 (Resources.withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()
      putMVar hold ()
      _ <- takeMVar heldDone
      useOnce pool `shouldReturn` Right 1
      Pool.release pool

settings :: Resources.ResourceSettings
settings = Resources.ResourceSettings (positiveInt @32) (positiveInt @32) (positiveInt @16) (positiveInt @1)

newShared :: Waiting.Waiting -> IO (Pool.Pool Int String)
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

awaitBlocked :: ThreadId -> IO ()
awaitBlocked worker = timeout 1_000_000 loop >>= (`shouldBe` Just ())
  where
    loop = threadStatus worker >>= \status -> case status of
      ThreadBlocked BlockedOnSTM -> pure ()
      ThreadBlocked BlockedOnMVar -> pure ()
      ThreadFinished -> pure ()
      ThreadDied -> pure ()
      _ -> yield >> loop

awaitSuccessful :: MVar (Either SomeException (Either (Pool.UsageError String) Int)) -> IO ()
awaitSuccessful done = timeout 200_000 (takeMVar done) >>= \result -> case result of
  Just (Right (Right 1)) -> pure ()
  _ -> failTest "FIFO successor did not receive its connection"

withTestResources :: Resources.ResourceSettings -> (Waiting.Waiting -> Resources.Resources -> IO a) -> IO a
withTestResources settings action = Waiting.withWaiting $ \waiting ->
  Resources.withResources waiting settings (action waiting)

assertWaitingOpen :: Waiting.Waiting -> IO ()
assertWaitingOpen waiting =
  Waiting.withWait waiting Waiting.NoDeadline
    (\waiter -> Waiting.tryGrant waiter (pure ()) >> pure ())
    (const (pure ())) (const (pure ())) pure `shouldReturn` Right ()
