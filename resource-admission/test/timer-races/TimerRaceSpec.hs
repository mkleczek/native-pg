module TimerRaceSpec (spec) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Resource.Waiting
import Control.Exception
import Data.Either (isLeft)
import GHC.Conc (ThreadStatus(..), BlockReason(..), threadStatus)
import GHC.Conc.Sync (unsafeIOToSTM)
import GHC.Event qualified as Timer
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = xfail "Waiting API scaffold: timer ownership follows in the next change" $ describe "timer key ownership races" $ do
  it "unregisters a key returned after a concurrent grant, before using the grant" $
    scenario $ \manager waiting ticket detached cleaned -> do
      entered <- newEmptyMVar
      (_, result) <- forkResult $ wait waiting ticket detached cleaned $ \value -> do
        putMVar entered ()
        pure value
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration manager
      await entered
      Timer.unregisterCount key >>= (`shouldBe` 1)
      await result >>= expectResult (Right 7)
      counts detached cleaned 1 1

  it "cancels while registration is in progress and unregisters the completed key" $
    scenario $ \manager waiting ticket detached cleaned -> do
      (requester, result) <- forkResult $ wait waiting ticket detached cleaned pure
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      (canceller, cancelled) <- forkResult (killThread requester)
      -- throwTo blocks until the masked registration can return its key.
      within (awaitThrowTo canceller)
      Timer.completeRegistration manager
      await cancelled >>= either throwIO pure
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)
      Timer.unregisterCount key >>= (`shouldBe` 1)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` False)
      counts detached cleaned 1 0

  it "cleans a grant offered while cancellation waits for the timer key" $
    scenario $ \manager waiting ticket detached cleaned -> do
      (requester, result) <- forkResult $ wait waiting ticket detached cleaned
        (const (fail "cancelled offer reached use"))
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      (canceller, cancelled) <- forkResult (killThread requester)
      within (awaitThrowTo canceller)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration manager
      await cancelled >>= either throwIO pure
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)
      Timer.unregisterCount key >>= (`shouldBe` 1)
      counts detached cleaned 1 1

  it "observes expiration delivered before registerTimeout returns its key" $
    scenario $ \manager waiting ticket detached cleaned -> do
      (_, result) <- forkResult $ wait waiting ticket detached cleaned pure
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      Timer.fireTimeout key
      -- A callback signals only: it must not detach on the timer thread.
      readTVarIO detached >>= (`shouldBe` 0)
      Timer.completeRegistration manager
      await result >>= expectResult (Left TimedOut)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` False)
      Timer.unregisterCount key >>= (`shouldBe` 1)
      counts detached cleaned 1 0

  it "rejects a signalled timeout on the producer thread without reserving" $
    Timer.withTimerControl $ \manager -> withWaiting $ \waiting -> do
      ticket <- newEmptyTMVarIO
      detachedBy <- newTVarIO []
      reserved <- newTVarIO (0 :: Int)
      deadline <- deadlineAfter 3600
      (_, result) <- forkResult $ withWait waiting deadline (\waiter -> putTMVar ticket waiter >> pure detachedBy)
        (\registration -> unsafeIOToSTM myThreadId >>= \tid -> modifyTVar' registration (tid :))
        (const (fail "expired waiter acquired a resource")) (pure @IO @Int)
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      Timer.fireTimeout key
      producer <- myThreadId
      atomically (tryGrant waiter (modifyTVar' reserved (+ 1) >> throwSTM (userError "reservation ran")))
        >>= (`shouldBe` False)
      readTVarIO reserved >>= (`shouldBe` 0)
      readTVarIO detachedBy >>= (`shouldBe` [producer])
      Timer.completeRegistration manager
      await result >>= expectResult (Left TimedOut)
      readTVarIO detachedBy >>= (`shouldBe` [producer])
      Timer.unregisterCount key >>= (`shouldBe` 1)

  it "preserves an offered resource when an already dequeued callback runs after unregister" $
    scenario $ \manager waiting ticket detached cleaned -> do
      entered <- newEmptyMVar
      finish <- newEmptyMVar
      (_, result) <- forkResult $ wait waiting ticket detached cleaned $ \value -> do
        putMVar entered ()
        takeMVar finish
        pure value
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration manager
      within (Timer.awaitUnregistered key)
      await entered
      Timer.fireTimeout key
      atomically (tryGrant waiter (pure 9)) >>= (`shouldBe` False)
      counts detached cleaned 1 0
      putMVar finish ()
      await result >>= expectResult (Right 7)
      Timer.fireTimeout key
      counts detached cleaned 1 1
      Timer.unregisterCount key >>= (`shouldBe` 1)

  it "detaches a pending waiter when arming throws before returning a key" $
    scenario $ \manager waiting ticket detached cleaned -> do
      (_, result) <- forkResult $ wait waiting ticket detached cleaned pure
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      Timer.failRegistration manager
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` False)
      Timer.unregisterCount key >>= (`shouldBe` 0)
      counts detached cleaned 1 0

  it "cleans a concurrent offer when arming throws before returning a key" $
    scenario $ \manager waiting ticket detached cleaned -> do
      (_, result) <- forkResult $ wait waiting ticket detached cleaned pure
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` True)
      Timer.failRegistration manager
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)
      Timer.unregisterCount key >>= (`shouldBe` 0)
      counts detached cleaned 1 1

scenario :: (Timer.TimerManager -> Waiting -> TMVar (Waiter Int) -> TVar Int -> TVar Int -> IO ()) -> IO ()
scenario action = Timer.withTimerControl $ \manager -> withWaiting $ \waiting -> do
  ticket <- newEmptyTMVarIO
  detached <- newTVarIO 0
  cleaned <- newTVarIO 0
  action manager waiting ticket detached cleaned

wait :: Waiting -> TMVar (Waiter Int) -> TVar Int -> TVar Int -> (Int -> IO Int) -> IO (Either WaitError Int)
wait waiting ticket detached cleaned use = do
  deadline <- deadlineAfter 3600
  withWait waiting deadline (\waiter -> putTMVar ticket waiter >> pure detached) (\registration -> modifyTVar' registration (+ 1))
    (const (atomically (modifyTVar' cleaned (+ 1)))) use

counts :: TVar Int -> TVar Int -> Int -> Int -> IO ()
counts detached cleaned expectedDetached expectedCleaned = do
  readTVarIO detached >>= (`shouldBe` expectedDetached)
  readTVarIO cleaned >>= (`shouldBe` expectedCleaned)

expectResult :: Either WaitError Int -> Either SomeException (Either WaitError Int) -> IO ()
expectResult expected = either throwIO (`shouldBe` expected)

forkResult :: IO a -> IO (ThreadId, MVar (Either SomeException a))
forkResult action = do
  result <- newEmptyMVar
  tid <- forkIO (try action >>= putMVar result)
  pure (tid, result)

within :: IO a -> IO a
within action = timeout 3000000 action >>= maybe (fail "timer race test exceeded three seconds") pure

await :: MVar a -> IO a
await = within . takeMVar

awaitTicket :: TMVar a -> IO a
awaitTicket = within . atomically . takeTMVar

awaitThrowTo :: ThreadId -> IO ()
awaitThrowTo tid = threadStatus tid >>= \status -> case status of
  ThreadBlocked BlockedOnException -> pure ()
  _ -> yield >> awaitThrowTo tid
