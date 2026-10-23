{-# LANGUAGE LambdaCase #-}

module TimerRaceSpec (spec) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Resource.Acquisition
import Control.Exception
import Control.Monad (replicateM_, void)
import Data.Either (isLeft)
import Data.Maybe (isNothing)
import GHC.Conc (ThreadStatus(..), BlockReason(..), threadStatus)
import GHC.Conc.Sync (unsafeIOToSTM)
import GHC.Event qualified as Timer
import Skeletest hiding (context)
import System.Mem (performGC)
import System.Timeout (timeout)

spec :: Spec
spec = describe "timer key ownership races" $ do
  it "returns an immediate reservation without registering a deadline timer" $
    Timer.withTimerControl $ \manager -> do
      deadline <- deadlineAfter 3600
      result <- acquire (newContext manager) deadline (pure (7 :: Int))
        (const (throwSTM (userError "immediate reservation was enqueued")))
        (\() -> throwSTM (userError "immediate reservation was removed"))
        (const (fail "claimed value reached cleanup"))
      result `shouldBe` Just 7
      Timer.tryRegistration manager >>= (\pending -> isNothing pending `shouldBe` True)

  it "preserves caller ownership when cancellation waits for an immediate reservation" $
    Timer.withTimerControl $ \manager -> do
      reserved <- newEmptyMVar
      commit <- newEmptyMVar
      cleaned <- newTVarIO (0 :: Int)
      deadline <- deadlineAfter 3600
      (requester, result) <- forkResult $ withAcquired (newContext manager) deadline
        (unsafeIOToSTM (uninterruptibleMask_ (putMVar reserved () >> takeMVar commit)))
        (const (throwSTM (userError "immediate reservation was enqueued")))
        (\() -> pure ()) (\() -> atomically (modifyTVar' cleaned (+ 1)))
        (const (fail "cancelled caller reached use"))
      await reserved
      (canceller, cancelled) <- forkResult (killThread requester)
      -- Cancellation is already pending before the reservation commits. The
      -- caller's bracket must install release before restoring its masking state.
      within (awaitThrowTo canceller)
      putMVar commit ()
      await cancelled >>= either throwIO pure
      await result >>= either
        (\exception -> fromException exception `shouldBe` Just ThreadKilled)
        (const (fail "cancelled reservation returned to caller"))
      readTVarIO cleaned >>= (`shouldBe` 1)
      Timer.tryRegistration manager >>= (\pending -> isNothing pending `shouldBe` True)

  it "registers and cancels through the supplied manager instead of the default manager" $
    Timer.withTimerControl $ \systemManager -> Timer.withIndependentTimerControl $ \manager -> do
      ticket <- newEmptyTMVarIO
      detached <- newTVarIO 0
      cleaned <- newTVarIO 0
      (_, result) <- forkResult $ wait (newContext manager) ticket detached cleaned pure
      request <- awaitTicket ticket
      (usedSupplied, key) <- within (registeredManager manager systemManager)
      atomically (tryGrant request (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration (if usedSupplied then manager else systemManager)
      await result >>= expectResult (Just 7)
      usedSupplied `shouldBe` True
      Timer.unregisterCount key >>= (`shouldBe` 1)
      Timer.tryRegistration systemManager >>= (\pending -> isNothing pending `shouldBe` True)
      counts detached cleaned 1 1

  it "keeps another context sharing the manager usable after application cancellation" $
    sharedContexts $ \requester _ result -> do
      killThread requester
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)

  it "keeps another context sharing the manager usable after an owner exception" $
    sharedContexts $ \_ request result -> do
      atomically (tryGrant request (throwSTM OwnerClosed)) >>= (`shouldBe` True)
      await result >>= expectOwnerClosed

  it "unregisters a key returned after a concurrent grant, before using the grant" $
    scenario $ \manager context ticket detached cleaned -> do
      entered <- newEmptyMVar
      (_, result) <- forkResult $ wait context ticket detached cleaned $ \value -> do
        putMVar entered ()
        pure value
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration manager
      await entered
      Timer.unregisterCount key >>= (`shouldBe` 1)
      await result >>= expectResult (Just 7)
      counts detached cleaned 1 1

  it "cancels while registration is in progress and unregisters the completed key" $
    scenario $ \manager context ticket detached cleaned -> do
      (requester, result) <- forkResult $ wait context ticket detached cleaned pure
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
    scenario $ \manager context ticket detached cleaned -> do
      (requester, result) <- forkResult $ wait context ticket detached cleaned
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
    scenario $ \manager context ticket detached cleaned -> do
      (_, result) <- forkResult $ wait context ticket detached cleaned pure
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      Timer.fireTimeout key
      -- A callback signals only: it must not detach on the timer thread.
      readTVarIO detached >>= (`shouldBe` 0)
      Timer.completeRegistration manager
      await result >>= expectResult Nothing
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` False)
      Timer.unregisterCount key >>= (`shouldBe` 1)
      counts detached cleaned 1 0

  it "completes a signalled timeout on the producer thread without reserving" $
    Timer.withTimerControl $ \manager -> do
      let context = newContext manager
      ticket <- newEmptyTMVarIO
      detachedBy <- newTVarIO []
      reserved <- newTVarIO (0 :: Int)
      deadline <- deadlineAfter 3600
      (_, result) <- forkResult $ withAcquired context deadline retry (\waiter -> putTMVar ticket waiter >> pure detachedBy)
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
      await result >>= expectResult Nothing
      readTVarIO detachedBy >>= (`shouldBe` [producer])
      Timer.unregisterCount key >>= (`shouldBe` 1)

  it "preserves an offered resource when an already dequeued callback runs after unregister" $
    scenario $ \manager context ticket detached cleaned -> do
      entered <- newEmptyMVar
      finish <- newEmptyMVar
      (_, result) <- forkResult $ wait context ticket detached cleaned $ \value -> do
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
      await result >>= expectResult (Just 7)
      Timer.fireTimeout key
      counts detached cleaned 1 1
      Timer.unregisterCount key >>= (`shouldBe` 1)

  it "detaches a pending waiter when arming throws before returning a key" $
    scenario $ \manager context ticket detached cleaned -> do
      (_, result) <- forkResult $ wait context ticket detached cleaned pure
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      Timer.failRegistration manager
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` False)
      Timer.unregisterCount key >>= (`shouldBe` 0)
      counts detached cleaned 1 0

  it "cleans a concurrent offer when arming throws before returning a key" $
    scenario $ \manager context ticket detached cleaned -> do
      (_, result) <- forkResult $ wait context ticket detached cleaned pure
      waiter <- awaitTicket ticket
      key <- within (Timer.registration manager)
      atomically (tryGrant waiter (pure 7)) >>= (`shouldBe` True)
      Timer.failRegistration manager
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)
      Timer.unregisterCount key >>= (`shouldBe` 0)
      counts detached cleaned 1 1

  it "transfers a timed grant without running acquire cleanup" $
    scenario $ \manager context ticket detached cleaned -> do
      (_, result) <- forkResult $ rawWait context ticket detached cleaned
      request <- awaitTicket ticket
      key <- within (Timer.registration manager)
      atomically (tryGrant request (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration manager
      await result >>= expectResult (Just 7)
      Timer.unregisterCount key >>= (`shouldBe` 1)
      counts detached cleaned 1 0

  it "reclaims an assigned grant when cancellation interrupts timer unregistration" $
    scenario $ \manager context ticket detached cleaned -> do
      (requester, result) <- forkResult $ rawWait context ticket detached cleaned
      request <- awaitTicket ticket
      key <- within (Timer.registration manager)
      Timer.pauseUnregistration key
      atomically (tryGrant request (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration manager
      within (Timer.awaitUnregistered key)
      killThread requester
      await result >>= (\outcome -> isLeft outcome `shouldBe` True)
      counts detached cleaned 1 1
      Timer.unregisterCount key >>= (`shouldBe` 1)
      Timer.completeUnregistration key
      atomically (tryGrant request (pure 9)) >>= (`shouldBe` False)

  it "rechecks the absolute deadline after a grant waits for timer unregistration" $
    scenario $ \manager context ticket detached cleaned -> do
      deadline <- deadlineAfter 0.03
      (_, result) <- forkResult $ acquire context deadline retry
        (\request -> putTMVar ticket request >> pure detached)
        (\registration -> modifyTVar' registration (+ 1))
        (const (atomically (modifyTVar' cleaned (+ 1))))
      request <- awaitTicket ticket
      key <- within (Timer.registration manager)
      Timer.pauseUnregistration key
      atomically (tryGrant request (pure 7)) >>= (`shouldBe` True)
      Timer.completeRegistration manager
      within (Timer.awaitUnregistered key)
      threadDelay 60000
      Timer.completeUnregistration key
      await result >>= expectResult Nothing
      counts detached cleaned 1 1

  it "settles grant and timeout races before claiming with one detach" $
    replicateM_ 100 $ scenario $ \manager context ticket detached cleaned -> do
      (_, result) <- forkResult $ rawWait context ticket detached cleaned
      request <- awaitTicket ticket
      key <- within (Timer.registration manager)
      start <- newEmptyTMVarIO
      (_, granted) <- forkResult $ atomically (readTMVar start >> tryGrant request (pure 7))
      (_, expired) <- forkResult $ atomically (readTMVar start) >> Timer.fireTimeout key
      atomically (putTMVar start ())
      grantWon <- await granted >>= either throwIO pure
      await expired >>= either throwIO pure
      Timer.completeRegistration manager
      await result >>= expectResult (if grantWon then Just 7 else Nothing)
      counts detached cleaned 1 0
      Timer.unregisterCount key >>= (`shouldBe` 1)

  it "allows a dequeued timer callback to outlive collection of its removal closure" $
    Timer.withTimerControl $ \manager -> do
      (finalized, key) <- grantedAnchor manager
      within (collect finalized)
      Timer.fireTimeout key
      Timer.unregisterCount key >>= (`shouldBe` 1)

scenario :: (Timer.TimerManager -> Context -> TMVar (Request Int) -> TVar Int -> TVar Int -> IO ()) -> IO ()
scenario action = Timer.withTimerControl $ \manager -> do
  let context = newContext manager
  ticket <- newEmptyTMVarIO
  detached <- newTVarIO 0
  cleaned <- newTVarIO 0
  action manager context ticket detached cleaned

wait :: Context -> TMVar (Request Int) -> TVar Int -> TVar Int -> (Int -> IO Int) -> IO (Maybe Int)
wait context ticket detached cleaned use = do
  deadline <- deadlineAfter 3600
  withAcquired context deadline retry (\waiter -> putTMVar ticket waiter >> pure detached) (\registration -> modifyTVar' registration (+ 1))
    (const (atomically (modifyTVar' cleaned (+ 1)))) use

counts :: TVar Int -> TVar Int -> Int -> Int -> IO ()
counts detached cleaned expectedDetached expectedCleaned = do
  readTVarIO detached >>= (`shouldBe` expectedDetached)
  readTVarIO cleaned >>= (`shouldBe` expectedCleaned)

expectResult :: Maybe Int -> Either SomeException (Maybe Int) -> IO ()
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
awaitThrowTo tid = threadStatus tid >>= \case
  ThreadBlocked BlockedOnException -> pure ()
  _ -> yield >> awaitThrowTo tid

registeredManager :: Timer.TimerManager -> Timer.TimerManager -> IO (Bool, Timer.TimeoutKey)
registeredManager supplied system = do
  chosen <- Timer.tryRegistration supplied
  case chosen of
    Just key -> pure (True, key)
    Nothing -> do
      fallback <- Timer.tryRegistration system
      case fallback of
        Just key -> pure (False, key)
        Nothing -> yield >> registeredManager supplied system

sharedContexts :: (ThreadId -> Request Int -> MVar (Either SomeException (Maybe Int)) -> IO ()) -> IO ()
sharedContexts finishFirst = Timer.withTimerControl $ \manager -> do
  firstTicket <- newEmptyTMVarIO
  secondTicket <- newEmptyTMVarIO
  detached <- newTVarIO 0
  cleaned <- newTVarIO 0
  (requester, firstResult) <- forkResult $ wait (newContext manager) firstTicket detached cleaned pure
  first <- awaitTicket firstTicket
  firstKey <- within (Timer.registration manager)
  Timer.completeRegistration manager
  within (awaitPending requester)
  (_, secondResult) <- forkResult $ wait (newContext manager) secondTicket detached cleaned pure
  second <- awaitTicket secondTicket
  secondKey <- within (Timer.registration manager)
  Timer.completeRegistration manager
  finishFirst requester first firstResult
  Timer.unregisterCount firstKey >>= (`shouldBe` 1)
  Timer.unregisterCount secondKey >>= (`shouldBe` 0)
  atomically (tryGrant first (pure 9)) >>= (`shouldBe` False)
  atomically (tryGrant second (pure 7)) >>= (`shouldBe` True)
  await secondResult >>= expectResult (Just 7)
  Timer.unregisterCount secondKey >>= (`shouldBe` 1)
  counts detached cleaned 2 1

awaitPending :: ThreadId -> IO ()
awaitPending tid = threadStatus tid >>= \case
  ThreadBlocked BlockedOnSTM -> pure ()
  _ -> yield >> awaitPending tid

-- Callers protect transfer with bracket and own release after acquisition.
withAcquired :: Context -> Deadline -> STM a -> (Request a -> STM handle) -> (handle -> STM ()) -> (a -> IO ()) -> (a -> IO b) -> IO (Maybe b)
withAcquired context deadline reserveNow enqueue remove release use =
  bracket (acquire context deadline reserveNow enqueue remove release) (mapM_ release) (traverse use)

data OwnerClosed = OwnerClosed deriving (Eq, Show)
instance Exception OwnerClosed

expectOwnerClosed :: Either SomeException a -> IO ()
expectOwnerClosed outcome = case outcome of
  Left exception -> fromException exception `shouldBe` Just OwnerClosed
  Right _ -> fail "expected owner exception"

rawWait :: Context -> TMVar (Request Int) -> TVar Int -> TVar Int -> IO (Maybe Int)
rawWait context ticket detached cleaned = do
  deadline <- deadlineAfter 3600
  acquire context deadline retry (\request -> putTMVar ticket request >> pure detached)
    (\registration -> modifyTVar' registration (+ 1))
    (const (atomically (modifyTVar' cleaned (+ 1))))

-- Keep anchor creation off the collecting stack while retaining the callback key.
{-# NOINLINE grantedAnchor #-}
grantedAnchor :: Timer.TimerManager -> IO (MVar (), Timer.TimeoutKey)
grantedAnchor manager = do
  anchor <- newTVarIO True
  finalized <- newEmptyMVar
  _ <- mkWeakTVar anchor (putMVar finalized ())
  ticket <- newEmptyTMVarIO
  deadline <- deadlineAfter 3600
  (_, result) <- forkResult $ acquire (newContext manager) deadline retry
    (putTMVar ticket) (const (readTVar anchor >>= check)) (const (pure ()))
  request <- awaitTicket ticket
  key <- within (Timer.registration manager)
  void (atomically (tryGrant request (pure ())))
  Timer.completeRegistration manager
  await result >>= either throwIO (`shouldBe` Just ())
  pure (finalized, key)

collect :: MVar () -> IO ()
collect finalized = do
  performGC
  tryReadMVar finalized >>= \case
    Just () -> pure ()
    Nothing -> threadDelay 1000 >> collect finalized
