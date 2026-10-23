module WaitingSpec (spec) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Resource.Waiting
import Control.Exception
import Control.Monad (forM, forM_, replicateM, replicateM_, void, when, unless)
import Data.List (sort)
import GHC.Conc.Sync (unsafeIOToSTM)
import Data.Either (isLeft)
import Skeletest
import TestSupport

spec :: Spec
spec = xfail "Waiting API scaffold: delivery and shutdown follow in the next change" $ describe "private delivery" $ do
  it "defers immediate grant removal until enqueue returns its opaque handle" $ withWaiting $ \waiting -> do
    removed <- newTVarIO ([] :: [String])
    result <- withWait waiting NoDeadline
      (\waiter -> do
        granted <- tryGrant waiter (pure (7 :: Int))
        unless granted (throwSTM (userError "immediate grant failed"))
        removals <- readTVar removed
        when (not (null removals)) (throwSTM (userError "removed before handle return"))
        pure "adapter-owned registration")
      (\registration -> modifyTVar' removed (registration :))
      (const (pure ())) (\value -> do
        readTVarIO removed >>= (`shouldBe` ["adapter-owned registration"])
        pure value)
    result `shouldBe` Right 7
    readTVarIO removed >>= (`shouldBe` ["adapter-owned registration"])
  it "defers synchronous rejection removal until its handle exists" $ withWaiting $ \waiting -> do
    removed <- newTVarIO ([] :: [Int])
    result <- withWait waiting NoDeadline
      (\waiter -> do
        refused <- reject waiter Closed
        unless refused (throwSTM (userError "synchronous rejection failed"))
        removals <- readTVar removed
        when (not (null removals)) (throwSTM (userError "removed before handle return"))
        pure (41 :: Int))
      (\registration -> modifyTVar' removed (registration :))
      (const (fail "rejected waiter acquired a resource")) (pure @IO @())
    result `shouldBe` Left Closed
    readTVarIO removed >>= (`shouldBe` [41])
  it "publishes grants and rejections before reentrant removal, exactly once" $ withWaiting $ \waiting ->
    forM_ [True, False] $ \offer -> do
      registered <- newEmptyTMVarIO
      removed <- newTVarIO (0 :: Int)
      reentered <- newTVarIO []
      reserved <- newTVarIO False
      (_, result) <- forkResult $ withWait waiting NoDeadline
        (\waiter -> putTMVar registered waiter >> pure waiter)
        (\waiter -> do
          count <- readTVar removed
          modifyTVar' removed (+ 1)
          -- Bound reentry so an incorrect Pending mailbox fails without looping.
          when (count == 0) $ do
            granted <- tryGrant waiter (writeTVar reserved True)
            refused <- reject waiter Closed
            writeTVar reentered [granted, refused])
        (const (pure ())) (pure @IO @())
      waiter <- within (atomically (takeTMVar registered))
      atomically (if offer then tryGrant waiter (pure ()) else reject waiter Closed) >>= (`shouldBe` True)
      await result >>= expectWaitResult (if offer then Right () else Left Closed)
      readTVarIO reentered >>= (`shouldBe` [False, False])
      readTVarIO reserved >>= (`shouldBe` False)
      readTVarIO removed >>= (`shouldBe` 1)
  it "rolls back an offered reservation when enqueue throws before returning its handle" $ withWaiting $ \waiting -> do
    reserved <- newTVarIO (0 :: Int)
    removed <- newTVarIO (0 :: Int)
    cleaned <- newTVarIO (0 :: Int)
    called <- newTVarIO False
    outcome <- try @RegistrationFailed $ withWait waiting NoDeadline
      (\waiter -> do
        void (tryGrant waiter (modifyTVar' reserved (+ 1)))
        throwSTM RegistrationFailed)
      (\() -> modifyTVar' removed (+ 1))
      (\() -> atomically (modifyTVar' cleaned (+ 1)))
      (\() -> atomically (writeTVar called True))
    outcome `shouldBe` Left RegistrationFailed
    readTVarIO reserved >>= (`shouldBe` 0)
    readTVarIO removed >>= (`shouldBe` 0)
    readTVarIO cleaned >>= (`shouldBe` 0)
    readTVarIO called >>= (`shouldBe` False)
  it "grants and cleans up exactly once with an unmasked callback" $ withWaiting $ \waiting -> do
    cleaned <- newTVarIO (0 :: Int)
    result <- withWait waiting NoDeadline
      (\waiter -> tryGrant waiter (pure (7 :: Int)) >> pure ())
      (const (pure ()))
      (\_ -> atomically (modifyTVar' cleaned (+ 1)))
      (\value -> getMaskingState >>= (\state -> state `shouldBe` Unmasked) >> pure value)
    result `shouldBe` Right 7
    readTVarIO cleaned >>= (`shouldBe` 1)
  it "cancels a pending waiter and removes its timer and registration" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 60
    registered <- newEmptyTMVarIO
    detached <- newTVarIO (0 :: Int)
    (tid, result) <- forkResult $ withWait waiting deadline
      (\waiter -> putTMVar registered waiter >> pure waiter)
      (\waiter -> do
        count <- readTVar detached
        modifyTVar' detached (+ 1)
        -- Cancellation must terminalize the mailbox before invoking removal.
        when (count == 0) $ do
          offered <- tryGrant waiter (throwSTM (userError "cancelled reservation ran"))
          refused <- reject waiter Closed
          when (offered || refused) (throwSTM (userError "removal observed pending mailbox")))
      (const (fail "unassigned cleanup")) pure
    waiter <- within (atomically (takeTMVar registered))
    killThread tid
    await result >>= (\outcome -> isLeft outcome `shouldBe` True)
    atomically (tryGrant waiter (pure ())) >>= (`shouldBe` False)
    readTVarIO detached >>= (`shouldBe` 1)
  it "cleans an assigned grant when cancellation arrives before callback completion" $ withWaiting $ \waiting -> do
    registered <- newEmptyTMVarIO
    entered <- newEmptyMVar
    hold <- newEmptyMVar
    cleaned <- newTVarIO (0 :: Int)
    detached <- newTVarIO (0 :: Int)
    (tid, result) <- forkResult $ withWait waiting NoDeadline
      (putTMVar registered) (const (modifyTVar' detached (+ 1)))
      (\() -> atomically (modifyTVar' cleaned (+ 1)))
      (\() -> putMVar entered () >> takeMVar hold)
    waiter <- within (atomically (takeTMVar registered))
    atomically (tryGrant waiter (pure ())) >>= (`shouldBe` True)
    await entered
    killThread tid
    await result >>= (\outcome -> isLeft outcome `shouldBe` True)
    readTVarIO cleaned >>= (`shouldBe` 1)
    readTVarIO detached >>= (`shouldBe` 1)
  it "returns a grant after a callback exception" $ withWaiting $ \waiting -> do
    cleaned <- newTVarIO False
    outcome <- try @SomeException $ withWait waiting NoDeadline
      (\waiter -> tryGrant waiter (pure ()) >> pure ()) (const (pure ()))
      (\() -> atomically (writeTVar cleaned True)) (\() -> throwIO (userError "callback failed"))
    isLeft outcome `shouldBe` True
    readTVarIO cleaned >>= (`shouldBe` True)
  it "rolls back a retrying compound reservation" $ withWaiting $ \waiting -> do
    registered <- newEmptyTMVarIO
    touched <- newTVarIO (0 :: Int)
    (_, result) <- forkResult $ withWait waiting NoDeadline (putTMVar registered) (const (pure ())) (const (pure ())) (pure @IO @())
    waiter <- within (atomically (takeTMVar registered))
    atomically (tryGrant waiter (writeTVar touched 1 >> retry)) >>= (`shouldBe` False)
    readTVarIO touched >>= (`shouldBe` 0)
    atomically (reject waiter Closed) >>= (`shouldBe` True)
    await result >>= (\outcome -> case outcome of Right value -> value `shouldBe` Left Closed; Left err -> throwIO err)
  it "delivers reservation exceptions to the requester and rolls back effects" $ withWaiting $ \waiting -> do
    registered <- newEmptyTMVarIO
    touched <- newTVarIO (0 :: Int)
    (_, result) <- forkResult $ withWait waiting NoDeadline (putTMVar registered) (const (pure ())) (const (pure ())) (pure @IO @())
    waiter <- within (atomically (takeTMVar registered))
    atomically (tryGrant waiter (writeTVar touched 1 >> throwSTM (userError "reserve failed"))) >>= (`shouldBe` True)
    await result >>= (\outcome -> isLeft outcome `shouldBe` True)
    readTVarIO touched >>= (`shouldBe` 0)
  it "skips enqueue and removal for an already expired deadline" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 0
    touched <- newTVarIO ([] :: [String])
    result <- withWait waiting deadline
      (const (modifyTVar' touched ("enqueue" :)))
      (const (modifyTVar' touched ("remove" :)))
      (const (fail "expired waiter acquired a resource")) (pure @IO @())
    result `shouldBe` Left TimedOut
    readTVarIO touched >>= (`shouldBe` [])
  it "times out a pending private wait" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 0.01
    result <- within $ withWait waiting deadline (const (pure ())) (const (pure ())) (const (pure ())) (pure @IO @())
    result `shouldBe` Left TimedOut
  it "detaches an expired waiter on its requesting thread" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 0.02
    detachedBy <- newTVarIO Nothing
    (requester, result) <- forkResult $ withWait waiting deadline (const (pure ()))
      (const (unsafeIOToSTM myThreadId >>= writeTVar detachedBy . Just))
      (const (fail "expired waiter acquired a resource")) (pure @IO @())
    await result >>= expectWaitResult (Left TimedOut)
    readTVarIO detachedBy >>= (`shouldBe` Just requester)
  it "expires once when enqueue crosses the absolute deadline" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 0.01
    detached <- newTVarIO (0 :: Int)
    result <- within $ withWait waiting deadline
      (const (unsafeIOToSTM (threadDelay 30000)))
      (const (modifyTVar' detached (+ 1)))
      (const (fail "expired waiter acquired a resource")) (pure @IO @())
    result `shouldBe` Left TimedOut
    readTVarIO detached >>= (`shouldBe` 1)
  it "stopping the timer service closes timed and untimed pending waiters" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 60
    registered <- newTQueueIO
    detached <- newTVarIO (0 :: Int)
    workers <- forM [NoDeadline, deadline] $ \expiry ->
      forkResult $ withWait waiting expiry (writeTQueue registered)
        (const (modifyTVar' detached (+ 1))) (const (pure ())) (pure @IO @())
    waiters <- replicateM 2 (within (atomically (readTQueue registered)))
    closeWaiting waiting
    forM_ workers $ \(_, result) -> await result >>= expectWaitResult (Left Closed)
    readTVarIO detached >>= (`shouldBe` 2)
    forM_ waiters $ \waiter -> atomically (tryGrant waiter (pure ())) >>= (`shouldBe` False)
  it "never reserves inventory after service closure" $ withWaiting $ \waiting -> do
    registered <- newEmptyTMVarIO
    reserved <- newTVarIO False
    (_, result) <- forkResult $ withWait waiting NoDeadline (putTMVar registered)
      (const (pure ())) (const (pure ())) (pure @IO @())
    waiter <- within (atomically (takeTMVar registered))
    closeWaiting waiting
    atomically (tryGrant waiter (writeTVar reserved True)) >>= (`shouldBe` False)
    readTVarIO reserved >>= (`shouldBe` False)
    await result >>= expectWaitResult (Left Closed)
  it "rejects later registrations after the timer service stops" $ withWaiting $ \waiting -> do
    closeWaiting waiting
    deadline <- deadlineAfter 60
    touched <- newTVarIO ([] :: [String])
    forM_ [NoDeadline, deadline] $ \expiry -> do
      later <- withWait waiting expiry (const (modifyTVar' touched ("enqueue" :)))
        (const (modifyTVar' touched ("remove" :))) (const (pure ())) (pure @IO @())
      later `shouldBe` Left Closed
      readTVarIO touched >>= (`shouldBe` [])
  it "repeated cancellation settles every detach before its deadline" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 60
    detached <- newTVarIO (0 :: Int)
    replicateM_ 100 $ do
      registered <- newEmptyTMVarIO
      (tid, result) <- forkResult $ withWait waiting deadline (putTMVar registered) (const (modifyTVar' detached (+ 1))) (const (pure ())) (pure @IO @())
      _ <- within (atomically (takeTMVar registered))
      killThread tid
      _ <- await result
      pure ()
    readTVarIO detached >>= (`shouldBe` 100)
  it "cleans an offered grant whose deadline elapsed before callback entry" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 0.01
    cleaned <- newTVarIO False
    called <- newTVarIO False
    result <- within $ withWait waiting deadline
      (\waiter -> do
        -- Delay this single transaction to deterministically cross the absolute
        -- deadline between the initial clock read and grant delivery.
        unsafeIOToSTM (threadDelay 30000)
        void (tryGrant waiter (pure ())))
      (const (pure ()))
      (\() -> atomically (writeTVar cleaned True))
      (\() -> atomically (writeTVar called True))
    result `shouldBe` Left TimedOut
    readTVarIO cleaned >>= (`shouldBe` True)
    readTVarIO called >>= (`shouldBe` False)
  it "routes concurrent completions to their adapter-owned handles" $ withWaiting $ \waiting -> do
    deadline <- deadlineAfter 60
    registered <- newTQueueIO
    removed <- newTVarIO ([] :: [Int])
    workers <- forM [1 .. 64 :: Int] $ \index ->
      forkResult $ withWait waiting (if even index then deadline else NoDeadline)
        (\waiter -> writeTQueue registered (index, waiter) >> pure index)
        (\registration -> modifyTVar' removed (registration :)) (const (pure ())) pure
    waiters <- replicateM 64 (within (atomically (readTQueue registered)))
    atomically $ forM_ waiters $ \(index, waiter) -> void (tryGrant waiter (pure index))
    forM_ (zip [1 .. 64 :: Int] workers) $ \(index, (_, result)) ->
      await result >>= either throwIO (`shouldBe` Right index)
    sort <$> readTVarIO removed >>= (`shouldBe` [1 .. 64])
    atomically (forM waiters (\(_, waiter) -> reject waiter Closed)) >>= (`shouldBe` replicate 64 False)
    length <$> readTVarIO removed >>= (`shouldBe` 64)
  it "releases timed grant callbacks before resource use finishes" $ withWaiting $ \waiting ->
    forM_ [True, False] $ \grantDuringEnqueue -> do
      (finalized, release, result) <- activeGrantAnchor waiting grantDuringEnqueue
      collected finalized `finally` putMVar release ()
      await result >>= expectWaitResult (Right ())

-- Keep construction on a separate stack frame so the test itself does not
-- retain the anchor while asking the collector to prove deadline callback removal.
{-# NOINLINE cancelledWaitingAnchor #-}
cancelledWaitingAnchor :: Waiting -> IO (MVar ())
cancelledWaitingAnchor waiting = do
  anchor <- newTVarIO True
  done <- newEmptyMVar
  _ <- mkWeakTVar anchor (putMVar done ())
  registered <- newEmptyTMVarIO
  deadline <- deadlineAfter 60
  (tid, result) <- forkResult $ withWait waiting deadline (putTMVar registered)
    (const (readTVar anchor >>= check)) (const (pure ())) (pure @IO @())
  _ <- within (atomically (takeTMVar registered))
  killThread tid
  _ <- await result
  pure done

data RegistrationFailed = RegistrationFailed deriving (Eq, Show)
instance Exception RegistrationFailed

expectWaitResult :: Either WaitError () -> Either SomeException (Either WaitError ()) -> IO ()
expectWaitResult expected = either throwIO (`shouldBe` expected)

{-# NOINLINE grantedWaitingAnchor #-}
grantedWaitingAnchor :: Waiting -> IO (MVar ())
grantedWaitingAnchor waiting = do
  anchor <- newTVarIO True
  done <- newEmptyMVar
  _ <- mkWeakTVar anchor (putMVar done ())
  deadline <- deadlineAfter 60
  registered <- newEmptyTMVarIO
  (_, result) <- forkResult $ withWait waiting deadline (putTMVar registered)
    (const (readTVar anchor >>= check)) (const (pure ())) (pure @IO @())
  waiter <- within (atomically (takeTMVar registered))
  atomically (tryGrant waiter (pure ())) >>= (`shouldBe` True)
  await result >>= expectWaitResult (Right ())
  pure done

{-# NOINLINE activeGrantAnchor #-}
activeGrantAnchor :: Waiting -> Bool -> IO (MVar (), MVar (), MVar (Either SomeException (Either WaitError ())))
activeGrantAnchor waiting grantDuringEnqueue = do
  anchor <- newTVarIO True
  done <- newEmptyMVar
  _ <- mkWeakTVar anchor (putMVar done ())
  entered <- newEmptyMVar
  release <- newEmptyMVar
  registered <- newEmptyTMVarIO
  deadline <- deadlineAfter 60
  (tid, result) <- forkResult $ withWait waiting deadline
    (\waiter -> do
      putTMVar registered waiter
      if grantDuringEnqueue then void (tryGrant waiter (pure ())) else pure ())
    (const (readTVar anchor >>= check)) (const (pure ()))
    (\() -> putMVar entered () >> takeMVar release)
  waiter <- within (atomically (takeTMVar registered))
  if grantDuringEnqueue then pure () else do
    awaitBlocked tid
    atomically (tryGrant waiter (pure ())) >>= (`shouldBe` True)
  await entered
  pure (done, release, result)
