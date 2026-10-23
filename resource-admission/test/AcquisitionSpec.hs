module AcquisitionSpec (spec) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Resource.Acquisition
import Control.Exception
import Control.Monad (forM, forM_, replicateM, replicateM_, void, when, unless)
import Data.Either (isLeft)
import Data.List (sort)
import Data.Maybe (isNothing)
import GHC.Conc.Sync (unsafeIOToSTM)
import Skeletest hiding (context)
import TestSupport

spec :: Spec
spec = describe "private acquisition" $ do
  it "reserves immediately without enqueue, removal, or cleanup" $ usingSystemTimer $ \context -> do
    touched <- newTVarIO ([] :: [String])
    result <- acquire context NoDeadline (pure (7 :: Int))
      (\request -> modifyTVar' touched ("enqueue" :) >> void (tryGrant request (pure 9)))
      (const (modifyTVar' touched ("remove" :)))
      (const (atomically (modifyTVar' touched ("cleanup" :))))
    result `shouldBe` Just 7
    readTVarIO touched >>= (`shouldBe` [])
  it "rolls back a retrying immediate reservation before fallback can grant" $ usingSystemTimer $ \context -> do
    available <- newEmptyTMVarIO
    touched <- newTVarIO False
    removed <- newTVarIO (0 :: Int)
    result <- acquire context NoDeadline
      (writeTVar touched True >> takeTMVar available)
      (\request -> do
        -- Inventory becomes available during registration: the fallback must
        -- offer it before committing, or no later producer can wake this caller.
        putTMVar available (7 :: Int)
        void (tryGrant request (takeTMVar available)))
      (const (modifyTVar' removed (+ 1))) (const (fail "claimed value reached cleanup"))
    result `shouldBe` Just 7
    readTVarIO touched >>= (`shouldBe` False)
    readTVarIO removed >>= (`shouldBe` 1)
    atomically (isEmptyTMVar available) >>= (`shouldBe` True)
  it "does not miss a release racing with fallback registration" $ usingSystemTimer $ \context -> do
    available <- newEmptyTMVarIO
    pending <- newTVarIO Nothing
    checkedEmpty <- newEmptyMVar
    released <- newEmptyMVar
    let reserveNow = tryTakeTMVar available >>= maybe
          (unsafeIOToSTM (putMVar checkedEmpty () >> takeMVar released) >> retry)
          pure
    (tid, result) <- forkResult $ acquire context NoDeadline reserveNow
      (writeTVar pending . Just)
      (const (writeTVar pending Nothing)) (const (pure ()))
    flip finally (killThread tid) $ do
      await checkedEmpty
      -- Release after the failed inventory check but before registration. The
      -- choice must restart when this write invalidates the inventory read.
      atomically $ do
        putTMVar available (7 :: Int)
        readTVar pending >>= mapM_ (\request -> void (tryGrant request (takeTMVar available)))
      putMVar released ()
      await result >>= either throwIO (`shouldBe` Just 7)
      readTVarIO pending >>= (\request -> isNothing request `shouldBe` True)
  it "rolls back an immediate reservation exception without registering" $ usingSystemTimer $ \context -> do
    reserved <- newTVarIO False
    registered <- newTVarIO False
    outcome <- try @OwnerClosed $ acquire context NoDeadline
      (writeTVar reserved True >> throwSTM OwnerClosed)
      (\request -> writeTVar registered True >> void (tryGrant request (pure ())))
      (const (pure ())) (const (fail "failed reservation reached cleanup"))
    outcome `shouldBe` Left OwnerClosed
    readTVarIO reserved >>= (`shouldBe` False)
    readTVarIO registered >>= (`shouldBe` False)
  it "cleans an immediate reservation when its deadline passes before transfer" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 0.01
    cleaned <- newTVarIO (0 :: Int)
    result <- acquire context deadline
      (unsafeIOToSTM (threadDelay 30000))
      (const (throwSTM (userError "immediate reservation was enqueued")))
      (\() -> throwSTM (userError "immediate reservation was removed"))
      (\() -> atomically (modifyTVar' cleaned (+ 1)))
    result `shouldBe` Nothing
    readTVarIO cleaned >>= (`shouldBe` 1)
  it "lets the caller bracket release an immediate grant when use is cancelled" $ usingSystemTimer $ \context -> do
    entered <- newEmptyMVar
    hold <- newEmptyMVar
    cleaned <- newTVarIO (0 :: Int)
    (tid, result) <- forkResult $ withAcquired context NoDeadline (pure ())
      (const (throwSTM (userError "immediate reservation was enqueued")))
      (\() -> pure ()) (\() -> atomically (modifyTVar' cleaned (+ 1)))
      (\() -> putMVar entered () >> takeMVar hold)
    await entered
    killThread tid
    await result >>= (\outcome -> isLeft outcome `shouldBe` True)
    readTVarIO cleaned >>= (`shouldBe` 1)
  it "transfers an immediate grant without running cleanup" $ usingSystemTimer $ \context -> do
    cleaned <- newTVarIO (0 :: Int)
    result <- acquire context NoDeadline retry
      (\request -> void (tryGrant request (pure (7 :: Int))))
      (const (pure ())) (const (atomically (modifyTVar' cleaned (+ 1))))
    result `shouldBe` Just 7
    readTVarIO cleaned >>= (`shouldBe` 0)
  it "transfers a deferred grant without running cleanup" $ usingSystemTimer $ \context -> do
    registered <- newEmptyTMVarIO
    cleaned <- newTVarIO (0 :: Int)
    (_, result) <- forkResult $ acquire context NoDeadline retry (putTMVar registered)
      (const (pure ())) (const (atomically (modifyTVar' cleaned (+ 1))))
    request <- within (atomically (takeTMVar registered))
    atomically (tryGrant request (pure (7 :: Int))) >>= (`shouldBe` True)
    await result >>= either throwIO (`shouldBe` Just 7)
    readTVarIO cleaned >>= (`shouldBe` 0)
  it "lets the caller bracket release after an unmasked callback or its exception" $ usingSystemTimer $ \context ->
    forM_ [False, True] $ \raises -> do
      cleaned <- newTVarIO (0 :: Int)
      outcome <- try @OwnerClosed $ withAcquired context NoDeadline retry
        (\request -> void (tryGrant request (pure (7 :: Int)))) (const (pure ()))
        (const (atomically (modifyTVar' cleaned (+ 1))))
        (\value -> do
          getMaskingState >>= (`shouldBe` Unmasked)
          if raises then throwIO OwnerClosed else pure value)
      outcome `shouldBe` if raises then Left OwnerClosed else Right (Just 7)
      readTVarIO cleaned >>= (`shouldBe` 1)
  it "defers immediate grant and error removal until enqueue returns its opaque handle" $ usingSystemTimer $ \context ->
    forM_ [True, False] $ \offer -> do
      removed <- newTVarIO ([] :: [String])
      reentered <- newTVarIO []
      reserved <- newTVarIO False
      result <- try @OwnerClosed $ acquire context NoDeadline retry
        (\request -> do
          completed <- tryGrant request (if offer then pure (7 :: Int) else throwSTM OwnerClosed)
          unless completed (throwSTM (userError "synchronous completion failed"))
          removals <- readTVar removed
          unless (null removals) (throwSTM (userError "removed before handle return"))
          pure ("adapter-owned registration", request))
        (\(registration, request) -> do
          removals <- readTVar removed
          modifyTVar' removed (registration :)
          -- Bound reentry so a broken terminal transition fails without looping.
          when (null removals) $ do
            granted <- tryGrant request (writeTVar reserved True >> pure 8)
            failed <- tryGrant request (throwSTM OwnerClosed)
            writeTVar reentered [granted, failed])
        (const (fail "claimed value reached cleanup"))
      result `shouldBe` if offer then Right (Just 7) else Left OwnerClosed
      readTVarIO removed >>= (`shouldBe` ["adapter-owned registration"])
      readTVarIO reentered >>= (`shouldBe` [False, False])
      readTVarIO reserved >>= (`shouldBe` False)
  it "publishes grants and errors before reentrant removal exactly once" $ usingSystemTimer $ \context ->
    forM_ [True, False] $ \offer -> do
      registered <- newEmptyTMVarIO
      removed <- newTVarIO (0 :: Int)
      reentered <- newTVarIO []
      reserved <- newTVarIO False
      (_, result) <- forkResult $ acquire context NoDeadline retry
        (\request -> putTMVar registered request >> pure request)
        (\request -> do
          count <- readTVar removed
          modifyTVar' removed (+ 1)
          when (count == 0) $ do
            granted <- tryGrant request (writeTVar reserved True)
            failed <- tryGrant request (throwSTM OwnerClosed)
            writeTVar reentered [granted, failed])
        (const (fail "claimed value reached cleanup"))
      request <- within (atomically (takeTMVar registered))
      atomically (tryGrant request (if offer then pure () else throwSTM OwnerClosed)) >>= (`shouldBe` True)
      atomically (tryGrant request (writeTVar reserved True)) >>= (`shouldBe` False)
      if offer then await result >>= expectWaitResult (Just ()) else await result >>= expectOwnerClosed
      readTVarIO reentered >>= (`shouldBe` [False, False])
      readTVarIO reserved >>= (`shouldBe` False)
      readTVarIO removed >>= (`shouldBe` 1)
  it "rolls back an offered reservation when enqueue throws before returning its handle" $ usingSystemTimer $ \context -> do
    reserved <- newTVarIO (0 :: Int)
    removed <- newTVarIO (0 :: Int)
    cleaned <- newTVarIO (0 :: Int)
    outcome <- try @OwnerClosed $ acquire context NoDeadline retry
      (\request -> do
        void (tryGrant request (modifyTVar' reserved (+ 1)))
        throwSTM OwnerClosed)
      (\() -> modifyTVar' removed (+ 1))
      (\() -> atomically (modifyTVar' cleaned (+ 1)))
    outcome `shouldBe` Left OwnerClosed
    readTVarIO reserved >>= (`shouldBe` 0)
    readTVarIO removed >>= (`shouldBe` 0)
    readTVarIO cleaned >>= (`shouldBe` 0)
  it "cancels a pending waiter and terminalizes it before removing its handle" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 60
    registered <- newEmptyTMVarIO
    detached <- newTVarIO (0 :: Int)
    (tid, result) <- forkResult $ acquire context deadline retry
      (\request -> putTMVar registered request >> pure request)
      (\request -> do
        count <- readTVar detached
        modifyTVar' detached (+ 1)
        when (count == 0) $ do
          offered <- tryGrant request (throwSTM (userError "cancelled reservation ran"))
          when offered (throwSTM (userError "removal observed pending delivery state")))
      (const (fail "unassigned cleanup"))
    request <- within (atomically (takeTMVar registered))
    killThread tid
    await result >>= (\outcome -> isLeft outcome `shouldBe` True)
    atomically (tryGrant request (pure ())) >>= (`shouldBe` False)
    readTVarIO detached >>= (`shouldBe` 1)
  it "settles cancel and grant races with exactly one cleanup owner" $ usingSystemTimer $ \context ->
    replicateM_ 100 $ do
      registered <- newEmptyTMVarIO
      start <- newEmptyTMVarIO
      detached <- newTVarIO (0 :: Int)
      abandoned <- newTVarIO (0 :: Int)
      released <- newTVarIO (0 :: Int)
      -- A masked caller owns every returned value even if cancellation is pending.
      (tid, result) <- forkResult $ mask_ $ do
        value <- acquire context NoDeadline retry (putTMVar registered)
          (const (modifyTVar' detached (+ 1)))
          (const (atomically (modifyTVar' abandoned (+ 1))))
        forM_ value (const (atomically (modifyTVar' released (+ 1))))
      request <- within (atomically (takeTMVar registered))
      (_, granted) <- forkResult $ atomically (readTMVar start >> tryGrant request (pure ()))
      (_, cancelled) <- forkResult $ atomically (readTMVar start) >> killThread tid
      atomically (putTMVar start ())
      grantWon <- await granted >>= either throwIO pure
      await cancelled >>= either throwIO pure
      _ <- await result
      readTVarIO detached >>= (`shouldBe` 1)
      reclaimed <- (+) <$> readTVarIO abandoned <*> readTVarIO released
      reclaimed `shouldBe` if grantWon then 1 else 0
  it "lets the caller release a claimed grant when use is cancelled" $ usingSystemTimer $ \context -> do
    registered <- newEmptyTMVarIO
    entered <- newEmptyMVar
    hold <- newEmptyMVar
    cleaned <- newTVarIO (0 :: Int)
    (tid, result) <- forkResult $ withAcquired context NoDeadline retry
      (putTMVar registered) (const (pure ()))
      (\() -> atomically (modifyTVar' cleaned (+ 1)))
      (\() -> putMVar entered () >> takeMVar hold)
    request <- within (atomically (takeTMVar registered))
    atomically (tryGrant request (pure ())) >>= (`shouldBe` True)
    await entered
    killThread tid
    await result >>= (\outcome -> isLeft outcome `shouldBe` True)
    readTVarIO cleaned >>= (`shouldBe` 1)
  it "rolls back a retrying compound reservation" $ usingSystemTimer $ \context -> do
    registered <- newEmptyTMVarIO
    touched <- newTVarIO (0 :: Int)
    (_, result) <- forkResult $ acquire context NoDeadline retry (putTMVar registered) (const (pure ())) (const (pure ()))
    request <- within (atomically (takeTMVar registered))
    atomically (tryGrant request (writeTVar touched 1 >> retry)) >>= (`shouldBe` False)
    readTVarIO touched >>= (`shouldBe` 0)
    atomically (tryGrant request (throwSTM OwnerClosed)) >>= (`shouldBe` True)
    await result >>= expectOwnerClosed
  it "delivers reservation exceptions to the requester and rolls back effects" $ usingSystemTimer $ \context -> do
    registered <- newEmptyTMVarIO
    touched <- newTVarIO (0 :: Int)
    (_, result) <- forkResult $ acquire context NoDeadline retry (putTMVar registered) (const (pure ())) (const (pure ()))
    request <- within (atomically (takeTMVar registered))
    atomically (tryGrant request (writeTVar touched 1 >> throwSTM OwnerClosed)) >>= (`shouldBe` True)
    await result >>= expectOwnerClosed
    readTVarIO touched >>= (`shouldBe` 0)
  it "skips enqueue and removal for an already expired deadline" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 0
    touched <- newTVarIO ([] :: [String])
    result <- acquire context deadline (modifyTVar' touched ("reserve" :))
      (const (modifyTVar' touched ("enqueue" :))) (const (modifyTVar' touched ("remove" :)))
      (const (fail "expired waiter acquired a resource"))
    result `shouldBe` (Nothing :: Maybe ())
    readTVarIO touched >>= (`shouldBe` [])
  it "times out a pending private wait" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 0.01
    result <- within $ acquire context deadline retry (const (pure ())) (const (pure ())) (const (pure @IO @() ()))
    result `shouldBe` (Nothing :: Maybe ())
  it "detaches an expired waiter on its requesting thread" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 0.02
    detachedBy <- newTVarIO Nothing
    (requester, result) <- forkResult $ acquire context deadline retry (const (pure ()))
      (const (unsafeIOToSTM myThreadId >>= writeTVar detachedBy . Just))
      (const (fail "expired waiter acquired a resource"))
    await result >>= expectWaitResult Nothing
    readTVarIO detachedBy >>= (`shouldBe` Just requester)
  it "expires once when enqueue crosses the absolute deadline" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 0.01
    detached <- newTVarIO (0 :: Int)
    result <- within $ acquire context deadline retry (const (unsafeIOToSTM (threadDelay 30000)))
      (const (modifyTVar' detached (+ 1))) (const (pure @IO @() ()))
    result `shouldBe` (Nothing :: Maybe ())
    readTVarIO detached >>= (`shouldBe` 1)
  it "cleans an offered grant whose absolute deadline elapsed before transfer" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 0.01
    cleaned <- newTVarIO False
    result <- within $ acquire context deadline retry
      (\request -> do
        -- Cross the deadline between the first clock read and ownership transfer.
        unsafeIOToSTM (threadDelay 30000)
        void (tryGrant request (pure ())))
      (const (pure ())) (\() -> atomically (writeTVar cleaned True))
    result `shouldBe` Nothing
    readTVarIO cleaned >>= (`shouldBe` True)
  it "owner errors leave unrelated and later requests usable" $ usingSystemTimer $ \context -> do
    registered <- newTQueueIO
    reserved <- newTVarIO False
    workers <- replicateM 2 $ forkResult $ acquire context NoDeadline retry (writeTQueue registered)
      (const (pure ())) (const (pure @IO @() ()))
    failed <- within (atomically (readTQueue registered))
    survivor <- within (atomically (readTQueue registered))
    atomically (tryGrant failed (throwSTM OwnerClosed)) >>= (`shouldBe` True)
    atomically (tryGrant failed (writeTVar reserved True)) >>= (`shouldBe` False)
    readTVarIO reserved >>= (`shouldBe` False)
    atomically (tryGrant survivor (pure ())) >>= (`shouldBe` True)
    outcomes <- forM workers $ \(_, result) -> await result
    length (filter isLeft outcomes) `shouldBe` 1
    forM_ outcomes $ either (\exception -> fromException exception `shouldBe` Just OwnerClosed) (`shouldBe` Just ())
    later <- acquire context NoDeadline retry (\request -> void (tryGrant request (pure ())))
      (const (pure ())) (const (pure ()))
    later `shouldBe` Just ()
  it "routes concurrent completions to their adapter-owned handles" $ usingSystemTimer $ \context -> do
    deadline <- deadlineAfter 60
    registered <- newTQueueIO
    removed <- newTVarIO ([] :: [Int])
    workers <- forM [1 .. 64 :: Int] $ \index ->
      forkResult $ acquire context (if even index then deadline else NoDeadline) retry
        (\request -> writeTQueue registered (index, request) >> pure index)
        (\registration -> modifyTVar' removed (registration :)) (const (pure ()))
    requests <- replicateM 64 (within (atomically (readTQueue registered)))
    atomically $ forM_ requests $ \(index, request) -> void (tryGrant request (pure index))
    forM_ (zip [1 .. 64 :: Int] workers) $ \(index, (_, result)) ->
      await result >>= either throwIO (`shouldBe` Just index)
    readTVarIO removed >>= (`shouldBe` [1 .. 64]) . sort
    atomically (forM requests (\(_, request) -> tryGrant request (throwSTM OwnerClosed))) >>= (`shouldBe` replicate 64 False)
    readTVarIO removed >>= (`shouldBe` 64) . length
  it "expires an earlier waiter while far-future waiters are pending" $ usingSystemTimer $ \context -> do
    capabilities <- getNumCapabilities
    registered <- newTQueueIO
    later <- deadlineAfter 60
    pending <- replicateM capabilities $ forkResult $ acquire context later retry (writeTQueue registered)
      (const (pure ())) (const (pure ()))
    requests <- replicateM capabilities (within (atomically (readTQueue registered)))
    earlier <- deadlineAfter 0.02
    outcome <- within $ acquire context earlier retry (const (pure ())) (const (pure ())) (const (pure @IO @() ()))
    outcome `shouldBe` (Nothing :: Maybe ())
    forM_ requests $ \request -> atomically (tryGrant request (pure ())) >>= (`shouldBe` True)
    forM_ pending $ \(_, result) -> await result >>= expectWaitResult (Just ())
  it "expires many waiters sharing one deadline" $ usingSystemTimer $ \context -> do
    capabilities <- getNumCapabilities
    let count = 192 * capabilities
    deadline <- deadlineAfter 1
    registered <- newTQueueIO
    detached <- newTVarIO (0 :: Int)
    workers <- replicateM count $ forkResult $ acquire context deadline retry (writeTQueue registered)
      (const (modifyTVar' detached (+ 1))) (const (fail "expired waiter acquired a resource"))
    requests <- replicateM count (within (atomically (readTQueue registered)))
    forM_ workers $ \(_, result) -> await result >>= expectWaitResult Nothing
    readTVarIO detached >>= (`shouldBe` count)
    forM_ requests $ \request -> atomically (tryGrant request (pure ())) >>= (`shouldBe` False)
  it "removing the earliest deadline preserves a later same-timestamp survivor" $ usingSystemTimer $ \context -> do
    registered <- newTQueueIO
    detached <- newTVarIO (0 :: Int)
    earliest <- deadlineAfter 0.5
    later <- deadlineAfter 1
    workers <- forM [earliest, later, later] $ \deadline ->
      forkResult $ acquire context deadline retry (writeTQueue registered)
        (const (modifyTVar' detached (+ 1))) (const (pure @IO @() ()))
    replicateM_ 3 (within (atomically (readTQueue registered)))
    case workers of
      [(first, firstResult), (second, secondResult), (_, survivor)] -> do
        killThread first
        killThread second
        await firstResult >>= (\outcome -> isLeft outcome `shouldBe` True)
        await secondResult >>= (\outcome -> isLeft outcome `shouldBe` True)
        await survivor >>= expectWaitResult Nothing
      _ -> fail "expected three registered waiters"
    readTVarIO detached >>= (`shouldBe` 3)
  it "grant releases its detach callback before a far-future deadline" $ usingSystemTimer $ \context -> do
    finalized <- grantedContextAnchor context
    collected finalized
  it "cancellation releases its detach callback before a far-future deadline" $ usingSystemTimer $ \context -> do
    finalized <- cancelledContextAnchor context
    collected finalized
  it "releases timed grant callbacks before caller use finishes" $ usingSystemTimer $ \context ->
    forM_ [True, False] $ \grantDuringEnqueue -> do
      (finalized, release, result) <- activeGrantAnchor context grantDuringEnqueue
      collected finalized `finally` putMVar release ()
      await result >>= expectWaitResult (Just ())

-- Callers protect acquisition handoff and own normal release with bracket.
withAcquired :: Context -> Deadline -> STM a -> (Request a -> STM handle) -> (handle -> STM ()) -> (a -> IO ()) -> (a -> IO b) -> IO (Maybe b)
withAcquired context deadline reserveNow enqueue remove release use =
  bracket (acquire context deadline reserveNow enqueue remove release) (mapM_ release) (traverse use)

data OwnerClosed = OwnerClosed deriving (Eq, Show)
instance Exception OwnerClosed

expectOwnerClosed :: Either SomeException a -> IO ()
expectOwnerClosed outcome = case outcome of
  Left exception -> fromException exception `shouldBe` Just OwnerClosed
  Right _ -> fail "expected owner exception"

expectWaitResult :: Maybe () -> Either SomeException (Maybe ()) -> IO ()
expectWaitResult expected = either throwIO (`shouldBe` expected)

-- A separate frame stops these helpers retaining the anchor during collection.
{-# NOINLINE cancelledContextAnchor #-}
cancelledContextAnchor :: Context -> IO (MVar ())
cancelledContextAnchor context = do
  anchor <- newTVarIO True
  done <- newEmptyMVar
  _ <- mkWeakTVar anchor (putMVar done ())
  registered <- newEmptyTMVarIO
  deadline <- deadlineAfter 60
  (tid, result) <- forkResult $ acquire context deadline retry (putTMVar registered)
    (const (readTVar anchor >>= check)) (const (pure @IO @() ()))
  _ <- within (atomically (takeTMVar registered))
  killThread tid
  _ <- await result
  pure done

{-# NOINLINE grantedContextAnchor #-}
grantedContextAnchor :: Context -> IO (MVar ())
grantedContextAnchor context = do
  anchor <- newTVarIO True
  done <- newEmptyMVar
  _ <- mkWeakTVar anchor (putMVar done ())
  deadline <- deadlineAfter 60
  registered <- newEmptyTMVarIO
  (_, result) <- forkResult $ acquire context deadline retry (putTMVar registered)
    (const (readTVar anchor >>= check)) (const (pure ()))
  request <- within (atomically (takeTMVar registered))
  atomically (tryGrant request (pure ())) >>= (`shouldBe` True)
  await result >>= expectWaitResult (Just ())
  pure done

{-# NOINLINE activeGrantAnchor #-}
activeGrantAnchor :: Context -> Bool -> IO (MVar (), MVar (), MVar (Either SomeException (Maybe ())))
activeGrantAnchor context grantDuringEnqueue = do
  anchor <- newTVarIO True
  done <- newEmptyMVar
  _ <- mkWeakTVar anchor (putMVar done ())
  entered <- newEmptyMVar
  release <- newEmptyMVar
  registered <- newEmptyTMVarIO
  deadline <- deadlineAfter 60
  (tid, result) <- forkResult $ withAcquired context deadline retry
    (\request -> do
      putTMVar registered request
      when grantDuringEnqueue (void (tryGrant request (pure ()))))
    (const (readTVar anchor >>= check)) (const (pure ()))
    (\() -> putMVar entered () >> takeMVar release)
  request <- within (atomically (takeTMVar registered))
  if grantDuringEnqueue then pure () else do
    awaitBlocked tid
    atomically (tryGrant request (pure ())) >>= (`shouldBe` True)
  await entered
  pure (done, release, result)
