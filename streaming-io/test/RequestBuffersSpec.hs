{-# LANGUAGE DataKinds #-}
module RequestBuffersSpec (spec) where

import Control.Concurrent.Resource.Waiting qualified as Waiting
import Control.Concurrent (forkFinally, killThread, ThreadId, yield)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically)
import Control.Exception (try)
import Control.Monad (forM_, replicateM)
import GHC.Conc (ThreadStatus (..), BlockReason (..), threadStatus)
import Data.Either (isLeft)
import Data.Maybe (isNothing)
import Data.Tuple (Solo)
import Network.Streaming.Buffer (Buffer, bufferMemory)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources qualified as Resources
import Network.Streaming.Resources
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "shared request buffer capability" $ do
  it "requires an ordinary block in addition to protected reserve" $ do
    isLeft (validateSettings (settings 1)) `shouldBe` True
    isLeft (validateSettings (ResourceSettings (positiveInt @24) (positiveInt @8) (positiveInt @8) (positiveInt @3))) `shouldBe` True
    validateSettings (settings 2) `shouldBe` Right ()

  it "lends a local primary and one reserve block, rejecting a third overlap" $
    withTestResources (settings 2) $ \resources -> withRequestBuffers resources $ \access -> do
      primary <- borrowBuffer access
      extra <- borrowBuffer access
      bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)
      result <- timeout 1_000_000 (try @IOError (borrowBuffer access))
      fmap isLeft result `shouldBe` Just True
      releaseBuffer extra
      releaseBuffer primary

  it "reuses the primary locally and ignores stale releases" $
    withTestResources (settings 2) $ \resources -> withRequestBuffers resources $ \access -> do
      first <- borrowBuffer access
      releaseBuffer first
      second <- borrowBuffer access
      bufferMemory (loanBuffer second) `shouldBe` bufferMemory (loanBuffer first)
      releaseBuffer first
      extra <- borrowBuffer access
      bufferMemory (loanBuffer extra) `shouldNotBe` bufferMemory (loanBuffer second)
      releaseBuffer second
      releaseBuffer extra

  it "keeps reserve out of both primary and compatibility admission" $
    withTestResources (settings 2) $ \resources -> withRequestBuffers resources $ \access -> do
      isNothing <$> atomically (tryAcquire @(Solo Buffer) resources) `shouldReturn` True
      blocked <- timeout 20_000 (withRequestBuffers resources (const (pure ())))
      blocked `shouldBe` Nothing
      withBuffer access (const (pure ()))

  it "returns extra capacity promptly while the primary stays local" $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        extra <- borrowBuffer first
        releaseBuffer extra
        timeout 1_000_000 (withBuffer second (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "reuses a released primary while its extra waits for global inventory" $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        heldReserve <- borrowBuffer second
        pending <- newEmptyMVar
        _ <- forkFinally (borrowBuffer first) (putMVar pending)
        isNothing <$> timeout 20_000 (readMVar pending) `shouldReturn` True
        third <- timeout 1_000_000 (try @IOError (borrowBuffer first))
        fmap isLeft third `shouldBe` Just True
        releaseBuffer primary
        result <- timeout 1_000_000 (takeMVar pending)
        case result of
          Just (Right loan) -> do
            bufferMemory (loanBuffer loan) `shouldBe` bufferMemory (loanBuffer primary)
            releaseBuffer loan
          _ -> failTest "primary return did not wake its local waiter"
        releaseBuffer heldReserve
        releaseBuffer otherPrimary

  it "cancels a pending extra without consuming a later return" $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        heldReserve <- borrowBuffer second
        pending <- newEmptyMVar
        worker <- forkFinally (borrowBuffer first) (putMVar pending)
        isNothing <$> timeout 20_000 (readMVar pending) `shouldReturn` True
        killThread worker
        outcome <- timeout 1_000_000 (takeMVar pending)
        fmap isLeft outcome `shouldBe` Just True
        releaseBuffer heldReserve
        timeout 1_000_000 (withBuffer first (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "serves queued extras in arrival order without a new arrival stealing supply" $
    withTestResources (settings 4) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second ->
        withRequestBuffers resources $ \third -> do
          primary <- borrowBuffer first
          otherPrimary <- borrowBuffer second
          thirdPrimary <- borrowBuffer third
          heldReserve <- borrowBuffer third
          enteredFirst <- newEmptyMVar
          enteredSecond <- newEmptyMVar
          holdFirst <- newEmptyMVar
          holdSecond <- newEmptyMVar
          doneFirst <- newEmptyMVar
          doneSecond <- newEmptyMVar
          firstWorker <- forkFinally
            (withBuffer first (const $ putMVar enteredFirst () >> takeMVar holdFirst))
            (putMVar doneFirst)
          awaitBlocked firstWorker
          secondWorker <- forkFinally
            (withBuffer second (const $ putMVar enteredSecond () >> takeMVar holdSecond))
            (putMVar doneSecond)
          awaitBlocked secondWorker
          releaseBuffer heldReserve
          timeout 1_000_000 (takeMVar enteredFirst) `shouldReturn` Just ()
          isNothing <$> tryReadMVar enteredSecond `shouldReturn` True
          putMVar holdFirst ()
          timeout 1_000_000 (takeMVar enteredSecond) `shouldReturn` Just ()
          putMVar holdSecond ()
          fmap isLeft <$> timeout 1_000_000 (takeMVar doneFirst) `shouldReturn` Just False
          fmap isLeft <$> timeout 1_000_000 (takeMVar doneSecond) `shouldReturn` Just False
          releaseBuffer thirdPrimary
          releaseBuffer otherPrimary
          releaseBuffer primary

  it "delivers reserve to an extra behind many blocked primary arrivals" $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        heldReserve <- borrowBuffer second
        primaries <- replicateM 128 $ do
          done <- newEmptyMVar
          worker <- forkFinally (withRequestBuffers resources (const (pure ()))) (putMVar done)
          awaitBlocked worker
          pure (worker, done)
        extraDone <- newEmptyMVar
        extra <- forkFinally (borrowBuffer first) (putMVar extraDone)
        awaitBlocked extra
        releaseBuffer heldReserve
        outcome <- timeout 1_000_000 (takeMVar extraDone)
        case outcome of
          Just (Right loan) -> releaseBuffer loan
          _ -> failTest "blocked primary arrivals stranded a reserve grant"
        forM_ primaries $ \(worker, done) -> do
          killThread worker
          fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just True
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "returns exactly once across cancellation racing an extra grant" $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        forM_ [1 .. 100 :: Int] $ \_ -> do
          heldReserve <- borrowBuffer second
          pending <- newEmptyMVar
          hold <- newEmptyMVar
          worker <- forkFinally (withBuffer first (const $ takeMVar hold)) (putMVar pending)
          awaitBlocked worker
          releaseBuffer heldReserve
          killThread worker
          fmap isLeft <$> timeout 1_000_000 (takeMVar pending) `shouldReturn` Just True
          timeout 1_000_000 (withBuffer second (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "returns exactly once across cancellation racing local primary delivery" $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        otherPrimary <- borrowBuffer second
        heldReserve <- borrowBuffer second
        forM_ [1 .. 100 :: Int] $ \_ -> do
          primary <- borrowBuffer first
          pending <- newEmptyMVar
          hold <- newEmptyMVar
          worker <- forkFinally (withBuffer first (const $ takeMVar hold)) (putMVar pending)
          awaitBlocked worker
          releaseBuffer primary
          killThread worker
          fmap isLeft <$> timeout 1_000_000 (takeMVar pending) `shouldReturn` Just True
          timeout 1_000_000 (withBuffer first $ \buffer ->
            bufferMemory buffer `shouldBe` bufferMemory (loanBuffer primary)) `shouldReturn` Just ()
        releaseBuffer heldReserve
        releaseBuffer otherPrimary

  it "expires escaped capabilities and returns outstanding loans on teardown" $
    withTestResources (settings 2) $ \resources -> do
      (access, primary, extra) <- withRequestBuffers resources $ \access -> do
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        pure (access, primary, extra)
      rejected <- try @IOError (borrowBuffer access)
      isLeft rejected `shouldBe` True
      releaseBuffer primary
      releaseBuffer extra
      timeout 1_000_000 (withRequestBuffers resources $ \fresh -> do
        first <- borrowBuffer fresh
        second <- borrowBuffer fresh
        releaseBuffer second
        releaseBuffer first) `shouldReturn` Just ()

  it "waits for an active withBuffer callback before request teardown returns" $
    withTestResources (settings 2) $ \resources -> do
      entered <- newEmptyMVar
      hold <- newEmptyMVar
      childDone <- newEmptyMVar
      requestDone <- newEmptyMVar
      exiting <- newEmptyMVar
      request <- forkFinally
        (withRequestBuffers resources $ \access -> do
          _ <- forkFinally (withBuffer access (const $ putMVar entered () >> takeMVar hold)) (putMVar childDone)
          takeMVar entered >> putMVar exiting ())
        (putMVar requestDone)
      takeMVar exiting
      awaitBlocked request
      isNothing <$> timeout 20_000 (readMVar requestDone) `shouldReturn` True
      timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
      putMVar hold ()
      fmap isLeft <$> timeout 1_000_000 (takeMVar childDone) `shouldReturn` Just False
      fmap isLeft <$> timeout 1_000_000 (takeMVar requestDone) `shouldReturn` Just False

  it "keeps a callback's loan exclusive if cancellation interrupts request teardown" $
    withTestResources (settings 2) $ \resources -> do
      entered <- newEmptyMVar
      hold <- newEmptyMVar
      childDone <- newEmptyMVar
      requestDone <- newEmptyMVar
      exiting <- newEmptyMVar
      request <- forkFinally
        (withRequestBuffers resources $ \access -> do
          _ <- forkFinally (withBuffer access (const $ putMVar entered () >> takeMVar hold)) (putMVar childDone)
          takeMVar entered >> putMVar exiting ())
        (putMVar requestDone)
      takeMVar exiting
      awaitBlocked request
      killThread request
      fmap isLeft <$> timeout 1_000_000 (takeMVar requestDone) `shouldReturn` Just True
      timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
      putMVar hold ()
      fmap isLeft <$> timeout 1_000_000 (takeMVar childDone) `shouldReturn` Just False
      timeout 1_000_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()

  it "keeps an active extra callback exclusive after its request is cancelled" $
    withTestResources (settings 3) $ \resources -> withRequestBuffers resources $ \other -> do
      otherPrimary <- borrowBuffer other
      entered <- newEmptyMVar
      exiting <- newEmptyMVar
      hold <- newEmptyMVar
      childDone <- newEmptyMVar
      requestDone <- newEmptyMVar
      request <- forkFinally
        (withRequestBuffers resources $ \access -> do
          _ <- borrowBuffer access
          _ <- forkFinally (withBuffer access (const $ putMVar entered () >> takeMVar hold)) (putMVar childDone)
          takeMVar entered >> putMVar exiting ())
        (putMVar requestDone)
      takeMVar exiting
      awaitBlocked request
      killThread request
      fmap isLeft <$> timeout 1_000_000 (takeMVar requestDone) `shouldReturn` Just True
      -- The first request's idle primary is returned, but its running extra
      -- still leaves only the protected reserve free for new primary admission.
      timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
      putMVar hold ()
      fmap isLeft <$> timeout 1_000_000 (takeMVar childDone) `shouldReturn` Just False
      timeout 1_000_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()
      releaseBuffer otherPrimary

  it "rejects an extra waiter when its request closes" $
    withTestResources (settings 3) $ \resources -> withRequestBuffers resources $ \other -> do
      _ <- borrowBuffer other
      pending <- newEmptyMVar
      withRequestBuffers resources $ \access -> do
        _ <- borrowBuffer access
        _ <- borrowBuffer other
        _ <- forkFinally (borrowBuffer access) (putMVar pending)
        isNothing <$> timeout 20_000 (readMVar pending) `shouldReturn` True
      outcome <- timeout 1_000_000 (takeMVar pending)
      fmap isLeft outcome `shouldBe` Just True

  it "returns the full request after asynchronous cancellation" $
    withTestResources (settings 2) $ \resources -> do
      started <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (withRequestBuffers resources $ \access -> do
          _ <- borrowBuffer access
          _ <- borrowBuffer access
          putMVar started ()
          takeMVar hold)
        (putMVar done)
      takeMVar started
      killThread worker
      outcome <- timeout 1_000_000 (takeMVar done)
      fmap isLeft outcome `shouldBe` Just True
      timeout 1_000_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()

  it "rejects blocked primary and extra admission on resource shutdown" $ do
    primaryDone <- newEmptyMVar
    extraDone <- newEmptyMVar
    holder <- withTestResources (settings 3) $ \resources -> do
      accessReady <- newEmptyMVar
      hold <- newEmptyMVar
      worker <- forkFinally
        (withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second ->
          putMVar accessReady (first, second) >> takeMVar hold)
        (const (pure ()))
      (first, second) <- takeMVar accessReady
      _ <- borrowBuffer first
      _ <- borrowBuffer second
      _ <- borrowBuffer second
      _ <- forkFinally (withRequestBuffers resources (const (pure ()))) (putMVar primaryDone)
      _ <- forkFinally (borrowBuffer first) (putMVar extraDone)
      isNothing <$> timeout 20_000 (readMVar primaryDone) `shouldReturn` True
      isNothing <$> timeout 20_000 (readMVar extraDone) `shouldReturn` True
      pure worker
    fmap isLeft <$> timeout 1_000_000 (takeMVar primaryDone) `shouldReturn` Just True
    fmap isLeft <$> timeout 1_000_000 (takeMVar extraDone) `shouldReturn` Just True
    killThread holder

settings :: Int -> ResourceSettings
settings count = ResourceSettings
  (case count of 1 -> positiveInt @8; 2 -> positiveInt @16; 3 -> positiveInt @24; _ -> positiveInt @32)
  (positiveInt @8) (positiveInt @8) (positiveInt @1)

-- A blocked mailbox confirms that admission registration has committed.
awaitBlocked :: ThreadId -> IO ()
awaitBlocked worker = do
  outcome <- timeout 1_000_000 loop
  outcome `shouldBe` Just ()
  where
    loop = threadStatus worker >>= \status -> case status of
      ThreadBlocked BlockedOnSTM -> pure ()
      ThreadBlocked BlockedOnMVar -> pure ()
      ThreadFinished -> pure ()
      ThreadDied -> pure ()
      _ -> yield >> loop

withTestResources :: ResourceSettings -> (Resources.Resources -> IO a) -> IO a
withTestResources settings action = Waiting.withWaiting $ \waiting ->
  Resources.withResources waiting settings action
