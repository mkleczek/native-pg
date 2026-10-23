{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
module RequestBuffersSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent (forkFinally, killThread, ThreadId, yield)
import Control.Concurrent.MVar
import Control.Exception (bracket, try)
import Control.Monad (forM_, replicateM)
import GHC.Conc (ThreadStatus (..), BlockReason (..), threadStatus)
import Data.Either (isLeft)
import Data.Maybe (isNothing)
import Network.Streaming.Buffer (Buffer, bufferMemory)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources qualified as Resources
import Network.Streaming.Resources
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = xfail "Request capability scaffold: scoped loans follow in the next change" $ describe "shared request buffer capability" $ do
  it "lends a local primary and one reserve block, rejecting a third overlap" $ bounded $
    withTestResources (settings 2) $ \resources -> withRequestBuffers resources $ \access -> do
      primary <- borrowBuffer access
      extra <- borrowBuffer access
      bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)
      result <- timeout 1_000_000 (try @IOError (borrowBuffer access))
      fmap isLeft result `shouldBe` Just True
      releaseBuffer extra
      releaseBuffer primary

  it "reuses the primary locally and ignores stale releases" $ bounded $
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

  it "keeps reserve out of primary admission" $ bounded $
    withTestResources (settings 2) $ \resources -> withRequestBuffers resources $ \access -> do
      blocked <- timeout 20_000 (withRequestBuffers resources (const (pure ())))
      blocked `shouldBe` Nothing
      withLoan access (const (pure ()))

  it "preserves a multi-block reserve across immediate request and extra loans" $ bounded $
    withTestResources (ResourceSettings (positiveInt @32) (positiveInt @8) (positiveInt @8) (positiveInt @2)) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        firstPrimary <- borrowBuffer first
        secondPrimary <- borrowBuffer second
        firstExtra <- borrowBuffer first
        secondExtra <- borrowBuffer second
        let memories = map (bufferMemory . loanBuffer) [firstPrimary, secondPrimary, firstExtra, secondExtra]
        forM_ (zip [0 :: Int ..] memories) $ \(index, memory) ->
          forM_ (drop (index + 1) memories) (memory `shouldNotBe`)
        releaseBuffer firstExtra
        releaseBuffer secondExtra
        timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
        timeout 1_000_000 (withLoan first (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer firstPrimary
        releaseBuffer secondPrimary

  it "returns extra capacity promptly while the primary stays local" $ bounded $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        extra <- borrowBuffer first
        releaseBuffer extra
        timeout 1_000_000 (withLoan second (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "reuses a released primary while its extra waits for global inventory" $ bounded $
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

  it "cancels a pending extra without consuming a later return" $ bounded $
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
        timeout 1_000_000 (withLoan first (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "serves queued extras in arrival order without a new arrival stealing supply" $ bounded $
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
            (withLoan first (const $ putMVar enteredFirst () >> takeMVar holdFirst))
            (putMVar doneFirst)
          awaitBlocked firstWorker
          secondWorker <- forkFinally
            (withLoan second (const $ putMVar enteredSecond () >> takeMVar holdSecond))
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

  it "delivers reserve to an extra behind many blocked primary arrivals" $ bounded $
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

  it "returns exactly once across cancellation racing an extra grant" $ bounded $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        forM_ [1 .. 100 :: Int] $ \_ -> do
          heldReserve <- borrowBuffer second
          pending <- newEmptyMVar
          hold <- newEmptyMVar
          worker <- forkFinally (withLoan first (const $ takeMVar hold)) (putMVar pending)
          awaitBlocked worker
          releaseBuffer heldReserve
          killThread worker
          fmap isLeft <$> timeout 1_000_000 (takeMVar pending) `shouldReturn` Just True
          timeout 1_000_000 (withLoan second (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "returns exactly once across cancellation racing local primary delivery" $ bounded $
    withTestResources (settings 3) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        otherPrimary <- borrowBuffer second
        heldReserve <- borrowBuffer second
        forM_ [1 .. 100 :: Int] $ \_ -> do
          primary <- borrowBuffer first
          pending <- newEmptyMVar
          hold <- newEmptyMVar
          worker <- forkFinally (withLoan first (const $ takeMVar hold)) (putMVar pending)
          awaitBlocked worker
          releaseBuffer primary
          killThread worker
          fmap isLeft <$> timeout 1_000_000 (takeMVar pending) `shouldReturn` Just True
          timeout 1_000_000 (withLoan first $ \buffer ->
            bufferMemory buffer `shouldBe` bufferMemory (loanBuffer primary)) `shouldReturn` Just ()
        releaseBuffer heldReserve
        releaseBuffer otherPrimary

  it "expires escaped capabilities and returns outstanding loans on teardown" $ bounded $
    withTestResources (settings 2) $ \resources -> do
      (access, primary, extra) <- withRequestBuffers resources $ \access -> do
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        pure (access, primary, extra)
      rejected <- try @IOError (borrowBuffer access)
      isLeft rejected `shouldBe` True
      timeout 1_000_000 (withRequestBuffers resources $ \fresh -> do
        first <- borrowBuffer fresh
        second <- borrowBuffer fresh
        releaseBuffer primary
        releaseBuffer extra
        timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
        releaseBuffer second
        releaseBuffer first) `shouldReturn` Just ()

  it "lets the caller join children before returning their shared request scope" $ bounded $
    withTestResources (settings 2) $ \resources -> do
      withRequestBuffers resources $ \access -> do
        entered <- newEmptyMVar
        hold <- newEmptyMVar
        done <- newEmptyMVar
        _ <- forkFinally
          (withLoan access (const $ putMVar entered () >> takeMVar hold))
          (putMVar done)
        takeMVar entered
        timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
        putMVar hold ()
        fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just False
      timeout 1_000_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()

  it "lets the caller cancel and join a child before request teardown" $ bounded $
    withTestResources (settings 2) $ \resources -> do
      withRequestBuffers resources $ \access -> do
        entered <- newEmptyMVar
        hold <- newEmptyMVar
        done <- newEmptyMVar
        child <- forkFinally
          (withLoan access (const $ putMVar entered () >> takeMVar hold))
          (putMVar done)
        takeMVar entered
        killThread child
        fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just True
        first <- borrowBuffer access
        second <- borrowBuffer access
        bufferMemory (loanBuffer first) `shouldNotBe` bufferMemory (loanBuffer second)
        releaseBuffer second
        releaseBuffer first
      timeout 1_000_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()

  it "rejects an extra waiter when its request closes" $ bounded $
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

  it "returns both owned loans when the request callback throws" $ bounded $
    withTestResources (settings 2) $ \resources -> do
      outcome <- try @IOError $ withRequestBuffers resources $ \access -> do
        _ <- borrowBuffer access
        _ <- borrowBuffer access
        ioError (userError "callback")
      isLeft outcome `shouldBe` True
      timeout 1_000_000 (withRequestBuffers resources $ \access -> do
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)) `shouldReturn` Just ()

  it "returns the full request after asynchronous cancellation" $ bounded $
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


settings :: Int -> ResourceSettings
settings count = ResourceSettings
  (case count of 1 -> positiveInt @8; 2 -> positiveInt @16; 3 -> positiveInt @24; _ -> positiveInt @32)
  (positiveInt @8) (positiveInt @8) (positiveInt @1)

-- Waiting on private delivery state confirms that admission registration has committed.
awaitBlocked :: ThreadId -> IO ()
awaitBlocked worker = do
  outcome <- timeout 1_000_000 loop
  outcome `shouldBe` Just ()
  where
    loop = threadStatus worker >>= \case
      ThreadBlocked BlockedOnSTM -> pure ()
      ThreadBlocked BlockedOnMVar -> pure ()
      ThreadFinished -> pure ()
      ThreadDied -> pure ()
      _ -> yield >> loop

withTestResources :: ResourceSettings -> (Resources.Resources -> IO a) -> IO a
withTestResources config action = getAcquisition >>= \waiting ->
  Resources.withResources waiting config action

withLoan :: BufferAccess -> (Buffer -> IO a) -> IO a
withLoan access action = bracket (borrowBuffer access) releaseBuffer (action . loanBuffer)

bounded :: IO () -> IO ()
bounded action = timeout 2000000 action >>= maybe (failTest "pending request implementation") pure
