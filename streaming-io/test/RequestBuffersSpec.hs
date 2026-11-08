{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
module RequestBuffersSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent (forkFinally, killThread, ThreadId, yield)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically, orElse)
import Control.Exception (bracket, try)
import Control.Monad (forM_, replicateM)
import GHC.Conc (ThreadStatus (..), BlockReason (..), threadStatus)
import Data.Either (isLeft)
import Data.Maybe (isNothing)
import Network.Streaming.Buffer (Buffer, bufferMemory)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import Network.Streaming.Watch (Watch (..))
import Skeletest
import System.IO.Error (ioeGetErrorString)
import System.Timeout (timeout)

spec :: Spec
spec = describe "shared request buffer capability" $ do
  it "lends a local primary and one reserve block, rejecting a third overlap" $
    withTestPool (settings 2) $ \pool -> withRequestBuffers pool $ \access -> do
      primary <- borrowBuffer access
      extra <- borrowBuffer access
      bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)
      result <- timeout 1_000_000 (try @IOError (borrowBuffer access))
      fmap isLeft result `shouldBe` Just True
      releaseBuffer extra
      releaseBuffer primary

  it "tells the primary loan from an extra one" $
    withTestPool (settings 2) $ \pool -> withRequestBuffers pool $ \access -> do
      primary <- borrowBuffer access
      extra <- borrowBuffer access
      isPrimaryLoan primary `shouldBe` True
      isPrimaryLoan extra `shouldBe` False
      releaseBuffer extra
      releaseBuffer primary
      -- the primary is lent again when it is free, and it is still the primary
      again <- borrowBuffer access
      isPrimaryLoan again `shouldBe` True
      releaseBuffer again

  it "reuses the primary locally and ignores stale releases" $
    withTestPool (settings 2) $ \pool -> withRequestBuffers pool $ \access -> do
      first <- borrowBuffer access
      releaseBuffer first
      second <- borrowBuffer access
      bufferMemory (loanBuffer second) `shouldBe` bufferMemory (loanBuffer first)
      releaseBuffer first
      extra <- borrowBuffer access
      bufferMemory (loanBuffer extra) `shouldNotBe` bufferMemory (loanBuffer second)
      releaseBuffer second
      releaseBuffer extra

  it "keeps reserve out of primary admission" $
    withTestPool (settings 2) $ \pool -> withRequestBuffers pool $ \access -> do
      blocked <- timeout 20_000 (withRequestBuffers pool (const (pure ())))
      blocked `shouldBe` Nothing
      withLoan access (const (pure ()))

  it "preserves a multi-block reserve across immediate request and extra loans" $
    withTestPool ((settings 3) {reserveBufferCount = positiveInt @2}) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
        firstPrimary <- borrowBuffer first
        secondPrimary <- borrowBuffer second
        firstExtra <- borrowBuffer first
        secondExtra <- borrowBuffer second
        let memories = map (bufferMemory . loanBuffer) [firstPrimary, secondPrimary, firstExtra, secondExtra]
        forM_ (zip [0 :: Int ..] memories) $ \(index, memory) ->
          forM_ (drop (index + 1) memories) (memory `shouldNotBe`)
        releaseBuffer firstExtra
        releaseBuffer secondExtra
        timeout 20_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Nothing
        timeout 1_000_000 (withLoan first (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer firstPrimary
        releaseBuffer secondPrimary

  it "returns extra capacity promptly while the primary stays local" $
    withTestPool (settings 3) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        extra <- borrowBuffer first
        releaseBuffer extra
        timeout 1_000_000 (withLoan second (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary

  it "reuses a released primary while its extra waits for global inventory" $
    withTestPool (settings 3) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
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
    withTestPool (settings 3) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
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

  it "serves queued extras in arrival order without a new arrival stealing supply" $
    withTestPool (settings 4) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second ->
        withRequestBuffers pool $ \third -> do
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

  it "delivers reserve to an extra behind many blocked primary arrivals" $
    withTestPool (settings 3) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        heldReserve <- borrowBuffer second
        primaries <- replicateM 128 $ do
          done <- newEmptyMVar
          worker <- forkFinally (withRequestBuffers pool (const (pure ()))) (putMVar done)
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
    withTestPool (settings 3) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
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

  it "returns exactly once across cancellation racing local primary delivery" $
    withTestPool (settings 3) $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
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

  it "expires escaped capabilities and returns outstanding loans on teardown" $
    withTestPool (settings 2) $ \pool -> do
      (access, primary, extra) <- withRequestBuffers pool $ \access -> do
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        pure (access, primary, extra)
      rejected <- try @IOError (borrowBuffer access)
      isLeft rejected `shouldBe` True
      timeout 1_000_000 (withRequestBuffers pool $ \fresh -> do
        first <- borrowBuffer fresh
        second <- borrowBuffer fresh
        releaseBuffer primary
        releaseBuffer extra
        timeout 20_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Nothing
        releaseBuffer second
        releaseBuffer first) `shouldReturn` Just ()

  it "lets the caller join children before returning their shared request scope" $
    withTestPool (settings 2) $ \pool -> do
      withRequestBuffers pool $ \access -> do
        entered <- newEmptyMVar
        hold <- newEmptyMVar
        done <- newEmptyMVar
        _ <- forkFinally
          (withLoan access (const $ putMVar entered () >> takeMVar hold))
          (putMVar done)
        takeMVar entered
        timeout 20_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Nothing
        putMVar hold ()
        fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just False
      timeout 1_000_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Just ()

  it "lets the caller cancel and join a child before request teardown" $
    withTestPool (settings 2) $ \pool -> do
      withRequestBuffers pool $ \access -> do
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
      timeout 1_000_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Just ()

  it "rejects an extra waiter when its request closes" $
    withTestPool (settings 3) $ \pool -> withRequestBuffers pool $ \other -> do
      _ <- borrowBuffer other
      pending <- newEmptyMVar
      withRequestBuffers pool $ \access -> do
        _ <- borrowBuffer access
        _ <- borrowBuffer other
        _ <- forkFinally (borrowBuffer access) (putMVar pending)
        isNothing <$> timeout 20_000 (readMVar pending) `shouldReturn` True
      outcome <- timeout 1_000_000 (takeMVar pending)
      fmap isLeft outcome `shouldBe` Just True

  it "returns both owned loans when the request callback throws" $
    withTestPool (settings 2) $ \pool -> do
      outcome <- try @IOError $ withRequestBuffers pool $ \access -> do
        _ <- borrowBuffer access
        _ <- borrowBuffer access
        ioError (userError "callback")
      isLeft outcome `shouldBe` True
      timeout 1_000_000 (withRequestBuffers pool $ \access -> do
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)) `shouldReturn` Just ()

  it "returns the full request after asynchronous cancellation" $
    withTestPool (settings 2) $ \pool -> do
      started <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (withRequestBuffers pool $ \access -> do
          _ <- borrowBuffer access
          _ <- borrowBuffer access
          putMVar started ()
          takeMVar hold)
        (putMVar done)
      takeMVar started
      killThread worker
      outcome <- timeout 1_000_000 (takeMVar done)
      fmap isLeft outcome `shouldBe` Just True
      timeout 1_000_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Just ()

  it "lets the caller cancel queued primary and extra admission and reuse the inventory" $
    withTestPool (settings 3) $ \pool -> do
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        heldReserve <- borrowBuffer second
        primaryDone <- newEmptyMVar
        extraDone <- newEmptyMVar
        queuedPrimary <- forkFinally (withRequestBuffers pool (const (pure ()))) (putMVar primaryDone)
        queuedExtra <- forkFinally (borrowBuffer first) (putMVar extraDone)
        awaitBlocked queuedPrimary
        awaitBlocked queuedExtra
        killThread queuedPrimary
        killThread queuedExtra
        fmap isLeft <$> timeout 1_000_000 (takeMVar primaryDone) `shouldReturn` Just True
        fmap isLeft <$> timeout 1_000_000 (takeMVar extraDone) `shouldReturn` Just True
        releaseBuffer heldReserve
        timeout 1_000_000 (withLoan first (const (pure ()))) `shouldReturn` Just ()
        releaseBuffer otherPrimary
        releaseBuffer primary
      timeout 1_000_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Just ()

  describe "non-blocking borrows" $ do
    it "tries the free primary, then an extra block while inventory has one and nobody waits" $
      withTestPool (settings 2) $ \pool -> withRequestBuffers pool $ \access -> do
        primary <- tryBorrowBuffer access >>= expectLoan
        extra <- tryBorrowBuffer access >>= expectLoan
        bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)
        rejects (tryBorrowBuffer access)
        releaseBuffer primary
        again <- tryBorrowBuffer access >>= expectLoan
        bufferMemory (loanBuffer again) `shouldBe` bufferMemory (loanBuffer primary)
        releaseBuffer again
        releaseBuffer extra

    it "reports no loan and registers nothing while inventory is empty" $
      withDrained $ \(Drained first _ _ held) -> do
        isNothing <$> tryBorrowBuffer first `shouldReturn` True
        isNothing <$> tryBorrowBuffer first `shouldReturn` True
        releaseBuffer held
        -- A registration left behind would have taken the returned block.
        _ <- tryBorrowBuffer first >>= expectLoan
        pure ()

    it "does not let a try overtake an earlier borrower in the FIFO" $
      withTestPool (settings 3) $ \pool -> withRequestBuffers pool $ \first -> do
        _ <- borrowBuffer first
        entered <- newEmptyMVar
        hold <- newEmptyMVar
        done <- newEmptyMVar
        arrival <- withRequestBuffers pool $ \second -> do
          _ <- borrowBuffer second
          extra <- borrowBuffer second
          -- A request that needs two free blocks waits before the one that is freed.
          arrival <- forkFinally
            (withRequestBuffers pool (const (putMVar entered () >> takeMVar hold)))
            (putMVar done)
          awaitBlocked arrival
          releaseBuffer extra
          isNothing <$> tryBorrowBuffer first `shouldReturn` True
          pure arrival
        -- The earlier borrower took the returned blocks first; the reserve is left.
        timeout 1_000_000 (takeMVar entered) `shouldReturn` Just ()
        extra <- tryBorrowBuffer first >>= expectLoan
        releaseBuffer extra
        putMVar hold ()
        fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just False
        killThread arrival

    it "fires a watch when the primary is released, and the next try claims that loan" $
      withDrained $ \(Drained first _ primary _) -> do
        watch <- watchBorrow first
        isReady watch `shouldReturn` False
        -- The registration is the pending extra: a third loan is refused.
        rejects (tryBorrowBuffer first)
        releaseBuffer primary
        awaitReady watch
        claimed <- tryBorrowBuffer first >>= expectLoan
        bufferMemory (loanBuffer claimed) `shouldBe` bufferMemory (loanBuffer primary)
        releaseBuffer claimed

    it "fires a watch when global inventory frees a block, and the next try claims it" $
      withDrained $ \(Drained first _ primary held) -> do
        watch <- watchBorrow first
        isReady watch `shouldReturn` False
        releaseBuffer held
        awaitReady watch
        claimed <- tryBorrowBuffer first >>= expectLoan
        bufferMemory (loanBuffer claimed) `shouldNotBe` bufferMemory (loanBuffer primary)
        -- An extra loan returns to the pool as before.
        releaseBuffer claimed
        _ <- tryBorrowBuffer first >>= expectLoan
        pure ()

    it "serves a borrow waiter before a watch registered after it" $
      withDrained $ \(Drained first second _ held) -> do
        done <- newEmptyMVar
        waiter <- forkFinally (borrowBuffer first) (putMVar done)
        awaitBlocked waiter
        watch <- watchBorrow second
        releaseBuffer held
        loan <- timeout 1_000_000 (takeMVar done) >>= \case
          Just (Right loan) -> pure loan
          _ -> failTest "the earlier waiter was not served first"
        isReady watch `shouldReturn` False
        releaseBuffer loan
        awaitReady watch
        claimed <- tryBorrowBuffer second >>= expectLoan
        releaseBuffer claimed

    it "serves a watch before a borrow waiter registered after it" $
      withDrained $ \(Drained first second _ held) -> do
        watch <- watchBorrow first
        done <- newEmptyMVar
        waiter <- forkFinally (borrowBuffer second) (putMVar done)
        awaitBlocked waiter
        releaseBuffer held
        awaitReady watch
        isNothing <$> tryReadMVar done `shouldReturn` True
        claimed <- tryBorrowBuffer first >>= expectLoan
        releaseBuffer claimed
        outcome <- timeout 1_000_000 (takeMVar done)
        case outcome of
          Just (Right loan) -> releaseBuffer loan
          _ -> failTest "the waiter behind the watch was not served"

    it "returns a fired but unclaimed watch's loan to the next waiter on cancel" $
      withDrained $ \(Drained first second _ held) -> do
        watch <- watchBorrow first
        done <- newEmptyMVar
        waiter <- forkFinally (borrowBuffer second) (putMVar done)
        awaitBlocked waiter
        releaseBuffer held
        awaitReady watch
        isNothing <$> tryReadMVar done `shouldReturn` True
        watchCancel watch
        outcome <- timeout 1_000_000 (takeMVar done)
        case outcome of
          Just (Right loan) -> releaseBuffer loan
          _ -> failTest "the cancelled grant did not reach the next waiter"

    it "returns a fired but unclaimed watch's loan to the inventory on cancel" $
      withDrained $ \(Drained first second _ held) -> do
        watch <- watchBorrow first
        releaseBuffer held
        awaitReady watch
        isNothing <$> tryBorrowBuffer second `shouldReturn` True
        watchCancel watch
        loan <- tryBorrowBuffer second >>= expectLoan
        releaseBuffer loan
        -- The first access has its extra slot back.
        _ <- tryBorrowBuffer first >>= expectLoan
        pure ()

    it "withdraws a pending watch on cancel without consuming a later return" $
      withDrained $ \(Drained first _ _ held) -> do
        watch <- watchBorrow first
        watchCancel watch
        releaseBuffer held
        _ <- tryBorrowBuffer first >>= expectLoan
        pure ()

    it "refuses a third overlapping loan promptly, to a try and to a watch" $ do
      withTestPool (settings 2) $ \pool -> withRequestBuffers pool $ \access -> do
        _ <- borrowBuffer access
        _ <- borrowBuffer access
        rejects (tryBorrowBuffer access)
        rejects (watchBorrow access)
      withDrained $ \(Drained first _ _ held) -> do
        watch <- watchBorrow first
        rejects (tryBorrowBuffer first)
        rejects (watchBorrow first)
        rejects (borrowBuffer first)
        watchCancel watch
        -- The cancelled registration freed the slot.
        isNothing <$> tryBorrowBuffer first `shouldReturn` True
        releaseBuffer held
        watch' <- watchBorrow first
        -- A granted loan still counts until it is claimed.
        rejects (watchBorrow first)
        claimed <- tryBorrowBuffer first >>= expectLoan
        rejects (tryBorrowBuffer first)
        rejects (watchBorrow first)
        releaseBuffer claimed
        watchCancel watch'

    it "grants a loan that is available now at once and holds it for the next try" $
      withTestPool (settings 3) $ \pool ->
        withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
          primary <- watchBorrow first
          isReady primary `shouldReturn` True
          rejects (watchBorrow first)
          claimedPrimary <- tryBorrowBuffer first >>= expectLoan
          _ <- borrowBuffer second
          extra <- watchBorrow first
          isReady extra `shouldReturn` True
          -- Nothing is left for another request to take in between.
          isNothing <$> tryBorrowBuffer second `shouldReturn` True
          claimedExtra <- tryBorrowBuffer first >>= expectLoan
          bufferMemory (loanBuffer claimedExtra) `shouldNotBe` bufferMemory (loanBuffer claimedPrimary)
          releaseBuffer claimedExtra
          releaseBuffer claimedPrimary
          -- An unclaimed hold returns on cancel.
          held <- watchBorrow first
          watchCancel held
          reborrowed <- borrowBuffer first
          bufferMemory (loanBuffer reborrowed) `shouldBe` bufferMemory (loanBuffer claimedPrimary)
          releaseBuffer reborrowed

    it "fires a failed registration on expiry, and the next try fails like a borrow" $
      withTestPool (settings 4) $ \pool ->
        withRequestBuffers pool $ \second -> withRequestBuffers pool $ \third -> do
          _ <- borrowBuffer second
          _ <- borrowBuffer third
          (first, watch) <- withRequestBuffers pool $ \first -> do
            _ <- borrowBuffer first
            _ <- borrowBuffer third
            watch <- watchBorrow first
            isReady watch `shouldReturn` False
            pure (first, watch)
          awaitReady watch
          expired <- try @IOError (tryBorrowBuffer first)
          borrowed <- try @IOError (borrowBuffer first)
          fmap ioeGetErrorString (either Just (const Nothing) expired)
            `shouldBe` Just "request buffer access is closed"
          fmap ioeGetErrorString (either Just (const Nothing) borrowed)
            `shouldBe` fmap ioeGetErrorString (either Just (const Nothing) expired)
          rejects (watchBorrow first)
          watchCancel watch
          watchCancel watch
          -- The failed registration left the FIFO, and the primary came back.
          _ <- tryBorrowBuffer second >>= expectLoan
          pure ()

    it "fires a granted registration on expiry and returns its loan" $
      withTestPool (settings 4) $ \pool ->
        withRequestBuffers pool $ \second -> withRequestBuffers pool $ \third -> do
          _ <- borrowBuffer second
          _ <- borrowBuffer third
          (first, watch) <- withRequestBuffers pool $ \first -> do
            _ <- borrowBuffer first
            held <- borrowBuffer third
            watch <- watchBorrow first
            releaseBuffer held
            awaitReady watch
            pure (first, watch)
          isReady watch `shouldReturn` True
          rejects (tryBorrowBuffer first)
          watchCancel watch
          -- Both of the expired request's blocks are available again.
          _ <- tryBorrowBuffer second >>= expectLoan
          _ <- tryBorrowBuffer third >>= expectLoan
          pure ()

    it "cancels idempotently, and a stale cancel leaves a later registration alone" $
      withDrained $ \(Drained first _ _ held) -> do
        watch <- watchBorrow first
        watchCancel watch
        watchCancel watch
        later <- watchBorrow first
        watchCancel watch
        -- The registration is still there, and still occupies the extra slot.
        rejects (watchBorrow first)
        isReady later `shouldReturn` False
        releaseBuffer held
        isReady later `shouldReturn` True
        watchCancel later
        watchCancel later
        _ <- tryBorrowBuffer first >>= expectLoan
        pure ()

    it "does nothing when cancelling after a claim" $
      withDrained $ \(Drained first second _ held) -> do
        watch <- watchBorrow first
        releaseBuffer held
        claimed <- tryBorrowBuffer first >>= expectLoan
        watchCancel watch
        -- The claimed loan was not returned.
        isNothing <$> tryBorrowBuffer second `shouldReturn` True
        releaseBuffer claimed
        taken <- tryBorrowBuffer second >>= expectLoan
        later <- watchBorrow first
        watchCancel watch
        rejects (watchBorrow first)
        isReady later `shouldReturn` False
        releaseBuffer taken
        isReady later `shouldReturn` True
        watchCancel later

settings :: Int -> PoolSettings
settings count = PoolSettings
  { bufferSize = positiveInt @8
  , ordinaryBufferCount = case count of 2 -> positiveInt @1; 3 -> positiveInt @2; _ -> positiveInt @3
  , reserveBufferCount = positiveInt @1
  , setupBufferCount = positiveInt @1
  }

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

withTestPool :: PoolSettings -> (BufferPool -> IO a) -> IO a
withTestPool config action = getAcquisition >>= \waiting ->
  newBufferPool waiting config >>= action

withLoan :: BufferAccess -> (Buffer -> IO a) -> IO a
withLoan access action = bracket (borrowBuffer access) releaseBuffer (action . loanBuffer)

-- Three requests in a pool of four blocks. Each holds its primary, and the
-- third also holds the one block left, so global inventory is empty. The
-- fields are the first and second request, the first's primary and the held
-- extra block.
data Drained = Drained BufferAccess BufferAccess BufferLoan BufferLoan

withDrained :: (Drained -> IO a) -> IO a
withDrained action = withTestPool (settings 4) $ \pool ->
  withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> withRequestBuffers pool $ \third -> do
    firstPrimary <- borrowBuffer first
    _ <- borrowBuffer second
    _ <- borrowBuffer third
    held <- borrowBuffer third
    action (Drained first second firstPrimary held)

expectLoan :: Maybe BufferLoan -> IO BufferLoan
expectLoan = maybe (failTest "expected a loan") pure

isReady :: Watch -> IO Bool
isReady watch = atomically ((True <$ watchReady watch) `orElse` pure False)

awaitReady :: Watch -> IO ()
awaitReady watch = timeout 1_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()

-- Fails promptly with an error instead of waiting.
rejects :: IO a -> IO ()
rejects action = fmap isLeft <$> timeout 1_000_000 (try @IOError action) `shouldReturn` Just True
