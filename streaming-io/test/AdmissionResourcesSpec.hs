{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
module AdmissionResourcesSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent (forkFinally, killThread, yield, ThreadId)
import Control.Concurrent.MVar
import Control.Exception (bracket, finally, try)
import Control.Monad (forM_)
import Data.Either (isLeft)
import Data.Maybe (isNothing)
import Data.Tuple (Solo)
import GHC.Conc (ThreadStatus (..), BlockReason (..), threadStatus)
import Network.Streaming.Buffer (Buffer, bufferCapacity)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "buffer-owned scheduling" $ do
    it "detaches immediate grants before serving a same-size FIFO" $
      withTestPool $ \pool -> do
        timeout 1_000_000 (withSetupBuffers @(Buffer, Buffer) pool (pure . capacities)) `shouldReturn` Just (8, 8)
        timeout 1_000_000 (withSetupBuffers @(Buffer, Buffer) pool (pure . capacities)) `shouldReturn` Just (8, 8)
        (_, releaseHeld) <- holdSetup @(Buffer, Buffer) pool
        firstEntered <- newEmptyMVar
        releaseFirst <- newEmptyMVar
        firstDone <- newEmptyMVar
        first <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (const (putMVar firstEntered () >> takeMVar releaseFirst)))
          (putMVar firstDone)
        awaitBlocked first
        secondEntered <- newEmptyMVar
        secondDone <- newEmptyMVar
        second <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (const (putMVar secondEntered ())))
          (putMVar secondDone)
        (do
          awaitBlocked second
          releaseHeld
          timeout 20_000 (withSetupBuffers @(Buffer, Buffer) pool (const (pure ()))) `shouldReturn` Nothing
          timeout 1_000_000 (takeMVar firstEntered) `shouldReturn` Just ()
          isNothing <$> tryReadMVar secondEntered `shouldReturn` True
          putMVar releaseFirst ()
          timeout 1_000_000 (takeMVar secondEntered) `shouldReturn` Just ()
          fmap isLeft <$> timeout 1_000_000 (takeMVar firstDone) `shouldReturn` Just False
          fmap isLeft <$> timeout 1_000_000 (takeMVar secondDone) `shouldReturn` Just False)
          `finally` (killThread first >> killThread second >> releaseHeld)

    it "delivers returned blocks to a queued ordinary grant" $
      withTestPool $ \pool -> do
        (_, releaseHeld) <- holdSetup @(Buffer, Buffer) pool
        done <- newEmptyMVar
        worker <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (pure . capacities))
          (putMVar done)
        awaitBlocked worker
        releaseHeld
        result <- timeout 1_000_000 (takeMVar done)
        case result of
          Just (Right outcome) -> outcome `shouldBe` (8, 8)
          _ -> failTest "buffer queue did not receive returned storage"
        withSetupBuffers @(Buffer, Buffer) pool (pure . capacities) `shouldReturn` (8, 8)

    it "selects the first eligible arrival while preserving FIFO" $
      withTestPool $ \pool -> do
        (_, releaseHeld) <- holdSetup @(Solo Buffer) pool
        largeEntered <- newEmptyMVar
        smallEntered <- newEmptyMVar
        holdSmall <- newEmptyMVar
        largeDone <- newEmptyMVar
        smallDone <- newEmptyMVar
        large <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (const (putMVar largeEntered ())))
          (putMVar largeDone)
        awaitBlocked large
        _ <- forkFinally
          (withSetupBuffers @(Solo Buffer) pool (const (putMVar smallEntered () >> takeMVar holdSmall)))
          (putMVar smallDone)
        timeout 1_000_000 (takeMVar smallEntered) `shouldReturn` Just ()
        isNothing <$> tryReadMVar largeEntered `shouldReturn` True
        timeout 20_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Nothing
        putMVar holdSmall ()
        timeout 1_000_000 (takeMVar smallDone) >>= maybe (failTest "small grant stuck") (either (failTest . show) pure)
        isNothing <$> tryReadMVar largeEntered `shouldReturn` True
        releaseHeld
        timeout 1_000_000 (takeMVar largeEntered) `shouldReturn` Just ()
        timeout 1_000_000 (takeMVar largeDone) >>= maybe (failTest "large grant stuck") (either (failTest . show) pure)

    it "chooses the older fitting head across different grant sizes" $
      withTestPool $ \pool -> do
        (_, releaseHeld) <- holdSetup @(Buffer, Buffer) pool
        largeEntered <- newEmptyMVar
        smallEntered <- newEmptyMVar
        releaseLarge <- newEmptyMVar
        largeDone <- newEmptyMVar
        smallDone <- newEmptyMVar
        large <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (const (putMVar largeEntered () >> takeMVar releaseLarge)))
          (putMVar largeDone)
        awaitBlocked large
        small <- forkFinally
          (withSetupBuffers @(Solo Buffer) pool (const (putMVar smallEntered ())))
          (putMVar smallDone)
        awaitBlocked small
        releaseHeld
        timeout 1_000_000 (takeMVar largeEntered) `shouldReturn` Just ()
        isNothing <$> tryReadMVar smallEntered `shouldReturn` True
        putMVar releaseLarge ()
        timeout 1_000_000 (takeMVar smallEntered) `shouldReturn` Just ()
        fmap isLeft <$> timeout 1_000_000 (takeMVar largeDone) `shouldReturn` Just False
        fmap isLeft <$> timeout 1_000_000 (takeMVar smallDone) `shouldReturn` Just False

    it "removes a cancelled head before delivering the next ordinary grant" $
      withTestPool $ \pool -> do
        (_, releaseHeld) <- holdSetup @(Buffer, Buffer) pool
        firstDone <- newEmptyMVar
        secondDone <- newEmptyMVar
        first <- forkFinally (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) (putMVar firstDone)
        awaitBlocked first
        second <- forkFinally (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) (putMVar secondDone)
        awaitBlocked second
        killThread first
        fmap isLeft <$> timeout 1_000_000 (takeMVar firstDone) `shouldReturn` Just True
        releaseHeld
        fmap isLeft <$> timeout 1_000_000 (takeMVar secondDone) `shouldReturn` Just False

    it "keeps setup storage independent of a blocked stream queue" $
      withTestPool $ \pool -> withRequestBuffers pool $ \_ -> withRequestBuffers pool $ \_ -> do
        done <- newEmptyMVar
        worker <- forkFinally (withRequestBuffers pool (const (pure ()))) (putMVar done)
        awaitBlocked worker
        timeout 1_000_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Just ()
        killThread worker
        fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just True

    it "preserves setup FIFO and returns supply after a queued waiter is cancelled" $
      withTestPool $ \pool -> do
        (_, releaseHeld) <- holdSetup @(Buffer, Buffer) pool
        firstEntered <- newEmptyMVar
        holdFirst <- newEmptyMVar
        firstDone <- newEmptyMVar
        cancelledDone <- newEmptyMVar
        lateDone <- newEmptyMVar
        first <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (const (putMVar firstEntered () >> takeMVar holdFirst)))
          (putMVar firstDone)
        awaitBlocked first
        cancelled <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (const (pure ())))
          (putMVar cancelledDone)
        awaitBlocked cancelled
        killThread cancelled
        fmap isLeft <$> timeout 1_000_000 (takeMVar cancelledDone) `shouldReturn` Just True
        releaseHeld
        timeout 1_000_000 (takeMVar firstEntered) `shouldReturn` Just ()
        late <- forkFinally
          (withSetupBuffers @(Buffer, Buffer) pool (const (pure ())))
          (putMVar lateDone)
        awaitBlocked late
        isNothing <$> tryReadMVar lateDone `shouldReturn` True
        putMVar holdFirst ()
        fmap isLeft <$> timeout 1_000_000 (takeMVar firstDone) `shouldReturn` Just False
        fmap isLeft <$> timeout 1_000_000 (takeMVar lateDone) `shouldReturn` Just False
        timeout 1_000_000 (withSetupBuffers @(Buffer, Buffer) pool (const (pure ()))) `shouldReturn` Just ()

    it "recovers every block when timeout races a queued setup grant" $
      withTestPool $ \pool -> forM_ [1 .. 100 :: Int] $ \_ -> do
        (_, releaseHeld) <- holdSetup @(Buffer, Buffer) pool
        done <- newEmptyMVar
        worker <- forkFinally
          (timeout 500 (withSetupBuffers @(Buffer, Buffer) pool (const (pure ()))))
          (putMVar done)
        awaitBlocked worker
        releaseHeld
        outcome <- timeout 1_000_000 (takeMVar done)
        fmap isLeft outcome `shouldBe` Just False
        timeout 1_000_000 (withSetupBuffers @(Buffer, Buffer) pool (pure . capacities)) `shouldReturn` Just (8, 8)

    it "checks an impossible demand before it can enter a buffer queue" $
      withTestPool $ \pool -> do
        rejected <- timeout 1_000_000 $ try @IOError $
          withSetupBuffers @(Buffer, Buffer, Buffer) pool (const (pure ()))
        fmap isLeft rejected `shouldBe` Just True

    it "lets the caller cancel queued grants across sizes and reuse all inventory" $
      withTestPool $ \pool ->
        bracket (holdSetup @(Buffer, Buffer) pool) snd $ \(_, releaseHeld) -> do
          let withQueued [] action = action []
              withQueued (acquire : remaining) action =
                bracket (start acquire) cancelAndJoin $ \child@(worker, _) -> do
                  -- Install cleanup before checking registration, so a failed
                  -- assertion also cancels and joins every earlier child.
                  awaitBlocked worker
                  withQueued remaining (action . (child :))
              start acquire = do
                stopped <- newEmptyMVar
                worker <- forkFinally acquire (putMVar stopped)
                pure (worker, stopped)
              cancelAndJoin (worker, stopped) = do
                killThread worker
                _ <- readMVar stopped
                pure ()
          withQueued
            [ withSetupBuffers @(Solo Buffer) pool (const (pure ()))
            , withSetupBuffers @(Buffer, Buffer) pool (const (pure ()))
            , withSetupBuffers @(Solo Buffer) pool (const (pure ()))
            ] $ \queued -> do
              mapM_ (killThread . fst) queued
              mapM_ (\(_, stopped) ->
                fmap isLeft <$> timeout 1_000_000 (readMVar stopped) `shouldReturn` Just True) queued
              releaseHeld
              forM_ [1 .. 3 :: Int] $ \_ ->
                timeout 1_000_000 (withSetupBuffers @(Buffer, Buffer) pool (pure . capacities)) `shouldReturn` Just (8, 8)

withTestPool :: (BufferPool -> IO a) -> IO a
withTestPool action = getAcquisition >>= \waiting ->
  newBufferPool waiting
    PoolSettings
      { bufferSize = positiveInt @8
      , ordinaryBufferCount = positiveInt @2
      , reserveBufferCount = positiveInt @1
      , setupBufferCount = positiveInt @2
      } >>= action

holdSetup :: forall grant. BufferGrant grant => BufferPool -> IO (grant, IO ())
holdSetup pool = do
  ready <- newEmptyMVar
  hold <- newEmptyMVar
  done <- newEmptyMVar
  _ <- forkFinally
    (withSetupBuffers @grant pool $ \buffers -> putMVar ready buffers >> takeMVar hold)
    (putMVar done)
  buffers <- takeMVar ready
  let finish = do
        _ <- tryPutMVar hold ()
        readMVar done >>= either (failTest . show) pure
  pure (buffers, finish)

capacities :: (Buffer, Buffer) -> (Int, Int)
capacities (input, output) = (bufferCapacity input, bufferCapacity output)

awaitBlocked :: ThreadId -> IO ()
awaitBlocked thread = do
  result <- timeout 1_000_000 go
  result `shouldBe` Just ()
  where
    go = threadStatus thread >>= \case
      ThreadBlocked BlockedOnSTM -> pure ()
      ThreadBlocked BlockedOnMVar -> pure ()
      ThreadFinished -> pure ()
      ThreadDied -> failTest "worker died before blocking"
      _ -> yield >> go
