{-# LANGUAGE DataKinds #-}
module AdmissionResourcesSpec (spec) where

import Control.Concurrent.Resource.Waiting qualified as Waiting
import Control.Concurrent (forkFinally, killThread, yield, ThreadId)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically)
import Control.Exception (finally, try)
import Data.Either (isLeft)
import Data.Maybe (isNothing)
import Data.Tuple (Solo)
import GHC.Conc (ThreadStatus (..), BlockReason (..), threadStatus)
import Network.Streaming.Buffer (Buffer, bufferCapacity)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "buffer-owned scheduling" $ do
    it "detaches immediate grants before serving a same-size FIFO" $
      withTestResources $ \resources -> do
        timeout 1_000_000 (withBuffers @(Buffer, Buffer) resources (pure . capacities)) `shouldReturn` Just (8, 8)
        timeout 1_000_000 (withBuffers @(Buffer, Buffer) resources (pure . capacities)) `shouldReturn` Just (8, 8)
        held <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
        firstEntered <- newEmptyMVar
        releaseFirst <- newEmptyMVar
        firstDone <- newEmptyMVar
        first <- forkFinally
          (withBuffers @(Buffer, Buffer) resources (const (putMVar firstEntered () >> takeMVar releaseFirst)))
          (putMVar firstDone)
        awaitBlocked first
        secondEntered <- newEmptyMVar
        secondDone <- newEmptyMVar
        second <- forkFinally
          (withBuffers @(Buffer, Buffer) resources (const (putMVar secondEntered ())))
          (putMVar secondDone)
        (do
          awaitBlocked second
          late <- atomically (release held >> tryAcquire @(Buffer, Buffer) resources)
          mapM_ (atomically . release) late
          isNothing late `shouldBe` True
          timeout 1_000_000 (takeMVar firstEntered) `shouldReturn` Just ()
          isNothing <$> tryReadMVar secondEntered `shouldReturn` True
          putMVar releaseFirst ()
          timeout 1_000_000 (takeMVar secondEntered) `shouldReturn` Just ()
          fmap isLeft <$> timeout 1_000_000 (takeMVar firstDone) `shouldReturn` Just False
          fmap isLeft <$> timeout 1_000_000 (takeMVar secondDone) `shouldReturn` Just False)
          `finally` (killThread first >> killThread second >> atomically (release held))

    it "delivers returned blocks to a queued ordinary grant" $
      withTestResources $ \resources -> do
        held <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
        done <- newEmptyMVar
        worker <- forkFinally
          (withBuffers @(Buffer, Buffer) resources (pure . capacities))
          (putMVar done)
        awaitBlocked worker
        atomically (release held)
        result <- timeout 1_000_000 (takeMVar done)
        case result of
          Just (Right outcome) -> outcome `shouldBe` (8, 8)
          _ -> failTest "buffer queue did not receive returned storage"
        withBuffers @(Buffer, Buffer) resources (pure . capacities) `shouldReturn` (8, 8)

    it "selects the first eligible arrival while preserving reserve and FIFO" $
      withTestResources $ \resources -> do
        held <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
        largeEntered <- newEmptyMVar
        smallEntered <- newEmptyMVar
        holdSmall <- newEmptyMVar
        largeDone <- newEmptyMVar
        smallDone <- newEmptyMVar
        large <- forkFinally
          (withBuffers @(Buffer, Buffer) resources (const (putMVar largeEntered ())))
          (putMVar largeDone)
        awaitBlocked large
        _ <- forkFinally
          (withBuffers @(Solo Buffer) resources (const (putMVar smallEntered () >> takeMVar holdSmall)))
          (putMVar smallDone)
        timeout 1_000_000 (takeMVar smallEntered) `shouldReturn` Just ()
        isNothing <$> tryReadMVar largeEntered `shouldReturn` True
        isNothing <$> atomically (tryAcquire @(Solo Buffer) resources) `shouldReturn` True
        putMVar holdSmall ()
        timeout 1_000_000 (takeMVar smallDone) >>= maybe (failTest "small grant stuck") (either (failTest . show) pure)
        isNothing <$> tryReadMVar largeEntered `shouldReturn` True
        atomically (release held)
        timeout 1_000_000 (takeMVar largeEntered) `shouldReturn` Just ()
        timeout 1_000_000 (takeMVar largeDone) >>= maybe (failTest "large grant stuck") (either (failTest . show) pure)

    it "chooses the older fitting head across different grant sizes" $
      withTestResources $ \resources -> do
        held <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
        largeEntered <- newEmptyMVar
        smallEntered <- newEmptyMVar
        releaseLarge <- newEmptyMVar
        largeDone <- newEmptyMVar
        smallDone <- newEmptyMVar
        large <- forkFinally
          (withBuffers @(Buffer, Buffer) resources (const (putMVar largeEntered () >> takeMVar releaseLarge)))
          (putMVar largeDone)
        awaitBlocked large
        small <- forkFinally
          (withBuffers @(Solo Buffer) resources (const (putMVar smallEntered ())))
          (putMVar smallDone)
        awaitBlocked small
        atomically (release held)
        timeout 1_000_000 (takeMVar largeEntered) `shouldReturn` Just ()
        isNothing <$> tryReadMVar smallEntered `shouldReturn` True
        putMVar releaseLarge ()
        timeout 1_000_000 (takeMVar smallEntered) `shouldReturn` Just ()
        fmap isLeft <$> timeout 1_000_000 (takeMVar largeDone) `shouldReturn` Just False
        fmap isLeft <$> timeout 1_000_000 (takeMVar smallDone) `shouldReturn` Just False

    it "removes a cancelled head before delivering the next ordinary grant" $
      withTestResources $ \resources -> do
        held <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
        firstDone <- newEmptyMVar
        secondDone <- newEmptyMVar
        first <- forkFinally (withBuffers @(Solo Buffer) resources (const (pure ()))) (putMVar firstDone)
        awaitBlocked first
        second <- forkFinally (withBuffers @(Solo Buffer) resources (const (pure ()))) (putMVar secondDone)
        awaitBlocked second
        killThread first
        fmap isLeft <$> timeout 1_000_000 (takeMVar firstDone) `shouldReturn` Just True
        atomically (release held)
        fmap isLeft <$> timeout 1_000_000 (takeMVar secondDone) `shouldReturn` Just False

    it "keeps setup storage independent of a blocked stream queue" $
      withTestResources $ \resources -> do
        held <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
        done <- newEmptyMVar
        worker <- forkFinally (withBuffers @(Solo Buffer) resources (const (pure ()))) (putMVar done)
        awaitBlocked worker
        timeout 1_000_000 (withSetupBuffers @(Solo Buffer) resources (const (pure ()))) `shouldReturn` Just ()
        atomically (release held)
        fmap isLeft <$> timeout 1_000_000 (takeMVar done) `shouldReturn` Just False

    it "checks an impossible demand before it can enter a buffer queue" $
      withTestResources $ \resources -> do
        rejected <- timeout 1_000_000 $ try @IOError $
          withBuffers @(Buffer, Buffer, Buffer) resources (const (pure ()))
        fmap isLeft rejected `shouldBe` Just True

    it "rejects every queued grant across lanes when resources retire" $ do
      queued <- withTestResources $ \resources -> do
        _ <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
        mapM (\acquire -> do
          stopped <- newEmptyMVar
          worker <- forkFinally acquire (putMVar stopped)
          awaitBlocked worker
          pure (worker, stopped))
          [ withBuffers @(Solo Buffer) resources (const (pure ()))
          , withBuffers @(Buffer, Buffer) resources (const (pure ()))
          , withBuffers @(Solo Buffer) resources (const (pure ()))
          ]
      mapM_ (\(_, stopped) ->
        fmap isLeft <$> timeout 1_000_000 (takeMVar stopped) `shouldReturn` Just True) queued
        `finally` mapM_ (killThread . fst) queued

    it "rejects grants after resources retire" $ do
      resources <- withTestResources pure
      rejected <- try @IOError (withBuffers @(Buffer, Buffer) resources (pure . capacities))
      isLeft rejected `shouldBe` True

withTestResources :: (Resources -> IO a) -> IO a
withTestResources action = Waiting.withWaiting $ \waiting ->
  withResources waiting
    (ResourceSettings (positiveInt @24) (positiveInt @8) (positiveInt @8) (positiveInt @1)) action

requireLease :: Maybe (Lease grant) -> IO (Lease grant)
requireLease = maybe (failTest "expected a complete buffer grant") pure

capacities :: (Buffer, Buffer) -> (Int, Int)
capacities (input, output) = (bufferCapacity input, bufferCapacity output)

awaitBlocked :: ThreadId -> IO ()
awaitBlocked thread = do
  result <- timeout 1_000_000 go
  result `shouldBe` Just ()
  where
    go = threadStatus thread >>= \status -> case status of
      ThreadBlocked BlockedOnSTM -> pure ()
      ThreadBlocked BlockedOnMVar -> pure ()
      ThreadFinished -> pure ()
      ThreadDied -> failTest "worker died before blocking"
      _ -> yield >> go
