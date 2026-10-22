{-# LANGUAGE DataKinds #-}
module ResourcesSpec (spec) where

import Control.Exception (try)
import Control.Concurrent (forkFinally, killThread)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import System.Timeout (timeout)
import Control.Concurrent.STM (atomically, retry, orElse)
import Data.Maybe (isNothing)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Storable (peek, poke)
import Data.Either (isLeft)
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt, positiveInt)
import Network.Streaming.Resources.Internal
import Skeletest

spec :: Spec
spec = do
 describe "resource settings" $ do
  it "rejects budgets smaller than one block" $
    mapM_ rejects [ResourceSettings (positiveInt @7) (positiveInt @16) (positiveInt @8), ResourceSettings (positiveInt @32) (positiveInt @7) (positiveInt @8)]

 describe "storage" $ do
  it "initializes blocks once and reuses their addresses and contents" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      first <- withBuffers resources (positiveInt @1) $ \[buffer] -> do
        bufferCapacity buffer `shouldBe` 8
        withForeignPtr (bufferMemory buffer) $ \pointer -> do
          peek pointer `shouldReturn` 0
          poke pointer 73
        pure (bufferMemory buffer)
      withBuffers resources (positiveInt @1) $ \[buffer] -> do
        bufferMemory buffer `shouldBe` first
        withForeignPtr (bufferMemory buffer) $ \pointer -> peek pointer `shouldReturn` 73

 describe "whole-block grants" $ do
  it "acquires every requested block or leaves all available blocks alone" $
    withResources (ResourceSettings (positiveInt @25) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      first <- atomically (tryAcquire resources (positiveInt @2)) >>= requireLease
      length (leaseBuffers first) `shouldBe` 2
      isNothing <$> atomically (tryAcquire resources (positiveInt @2)) `shouldReturn` True
      lastBlock <- atomically (tryAcquire resources (positiveInt @1)) >>= requireLease
      isNothing <$> atomically (tryAcquire resources (positiveInt @1)) `shouldReturn` True
      atomically (release first >> release lastBlock)
      allBlocks <- atomically (tryAcquire resources (positiveInt @3)) >>= requireLease
      length (leaseBuffers allBlocks) `shouldBe` 3
      atomically (release allBlocks)
  it "throws for loan counts that exceed capacity" $
    withResources (ResourceSettings (positiveInt @24) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      largest <- requirePositive (toInteger (maxBound :: Int))
      mapM_ (\count -> do
        result <- try (atomically (tryAcquire resources count)) :: IO (Either IOError (Maybe Lease))
        isLeft result `shouldBe` True) [positiveInt @4, largest]
  it "rolls back a grant with other admission checks in the same transaction" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      _ <- atomically $ (tryAcquire resources (positiveInt @1) >> retry) `orElse` pure ()
      lease <- atomically (tryAcquire resources (positiveInt @1)) >>= requireLease
      atomically (release lease)

 describe "lease ownership" $ do
  it "returns a lease only once even after another borrower acquires its blocks" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      first <- atomically (tryAcquire resources (positiveInt @1)) >>= requireLease
      atomically (release first >> release first)
      second <- atomically (tryAcquire resources (positiveInt @1)) >>= requireLease
      atomically (release first)
      isNothing <$> atomically (tryAcquire resources (positiveInt @1)) `shouldReturn` True
      atomically (release second)
  it "rejects acquisition after the owning scope ends, including after a late return" $ do
    (resources, lease) <- withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      lease <- atomically (tryAcquire resources (positiveInt @1)) >>= requireLease
      pure (resources, lease)
    atomically (release lease)
    result <- try (atomically (tryAcquire resources (positiveInt @1))) :: IO (Either IOError (Maybe Lease))
    isLeft result `shouldBe` True
  it "returns buffers after callback exceptions" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      result <- try (withBuffers resources (positiveInt @1) $ const $ ioError (userError "callback")) :: IO (Either IOError ())
      isLeft result `shouldBe` True
      timeout 1000000 (withBuffers resources (positiveInt @1) (pure . length)) `shouldReturn` Just 1
  it "returns buffers after asynchronous cancellation" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      started <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (withBuffers resources (positiveInt @1) $ const $ putMVar started () >> takeMVar hold)
        (const $ putMVar done ())
      timeout 1000000 (takeMVar started) `shouldReturn` Just ()
      killThread worker
      timeout 1000000 (takeMVar done) `shouldReturn` Just ()
      timeout 1000000 (withBuffers resources (positiveInt @1) (pure . length)) `shouldReturn` Just 1
  it "cancels blocked acquisition without consuming a future return" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      lease <- atomically (tryAcquire resources (positiveInt @1)) >>= requireLease
      started <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (putMVar started () >> withBuffers resources (positiveInt @1) (pure . length))
        (putMVar done)
      takeMVar started
      killThread worker
      outcome <- timeout 1000000 (takeMVar done)
      fmap isLeft outcome `shouldBe` Just True
      atomically (release lease)
      timeout 1000000 (withBuffers resources (positiveInt @1) (pure . length)) `shouldReturn` Just 1

 describe "setup capacity" $ do
  xfail "separate setup capacity is pending" $ it "admits setup while every stream block is busy" $
    withResources (ResourceSettings (positiveInt @16) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      streams <- atomically (tryAcquire resources (positiveInt @2)) >>= requireLease
      setup <- atomically (tryAcquireSetup resources (positiveInt @1)) >>= requireLease
      map bufferMemory (leaseBuffers setup) `shouldNotBe` map bufferMemory (leaseBuffers streams)
      isNothing <$> atomically (tryAcquireSetup resources (positiveInt @1)) `shouldReturn` True
      atomically (release streams >> release setup)
  xfail "setup count validation is pending" $ it "validates requests against setup capacity" $
    withResources (ResourceSettings (positiveInt @16) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      result <- try (atomically (tryAcquireSetup resources (positiveInt @2))) :: IO (Either IOError (Maybe Lease))
      isLeft result `shouldBe` True
  xfail "setup bracket isolation is pending" $ it "reuses setup storage without taking a stream block" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      setupMemory <- withSetupBuffers resources (positiveInt @1) $ \[buffer] -> pure (bufferMemory buffer)
      streams <- atomically (tryAcquire resources (positiveInt @1)) >>= requireLease
      timeout 1000000 (withSetupBuffers resources (positiveInt @1) $ \[buffer] -> do
        bufferMemory buffer `shouldBe` setupMemory
        bufferMemory buffer `shouldNotBe` bufferMemory (head (leaseBuffers streams))) `shouldReturn` Just ()
      atomically (release streams)

requireLease :: Maybe Lease -> IO Lease
requireLease = maybe (failTest "expected an available lease") pure

rejects :: ResourceSettings -> IO ()
rejects settings = do
  result <- try (withResources settings (const (pure ()))) :: IO (Either IOError ())
  isLeft result `shouldBe` True

requirePositive :: Integer -> IO PositiveInt
requirePositive = maybe (failTest "expected a positive Int") pure . mkPositiveInt
