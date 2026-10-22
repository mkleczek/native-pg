{-# LANGUAGE DataKinds #-}
module ResourcesSpec (spec) where

import Control.Exception (try)
import Control.Concurrent (forkFinally, killThread)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import System.Timeout (timeout)
import Control.Concurrent.STM (atomically, retry, orElse)
import Data.Maybe (isNothing)
import Data.Tuple (Solo (MkSolo))
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

 describe "pure settings validation" $ do
  it "validates without creating resource storage" $ do
    validateSettings (ResourceSettings (positiveInt @25) (positiveInt @9) (positiveInt @8)) `shouldBe` Right ()
    isLeft (validateSettings (ResourceSettings (positiveInt @7) (positiveInt @8) (positiveInt @8))) `shouldBe` True
    isLeft (validateSettings (ResourceSettings (positiveInt @8) (positiveInt @7) (positiveInt @8))) `shouldBe` True
  it "rejects budgets whose sum exceeds Int without allocating" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    almostLargest <- requirePositive (toInteger (maxBound :: Int) - 1)
    isLeft (validateSettings (ResourceSettings largest (positiveInt @1) (positiveInt @1))) `shouldBe` True
    validateSettings (ResourceSettings almostLargest (positiveInt @1) (positiveInt @1)) `shouldBe` Right ()

 describe "storage" $ do
  it "initializes blocks once and reuses their addresses and contents" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      first <- withBuffers @(Solo Buffer) resources $ withOneBuffer $ \buffer -> do
        bufferCapacity buffer `shouldBe` 8
        withForeignPtr (bufferMemory buffer) $ \pointer -> do
          peek pointer `shouldReturn` 0
          poke pointer 73
        pure (bufferMemory buffer)
      withBuffers @(Solo Buffer) resources $ withOneBuffer $ \buffer -> do
        bufferMemory buffer `shouldBe` first
        withForeignPtr (bufferMemory buffer) $ \pointer -> peek pointer `shouldReturn` 73

 describe "whole-block grants" $ do
  it "acquires every requested block or leaves all available blocks alone" $
    withResources (ResourceSettings (positiveInt @25) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      first <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
      let (firstBuffer, secondBuffer) = leaseBuffers first
      bufferMemory firstBuffer `shouldNotBe` bufferMemory secondBuffer
      isNothing <$> atomically (tryAcquire @(Buffer, Buffer) resources) `shouldReturn` True
      lastBlock <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      isNothing <$> atomically (tryAcquire @(Solo Buffer) resources) `shouldReturn` True
      atomically (release lastBlock >> release first)
      allBlocks <- atomically (tryAcquire @(Buffer, Buffer, Buffer) resources) >>= requireLease
      let (a, b, c) = leaseBuffers allBlocks
      map bufferMemory [a, b, c] `shouldBe` map bufferMemory [firstBuffer, secondBuffer, onlyBuffer (leaseBuffers lastBlock)]
      atomically (release allBlocks)
  it "returns five-block tuples without copying buffer payloads" $
    withResources (ResourceSettings (positiveInt @40) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      original <- withBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) resources $ \(a, b, c, d, e) -> do
        withForeignPtr (bufferMemory c) $ \pointer -> poke pointer 91
        pure (map bufferMemory [a, b, c, d, e])
      withBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) resources $ \(a, b, c, d, e) -> do
        map bufferMemory [a, b, c, d, e] `shouldBe` original
        withForeignPtr (bufferMemory c) $ \pointer -> peek pointer `shouldReturn` 91
  it "throws for loan counts that exceed capacity" $
    withResources (ResourceSettings (positiveInt @24) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      result <- try (atomically (tryAcquire @(Buffer, Buffer, Buffer, Buffer) resources)) :: IO (Either IOError (Maybe (Lease (Buffer, Buffer, Buffer, Buffer))))
      isLeft result `shouldBe` True
  it "rolls back a grant with other admission checks in the same transaction" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      _ <- atomically $ (tryAcquire @(Solo Buffer) resources >> retry) `orElse` pure ()
      lease <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      atomically (release lease)

 describe "lease ownership" $ do
  it "returns a lease only once even after another borrower acquires its blocks" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      first <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      atomically (release first >> release first)
      second <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      atomically (release first)
      isNothing <$> atomically (tryAcquire @(Solo Buffer) resources) `shouldReturn` True
      atomically (release second)
  it "rejects acquisition after the owning scope ends, including after a late return" $ do
    (resources, lease) <- withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      lease <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      pure (resources, lease)
    atomically (release lease)
    result <- try (atomically (tryAcquire @(Solo Buffer) resources)) :: IO (Either IOError (Maybe (Lease (Solo Buffer))))
    isLeft result `shouldBe` True
  it "returns buffers after callback exceptions" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      result <- try (withBuffers @(Solo Buffer) resources $ const $ ioError (userError "callback")) :: IO (Either IOError ())
      isLeft result `shouldBe` True
      timeout 1000000 (withBuffers @(Solo Buffer) resources (const (pure (1 :: Int)))) `shouldReturn` Just 1
  it "returns buffers after asynchronous cancellation" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      started <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (withBuffers @(Solo Buffer) resources $ const $ putMVar started () >> takeMVar hold)
        (const $ putMVar done ())
      timeout 1000000 (takeMVar started) `shouldReturn` Just ()
      killThread worker
      timeout 1000000 (takeMVar done) `shouldReturn` Just ()
      timeout 1000000 (withBuffers @(Solo Buffer) resources (const (pure (1 :: Int)))) `shouldReturn` Just 1
  it "cancels blocked acquisition without consuming a future return" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      lease <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      started <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (putMVar started () >> withBuffers @(Solo Buffer) resources (const (pure (1 :: Int))))
        (putMVar done)
      takeMVar started
      killThread worker
      outcome <- timeout 1000000 (takeMVar done)
      fmap isLeft outcome `shouldBe` Just True
      atomically (release lease)
      timeout 1000000 (withBuffers @(Solo Buffer) resources (const (pure (1 :: Int)))) `shouldReturn` Just 1

  it "wakes blocked borrowers when their resource scope retires" $ do
    started <- newEmptyMVar
    done <- newEmptyMVar
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      _ <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      _ <- forkFinally
        (putMVar started () >> withBuffers @(Solo Buffer) resources (const (pure (1 :: Int))))
        (putMVar done)
      takeMVar started
    outcome <- timeout 1000000 (takeMVar done)
    fmap isLeft outcome `shouldBe` Just True

 describe "setup capacity" $ do
  it "admits setup while every stream block is busy" $
    withResources (ResourceSettings (positiveInt @16) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      streams <- atomically (tryAcquire @(Buffer, Buffer) resources) >>= requireLease
      setup <- atomically (tryAcquireSetup @(Solo Buffer) resources) >>= requireLease
      let MkSolo setupBuffer = leaseBuffers setup
          (streamA, streamB) = leaseBuffers streams
      bufferMemory setupBuffer `shouldNotBe` bufferMemory streamA
      bufferMemory setupBuffer `shouldNotBe` bufferMemory streamB
      isNothing <$> atomically (tryAcquireSetup @(Solo Buffer) resources) `shouldReturn` True
      atomically (release streams >> release setup)
  it "validates requests against setup capacity" $
    withResources (ResourceSettings (positiveInt @16) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      result <- try (atomically (tryAcquireSetup @(Buffer, Buffer) resources)) :: IO (Either IOError (Maybe (Lease (Buffer, Buffer))))
      isLeft result `shouldBe` True
  it "reuses setup storage without taking a stream block" $
    withResources (ResourceSettings (positiveInt @8) (positiveInt @8) (positiveInt @8)) $ \resources -> do
      setupMemory <- withSetupBuffers @(Solo Buffer) resources $ withOneBuffer $ \buffer -> pure (bufferMemory buffer)
      streams <- atomically (tryAcquire @(Solo Buffer) resources) >>= requireLease
      streamMemory <- withOneBuffer (pure . bufferMemory) (leaseBuffers streams)
      timeout 1000000 (withSetupBuffers @(Solo Buffer) resources $ withOneBuffer $ \buffer -> do
        bufferMemory buffer `shouldBe` setupMemory
        bufferMemory buffer `shouldNotBe` streamMemory) `shouldReturn` Just ()
      atomically (release streams)

withOneBuffer :: (Buffer -> IO a) -> Solo Buffer -> IO a
withOneBuffer action (MkSolo buffer) = action buffer

onlyBuffer :: Solo Buffer -> Buffer
onlyBuffer (MkSolo buffer) = buffer

requireLease :: Maybe (Lease grant) -> IO (Lease grant)
requireLease = maybe (failTest "expected an available lease") pure

rejects :: ResourceSettings -> IO ()
rejects settings = do
  result <- try (withResources settings (const (pure ()))) :: IO (Either IOError ())
  isLeft result `shouldBe` True

requirePositive :: Integer -> IO PositiveInt
requirePositive = maybe (failTest "expected a positive Int") pure . mkPositiveInt
