{-# LANGUAGE DataKinds #-}
module ResourcesSpec (spec) where

import Control.Exception (try)
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

requireLease :: Maybe Lease -> IO Lease
requireLease = maybe (failTest "expected an available lease") pure

rejects :: ResourceSettings -> IO ()
rejects settings = do
  result <- try (withResources settings (const (pure ()))) :: IO (Either IOError ())
  isLeft result `shouldBe` True

requirePositive :: Integer -> IO PositiveInt
requirePositive = maybe (failTest "expected a positive Int") pure . mkPositiveInt
