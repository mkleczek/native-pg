{-# LANGUAGE DataKinds #-}
module ResourcesSpec (spec) where

import Control.Exception (try)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Storable (peek, poke)
import Data.Either (isLeft)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources.Internal
import Skeletest

spec :: Spec
spec = do
 describe "resource settings" $ do
  it "rejects budgets smaller than one block" $
    mapM_ rejects [ResourceSettings (positiveInt @7) (positiveInt @16) (positiveInt @8), ResourceSettings (positiveInt @32) (positiveInt @7) (positiveInt @8)]

 describe "storage" $ do
  xfail "resource storage is pending" $ it "initializes blocks once and reuses their addresses and contents" $
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

rejects :: ResourceSettings -> IO ()
rejects settings = do
  result <- try (withResources settings (const (pure ()))) :: IO (Either IOError ())
  isLeft result `shouldBe` True
