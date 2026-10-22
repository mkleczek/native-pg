{-# LANGUAGE DataKinds #-}
module ResourcesSpec (spec) where

import Control.Exception (try)
import Data.Either (isLeft)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources.Internal
import Skeletest

spec :: Spec
spec = describe "resource settings" $ do
  it "rejects budgets smaller than one block" $
    mapM_ rejects [ResourceSettings (positiveInt @7) (positiveInt @16) (positiveInt @8), ResourceSettings (positiveInt @32) (positiveInt @7) (positiveInt @8)]

rejects :: ResourceSettings -> IO ()
rejects settings = do
  result <- try (withResources settings (const (pure ()))) :: IO (Either IOError ())
  isLeft result `shouldBe` True
