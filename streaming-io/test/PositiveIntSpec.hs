{-# LANGUAGE DataKinds #-}

module PositiveIntSpec (spec) where

import Data.Maybe (isNothing)
import Network.Streaming.PositiveInt
import Skeletest

spec :: Spec
spec = do
 describe "positive integers" $ do
  it "constructs positive constants" $ do
    positiveIntValue (positiveInt @1) `shouldBe` 1
    positiveIntValue (positiveInt @1024) `shouldBe` 1024
  it "rejects nonpositive runtime values" $
    mapM_ (\value -> isNothing (mkPositiveInt value) `shouldBe` True)
      [0, -1, negate (toInteger (maxBound :: Int)) - 1]
  it "checks the Integer upper bound before narrowing" $ do
    let largest = toInteger (maxBound :: Int)
    fmap positiveIntValue (mkPositiveInt largest) `shouldBe` Just maxBound
    mapM_ (\value -> isNothing (mkPositiveInt value) `shouldBe` True)
      [largest + 1, 2 * (largest + 1) + 1, 10 ^ (100 :: Int)]
  it "preserves valid runtime values" $ do
    mkPositiveInt 1 `shouldBe` Just (positiveInt @1)
    mkPositiveInt 1024 `shouldBe` Just (positiveInt @1024)
  it "compares and shows the underlying value" $ do
    (positiveInt @1 < positiveInt @2) `shouldBe` True
    show (positiveInt @1024) `shouldBe` "1024"
