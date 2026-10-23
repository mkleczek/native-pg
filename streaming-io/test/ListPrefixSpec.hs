module ListPrefixSpec (spec) where

import Data.Tuple (Solo (MkSolo))
import Network.Streaming.Internal.ListPrefix
import Skeletest

spec :: Spec
spec = describe "fixed list prefixes" $ do
  it "takes a singleton and keeps its remainder" $ do
    prefixSize @(Solo Int) `shouldBe` 1
    takePrefix @(Solo Int) [1, 2, 3] `shouldBe` Just (MkSolo 1, [2, 3])
    takePrefix @(Solo Int) [] `shouldBe` Nothing
    prepend (MkSolo (1 :: Int)) [2, 3] `shouldBe` [1, 2, 3]
  it "preserves tuple and list order for every supported arity" $ do
    prefixSize @(Int, Int) `shouldBe` 2
    prefixSize @(Int, Int, Int) `shouldBe` 3
    prefixSize @(Int, Int, Int, Int) `shouldBe` 4
    prefixSize @(Int, Int, Int, Int, Int) `shouldBe` 5
    takePrefix @(Int, Int) [1, 2, 3] `shouldBe` Just ((1, 2), [3])
    takePrefix @(Int, Int, Int) [1, 2, 3, 4] `shouldBe` Just ((1, 2, 3), [4])
    takePrefix @(Int, Int, Int, Int) [1, 2, 3, 4, 5] `shouldBe` Just ((1, 2, 3, 4), [5])
    takePrefix @(Int, Int, Int, Int, Int) [1, 2, 3, 4, 5, 6] `shouldBe` Just ((1, 2, 3, 4, 5), [6])
    prepend ((1, 2) :: (Int, Int)) [3] `shouldBe` [1, 2, 3]
    prepend ((1, 2, 3) :: (Int, Int, Int)) [4] `shouldBe` [1, 2, 3, 4]
    prepend ((1, 2, 3, 4) :: (Int, Int, Int, Int)) [5] `shouldBe` [1, 2, 3, 4, 5]
    prepend ((1, 2, 3, 4, 5) :: (Int, Int, Int, Int, Int)) [6] `shouldBe` [1, 2, 3, 4, 5, 6]
  it "rejects every shorter input without requiring a remainder" $ do
    mapM_ (\xs -> takePrefix @(Int, Int) xs `shouldBe` Nothing) [[], [1]]
    mapM_ (\xs -> takePrefix @(Int, Int, Int) xs `shouldBe` Nothing) [[], [1], [1, 2]]
    mapM_ (\xs -> takePrefix @(Int, Int, Int, Int) xs `shouldBe` Nothing) [[], [1], [1, 2], [1, 2, 3]]
    mapM_ (\xs -> takePrefix @(Int, Int, Int, Int, Int) xs `shouldBe` Nothing) [[], [1], [1, 2], [1, 2, 3], [1, 2, 3, 4]]
  it "leaves the unconsumed tail lazy" $ do
    fmap fst (takePrefix @(Int, Int) (1 : 2 : error "forced remainder")) `shouldBe` Just (1, 2)
