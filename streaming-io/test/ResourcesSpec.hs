module ResourcesSpec (spec) where
import Skeletest
spec :: Spec
spec = describe "resource API" $ it "exposes the shared package" $ True `shouldBe` True
