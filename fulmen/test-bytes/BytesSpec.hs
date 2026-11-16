{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

-- | What the head of a request and of a response need of bytes: names that are compared without regard to case, which a
-- request does for each of its header names, so that a comparison must not allocate.
module BytesSpec (spec) where

import Control.Monad (when)
import Data.ByteString (ByteString)
import Data.Word (Word64)
import GHC.Stats (allocated_bytes, getRTSStats)
import Network.Fulmen.Internal.Bytes (equalsLower)
import Skeletest
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)
import System.Mem (performMinorGC)

spec :: Spec
spec = describe "equalsLower" $ do
  it "is true for the lower-case name in any case, and false for another name, or a name of another length" $ do
    equalsLower "content-type" "content-type" `shouldBe` True
    equalsLower "content-type" "Content-Type" `shouldBe` True
    equalsLower "content-type" "CONTENT-TYPE" `shouldBe` True
    equalsLower "content-type" "Content-Typo" `shouldBe` False
    equalsLower "content-type" "Content-Typ" `shouldBe` False
    equalsLower "content-type" "Content-Types" `shouldBe` False
    equalsLower "" "" `shouldBe` True
    equalsLower "a" "" `shouldBe` False
    -- A character that is not a letter is itself, in either case: the bytes between Z and a are not lowered.
    equalsLower "a_b" "A_B" `shouldBe` True
    equalsLower "a_b" "A\DEL" `shouldBe` False

  it "allocates no more than a few bytes for a comparison" $ do
    -- The names of a request's headers, in the case that clients use, and the one name that they are compared with.
    let comparisons = 200000 :: Int
    _ <- pure $! compared 1000
    bytes <- allocatedBy (pure $! compared comparisons)
    let each = fromIntegral bytes / fromIntegral comparisons :: Double
    lookupEnv "BYTES_COST_REPORT" >>= \asked -> when (asked /= Nothing) (hPutStrLn stderr ("equalsLower | " <> show (round each :: Int) <> " bytes a comparison"))
    when (each > 8) $ failTest ("a comparison allocates " <> show (round each :: Int) <> " bytes")

-- | How many of this many comparisons of the header names in turn with "content-type" are true.
compared :: Int -> Int
compared count = go 0 0 (cycle headerNames)
  where
    go !done !hits names = case names of
      name : others
        | done >= count -> hits
        | otherwise -> go (done + 1) (if equalsLower "content-type" name then hits + 1 else hits) others
      [] -> hits
{-# NOINLINE compared #-}

-- | The names of the headers of a request, as a browser sends them: a few of them are the one that is looked for.
headerNames :: [ByteString]
headerNames = ["Host", "Content-Type", "User-Agent", "Accept", "Accept-Encoding", "Authorization", "Prefer", "Content-Length", "Connection", "content-type", "Range", "Content-Profile"]
{-# NOINLINE headerNames #-}

-- | The bytes that the action allocated, with a minor collection on both sides, which the run time counts a nursery at.
allocatedBy :: IO a -> IO Word64
allocatedBy action = do
  performMinorGC
  before <- allocated_bytes <$> getRTSStats
  _ <- action
  performMinorGC
  after <- allocated_bytes <$> getRTSStats
  pure (after - before)
