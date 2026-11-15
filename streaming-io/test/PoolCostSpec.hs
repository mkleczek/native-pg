{-# LANGUAGE RankNTypes #-}

-- | What a request pays the pool when it finds an item free, as a connection pool's checkouts do: the bytes that a loan of
-- the item allocates, which are the difference between a long run and a short one. A deadline on the checkout is armed when
-- the checkout has to wait, and costs nothing when an item is free, so the cases run each loan with a deadline and without
-- one. The ceilings are what each arrow allocated when they were set: a change that makes a loan dearer says so here, as one
-- that makes it cheaper takes the ceiling down. The suite runs with @-T@ ("-with-rtsopts" in its cabal stanza) so that
-- allocation is counted. With @POOL_COST_REPORT@ set, each case says what it measured, on stderr.
module PoolCostSpec (spec) where

import Control.Arrow ((>>>))
import Control.Monad (forM_, void, when)
import Data.Word (Word64)
import GHC.Stats (allocated_bytes, getRTSStats)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, times)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.Pool
import Network.Streaming.Threads (runThreads)
import Skeletest
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)
import System.Mem (performMinorGC)

-- | A program of this many loans, for any arrow that has what 'Runtime' bundles.
type Program = forall arr. (Runtime arr) => Int -> arr () ()

-- | An arrow, how it runs a program for the unit, and the most that a loan of a free item may allocate, in bytes.
data Runner = Runner
  { runnerName :: String,
    runnerRun :: Program -> Int -> IO (),
    runnerCeiling :: Double
  }

runners :: [Runner]
runners =
  [ Runner "threads" (\program count -> void (runThreads (program count) ())) 1752,
    Runner "delimited" (\program count -> void (runDelimited (program count) ())) 1279,
    Runner "compiled" (\program count -> void (runCompiled (program count) ())) 2037
  ]

spec :: Spec
spec = describe "a loan of a free item" $ forM_ runners $ \runner -> describe (runnerName runner) $ do
  it "allocates no more than it did" $ do
    pool <- newPool [7 :: Int]
    bytes <- perLoan runner (\count -> times count (withLoan pool done >>> done))
    reported runner "withLoan" bytes
    when (bytes > runnerCeiling runner + tolerance) $
      failTest ("a loan allocates " <> show (round bytes :: Int) <> " bytes, where the ceiling is " <> show (round (runnerCeiling runner) :: Int))

  it "costs no more with a deadline than without one" $ do
    pool <- newPool [7 :: Int]
    without <- perLoan runner (\count -> times count (withLoan pool done >>> done))
    with <- perLoan runner (\count -> times count (withLoanWithin (secs 3600) pool done >>> done))
    reported runner "withLoanWithin" with
    atMostMore tolerance without with

-- | The most that a loan with a deadline may allocate more than one without, in bytes: a few words, for the noise of the
-- count, which is none in practice.
tolerance :: Double
tolerance = 24

-- | The bytes that a loan allocates: the difference between a long run and a short one, after a run that loads the code.
perLoan :: Runner -> Program -> IO Double
perLoan runner program = do
  run 1000
  few <- allocatedBy (run 20000)
  many <- allocatedBy (run 120000)
  pure ((fromIntegral many - fromIntegral few) / 100000)
  where
    run = runnerRun runner program

-- | The bytes that the action allocated: the runtime counts a nursery when it is collected, so a collection is made on both
-- sides.
allocatedBy :: IO () -> IO Word64
allocatedBy action = do
  performMinorGC
  before <- allocated_bytes <$> getRTSStats
  action
  performMinorGC
  after <- allocated_bytes <$> getRTSStats
  pure (after - before)

-- | The loan with the deadline allocates no more than the one without it, but for the allowance: the difference is of whole
-- words, whatever the count of a long run says of it.
atMostMore :: Double -> Double -> Double -> IO ()
atMostMore allowance without with =
  if fromIntegral (round ((with - without) / 8) * 8 :: Int) <= allowance
    then pure ()
    else failTest ("a loan allocates " <> show (round with :: Int) <> " bytes with a deadline, where it allocates " <> show (round without :: Int) <> " without one")

-- | Says what the case measured, if the run asks for it.
reported :: Runner -> String -> Double -> IO ()
reported runner label bytes =
  lookupEnv "POOL_COST_REPORT" >>= \asked ->
    when (asked /= Nothing) (hPutStrLn stderr ("pool-cost " <> runnerName runner <> " | " <> label <> " | " <> show (round bytes :: Int)))
