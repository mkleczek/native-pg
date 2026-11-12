{-# LANGUAGE LambdaCase #-}

-- | Measures what crosses cores. Run with @+RTS -T@ (allocation) and @-N@ for the cores it uses:
--
-- * @pingpong KIND ROUNDS@: round trips between two parts over tickets made as they go, in ns and bytes per round
--   trip. KIND is @local@ (two parts on one core, local tickets), @compiled@ (the same, run by 'Compiled'),
--   @remote@ (two cores, remote tickets), @threads@ (two threads, tickets) or @remote-threads@ (two threads,
--   remote tickets).
-- * @pool KIND CORES PARTS CONNECTIONS SECONDS@: parts that check a connection out of one pool, do a little work,
--   and release it, for the time given: checkouts per second, how evenly the parts and cores got them, and
--   allocation per checkout. KIND is @delimited@ (CORES cores of PARTS parts each) or @threads@ (the same number of
--   threads).
-- * @wake KIND SAMPLES@: how long a part waiting for a ticket on a core whose scheduler waits in its poller takes
--   to go on after another core gives it a value. KIND is @delimited@ or @threads@.
module Main (main) where

import Control.Arrow (Arrow, ArrowChoice, arr, returnA, (>>>), (^>>), (|||))
import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (evaluate)
import Control.Monad (replicateM, unless, void, when)
import Data.Functor (($>))
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import Data.List (sort)
import Data.Maybe (isJust)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtr, withForeignPtr)
import Foreign.Storable (peek, poke)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import Network.Directions.Classes
import Network.Directions.Compiled
import Network.Directions.Delimited
import Network.Directions.Loop (Report (..))
import Network.Directions.PingPong
import Network.Directions.Pool
import Network.Directions.Proxy (together)
import Network.Directions.Threads
import System.Environment (getArgs, lookupEnv)
import System.IO (hPrint, stderr)
import System.Mem (performMajorGC)
import Text.Printf (printf)

main :: IO ()
main = do
  stats <- getRTSStatsEnabled
  unless stats $ ioError (userError "run with +RTS -T")
  getArgs >>= \case
    ["pingpong", kind, rounds] -> pingPongRun kind (read rounds)
    ["pool", kind, cores, parts, conns, seconds] -> poolRun kind (read cores) (read parts) (read conns) (read seconds)
    ["wake", kind, samples] -> wakeRun kind (read samples)
    _ -> ioError (userError "usage: directions-cores-bench (pingpong KIND ROUNDS | pool KIND CORES PARTS CONNECTIONS SECONDS | wake KIND SAMPLES) +RTS -T -N..")

-- | Bytes allocated by every thread so far, once a major collection has counted them.
allocated :: IO Integer
allocated = performMajorGC *> (fromIntegral . allocated_bytes <$> getRTSStats)

-- | How long the action took, in seconds, and how many bytes it allocated.
measured :: IO () -> IO (Double, Integer)
measured action = do
  before <- allocated
  started <- getMonotonicTimeNSec
  action
  ended <- getMonotonicTimeNSec
  after <- allocated
  pure (fromIntegral (ended - started) / 1e9, after - before)

pingPongRun :: String -> Int -> IO ()
pingPongRun kind rounds = do
  handoff <- newHandoff
  (seconds, bytes) <- measured $ case kind of
    "local" -> void (runDelimited (pingPong rounds) ())
    "compiled" -> void (runCompiled (pingPong rounds) ())
    "remote" -> void (runCores 2 (pure handoff) (\shared core -> if core == 0 then remoteClient shared rounds else remoteServer shared rounds))
    "threads" -> runThreads (pingPong rounds) ()
    "remote-threads" -> runThreads (concurrently (remoteClient handoff rounds) (remoteServer handoff rounds) >>> arr (const ())) ()
    _ -> ioError (userError ("pingpong: no kind " <> kind))
  printf "pingpong %s rounds=%d ns_per_rt=%.1f bytes_per_rt=%.1f\n" kind rounds (seconds * 1e9 / fromIntegral rounds) (fromIntegral bytes / fromIntegral rounds :: Double)

-- | A count that one part adds to and nothing else does, which adding to allocates nothing for.
newtype Counter = Counter (ForeignPtr Int)

newCounter :: IO Counter
newCounter = do
  pointer <- mallocForeignPtr
  withForeignPtr pointer (`poke` 0)
  pure (Counter pointer)

bump :: Counter -> IO ()
bump (Counter pointer) = withForeignPtr pointer (\p -> peek p >>= poke p . (+ 1))

count :: Counter -> IO Int
count (Counter pointer) = withForeignPtr pointer peek

-- | A part that checks a connection out, works a little while it has it, and releases it, until told to stop.
part :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Pool Int -> IORef Bool -> Counter -> arr () ()
part pool stop counter = loop
  where
    loop = now (\() -> readIORef stop) >>> (\stopped -> if stopped then Left () else Right ()) ^>> (returnA ||| turn)
    turn = checkout pool >>> now hold >>> release pool >>> loop
    hold conn = (bump counter *> evaluate (work 40 conn)) $> conn
    work :: Int -> Int -> Int
    work 0 acc = acc
    work n acc = work (n - 1) (acc * 31 + n)

-- | Runs the arrows at once as a tree of pairs: with 'together', a chain, each thread is forked by the one before
-- it, and with threads that never wait, the last of a hundred starts when the others have had a time slice each.
balanced :: (Arrow arr, Concurrent arr) => [arr () ()] -> arr () ()
balanced [] = returnA
balanced [one] = one
balanced parts' = let (front, back) = splitAt (length parts' `div` 2) parts' in concurrently (balanced front) (balanced back) >>> arr (const ())

poolRun :: String -> Int -> Int -> Int -> Int -> IO ()
poolRun kind cores parts conns seconds = do
  pool <- newPool [0 .. conns - 1]
  stop <- newIORef False
  counters <- replicateM cores (replicateM parts newCounter)
  _ <- forkIO (threadDelay (seconds * 1000000) *> atomicWriteIORef stop True)
  (elapsed, bytes) <- measured $ case kind of
    "delimited" -> void (runCores cores (pure counters) (\shared core -> together [part pool stop counter | counter <- shared !! core]))
    "threads" -> runThreads (balanced [part pool stop counter | counter <- concat counters]) ()
    _ -> ioError (userError ("pool: no kind " <> kind))
  free pool >>= \freed -> when (length freed /= conns) (ioError (userError ("the pool has " <> show (length freed) <> " connections of " <> show conns)))
  perPart <- traverse (traverse count) counters
  debug <- lookupEnv "DIRECTIONS_BENCH_COUNTS"
  when (isJust debug) (hPrint stderr perPart)
  let total = sum (map sum perPart)
      parts' = map fromIntegral (concat perPart) :: [Double]
      mean = sum parts' / fromIntegral (length parts')
      deviation = sqrt (sum [(x - mean) ^ (2 :: Int) | x <- parts'] / fromIntegral (length parts'))
      perCore = map (fromIntegral . sum) perPart :: [Double]
  printf
    "pool %s cores=%d parts=%d conns=%d checkouts=%d per_s=%.0f bytes_per_checkout=%.1f part_min_over_mean=%.3f part_max_over_mean=%.3f part_cv=%.3f core_min_over_max=%.3f\n"
    kind
    cores
    parts
    conns
    total
    (fromIntegral total / elapsed)
    (fromIntegral bytes / fromIntegral total :: Double)
    (minimum parts' / mean)
    (maximum parts' / mean)
    (deviation / mean)
    (minimum perCore / maximum perCore)

wakeRun :: String -> Int -> IO ()
wakeRun kind samples = do
  wake <- newWake
  waits <- case kind of
    "delimited" -> do
      reports <- runCoresReport 2 (pure wake) (\shared core -> if core == 0 then sleeper shared samples else waker shared 500 samples)
      pure (Just (sum (map (reportWaits . snd) (take 1 reports))))
    "threads" -> Nothing <$ runThreads (concurrently (sleeper wake samples) (waker wake 500 samples) >>> arr (const ())) ()
    _ -> ioError (userError ("wake: no kind " <> kind))
  latencies <- sort . map fromIntegral <$> wakeLatencies wake :: IO [Double]
  let at :: Double -> Double
      at fraction = latencies !! min (length latencies - 1) (floor (fraction * fromIntegral (length latencies)))
  printf "wake %s samples=%d blocked=%s median_us=%.1f p90_us=%.1f p99_us=%.1f min_us=%.1f max_us=%.1f\n" kind samples (maybe "-" show waits) (at 0.5 / 1000) (at 0.9 / 1000) (at 0.99 / 1000) (minimum latencies / 1000) (maximum latencies / 1000)
