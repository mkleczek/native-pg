{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- | Measures what crosses cores. Run with @+RTS -T@ (allocation) and @-N@ for the cores it uses:
--
-- * @pingpong KIND ROUNDS@: round trips between two parts over tickets made as they go, in ns and bytes per round
--   trip. KIND is @local@ (two parts on one core, tickets of reach @'Here@), @compiled@ (the same, run by
--   'Compiled'), @anywhere@ (two parts on one core, tickets of reach @'Anywhere@), @remote@ (two cores, tickets of
--   reach @'Anywhere@), @threads@ (two threads, tickets) or @remote-threads@ (two threads, the tickets left in a
--   handoff, as @remote@ does).
-- * @pool KIND CORES PARTS CONNECTIONS SECONDS@: parts that check a connection out of one pool, do a little work,
--   and release it, for the time given: checkouts per second, how evenly the parts and cores got them, and
--   allocation per checkout. KIND is @delimited@ (CORES cores of PARTS parts each) or @threads@ (the same number of
--   threads).
-- * @wake KIND SAMPLES@: how long a part waiting for a ticket on a core whose scheduler waits in its poller takes
--   to go on after another core gives it a value. KIND is @delimited@ or @threads@.
-- * @scope KIND VARIANT ROUNDS [PARTS]@: what a scope costs, entered and left, in ns and bytes, above the loop it is
--   in. KIND is @threads@, @delimited@ or @compiled@; VARIANT is @bare@ (in the first part, which is in no cancel
--   scope), @half@ (in a half of a concurrent pair, which is in one) or @pair@ (what a concurrent pair costs, made
--   in a half of one: its scope and its two parts). With PARTS, that many parts wait for ever beside the loop, each
--   in a scope, as the parts of a server wait for their next request (the loop is then in a half of a race with
--   them: VARIANT @half@ or @pair@, which begins once they all wait). The seconds of the run of nothing and of the
--   run with the scopes are printed too: with PARTS, a run of nothing is making the parts and cancelling them.
-- * @cancel KIND SAMPLES@: how long a part that waits for a ticket takes to run its release, after the first to end
--   of a race, which cancels it, has ended. KIND is as above.
-- * @within KIND VARIANT ROUNDS [PARTS]@: what a deadline costs that doesn't pass, set and cancelled, in ns and bytes,
--   above the loop it is in; the timer wheel of the scheduler (@delimited@, @compiled@), or GHC's timer manager
--   (@threads@). KIND, VARIANT and PARTS are as for @scope@.
-- * @deadline KIND SAMPLES@: how late a deadline of 5 ms passes, on a scheduler that has nothing else to do.
module Main (main) where

import Control.Arrow (Arrow, ArrowChoice, arr, returnA, second, (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (evaluate)
import Control.Monad (replicateM, unless, void, when)
import Data.Functor (($>))
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, modifyIORef', newIORef, readIORef)
import Data.List (sort)
import Data.Maybe (isJust)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtr, withForeignPtr)
import Foreign.Storable (peek, poke)
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import Network.Directions.Classes
import Network.Directions.Combinators (keeping, timesWith, untilJust)
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
    ["scope", kind, variant, rounds] -> scopeRun kind variant (read rounds) 0
    ["scope", kind, variant, rounds, parts] -> scopeRun kind variant (read rounds) (read parts)
    ["cancel", kind, samples] -> cancelRun kind (read samples)
    ["within", kind, variant, rounds] -> withinRun kind variant (read rounds) 0
    ["within", kind, variant, rounds, parts] -> withinRun kind variant (read rounds) (read parts)
    ["deadline", kind, samples] -> deadlineRun kind (read samples)
    _ -> ioError (userError "usage: directions-cores-bench (pingpong KIND ROUNDS | pool KIND CORES PARTS CONNECTIONS SECONDS | wake KIND SAMPLES | scope KIND VARIANT ROUNDS [PARTS] | cancel KIND SAMPLES | within KIND VARIANT ROUNDS [PARTS] | deadline KIND SAMPLES) +RTS -T -N..")

-- | Bytes allocated by every thread so far, once a major collection has counted them.
allocated :: IO Integer
allocated = performMajorGC *> (fromIntegral . allocated_bytes <$> getRTSStats)

-- | How long the action took, in seconds, and how many bytes it allocated.
measured :: IO () -> IO (Double, Integer)
measured action = do
  before <- allocated
  started <- monotonicNanoseconds
  action
  ended <- monotonicNanoseconds
  after <- allocated
  pure (fromIntegral (ended - started) / 1e9, after - before)

pingPongRun :: String -> Int -> IO ()
pingPongRun kind rounds = do
  handoff <- newHandoff
  (seconds, bytes) <- measured $ case kind of
    "local" -> void (runDelimited (pingPong rounds) ())
    "compiled" -> void (runCompiled (pingPong rounds) ())
    "anywhere" -> void (runDelimited (pingPongAt @'Anywhere rounds) ())
    "remote" -> void (runCores 2 (pure handoff) (\shared core -> if core == 0 then find shared >>> client rounds else leave shared >>> server rounds))
    "threads" -> runThreads (pingPong rounds) ()
    "remote-threads" -> runThreads (concurrently (find handoff >>> client rounds) (leave handoff >>> server rounds) >>> arr (const ())) ()
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
part :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool Int -> IORef Bool -> Counter -> arr () ()
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

-- | A program for every arrow that has scopes and time.
type Program a = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr, Concurrent arr, Exchange arr (Local arr), Scope arr, Race arr, Time arr, Within arr) => arr () a

runProgram :: String -> Program a -> IO a
runProgram kind program = case kind of
  "threads" -> runThreads program ()
  "delimited" -> fst <$> runDelimited program ()
  "compiled" -> fst <$> runCompiled program ()
  _ -> ioError (userError ("no kind " <> kind))

-- | A part that holds nothing, and waits for ever in a scope of its own, once it has counted itself in.
waiter :: forall arr. (Arrow arr, Effect arr, Exchange arr (Local arr), Scope arr) => IORef Int -> arr () ()
waiter waiting = scope returnA (arr (const ())) (arr snd >>> now (\() -> atomicModifyIORef' waiting (\n -> (n + 1, ()))) >>> ticket @arr @(Local arr) >>> arr fst >>> await)

-- | The loop in the first part (VARIANT @bare@), in a half of a pair, or, with that many parts that wait beside it,
-- in a half of a race with them, which begins once they all wait: so that the run of the loop and the run of
-- nothing both make and end the parts, and what is measured is the loop.
inside :: (ArrowChoice arr, Effect arr, GiveWay arr, Concurrent arr, Race arr, Exchange arr (Local arr), Scope arr) => IORef Int -> String -> Int -> arr () () -> arr () ()
inside waiting variant parts loop
  | parts > 0 = race (untilJust ((\n -> if n == parts then Just () else Nothing) <$> readIORef waiting) >>> loop) (balanced (replicate parts (waiter waiting))) >>^ const ()
  | variant == "bare" = loop
  | otherwise = concurrently loop returnA >>^ const ()

-- | Measures the program that @inside@ makes of the loop: the time and the bytes.
measuredInside :: String -> String -> Int -> Program () -> IO (Double, Integer)
measuredInside kind variant parts loop = newIORef 0 >>= \waiting -> measured (runProgram kind (inside waiting variant parts loop))

-- | Rounds of a scope, and of nothing, in the part or in a half of a pair, with that many parts that wait beside
-- it: the difference, per round.
scopeRun :: String -> String -> Int -> Int -> IO ()
scopeRun kind variant rounds parts = do
  when (parts > 0 && variant == "bare") $ ioError (userError "scope: with PARTS the loop is in a half: VARIANT half or pair")
  let step :: Program ()
      step = case variant of
        "pair" -> concurrently returnA returnA >>^ const ()
        _ -> scope returnA (arr (const ())) (arr snd)
  -- Once to warm up, then each measured.
  void (measuredInside kind variant parts (timesWith rounds returnA))
  (baseSeconds, baseBytes) <- measuredInside kind variant parts (timesWith rounds returnA)
  (scopeSeconds, scopeBytes) <- measuredInside kind variant parts (timesWith rounds step)
  printf "scope %s %s rounds=%d parts=%d ns_per_scope=%.1f bytes_per_scope=%.1f base_s=%.4f scoped_s=%.4f\n" kind variant rounds parts ((scopeSeconds - baseSeconds) * 1e9 / fromIntegral rounds) (fromIntegral (scopeBytes - baseBytes) / fromIntegral rounds :: Double) baseSeconds scopeSeconds

-- | How long a part that waits takes to run its release, after the part that cancels it has ended.
cancelRun :: String -> Int -> IO ()
cancelRun kind samples = do
  stamp <- newIORef 0
  latencies <- newIORef []
  let released :: (Effect arr) => arr a ()
      released = now (\_ -> monotonicNanoseconds >>= \late -> readIORef stamp >>= \early -> modifyIORef' latencies ((late - early) :))
      -- The other part waits a little first, so that this one waits when it ends.
      winner :: (Effect arr) => arr () ()
      winner = now (\() -> threadDelay 200 *> monotonicNanoseconds >>= atomicWriteIORef stamp)
      waits :: forall arr a. (Arrow arr, Exchange arr (Local arr)) => arr a ()
      waits = arr (const ()) >>> ticket @arr @(Local arr) >>> arr fst >>> await
      program :: Program ()
      program = timesWith samples (race (scope returnA released (arr snd >>> waits)) winner >>^ const ())
  runProgram kind program
  sorted <- sort . map fromIntegral <$> readIORef latencies :: IO [Double]
  let at :: Double -> Double
      at fraction = sorted !! min (length sorted - 1) (floor (fraction * fromIntegral (length sorted)))
  printf "cancel %s samples=%d median_us=%.1f p90_us=%.1f p99_us=%.1f min_us=%.1f max_us=%.1f\n" kind samples (at 0.5 / 1000) (at 0.9 / 1000) (at 0.99 / 1000) (minimum sorted / 1000) (maximum sorted / 1000)

-- | Rounds of a deadline that doesn't pass, and of nothing, in the part or in a half of a pair, with that many parts
-- that wait beside it: the difference, per round.
withinRun :: String -> String -> Int -> Int -> IO ()
withinRun kind variant rounds parts = do
  when (parts > 0 && variant == "bare") $ ioError (userError "within: with PARTS the loop is in a half: VARIANT half")
  void (measuredInside kind variant parts (timesWith rounds returnA))
  (baseSeconds, baseBytes) <- measuredInside kind variant parts (timesWith rounds returnA)
  (limitedSeconds, limitedBytes) <- measuredInside kind variant parts (timesWith rounds (within (secs 10) returnA >>^ const ()))
  printf "within %s %s rounds=%d parts=%d ns_per_within=%.1f bytes_per_within=%.1f\n" kind variant rounds parts ((limitedSeconds - baseSeconds) * 1e9 / fromIntegral rounds) (fromIntegral (limitedBytes - baseBytes) / fromIntegral rounds :: Double)

-- | How late a deadline passes: the time from setting it to the end of the part that waits under it, less the
-- deadline's own 5 ms.
deadlineRun :: String -> Int -> IO ()
deadlineRun kind samples = do
  lateness <- newIORef []
  let waits :: forall arr. (Arrow arr, Exchange arr (Local arr)) => arr () ()
      waits = ticket @arr @(Local arr) >>> arr fst >>> await
      program :: Program ()
      program =
        timesWith samples $
          keeping (now (\() -> monotonicNanoseconds))
            >>> second (within (millis 5) waits)
            >>> now (\(started, _) -> monotonicNanoseconds >>= \ended -> modifyIORef' lateness ((ended - started - 5000000) :))
  runProgram kind program
  sorted <- sort . map fromIntegral <$> readIORef lateness :: IO [Double]
  let at :: Double -> Double
      at fraction = sorted !! min (length sorted - 1) (floor (fraction * fromIntegral (length sorted)))
  printf "deadline %s samples=%d median_us=%.1f p90_us=%.1f p99_us=%.1f min_us=%.1f max_us=%.1f\n" kind samples (at 0.5 / 1000) (at 0.9 / 1000) (at 0.99 / 1000) (minimum sorted / 1000) (maximum sorted / 1000)
