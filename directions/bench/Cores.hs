{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Measures what crosses cores. Run with @+RTS -T@ (allocation) and @-N@ for the cores it uses:
--
-- * @pingpong KIND ROUNDS@: round trips between two parts over tickets made as they go, in ns and bytes per round
--   trip. KIND is @local@ (two parts on one core, tickets of reach @'Here@), @compiled@ (the same, run by
--   'Compiled'), @anywhere@ (two parts on one core, tickets of reach @'Anywhere@), @compiled-anywhere@ (the same, run
--   by 'Compiled'), @remote@ (two cores, tickets of reach @'Anywhere@), @threads@ (two threads, tickets) or
--   @remote-threads@ (two threads, the tickets left in a handoff, as @remote@ does).
-- * @pool KIND CORES PARTS CONNECTIONS SECONDS@: parts that check a connection out of one pool, do a little work,
--   and release it, for the time given: checkouts per second, how evenly the parts and cores got them, and
--   allocation per checkout. KIND is @delimited@ (CORES cores of PARTS parts each) or @threads@ (the same number of
--   threads).
-- * @loans KIND VARIANT PARTS ROUNDS@: requests on a pool of buffers, PARTS parts that each make ROUNDS of them one
--   after another, in ns and bytes per request. KIND is @threads@, @delimited@ or @compiled@; VARIANT is @request@
--   (its admission and the end of its scope), @primary@ (and one borrow and release of the primary) or @both@
--   (and an extra loan, from the reserve). The pool has buffers for every part, so no request waits.
-- * @sockets KIND PAIRS ROUNDS@: PAIRS pairs of parts, each a client and a server over a socket pair of its own, that send a
--   byte to each other ROUNDS times each way: in ns and bytes per round trip. A round trip waits twice, once in each
--   part, so the cost of a wait on a descriptor is in it. KIND is @delimited@, @compiled@ or @threads@.
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

import Control.Arrow (Arrow, ArrowChoice, arr, returnA, second, (&&&), (***), (>>>), (^>>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (evaluate, finally)
import Control.Monad (replicateM, unless, void, when)
import Data.Bool (bool)
import Data.Functor (($>))
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, modifyIORef', newIORef, readIORef)
import Data.List (sort)
import Data.Maybe (isJust)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtr, withForeignPtr)
import Foreign.Storable (peek, poke)
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Directions.PingPong
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.BufferPool (BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, concurrently, done, iterateWith, keeping, localTicket, tap, times, timesWith, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (Report (..))
import Network.Streaming.Mode (Instrumented, ModeOf)
import qualified Network.Streaming.Loans as Loans
import Network.Streaming.Pool
import Network.Streaming.PositiveInt (mkPositiveInt, positiveInt)
import Network.Streaming.Threads
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
    ["loans", kind, variant, parts, rounds] -> loansRun kind variant (read parts) (read rounds)
    ["sockets", kind, pairs, rounds] -> socketsRun kind (read pairs) (read rounds)
    ["wake", kind, samples] -> wakeRun kind (read samples)
    ["scope", kind, variant, rounds] -> scopeRun kind variant (read rounds) 0
    ["scope", kind, variant, rounds, parts] -> scopeRun kind variant (read rounds) (read parts)
    ["cancel", kind, samples] -> cancelRun kind (read samples)
    ["within", kind, variant, rounds] -> withinRun kind variant (read rounds) 0
    ["within", kind, variant, rounds, parts] -> withinRun kind variant (read rounds) (read parts)
    ["deadline", kind, samples] -> deadlineRun kind (read samples)
    _ -> ioError (userError "usage: directions-cores-bench (pingpong KIND ROUNDS | pool KIND CORES PARTS CONNECTIONS SECONDS | loans KIND VARIANT PARTS ROUNDS | sockets KIND PAIRS ROUNDS | wake KIND SAMPLES | scope KIND VARIANT ROUNDS [PARTS] | cancel KIND SAMPLES | within KIND VARIANT ROUNDS [PARTS] | deadline KIND SAMPLES) +RTS -T -N..")

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
    "compiled-anywhere" -> void (runCompiled (pingPongAt @'Anywhere rounds) ())
    "remote" -> void (runCores 2 (pure handoff) (\shared core -> if core == 0 then find shared >>> client rounds else leave shared >>> server rounds))
    "threads" -> runThreads (pingPong rounds) ()
    "remote-threads" -> runThreads (concurrently (find handoff >>> client rounds) (leave handoff >>> server rounds) >>> done) ()
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
part :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool Int -> IORef Bool -> Counter -> arr () ()
part pool stop counter = iterateWith (now (const (bool (Again ()) (Finish ()) <$> readIORef stop))) turn
  where
    turn = checkout pool >>> now hold >>> release pool
    hold conn = (bump counter *> evaluate (work 40 conn)) $> conn
    work :: Int -> Int -> Int
    work 0 acc = acc
    work n acc = work (n - 1) (acc * 31 + n)

-- | Runs the arrows at once as a tree of pairs: with 'together', a chain, each thread is forked by the one before
-- it, and with threads that never wait, the last of a hundred starts when the others have had a time slice each.
balanced :: (Arrow arr, Concurrent arr, Exchange arr (Local arr)) => [arr () ()] -> arr () ()
balanced [] = returnA
balanced [one] = one
balanced parts' = let (front, back) = splitAt (length parts' `div` 2) parts' in concurrently (balanced front) (balanced back) >>> done

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

-- | A program that runs under every arrow that has what 'Runtime' bundles.
type RuntimeProgram a = forall arr. (Runtime arr) => arr () a

runRuntimeProgram :: String -> RuntimeProgram a -> IO a
runRuntimeProgram kind program = case kind of
  "threads" -> runThreads program ()
  "delimited" -> fst <$> runDelimited program ()
  "compiled" -> fst <$> runCompiled program ()
  _ -> ioError (userError ("no kind " <> kind))

loansRun :: String -> String -> Int -> Int -> IO ()
loansRun kind variant parts rounds = do
  let positive = maybe (ioError (userError "a count of a pool is positive")) pure . mkPositiveInt . toInteger
  pool <- do
    size <- positive 4096
    ordinary <- positive parts
    reserved <- positive parts
    setup <- positive 1
    newBufferPool (BufferPoolSettings size ordinary reserved setup)
  let body :: (Runtime arr) => arr Loans.Loans ()
      body = case variant of
        "request" -> done
        "primary" -> Loans.borrow >>> Loans.release
        "both" -> (Loans.borrow &&& Loans.borrow) >>> (Loans.release *** Loans.release) >>> done
        _ -> error ("loans: no variant " <> variant)
      oneRequest :: (Runtime arr) => arr () ()
      oneRequest = (pool,) ^>> Loans.withRequestBuffers (fst ^>> body)
      program :: RuntimeProgram ()
      program = case kind of
        "threads" -> balanced (replicate parts (times rounds oneRequest))
        _ -> together (replicate parts (times rounds oneRequest))
  (seconds, bytes) <- measured (runRuntimeProgram kind program)
  let requests = fromIntegral (parts * rounds) :: Double
  printf "loans %s %s parts=%d requests=%d ns_per_request=%.1f bytes_per_request=%.1f\n" kind variant parts (parts * rounds) (seconds * 1e9 / requests) (fromIntegral bytes / requests :: Double)

-- | A client and a server over a socket pair: the client sends a byte and waits for it to come back, and the server
-- sends back each byte it receives, for the rounds. The buffers are what each receives into.
socketPingPong :: (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => Buffer -> Buffer -> Int -> arr (Socket, Socket) ()
socketPingPong forClient forServer rounds =
  together [fst ^>> timesWith rounds clientStep >>> done, snd ^>> timesWith rounds serverStep >>> done]
  where
    clientStep =
      tap ((,[view forClient 1]) ^>> sendAll)
        >>> tap ((,roomOf forClient) ^>> receive >>> done)
    serverStep =
      tap ((,roomOf forServer) ^>> receive >>> done)
        >>> tap ((,[view forServer 1]) ^>> sendAll)

socketsRun :: String -> Int -> Int -> IO ()
socketsRun kind pairs rounds = do
  sockets <- replicateM pairs (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol)
  buffers <- replicateM pairs ((,) <$> newBuffer (positiveInt @16) <*> newBuffer (positiveInt @16))
  let program :: (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => arr () ()
      program = together [const ends ^>> socketPingPong forClient forServer rounds | (ends, (forClient, forServer)) <- zip sockets buffers]
  (seconds, bytes) <-
    measured
      ( case kind of
          "delimited" -> void (runDelimited program ())
          "compiled" -> void (runCompiled program ())
          "threads" -> runThreads program ()
          _ -> ioError (userError ("sockets: no kind " <> kind))
      )
      `finally` mapM_ (\(one, other) -> Socket.close one *> Socket.close other) sockets
  let trips = fromIntegral (pairs * rounds) :: Double
  printf "sockets %s pairs=%d rounds=%d ns_per_rt=%.1f bytes_per_rt=%.1f\n" kind pairs rounds (seconds * 1e9 / trips) (fromIntegral bytes / trips :: Double)

wakeRun :: String -> Int -> IO ()
wakeRun kind samples = do
  wake <- newWake
  waits <- case kind of
    "delimited" -> do
      reports <- runCoresReport 2 (pure wake) (\shared core -> if core == 0 then sleeper shared samples else waker shared 500 samples)
      pure (Just (sum (map (reportWaits . snd) (take 1 reports))))
    "threads" -> Nothing <$ runThreads (concurrently (sleeper wake samples) (waker wake 500 samples) >>> done) ()
    _ -> ioError (userError ("wake: no kind " <> kind))
  latencies <- sort . map fromIntegral <$> wakeLatencies wake :: IO [Double]
  let at :: Double -> Double
      at fraction = latencies !! min (length latencies - 1) (floor (fraction * fromIntegral (length latencies)))
  printf "wake %s samples=%d blocked=%s median_us=%.1f p90_us=%.1f p99_us=%.1f min_us=%.1f max_us=%.1f\n" kind samples (maybe "-" show waits) (at 0.5 / 1000) (at 0.9 / 1000) (at 0.99 / 1000) (minimum latencies / 1000) (maximum latencies / 1000)

-- | A program for every arrow that has scopes and time.
type Program a = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr, Concurrent arr, Exchange arr (Local arr), Scope arr, Race arr, Time arr, Within arr, Annotate arr) => arr () a

runProgram :: String -> Program a -> IO a
runProgram kind program = case kind of
  "threads" -> runThreads program ()
  "delimited" -> fst <$> runDelimited program ()
  "compiled" -> fst <$> runCompiled program ()
  _ -> ioError (userError ("no kind " <> kind))

-- | A part that holds nothing, and waits for ever in a scope of its own, once it has counted itself in.
waiter :: (Arrow arr, Effect arr, Exchange arr (Local arr), Scope arr) => IORef Int -> arr () ()
waiter waiting = scope returnA done (now (const (atomicModifyIORef' waiting (\n -> (n + 1, ())))) >>> localTicket >>> fst ^>> await)

-- | The loop in the first part (VARIANT @bare@), in a half of a pair, or, with that many parts that wait beside it,
-- in a half of a race with them, which begins once they all wait: so that the run of the loop and the run of
-- nothing both make and end the parts, and what is measured is the loop.
inside :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Concurrent arr, Race arr, Exchange arr (Local arr), Scope arr) => IORef Int -> String -> Int -> arr () () -> arr () ()
inside waiting variant parts loop
  | parts > 0 = race (untilJust ((\n -> if n == parts then Just () else Nothing) <$> readIORef waiting) >>> loop) (balanced (replicate parts (waiter waiting))) >>> done
  | variant == "bare" = loop
  | otherwise = concurrently loop returnA >>> done

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
        "pair" -> concurrently returnA returnA >>> done
        _ -> scope returnA done (arr snd)
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
      released = now (const (monotonicNanoseconds >>= \late -> readIORef stamp >>= \early -> modifyIORef' latencies ((late - early) :)))
      -- The other part waits a little first, so that this one waits when it ends.
      winner :: (Effect arr) => arr () ()
      winner = now (const (threadDelay 200 *> monotonicNanoseconds >>= atomicWriteIORef stamp))
      waits :: (Arrow arr, Exchange arr (Local arr)) => arr a ()
      waits = done >>> localTicket >>> fst ^>> await
      program :: Program ()
      program = timesWith samples (race (scope returnA released (snd ^>> waits)) winner >>> done)
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
  (limitedSeconds, limitedBytes) <- measuredInside kind variant parts (timesWith rounds (within (secs 10) returnA >>> done))
  printf "within %s %s rounds=%d parts=%d ns_per_within=%.1f bytes_per_within=%.1f\n" kind variant rounds parts ((limitedSeconds - baseSeconds) * 1e9 / fromIntegral rounds) (fromIntegral (limitedBytes - baseBytes) / fromIntegral rounds :: Double)

-- | How late a deadline passes: the time from setting it to the end of the part that waits under it, less the
-- deadline's own 5 ms.
deadlineRun :: String -> Int -> IO ()
deadlineRun kind samples = do
  lateness <- newIORef []
  let waits :: (Arrow arr, Exchange arr (Local arr)) => arr () ()
      waits = localTicket >>> fst ^>> await
      program :: Program ()
      program =
        timesWith samples $
          keeping (now (const monotonicNanoseconds))
            >>> second (within (millis 5) waits)
            >>> now (\(started, _) -> monotonicNanoseconds >>= \ended -> modifyIORef' lateness ((ended - started - 5000000) :))
  runProgram kind program
  sorted <- sort . map fromIntegral <$> readIORef lateness :: IO [Double]
  let at :: Double -> Double
      at fraction = sorted !! min (length sorted - 1) (floor (fraction * fromIntegral (length sorted)))
  printf "deadline %s samples=%d median_us=%.1f p90_us=%.1f p99_us=%.1f min_us=%.1f max_us=%.1f\n" kind samples (at 0.5 / 1000) (at 0.9 / 1000) (at 0.99 / 1000) (minimum sorted / 1000) (maximum sorted / 1000)
