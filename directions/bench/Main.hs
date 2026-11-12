-- | Moves bytes from a client through the proxy to an echo server and back,
-- with each way to run the program, and says what each did per MiB that went
-- each way: how fast, how many steps and waits (one thread only), and how much
-- it allocated (with @+RTS -T@). A step of 'Loop' is any primitive; one of
-- 'Compiled' and 'Delimited' is a receive, a send, a wait, or a ticket.
-- Usage: @directions-bench [MiB [run]]@: 64 MiB by default, and every run
-- unless one is named.
module Main (main) where

import Control.Arrow (ArrowChoice, first, returnA, (>>>), (^>>), (|||))
import Control.Exception (bracket)
import Control.Monad (unless, when)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.List (isPrefixOf)
import Data.Maybe (listToMaybe)
import GHC.Clock (getMonotonicTime)
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import Network.Directions.Classes
import Network.Directions.Compiled
import Network.Directions.Delimited
import Network.Directions.Loop
import Network.Directions.Proxy
import Network.Directions.Simulated
import Network.Directions.Threads
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import System.Environment (getArgs)
import Text.Printf (printf)

main :: IO ()
main = do
  arguments <- getArgs
  let mib = maybe 64 read (listToMaybe arguments)
      -- A second argument picks the runs whose names start with it.
      picked name = all (`isPrefixOf` name) (take 1 (drop 1 arguments))
      measure name size run = when (picked name) (measureRun name size run)
  printf "%-24s %6s %9s %10s %10s %12s\n" "run" "MiB" "MiB/s" "steps/MiB" "waits/MiB" "alloc/MiB"
  measure "threads, sockets" mib (overSockets (\program -> Nothing <$ runThreads program ()))
  measure "loop, sockets" mib (overSockets (\program -> Just . snd <$> runLoop program ()))
  measure "compiled, sockets" mib (overSockets (\program -> Just . snd <$> runCompiled program ()))
  measure "delimited, sockets" mib (overSockets (\program -> Just . snd <$> runDelimited program ()))
  measure "loop, simulated" mib (overSimulated (\program -> Just . snd <$> runLoop program ()))
  measure "compiled, simulated" mib (overSimulated (\program -> Just . snd <$> runCompiled program ()))
  measure "delimited, simulated" mib (overSimulated (\program -> Just . snd <$> runDelimited program ()))
  measure "simulation" (min mib 8) (overSimulated (\program -> Just . simulatedReport <$> runSimulation 1 maxBound program ()))

-- | Runs the round trip of this many MiB, and prints what it did per MiB.
measureRun :: String -> Int -> (Int -> IO (Maybe Report, Int)) -> IO ()
measureRun name mib run = do
  stats <- getRTSStatsEnabled
  before <- if stats then allocated_bytes <$> getRTSStats else pure 0
  started <- getMonotonicTime
  (report, received) <- run (mib * 1048576)
  ended <- getMonotonicTime
  after <- if stats then allocated_bytes <$> getRTSStats else pure 0
  unless (received == mib * 1048576) $ ioError (userError (name <> ": the client got back " <> show received <> " bytes"))
  let per :: (Integral a) => a -> String
      per count = printf "%.1f" (fromIntegral count / fromIntegral mib :: Double)
      counted field = maybe "-" (per . field) report
      allocation = if stats then per (after - before) else "(+RTS -T)"
  printf "%-24s %6d %9.1f %10s %10s %12s\n" name mib (fromIntegral mib / (ended - started) :: Double) (counted reportSteps) (counted reportWaits) allocation

-- | The round trip over socket pairs: the run's report, and how many bytes the
-- client got back.
overSockets :: (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr) => (arr () () -> IO (Maybe Report)) -> Int -> IO (Maybe Report, Int)
overSockets run size = withPair $ \client front -> withPair $ \back server -> roundTrip run size (client, front, back, server)
  where
    withPair :: (Socket -> Socket -> IO a) -> IO a
    withPair =
      bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
        . uncurry

-- | The round trip over simulated links that hold 64 KiB each way.
overSimulated :: (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr) => (arr () () -> IO (Maybe Report)) -> Int -> IO (Maybe Report, Int)
overSimulated run size = do
  (client, front) <- connect 65536
  (back, server) <- connect 65536
  roundTrip run size (client, front, back, server)

roundTrip :: (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => (arr () () -> IO (Maybe Report)) -> Int -> (end, end, end, end) -> IO (Maybe Report, Int)
roundTrip run size (client, front, back, server) = do
  received <- newIORef 0
  forSending <- newBuffer 16384
  forCounting <- newBuffer 16384
  requests <- newBuffer 16384
  answers <- newBuffer 16384
  forServer <- newBuffer 16384
  report <-
    run $
      together
        [ sending forSending size client,
          counting forCounting client received,
          proxy requests answers front back,
          echo forServer server
        ]
  (,) report <$> readIORef received

-- | Sends this many bytes from the buffer, whatever is in it, then ends the
-- output.
sending :: (ArrowChoice arr, Link arr end) => Buffer -> Int -> end -> arr () ()
sending buffer total end = const total ^>> loop
  where
    loop =
      (\left -> if left == 0 then Left () else Right left)
        ^>> (shutdownSend end ||| (chunk ^>> first (sendAll end) >>> snd ^>> loop))
    chunk left = let count = min left (bufferSize buffer) in ([view buffer count], left - count)

-- | Counts what arrives, until the end of the stream.
counting :: (ArrowChoice arr, Effect arr, Link arr end) => Buffer -> end -> IORef Int -> arr () ()
counting buffer end total = loop
  where
    loop =
      const (roomOf buffer)
        ^>> receive end
        >>> (\count -> if count == 0 then Left () else Right count)
        ^>> (returnA ||| (now (\count -> modifyIORef' total (+ count)) >>> loop))
