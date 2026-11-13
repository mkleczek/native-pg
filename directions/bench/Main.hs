{-# LANGUAGE DataKinds #-}
{-# LANGUAGE QuantifiedConstraints #-}

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
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import Network.Directions.Proxy
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (Buffer, bufferCapacity, newBuffer)
import Network.Streaming.Classes
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Simulated
import Network.Streaming.Threads
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
  measure "threads, sockets" mib (overSockets (\built ends -> built >>= \program -> Nothing <$ runThreads program ends))
  measure "loop, sockets" mib (overSockets (\built ends -> built >>= \program -> Just . snd <$> runLoop program ends))
  measure "compiled, sockets" mib (overSockets (\built ends -> built >>= \program -> Just . snd <$> runCompiled program ends))
  measure "delimited, sockets" mib (overSockets (\built ends -> Just . snd <$> runDelimitedWith built ends))
  measure "loop, simulated" mib (overSimulated (\built ends -> built >>= \program -> Just . snd <$> runLoop program ends))
  measure "compiled, simulated" mib (overSimulated (\built ends -> built >>= \program -> Just . snd <$> runCompiled program ends))
  measure "delimited, simulated" mib (overSimulated (\built ends -> Just . snd <$> runDelimitedWith built ends))
  measure "simulation" (min mib 8) (overSimulated (\built ends -> built >>= \program -> Just . simulatedReport <$> runSimulation 1 maxBound program ends))

-- | Runs the round trip of this many MiB, and prints what it did per MiB.
measureRun :: String -> Int -> (Int -> IO (Maybe Report, Int)) -> IO ()
measureRun name mib run = do
  stats <- getRTSStatsEnabled
  before <- if stats then allocated_bytes <$> getRTSStats else pure 0
  started <- monotonicNanoseconds
  (report, received) <- run (mib * 1048576)
  ended <- monotonicNanoseconds
  after <- if stats then allocated_bytes <$> getRTSStats else pure 0
  unless (received == mib * 1048576) $ ioError (userError (name <> ": the client got back " <> show received <> " bytes"))
  let per :: (Integral a) => a -> String
      per count = printf "%.1f" (fromIntegral count / fromIntegral mib :: Double)
      counted field = maybe "-" (per . field) report
      allocation = if stats then per (after - before) else "(+RTS -T)"
  printf "%-24s %6d %9.1f %10s %10s %12s\n" name mib (fromIntegral mib * 1e9 / fromIntegral (ended - started) :: Double) (counted reportSteps) (counted reportWaits) allocation

-- | The round trip over socket pairs: the run's report, and how many bytes the
-- client got back.
overSockets :: (Built Socket -> Ends Socket -> IO (Maybe Report)) -> Int -> IO (Maybe Report, Int)
overSockets run size = withPair $ \client front -> withPair $ \back server -> roundTrip run size (client, front, back, server)
  where
    withPair :: (Socket -> Socket -> IO a) -> IO a
    withPair =
      bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
        . uncurry

-- | The round trip over simulated links that hold 64 KiB each way.
overSimulated :: (Built SimEnd -> Ends SimEnd -> IO (Maybe Report)) -> Int -> IO (Maybe Report, Int)
overSimulated run size = do
  (client, front) <- connect 65536
  (back, server) <- connect 65536
  roundTrip run size (client, front, back, server)

roundTrip :: (forall arr. (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr) => Link arr end) => (Built end -> Ends end -> IO (Maybe Report)) -> Int -> Ends end -> IO (Maybe Report, Int)
roundTrip run size ends = do
  received <- newIORef 0
  report <-
    run
      ( do
          forSending <- newBuffer (positiveInt @16384)
          forCounting <- newBuffer (positiveInt @16384)
          requests <- newBuffer (positiveInt @16384)
          answers <- newBuffer (positiveInt @16384)
          forServer <- newBuffer (positiveInt @16384)
          pure $
            together
              [ (\(client, _, _, _) -> client) ^>> sending forSending size,
                (\(client, _, _, _) -> client) ^>> counting forCounting received,
                (\(_, front, back, _) -> (front, back)) ^>> proxy requests answers,
                (\(_, _, _, server) -> server) ^>> echo forServer
              ]
      )
      ends
  (,) report <$> readIORef received

-- | The four ends of the round trip: the client's, the proxy's two, and the server's.
type Ends end = (end, end, end, end)

-- | A program made in IO, which each arrow runs, and which is given the ends.
type Built end = forall arr. (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr) => IO (arr (Ends end) ())

-- | Sends this many bytes from the buffer, whatever is in it, then ends the
-- output.
sending :: (ArrowChoice arr, Link arr end) => Buffer -> Int -> arr end ()
sending buffer total = (,total) ^>> loop
  where
    loop =
      (\(end, left) -> if left == 0 then Left end else Right (end, left))
        ^>> (shutdownSend ||| (chunk ^>> first sendAll >>> snd ^>> loop))
    chunk (end, left) = let count = min left (bufferCapacity buffer) in ((end, [view buffer count]), (end, left - count))

-- | Counts what arrives, until the end of the stream.
counting :: (ArrowChoice arr, Effect arr, Link arr end) => Buffer -> IORef Int -> arr end ()
counting buffer total = loop
  where
    loop =
      (\end -> ((end, roomOf buffer), end))
        ^>> first receive
        >>> (\(count, end) -> if count == 0 then Left () else Right (count, end))
        ^>> (returnA ||| (first (now (\count -> modifyIORef' total (+ count))) >>> snd ^>> loop))
