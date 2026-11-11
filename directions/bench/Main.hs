-- | Moves bytes from a client through the proxy to an echo server and back,
-- with each driver, and says what each did per MiB that went each way: how
-- long it took, how many steps and waits, and how much it allocated (with
-- @+RTS -T@). Usage: @directions-bench [MiB]@, 64 by default.
module Main (main) where

import Control.Exception (bracket)
import Control.Monad (unless)
import qualified Data.ByteString as BS
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.Maybe (listToMaybe)
import Foreign.Marshal.Utils (fillBytes)
import GHC.Clock (getMonotonicTime)
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import Network.Directions.Drive
import Network.Directions.Ends
import Network.Directions.Proxy
import Network.Directions.Simulated
import Network.Directions.Stage
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Link (Link)
import System.Environment (getArgs)
import Text.Printf (printf)

main :: IO ()
main = do
  mib <- maybe 64 read . listToMaybe <$> getArgs
  printf "%-22s %8s %9s %10s %10s %12s\n" "driver" "MiB" "MiB/s" "steps/MiB" "waits/MiB" "alloc/MiB"
  measure "threads, sockets" mib (overSockets runThreads)
  measure "one thread, sockets" mib (overSockets runLoop)
  measure "one thread, simulated" mib (overSimulated runLoop)
  measure "simulation" (min mib 8) (overSimulated (fmap simulatedReport . runSimulation 1 maxBound))

-- | Runs the round trip of this many MiB, and prints what it did per MiB.
measure :: String -> Int -> (Int -> IO (Report, Int)) -> IO ()
measure name mib run = do
  stats <- getRTSStatsEnabled
  before <- if stats then allocated_bytes <$> getRTSStats else pure 0
  started <- getMonotonicTime
  (Report steps waits, received) <- run (mib * 1048576)
  ended <- getMonotonicTime
  after <- if stats then allocated_bytes <$> getRTSStats else pure 0
  unless (received == mib * 1048576) $ ioError (userError (name <> ": the client got back " <> show received <> " bytes"))
  let per :: (Integral a) => a -> Double
      per count = fromIntegral count / fromIntegral mib
      allocation :: String
      allocation = if stats then printf "%12.0f" (per (after - before)) else printf "%12s" "(+RTS -T)"
  printf "%-22s %8d %9.1f %10.1f %10.1f %s\n" name mib (fromIntegral mib / (ended - started)) (per steps) (per waits) allocation

-- | The round trip over socket pairs: the run's report, and how many bytes the
-- client got back.
overSockets :: ([Stage] -> IO Report) -> Int -> IO (Report, Int)
overSockets run size = withPair $ \client front -> withPair $ \back server -> roundTrip run size client front back server
  where
    withPair :: (Socket -> Socket -> IO a) -> IO a
    withPair =
      bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
        . uncurry

-- | The round trip over simulated links that hold 64 KiB each way.
overSimulated :: ([Stage] -> IO Report) -> Int -> IO (Report, Int)
overSimulated run size = do
  (client, front) <- connect 65536
  (back, server) <- connect 65536
  roundTrip run size client front back server

roundTrip :: (Link c, HalfClose c, Link s, HalfClose s) => ([Stage] -> IO Report) -> Int -> c -> c -> s -> s -> IO (Report, Int)
roundTrip run size client front back server = do
  received <- newIORef 0
  report <- run ([pattern size ~> sending client, receiving client ~> counting received] <> proxy front back <> [echo server])
  (,) report <$> readIORef received

-- | Gives this many bytes of a pattern, without holding them.
pattern :: Int -> Stage
pattern total = Stage (pure total) step
  where
    step left _ output
      | left == 0 = pure Done
      | otherwise =
          withRoom output $ \pointer room ->
            if room == 0
              then pure (Await left [Room])
              else do
                let count = min room left
                fillBytes pointer 97 count
                Again (left - count) <$ produce output count

-- | Counts its input, until its input ends.
counting :: IORef Int -> Stage
counting total = Stage (pure ()) step
  where
    step () input _ = do
      count <- BS.length <$> pendingBytes input
      if count == 0
        then (\ended -> if ended then Done else Await () [Input]) <$> inputEnded input
        else Again () <$ (modifyIORef' total (+ count) *> consume input count)
