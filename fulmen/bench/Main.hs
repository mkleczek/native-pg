{-# LANGUAGE OverloadedStrings #-}

-- | A quick measurement of the server: it serves a static body of 16 KiB or of 1 MiB (in 64 pieces of 16 KiB) to
-- keep-alive clients over loopback, and says how many requests it served in a second and how many bytes it allocated
-- for each.
--
-- It has two modes, which are run as two processes:
--
-- > fulmen-bench server delimited|threads CORES CONNECTIONS +RTS -T -N2
-- > fulmen-bench client PORT CONNECTIONS SECONDS /16k|/1m
--
-- The server prints the port it listens on, and serves until it is killed. The client asks the server to note its
-- allocation (@GET /reset@), runs its connections for the time with the path, and asks for the difference
-- (@GET /report@), over a connection of its own that it keeps all the while: the server needs a worker for it besides
-- the ones for the connections that the client runs. The server is the arrow runtime's, the client is plain IO: a GHC thread for each connection, which
-- blocks on its socket. The server builds its program where its arrow is known, in 'server', so that GHC specialises it to the arrow.
module Main (main) where

import Control.Arrow (first, (&&&), (>>>), (^>>))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (finally)
import Control.Monad (replicateM, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.Char (toLower)
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, newIORef, readIORef)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Stats (RTSStats (..), getRTSStats)
import qualified Network.Fulmen as Fulmen
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.BufferPool (BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes (Effect (..), Runtime)
import Network.Streaming.Combinators (consume)
import Network.Streaming.Delimited (runCores)
import Network.Streaming.PositiveInt (mkPositiveInt)
import Network.Streaming.Threads (runThreads)
import System.Environment (getArgs)
import System.Exit (die)
import System.IO (BufferMode (..), hSetBuffering, stdout)

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["server", kind, cores, connections] -> server kind (read cores) (read connections)
    ["client", port, connections, seconds, path] -> client (read port) (read connections) (read seconds) (C.pack path)
    _ -> die "usage: fulmen-bench server delimited|threads CORES CONNECTIONS | client PORT CONNECTIONS SECONDS /16k|/1m"

-- * The server

-- | What the server counts for the report: the requests, and the allocation and request count when it was asked to
-- note them.
data Counters = Counters
  { requests :: !(IORef Int),
    marked :: !(IORef (Word64, Int))
  }

server :: String -> Int -> Int -> IO ()
server kind cores connections = do
  hSetBuffering stdout LineBuffering
  listening <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
  Socket.setSocketOption listening Socket.ReuseAddr 1
  Socket.bind listening (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
  Socket.listen listening 1024
  port <- Socket.socketPort listening
  let positive n = maybe (die "a count that is not positive") pure (mkPositiveInt n)
  size <- positive 16384
  ordinary <- positive (fromIntegral connections + 2)
  reserve <- positive 2
  setup <- positive 1
  pool <- newBufferPool (BufferPoolSettings size ordinary reserve setup)
  counters <- Counters <$> newIORef 0 <*> newIORef (0, 0)
  let settings = (Fulmen.defaultSettings listening pool) {Fulmen.connectionsPerCore = connections, Fulmen.clientTimeout = Nothing}
  shared <- Fulmen.newCores cores
  putStrLn ("port " <> show port)
  case kind of
    "delimited" -> void (runCores cores (pure shared) (\cores' core -> const (Fulmen.onCore cores' core) ^>> Fulmen.serve settings (handler counters)))
    "threads" -> runThreads (Fulmen.serve settings (handler counters)) shared
    _ -> die "the server is delimited or threads"

-- | The 16 KiB body, and the 1 MiB body as 64 pieces of 16 KiB, which are made once.
small :: ByteString
small = B.replicate 16384 120
{-# NOINLINE small #-}

large :: [ByteString]
large = [B.copy (B.replicate 16384 121) | _ <- [1 .. 64 :: Int]]
{-# NOINLINE large #-}

-- | Answers each request with the body its path names, and the two requests that the client makes of the server
-- itself.
handler :: (Runtime arr) => Counters -> arr (Fulmen.Call, Fulmen.Request) ()
handler counters = now (react counters) >>> answer

-- | The pieces of the answer, and their length.
react :: Counters -> (Fulmen.Call, Fulmen.Request) -> IO (Fulmen.Call, ([ByteString], Int))
react counters (call, request) = case Fulmen.target request of
  "/16k" -> count >> pure (call, ([small], 16384))
  "/1m" -> count >> pure (call, (large, 1048576))
  "/reset" -> do
    stats <- getRTSStats
    served <- readIORef (requests counters)
    atomicWriteIORef (marked counters) (allocated_bytes stats, served)
    pure (call, (["ok"], 2))
  "/report" -> do
    stats <- getRTSStats
    served <- readIORef (requests counters)
    (allocated, before) <- readIORef (marked counters)
    let text = C.pack ("requests=" <> show (served - before) <> " allocated=" <> show (allocated_bytes stats - allocated))
    pure (call, ([text], B.length text))
  _ -> pure (call, (["?"], 1))
  where
    count = atomicModifyIORef' (requests counters) (\n -> (n + 1, ()))

-- | The head with the length, the pieces, and the end.
answer :: (Runtime arr) => arr (Fulmen.Call, ([ByteString], Int)) ()
answer =
  withHead
    ^>> first Fulmen.respond
    >>> (id &&& fst)
    ^>> consume Fulmen.send
    >>> Fulmen.finish
  where
    withHead (call, (pieces, size)) =
      ((call, (Fulmen.Status 200, [("Content-Type", "application/octet-stream")], Fulmen.Known size)), pieces)

-- * The client

client :: Int -> Int -> Double -> ByteString -> IO ()
client port connections seconds path = do
  control <- connectTo port
  _ <- roundTrip control "/reset"
  served <- newIORef (0 :: Int)
  received <- newIORef (0 :: Int)
  started <- getMonotonicTimeNSec
  let deadline = started + round (seconds * 1e9)
      connection = do
        socket <- connectTo port
        let go = do
              now' <- getMonotonicTimeNSec
              if now' >= deadline
                then Socket.close socket
                else do
                  (size, _) <- roundTrip socket path
                  atomicModifyIORef' served (\n -> (n + 1, ()))
                  atomicModifyIORef' received (\n -> (n + size, ()))
                  go
        go
  finished' <- replicateM connections newEmptyMVar
  mapM_ (\done -> forkIO (connection `finally` putMVar done ())) finished'
  mapM_ takeMVar finished'
  stopped <- getMonotonicTimeNSec
  (_, report) <- roundTrip control "/report"
  total <- readIORef served
  bytes <- readIORef received
  let elapsed = fromIntegral (stopped - started) / 1e9 :: Double
  putStrLn
    ( show total
        <> " requests in "
        <> show elapsed
        <> " s: "
        <> show (fromIntegral total / elapsed)
        <> " requests/s, "
        <> show (fromIntegral bytes / elapsed / 1048576 :: Double)
        <> " MiB/s; server: "
        <> C.unpack report
    )

connectTo :: Int -> IO Socket
connectTo port = do
  socket <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
  Socket.connect socket (Socket.SockAddrInet (fromIntegral port) (Socket.tupleToHostAddress (127, 0, 0, 1)))
  Socket.setSocketOption socket Socket.NoDelay 1
  pure socket

-- | One request, and the whole of its response: the length of its body, and the body if it is short.
roundTrip :: Socket -> ByteString -> IO (Int, ByteString)
roundTrip socket target = do
  SocketBytes.sendAll socket ("GET " <> target <> " HTTP/1.1\r\nHost: bench\r\n\r\n")
  headPart <- readHead B.empty
  let (headBytes, rest) = B.breakSubstring "\r\n\r\n" headPart
      afterHead = B.drop 4 rest
      size = contentLength headBytes
  body <- readBody (size - B.length afterHead) (B.take size afterHead)
  pure (size, body)
  where
    readHead acc
      | "\r\n\r\n" `B.isInfixOf` acc = pure acc
      | otherwise = do
          chunk <- SocketBytes.recv socket 65536
          if B.null chunk then die "the server closed the connection" else readHead (acc <> chunk)
    -- The bytes of a body that is long are dropped as they come: the client counts them, and keeps the short ones.
    readBody missing kept
      | missing <= 0 = pure kept
      | otherwise = do
          chunk <- SocketBytes.recv socket 65536
          if B.null chunk
            then die "the server closed the connection"
            else readBody (missing - B.length chunk) (if B.length kept < 256 then kept <> B.take missing chunk else kept)

-- | The value of the @Content-Length@ field of a head.
contentLength :: ByteString -> Int
contentLength headBytes = case [value | line <- C.lines headBytes, let (name, value) = C.break (== ':') line, C.map toLowerAscii name == "content-length"] of
  value : _ -> maybe 0 fst (C.readInt (C.dropWhile (== ' ') (C.drop 1 value)))
  [] -> 0
  where
    toLowerAscii = toLower
