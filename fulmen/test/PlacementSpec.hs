{-# LANGUAGE OverloadedStrings #-}

-- | Where a server that runs on several cores puts its new connections: a client opens connections and keeps them, and
-- each core is to serve as many of them as the other, instead of one core serving them all while the other has workers
-- that are free.
module PlacementSpec (spec) where

import Control.Arrow ((&&&), (>>>), (^>>))
import Control.Concurrent (forkFinally, getNumCapabilities, killThread, myThreadId, rtsSupportsBoundThreads, setNumCapabilities, threadCapability, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar)
import Control.Exception (bracket)
import Control.Monad (forM_, replicateM, void, when)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.List (sort)
import Network.Fulmen (Call, Request)
import qualified Network.Fulmen as Fulmen
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.Classes
import Network.Streaming.Combinators (consume, keeping)
import Network.Streaming.Delimited (runCores)
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = parallelCores "a core's thread is pinned to the capability of its number, and the non-threaded run time has one" $ describe "placement" $
  it "gives new connections that are kept open to the cores in turn, when the workers that are free are all of one core" $ do
    -- Each core runs on the capability of its number.
    getNumCapabilities >>= \capabilities -> when (capabilities < 2) (setNumCapabilities 2)
    bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
      Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
      Socket.listen accepting 128
      port <- Socket.socketPort accepting
      let settings = Fulmen.defaultSettings {Fulmen.connectionsPerCore = 4}
          serving cores =
            runCores 2 (pure cores) (\shared core -> const (Fulmen.onCore shared core) ^>> Fulmen.serve settings (Fulmen.borrowed accepting) answerWithCore)
      cores <- Fulmen.newCores settings 2
      stopped <- newEmptyMVar
      bracket (forkFinally (void (serving cores)) (putMVar stopped)) killThread $ \_ -> do
        -- The workers of both cores offer themselves at the start, and the connections come when they all have.
        threadDelay 200000
        placed <- timeout 20000000 $
          -- Every worker serves a connection, so the acceptor waits for a worker, and has the first that is freed: one
          -- of core 0. Then the workers of core 1 are freed, and last the others of core 0: they are the free workers that
          -- the pool gives first, the one that was freed last first.
          bracket (replicateM 8 (opened port)) (mapM_ (Socket.close . fst)) $ \held -> do
            let (firstFreed, lastFreed) = splitAt 1 (on 0 held)
            forM_ [firstFreed, on 1 held, lastFreed] $ \group -> do
              mapM_ (Socket.close . fst) group
              threadDelay 100000
            bracket (replicateM 4 (opened port)) (mapM_ (Socket.close . fst)) (pure . sort . map snd)
        placed `shouldBe` Just [0, 0, 1, 1 :: Int]

-- | Answers with the number of the core that runs the handler, as a digit: a core's thread is pinned to the capability
-- of its number.
answerWithCore :: (Runtime arr) => arr (Call, Request) ()
answerWithCore =
  (,(Fulmen.Status 200, [], Fulmen.Known 1))
    . fst
    ^>> Fulmen.respond
    >>> keeping (now (const core))
    >>> ((\(digit, answering) -> (answering, [digit])) &&& snd)
    ^>> consume Fulmen.send
    >>> Fulmen.finish
  where
    core = (\(capability, _) -> C.pack (show capability)) <$> (threadCapability =<< myThreadId)

-- | The connections that the server's core of this number serves.
on :: Int -> [(Socket, Int)] -> [(Socket, Int)]
on core = filter ((== core) . snd)

-- | A new connection to the server, and the number of the core that answers a request on it.
opened :: Socket.PortNumber -> IO (Socket, Int)
opened port = do
  sock <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
  Socket.connect sock (Socket.SockAddrInet port (Socket.tupleToHostAddress (127, 0, 0, 1)))
  (,) sock <$> askCore sock

-- | Sends a request on the connection, and gives the number of the core that answered it.
askCore :: Socket -> IO Int
askCore sock = do
  SocketBytes.sendAll sock "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
  answer <- readAnswer B.empty
  pure (read (C.unpack (B.takeEnd 1 answer)))
  where
    readAnswer sofar
      | B.length (snd (B.breakSubstring "\r\n\r\n" sofar)) > 4 = pure sofar
      | otherwise = SocketBytes.recv sock 4096 >>= \bytes -> if B.null bytes then failTest ("the connection ended before the answer: " <> show sofar) else readAnswer (sofar <> bytes)

-- | Skips the case on the non-threaded run time, which has one capability.
parallelCores :: String -> Spec -> Spec
parallelCores reason = if rtsSupportsBoundThreads then id else skip reason
