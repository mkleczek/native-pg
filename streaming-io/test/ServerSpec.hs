module ServerSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Control.Concurrent (forkFinally, killThread, newEmptyMVar, putMVar, readMVar)
import Control.Exception (bracket, throwIO)
import Control.Monad (void)
import qualified Data.ByteString as B
import Data.IORef (atomicModifyIORef', newIORef)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as Wire
import Network.Streaming.Classes (Block (block), Effect (now), Event (Readable), Link (awaitInput, sendAll), Race (race))
import Network.Streaming.Combinators (done, tap)
import Network.Streaming.Delimited (Delimited, runDelimited)
import Network.Streaming.Server (ServerCore, borrowed, newServerCores, serveConnections)
import Skeletest
import System.Posix.Types (Fd (..))
import System.Timeout (timeout)

spec :: Spec
spec = describe "connection server" $
  it "frees a failed worker's slot and closes a running worker on cancellation" $
    bracket tcpSocket Socket.close $ \listener -> do
      Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
      Socket.listen listener 8
      address <- Socket.getSocketName listener
      bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(reader, writer) -> Socket.close reader *> Socket.close writer) $ \(reader, writer) -> do
        stopping <- Readable . Fd <$> Socket.unsafeFdSocket reader
        cores <- newServerCores 1
        first <- newIORef True
        stopped <- newEmptyMVar
        let start (socket, reply) = do
              failFirst <- atomicModifyIORef' first (\isFirst -> (False, isFirst))
              if failFirst then ioError (userError "worker failed") else pure (socket, [reply])
            worker :: Delimited s (Socket, B.ByteString) ()
            worker = now start >>> tap sendAll >>> (fst ^>> awaitInput)
            server :: Delimited s (ServerCore, B.ByteString) ()
            server = race (const stopping ^>> block) (serveConnections 1 (borrowed listener) worker) >>> done
        bracket (forkFinally (void (runDelimited server (cores, B.singleton 42))) (putMVar stopped)) killThread $ \_ -> do
          bracket tcpSocket Socket.close $ \client -> do
            Socket.connect client address
            closed <- timeout 5000000 (Wire.recv client 1)
            closed `shouldBe` Just B.empty
          bracket tcpSocket Socket.close $ \client -> do
            Socket.connect client address
            reply <- timeout 5000000 (Wire.recv client 1)
            reply `shouldBe` Just (B.singleton 42)
            Wire.sendAll writer (B.singleton 1)
            ended <- timeout 5000000 (readMVar stopped)
            case ended of
              Just (Right ()) -> pure ()
              Just (Left failure) -> throwIO failure
              Nothing -> failTest "server did not stop"
            closed <- timeout 5000000 (Wire.recv client 1)
            closed `shouldBe` Just B.empty
  where
    tcpSocket :: IO Socket
    tcpSocket = Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
