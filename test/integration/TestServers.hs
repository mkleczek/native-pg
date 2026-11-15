{-# LANGUAGE ScopedTypeVariables #-}

-- | Servers that the cases stand up: one that accepts connections and never answers, and one that plays a script for each
-- connection that comes, with the bytes of the protocol's messages that a script needs.
module TestServers
  ( withSilentServer,
    withScripted,
    frame,
    int32,
    receiveStartup,
    drain,
  )
where

import Control.Concurrent (forkIO, killThread)
import Control.Exception (SomeException, bracket, catch, finally)
import Control.Monad (unless, void)
import Data.Bits (shiftR)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Foldable (traverse_)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import System.Directory (getTemporaryDirectory, removeDirectoryRecursive)
import System.FilePath ((</>))
import System.Posix.Temp (mkdtemp)

-- | Runs the action with the directory of a Unix-domain socket that accepts connections but never answers.
withSilentServer :: (FilePath -> IO a) -> IO a
withSilentServer action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-silent-")) removeDirectoryRecursive $ \directory ->
    bracket (Socket.socket Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
      Socket.bind sock (Socket.SockAddrUnix (directory </> ".s.PGSQL.5432"))
      Socket.listen sock 4
      action directory


-- | Runs the action with the port at 127.0.0.1 of a server that serves the connections that come, one by each script in
-- turn, which is given its socket. A connection is closed when its script ends, and what goes wrong in it shows in the client.
withScripted :: [Socket.Socket -> IO ()] -> (Socket.PortNumber -> IO a) -> IO a
withScripted scripts action =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \listener -> do
    Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen listener 4
    -- Each connection is served on a thread of its own, since the first is still open while the second is made.
    let serve script = do
          (client, _) <- Socket.accept listener
          void (forkIO (script client `finally` Socket.close client `catch` \(_ :: SomeException) -> pure ()))
    bracket (forkIO (traverse_ serve scripts `catch` \(_ :: SomeException) -> pure ())) killThread $ \_ -> action =<< Socket.socketPort listener

-- | A message of the server: its tag, its length, and its payload.
frame :: Char -> ByteString -> ByteString
frame kind payload = Char8.singleton kind <> int32 (BS.length payload + 4) <> payload

-- | The four bytes of a number, the most significant first.
int32 :: Int -> ByteString
int32 number = BS.pack [fromIntegral (number `shiftR` shift) | shift <- [24, 16, 8, 0]]

-- | Takes the client's startup message, which has no tag: its length, and the rest.
receiveStartup :: Socket.Socket -> IO ByteString
receiveStartup client = do
  size <- BS.foldl' (\total byte -> total * 256 + fromIntegral byte) 0 <$> SocketBytes.recv client 4
  SocketBytes.recv client (size - 4)


-- | Takes what the client sends until it closes the connection.
drain :: Socket.Socket -> IO ()
drain client = SocketBytes.recv client 4096 >>= \received -> unless (BS.null received) (drain client)
