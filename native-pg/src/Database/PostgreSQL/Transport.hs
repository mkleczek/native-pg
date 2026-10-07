{-# LANGUAGE MultiWayIf #-}

-- | Raw sockets: connecting, reading into the parser's buffer, and sending
-- serialized messages.
module Database.PostgreSQL.Transport
  ( Address (..),
    StreamLengthError (..),
    connectSocket,
    sendOutput,
    socketInputSource,
  )
where

import Control.Exception (Exception, IOException, bracketOnError, catch, throwIO)
import Control.Monad (unless)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder.Extra as Builder
import qualified Data.ByteString.Lazy as LBS
import Data.Foldable (for_)
import Database.PostgreSQL.Protocol.Codec
  ( ByteStream (..),
    InputSource (..),
    Output,
    outputBuilder,
    outputLength,
    outputStreams,
  )
import Network.Socket
  ( AddrInfo (..),
    Family (AF_UNIX),
    HostName,
    PortNumber,
    SockAddr (SockAddrUnix),
    Socket,
    SocketOption (NoDelay),
    SocketType (Stream),
    close,
    connect,
    defaultHints,
    defaultProtocol,
    getAddrInfo,
    openSocket,
    recvBuf,
    setSocketOption,
    socket,
  )
import Network.Socket.ByteString (sendAll, sendMany)

data Address
  = -- | A host name or IP address, and a port.
    TcpAddress HostName PortNumber
  | -- | The path of a Unix-domain socket, e.g.
    -- @/var/run/postgresql/.s.PGSQL.5432@.
    UnixAddress FilePath
  deriving (Eq, Show)

-- | Connects to the address. For a host name, the addresses it resolves to
-- are tried in order.
connectSocket :: Address -> IO Socket
connectSocket (UnixAddress path) =
  bracketOnError (socket AF_UNIX Stream defaultProtocol) close $ \sock -> do
    connect sock (SockAddrUnix path)
    pure sock
connectSocket (TcpAddress host port) =
  connectFirst =<< getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just host) (Just (show port))
  where
    connectFirst [] = ioError (userError ("no address found for " <> host))
    connectFirst [address] = connectTo address
    connectFirst (address : others) =
      connectTo address `catch` \(_ :: IOException) -> connectFirst others
    connectTo address =
      bracketOnError (openSocket address) close $ \sock -> do
        -- Messages are small and sent whole, so don't wait to coalesce them.
        setSocketOption sock NoDelay 1
        connect sock (addrAddress address)
        pure sock

-- | Reads from the socket into the parser's buffer, which a new allocation
-- makes the given size.
socketInputSource :: Socket -> Int -> InputSource
socketInputSource sock readSize =
  InputSource
    { readInputInto = recvBuf sock,
      inputReadSize = readSize
    }

-- | A parameter stream gave a different number of bytes than it declared.
-- The message it belongs to is incomplete, so the connection is unusable.
data StreamLengthError = StreamLengthError
  { declaredLength :: Int,
    -- | At least this many.
    deliveredLength :: Int
  }
  deriving (Show)

instance Exception StreamLengthError

-- | Sends the output with a gathering write per run of bytes between streams.
-- Large byte strings in it, and the chunks of its streams, are sent from
-- where they are, without being copied.
sendOutput :: Socket -> Output -> IO ()
sendOutput sock output = do
  sendBuilder (outputBuilder output)
  for_ (outputStreams output) $ \(source, after) -> do
    sendStream source
    sendBuilder after
  where
    sendBuilder builder =
      case LBS.toChunks (Builder.toLazyByteStringWith strategy LBS.empty builder) of
        [] -> pure ()
        chunks -> sendMany sock chunks
    strategy = Builder.untrimmedStrategy (max 1 (min (outputLength output) Builder.smallChunkSize)) Builder.defaultChunkSize
    sendStream source = go 0
      where
        go sent = do
          chunk <- byteStreamNext source
          let delivered = sent + BS.length chunk
          if
            | BS.null chunk ->
                unless (sent == byteStreamLength source) (throwIO (StreamLengthError (byteStreamLength source) sent))
            | delivered > byteStreamLength source ->
                throwIO (StreamLengthError (byteStreamLength source) delivered)
            | otherwise -> sendAll sock chunk *> go delivered
