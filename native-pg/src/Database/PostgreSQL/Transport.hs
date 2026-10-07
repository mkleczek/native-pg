{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | Raw sockets: connecting, reading into the parser's buffer, and sending
-- serialized messages.
module Database.PostgreSQL.Transport
  ( Address (..),
    ConnectError (..),
    StreamLengthError (..),
    TcpOptions (..),
    connectSocket,
    defaultTcpOptions,
    sendOutput,
    socketInputSource,
  )
where

import Control.Exception (Exception (..), IOException, bracketOnError, catch, throwIO, try)
import Control.Monad (unless, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder.Extra as Builder
import qualified Data.ByteString.Lazy as LBS
import Data.Foldable (for_)
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Database.PostgreSQL.Protocol.Codec
  ( ByteStream (..),
    InputSource (..),
    Output,
    outputBuilder,
    outputLength,
    outputStreams,
  )
import Foreign.C.Error (Errno (..), eCONNREFUSED, eNOENT)
import GHC.IO.Exception (IOException (..))
import Network.Socket
  ( AddrInfo (..),
    Family (AF_UNIX),
    HostName,
    NameInfoFlag (NI_NUMERICHOST),
    PortNumber,
    SockAddr (SockAddrUnix),
    Socket,
    SocketOption (KeepAlive, NoDelay, SockOpt, UserTimeout),
    SocketType (Stream),
    close,
    connect,
    defaultHints,
    defaultProtocol,
    getAddrInfo,
    getNameInfo,
    isSupportedSocketOption,
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

-- | Options for TCP connections. Unix-domain sockets ignore them.
data TcpOptions = TcpOptions
  { -- | Probe an idle connection, so that a peer that vanished without closing
    -- it is noticed.
    tcpKeepalives :: Bool,
    -- | Seconds of idleness before the first probe, seconds between probes,
    -- and how many unanswered probes end the connection. 'Nothing' keeps the
    -- system's default, as do platforms without the option.
    tcpKeepalivesIdle :: Maybe Int,
    tcpKeepalivesInterval :: Maybe Int,
    tcpKeepalivesCount :: Maybe Int,
    -- | Milliseconds that sent data may stay unacknowledged before the
    -- connection is closed, on platforms with the option.
    tcpUserTimeout :: Maybe Int
  }
  deriving (Eq, Show)

-- | Keepalives with the system's timings, as libpq has by default.
defaultTcpOptions :: TcpOptions
defaultTcpOptions = TcpOptions True Nothing Nothing Nothing Nothing

-- | Connecting failed. 'displayException' words it as libpq does.
data ConnectError
  = -- | The host name doesn't resolve, for the resolver's reason.
    UnresolvedHost HostName String
  | -- | Connecting to each address the host name resolved to failed: each
    -- address, numeric, and its failure.
    UnreachableHost HostName PortNumber [(String, IOException)]
  | -- | Connecting to the Unix-domain socket failed.
    UnreachableSocket FilePath IOException
  deriving (Show)

instance Exception ConnectError where
  displayException = \case
    UnresolvedHost host reason ->
      "could not translate host name " <> show host <> " to address: " <> reason
    UnreachableHost host port failures ->
      intercalate "\n" $
        [ "connection to server at " <> show host <> numeric address <> ", port " <> show port <> " failed: " <> describe failure tcpHint
        | (address, failure) <- failures
        ]
      where
        numeric address
          | address == host = ""
          | otherwise = " (" <> address <> ")"
    UnreachableSocket path failure ->
      "connection to server on socket " <> show path <> " failed: " <> describe failure socketHint
    where
      describe failure hint = ioe_description failure <> maybe "" (hint . Errno) (ioe_errno failure)
      tcpHint errno
        | errno == eCONNREFUSED = "\n\tIs the server running on that host and accepting TCP/IP connections?"
        | otherwise = ""
      socketHint errno
        | errno == eNOENT || errno == eCONNREFUSED = "\n\tIs the server running locally and accepting connections on that socket?"
        | otherwise = ""

-- | Connects to the address, or throws 'ConnectError'. For a host name, the
-- addresses it resolves to are tried in order.
connectSocket :: TcpOptions -> Address -> IO Socket
connectSocket _ (UnixAddress path) =
  bracketOnError (socket AF_UNIX Stream defaultProtocol) close $ \sock -> do
    connect sock (SockAddrUnix path) `catch` (throwIO . UnreachableSocket path)
    pure sock
connectSocket options (TcpAddress host port) = do
  addresses <-
    getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just host) (Just (show port))
      `catch` \failure -> throwIO (UnresolvedHost host (ioe_description failure))
  connectFirst [] addresses
  where
    connectFirst [] [] = throwIO (UnresolvedHost host "no address found")
    connectFirst failures [] = throwIO (UnreachableHost host port (reverse failures))
    connectFirst failures (address : others) =
      try (connectTo address) >>= \case
        Right sock -> pure sock
        Left failure -> do
          numeric <- fst <$> getNameInfo [NI_NUMERICHOST] True False (addrAddress address)
          connectFirst ((fromMaybe host numeric, failure) : failures) others
    connectTo address =
      bracketOnError (openSocket address) close $ \sock -> do
        -- Messages are small and sent whole, so don't wait to coalesce them.
        setSocketOption sock NoDelay 1
        setTcpOptions sock options
        connect sock (addrAddress address)
        pure sock

setTcpOptions :: Socket -> TcpOptions -> IO ()
setTcpOptions sock options = do
  when (tcpKeepalives options) $ do
    setSocketOption sock KeepAlive 1
    for_ (tcpKeepalivesIdle options) (setIfSupported keepalivesIdle)
    for_ (tcpKeepalivesInterval options) (setIfSupported keepalivesInterval)
    for_ (tcpKeepalivesCount options) (setIfSupported keepalivesCount)
  for_ (tcpUserTimeout options) (setIfSupported UserTimeout)
  where
    setIfSupported option value =
      when (isSupportedSocketOption option) (setSocketOption sock option value)

-- | TCP_KEEPIDLE (TCP_KEEPALIVE on macOS), TCP_KEEPINTVL and TCP_KEEPCNT,
-- which the network package doesn't name, at level IPPROTO_TCP.
keepalivesIdle, keepalivesInterval, keepalivesCount :: SocketOption
#if defined(linux_HOST_OS)
keepalivesIdle = SockOpt 6 4
keepalivesInterval = SockOpt 6 5
keepalivesCount = SockOpt 6 6
#elif defined(darwin_HOST_OS)
keepalivesIdle = SockOpt 6 0x10
keepalivesInterval = SockOpt 6 0x101
keepalivesCount = SockOpt 6 0x102
#else
keepalivesIdle = SockOpt (-1) (-1)
keepalivesInterval = SockOpt (-1) (-1)
keepalivesCount = SockOpt (-1) (-1)
#endif

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
