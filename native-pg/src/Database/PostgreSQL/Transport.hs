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

    -- * Output sent as answers are read
    Unsent,
    OutputBuffer,
    newOutputBuffer,
    unsent,
    sendSome,
  )
where

import Control.Concurrent (threadWaitReadSTM, threadWaitWrite, threadWaitWriteSTM)
import Control.Exception (Exception (..), IOException, bracketOnError, catch, finally, throwIO, try)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_)
import Data.IORef (IORef, readIORef, writeIORef)
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.Word (Word8)
import Database.PostgreSQL.Protocol.Codec
  ( ByteStream (..),
    InputSource (..),
    Output,
    Uncopied (..),
    outputCopied,
    writeOutput,
  )
import Foreign.C.Error (Errno (..), eCONNREFUSED, eNOENT)
import Foreign.ForeignPtr (ForeignPtr)
import GHC.Conc (atomically, orElse)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
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
    withFdSocket,
  )
import System.Posix.Types (Fd (..))
#if defined(mingw32_HOST_OS)
import Network.Socket.ByteString (sendAll)
#else
import qualified Data.ByteString.Unsafe as BS
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CChar, CInt (..), CSize (..))
import Foreign.Ptr (Ptr)
import qualified System.Posix.Types as Posix
#endif

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
-- makes the given size. While output is unsent, waiting for input also sends
-- it as the socket takes it.
socketInputSource :: Socket -> IORef Unsent -> Int -> InputSource
socketInputSource sock pending readSize =
  InputSource
    { readInputInto = \pointer size -> sendWhileWaiting *> recvBuf sock pointer size,
      inputReadSize = readSize
    }
  where
    sendWhileWaiting =
      readIORef pending >>= \case
        Unsent [] -> pure ()
        left -> do
          readable <- awaitReadableOrWritable sock
          unless readable $ do
            writeIORef pending =<< sendSome sock left
            sendWhileWaiting

-- | A parameter stream gave a different number of bytes than it declared.
-- The message it belongs to is incomplete, so the connection is unusable.
data StreamLengthError = StreamLengthError
  { declaredLength :: Int,
    -- | At least this many.
    deliveredLength :: Int
  }
  deriving (Show)

instance Exception StreamLengthError

-- | Sends the output, waiting for room in the socket as needed.
sendOutput :: Socket -> Output -> IO ()
sendOutput sock output = go =<< unsent Nothing output
  where
    go left =
      sendSome sock left >>= \case
        Unsent [] -> pure ()
        rest -> withFdSocket sock (threadWaitWrite . Fd) *> go rest

-- | What's left of an output to send. The server may answer the first
-- messages of an output before it has read the rest, and stop reading until
-- its answers are read. So output can be sent as the answers are read.
newtype Unsent = Unsent [Part]

-- | Bytes, or a stream with how many bytes it has given.
data Part
  = PartBytes ByteString
  | PartStream ByteStream Int

-- | Memory that outputs are written to, reused once what was written to it
-- is sent.
data OutputBuffer = OutputBuffer (ForeignPtr Word8) Int

newOutputBuffer :: Int -> IO OutputBuffer
newOutputBuffer size = (`OutputBuffer` size) <$> mallocPlainForeignPtrBytes size

-- | All of the output. Its copied bytes are written to the buffer if there's
-- one and they fit, and to a new one of their size otherwise. The buffer
-- mustn't be written to again until they're sent. Long byte strings, and the
-- chunks of its streams, are sent from where they are, without being copied.
unsent :: Maybe OutputBuffer -> Output -> IO Unsent
unsent reused output = do
  memory <- case reused of
    Just (OutputBuffer memory size) | outputCopied output <= size -> pure memory
    _ -> mallocPlainForeignPtrBytes (outputCopied output)
  parts <- writeOutput memory output
  pure (Unsent (map part parts))
  where
    part (Left copied) = PartBytes copied
    part (Right (UncopiedBytes value)) = PartBytes value
    part (Right (UncopiedStream source)) = PartStream source 0

-- | Sends what the socket takes without waiting for room in it, pulling
-- streams as it goes, and gives what's left.
sendSome :: Socket -> Unsent -> IO Unsent
sendSome sock (Unsent parts) = Unsent <$> go parts
  where
    go = \case
      [] -> pure []
      PartBytes chunk : others -> do
        sent <- trySend sock chunk
        if sent == BS.length chunk then go others else pure (PartBytes (BS.drop sent chunk) : others)
      PartStream source given : others -> do
        chunk <- byteStreamNext source
        let delivered = given + BS.length chunk
        if
          | BS.null chunk -> do
              unless (given == byteStreamLength source) (throwIO (StreamLengthError (byteStreamLength source) given))
              go others
          | delivered > byteStreamLength source ->
              throwIO (StreamLengthError (byteStreamLength source) delivered)
          | otherwise -> go (PartBytes chunk : PartStream source delivered : others)

-- | Waits until the socket has input, or room for output. True if it has
-- input.
awaitReadableOrWritable :: Socket -> IO Bool
awaitReadableOrWritable sock = withFdSocket sock $ \fd -> do
  (readable, stopReadable) <- threadWaitReadSTM (Fd fd)
  (writable, stopWritable) <- threadWaitWriteSTM (Fd fd)
  atomically ((True <$ readable) `orElse` (False <$ writable)) `finally` (stopReadable *> stopWritable)

-- | Sends what the socket takes of the bytes without waiting for room in it,
-- and gives how many it took.
trySend :: Socket -> ByteString -> IO Int
#if defined(mingw32_HOST_OS)
-- Sockets block on Windows, so it waits until all are sent, and the answers
-- are read only after the whole output is sent.
trySend sock bytes = BS.length bytes <$ sendAll sock bytes
#else
trySend sock bytes =
  withFdSocket sock $ \fd ->
    BS.unsafeUseAsCStringLen bytes $ \(pointer, size) ->
      let attempt =
            c_send fd pointer (fromIntegral size) 0 >>= \case
              -1 ->
                Errno.getErrno >>= \errno ->
                  if
                    | errno == Errno.eINTR -> attempt
                    | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> pure 0
                    | otherwise -> Errno.throwErrno "send"
              sent -> pure (fromIntegral sent)
       in attempt

-- network's sockets don't block, so a full socket fails with EAGAIN.
foreign import ccall unsafe "send"
  c_send :: CInt -> Ptr CChar -> CSize -> CInt -> IO Posix.CSsize
#endif
