{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | Raw sockets: connecting, reading into the parser's buffer, and sending
-- serialized messages, over a link that's the socket itself or TLS over it.
module Database.PostgreSQL.Transport
  ( Address (..),
    ConnectError (..),
    StreamLengthError (..),
    TcpOptions (..),
    connectSocket,
    defaultTcpOptions,

    -- * Links
    Link (..),
    Channel (..),
    newChannel,

    -- * TLS links
    TlsClient (..),
    TlsSettings (..),
    ServerVerification (..),
    RootCertificates (..),
    TlsVersion (..),
    TlsError (..),

    -- * Output sent as answers are read
    Unsent,
    OutputBuffer,
    newOutputBuffer,
    unsent,
  )
where

import Control.Concurrent (threadWaitRead, threadWaitReadSTM, threadWaitWrite, threadWaitWriteSTM)
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
    setSocketOption,
    socket,
    withFdSocket,
  )
import System.Posix.Types (Fd (..))
#if defined(mingw32_HOST_OS)
import Foreign.Ptr (Ptr)
import Network.Socket (recvBuf)
import Network.Socket.ByteString (sendMany)
#else
import qualified Data.ByteString.Unsafe as BS
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CChar, CInt (..), CSize (..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (pokeByteOff, sizeOf)
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

-- | A stream of bytes to the server over a connected socket: the socket
-- itself, or TLS over it. Its operations don't wait: the connection waits on
-- the socket.
class Link link where
  -- | The socket the link reads and writes.
  linkSocket :: link -> Socket

  -- | Reads what's ready into the buffer, up to its size, without waiting:
  -- 'Nothing' if nothing is, and 0 at the end of the stream.
  receiveReady :: link -> Ptr Word8 -> Int -> IO (Maybe Int)

  -- | Takes what it can of the pieces without waiting, and gives how many
  -- bytes it took.
  sendReady :: link -> [ByteString] -> IO Int

  -- | Writes what it took but hasn't written yet, without waiting. True if
  -- nothing is left.
  flushReady :: link -> IO Bool

  -- | Closes the link and its socket.
  closeLink :: link -> IO ()

instance Link Socket where
  linkSocket = id
  {-# INLINE linkSocket #-}
  receiveReady = tryReceive
  {-# INLINE receiveReady #-}
  sendReady = trySend
  {-# INLINE sendReady #-}
  flushReady _ = pure True
  {-# INLINE flushReady #-}
  closeLink = close
  {-# INLINE closeLink #-}

-- | Starts TLS over a connected socket, as a client of the named server, and
-- gives the channel over it, made as 'newChannel' makes one. A library apart,
-- native-pg-tls, has one, so that this one has no C. Throws 'TlsError' if
-- TLS fails.
newtype TlsClient = TlsClient
  { startTlsChannel :: TlsSettings -> HostName -> Socket -> IO (IORef Unsent -> Int -> Channel)
  }

-- | How a TLS link is configured.
data TlsSettings = TlsSettings
  { tlsVerification :: ServerVerification,
    -- | A certificate chain and its private key, in PEM files, for the
    -- server to authenticate the client with, if it asks.
    tlsClientCertificate :: Maybe (FilePath, FilePath),
    -- | Certificate revocation lists, in PEM files, which every certificate
    -- of the server's chain is checked against when it's verified.
    tlsRevocationLists :: [FilePath],
    -- | The versions of TLS the connection may use.
    tlsVersions :: [TlsVersion],
    -- | Whether to send the server's name, unless it's an IP address, as
    -- libpq's @sslsni@ asks.
    tlsServerNameIndication :: Bool
  }
  deriving (Eq, Show)

-- | The versions of TLS rustls has.
data TlsVersion = Tls12 | Tls13
  deriving (Eq, Ord, Show, Enum, Bounded)

-- | How the server's certificate is checked.
data ServerVerification
  = -- | Not at all: the connection is encrypted, but the server may not be
    -- the one asked for, as with libpq's @sslmode=require@.
    NoVerification
  | -- | Against root certificates, and the server's name, as with libpq's
    -- @sslmode=verify-full@.
    VerifyFull RootCertificates
  deriving (Eq, Show)

-- | The certificates of the authorities a server's certificate has to come
-- from.
data RootCertificates
  = -- | In a PEM file.
    RootCertificateFile FilePath
  | -- | The system's, as with libpq's @sslrootcert=system@.
    SystemRootCertificates
  deriving (Eq, Show)

-- | TLS failed: the handshake, the server's certificate, or a record.
-- 'displayException' words it as libpq does.
newtype TlsError = TlsError String
  deriving (Show)

instance Exception TlsError where
  displayException (TlsError reason) = "SSL error: " <> reason

-- | A connection's input and output over its link. It's made with the
-- connection, from code specialized to the link, so that using it passes no
-- dictionary, and the rest of the client doesn't know which link it has.
data Channel = Channel
  { -- | Reads input. While output is unsent, waiting for input also sends it
    -- as the link takes it.
    channelInput :: InputSource,
    -- | Sends what the link takes without waiting, and gives what's left.
    channelSendSome :: Unsent -> IO Unsent,
    -- | Sends the output, waiting for room as needed.
    channelSend :: Output -> IO (),
    channelClose :: IO ()
  }

-- | The channel over a link, given where the output that waits to be sent is
-- kept, and the size of a read.
newChannel :: Link link => link -> IORef Unsent -> Int -> Channel
newChannel link pending readSize =
  Channel
    { channelInput = linkInputSource link pending readSize,
      channelSendSome = sendSome link,
      channelSend = sendOutput link,
      channelClose = closeLink link
    }
{-# INLINABLE newChannel #-}
{-# SPECIALIZE newChannel :: Socket -> IORef Unsent -> Int -> Channel #-}

-- | Reads from the link into the parser's buffer, which a new allocation
-- makes the given size. While output is unsent, waiting for input also sends
-- it as the link takes it.
linkInputSource :: Link link => link -> IORef Unsent -> Int -> InputSource
linkInputSource link pending readSize =
  InputSource
    { readInputInto = receive,
      inputReadSize = readSize
    }
  where
    receive pointer size =
      receiveReady link pointer size >>= \case
        Just received -> pure received
        Nothing -> do
          left <- readIORef pending
          flushed <- flushReady link
          case left of
            Unsent [] | flushed -> withFdSocket (linkSocket link) (threadWaitRead . Fd)
            _ -> do
              readable <- awaitReadableOrWritable (linkSocket link)
              unless readable $ writeIORef pending =<< sendSome link left
          receive pointer size
{-# INLINABLE linkInputSource #-}

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
sendOutput :: Link link => link -> Output -> IO ()
sendOutput link output = go =<< unsent Nothing output
  where
    go left =
      sendSome link left >>= \case
        Unsent [] -> do
          flushed <- flushReady link
          unless flushed (awaitWritable *> go (Unsent []))
        rest -> awaitWritable *> go rest
    awaitWritable = withFdSocket (linkSocket link) (threadWaitWrite . Fd)
{-# INLINABLE sendOutput #-}

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

-- | Bytes gathered for one send, and how many pieces at most. A client's
-- stream comes in chunks of up to 16 KiB from warp, and sending each on its
-- own wakes the server once per chunk.
gatherSize, gatherPieces :: Int
gatherSize = 262144
gatherPieces = 16

-- | Sends what the link takes without waiting for room in the socket,
-- pulling streams as it goes, and gives what's left. Each send gathers the
-- parts up front, pulling a stream's chunks, up to 'gatherSize' bytes or
-- 'gatherPieces' pieces. Pulling may wait for the stream's client, so the
-- bytes before a stream are sent before it's pulled: the server starts on
-- its statement, and its statement_timeout, as it would without the stream.
sendSome :: Link link => link -> Unsent -> IO Unsent
sendSome link (Unsent parts) = Unsent <$> go parts
  where
    go remaining = do
      (gathered, rest) <- gather 0 [] remaining
      if null gathered
        then pure rest
        else do
          sent <- sendReady link gathered
          case dropSent sent gathered of
            [] -> go rest
            left -> pure (map PartBytes left <> rest)
    -- the pieces so far, last first, and their size
    gather size pieces remaining
      | size >= gatherSize || length pieces >= gatherPieces = pure (reverse pieces, remaining)
      | otherwise = case remaining of
          [] -> pure (reverse pieces, [])
          PartBytes chunk : others -> gather (size + BS.length chunk) (chunk : pieces) others
          PartStream _ 0 : _ | not (null pieces) -> pure (reverse pieces, remaining)
          PartStream source given : others -> do
            chunk <- byteStreamNext source
            let delivered = given + BS.length chunk
            if
              | BS.null chunk -> do
                  unless (given == byteStreamLength source) (throwIO (StreamLengthError (byteStreamLength source) given))
                  gather size pieces others
              | delivered > byteStreamLength source ->
                  throwIO (StreamLengthError (byteStreamLength source) delivered)
              | otherwise -> gather (size + BS.length chunk) (chunk : pieces) (PartStream source delivered : others)
    dropSent sent = \case
      chunk : others
        | sent >= BS.length chunk -> dropSent (sent - BS.length chunk) others
        | otherwise -> BS.drop sent chunk : others
      [] -> []
{-# INLINABLE sendSome #-}

-- | Waits until the socket has input, or room for output. True if it has
-- input.
awaitReadableOrWritable :: Socket -> IO Bool
awaitReadableOrWritable sock = withFdSocket sock $ \fd -> do
  (readable, stopReadable) <- threadWaitReadSTM (Fd fd)
  (writable, stopWritable) <- threadWaitWriteSTM (Fd fd)
  atomically ((True <$ readable) `orElse` (False <$ writable)) `finally` (stopReadable *> stopWritable)

-- | Reads what the socket has into the buffer, without waiting: 'Nothing' if
-- it has nothing, and 0 at the end of the stream.
tryReceive :: Socket -> Ptr Word8 -> Int -> IO (Maybe Int)
#if defined(mingw32_HOST_OS)
-- Sockets block on Windows, so it waits for input.
tryReceive sock pointer size = Just <$> recvBuf sock pointer size
#else
tryReceive sock pointer size =
  withFdSocket sock $ \fd ->
    let attempt =
          c_recv fd pointer (fromIntegral size) 0 >>= \case
            -1 ->
              Errno.getErrno >>= \errno ->
                if
                  | errno == Errno.eINTR -> attempt
                  | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> pure Nothing
                  | otherwise -> Errno.throwErrno "recv"
            received -> pure (Just (fromIntegral received))
     in attempt

foreign import ccall unsafe "recv"
  c_recv :: CInt -> Ptr Word8 -> CSize -> CInt -> IO Posix.CSsize
#endif

-- | Sends what the socket takes of the pieces, in one call, without waiting
-- for room in it, and gives how many bytes it took.
trySend :: Socket -> [ByteString] -> IO Int
#if defined(mingw32_HOST_OS)
-- Sockets block on Windows, so it waits until all are sent, and the answers
-- are read only after the whole output is sent.
trySend sock pieces = sum (map BS.length pieces) <$ sendMany sock pieces
#else
trySend sock pieces =
  withFdSocket sock $ \fd ->
    allocaBytes (length pieces * iovecSize) $ \iovecs ->
      let -- each piece's struct iovec: where it is, and its length
          describe _ [] = attempt
          describe offset (piece : others) =
            BS.unsafeUseAsCStringLen piece $ \(pointer, size) -> do
              pokeByteOff iovecs offset pointer
              pokeByteOff iovecs (offset + sizeOf pointer) (fromIntegral size :: CSize)
              describe (offset + iovecSize) others
          attempt =
            c_writev fd iovecs (fromIntegral (length pieces)) >>= \case
              -1 ->
                Errno.getErrno >>= \errno ->
                  if
                    | errno == Errno.eINTR -> attempt
                    | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> pure 0
                    | otherwise -> Errno.throwErrno "writev"
              sent -> pure (fromIntegral sent)
       in describe 0 pieces
  where
    iovecSize = sizeOf (nullPtr :: Ptr CChar) + sizeOf (0 :: CSize)

-- network's sockets don't block, so a full socket fails with EAGAIN.
foreign import ccall unsafe "writev"
  c_writev :: CInt -> Ptr () -> CInt -> IO Posix.CSsize
#endif
