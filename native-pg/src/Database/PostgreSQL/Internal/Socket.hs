{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}

-- | Connecting a socket without waiting in IO: the @connect@ of the operating system, on a socket that doesn't
-- block, then a block of the arrow until the descriptor is writable, which says that the connection was made or has
-- failed, and @SO_ERROR@, which says which. A host is given as a numeric address, since resolving a name waits for
-- the network: a name that isn't numeric fails with 'UnresolvedHost'.
module Database.PostgreSQL.Internal.Socket
  ( connectSocket,
    isQuiet,
  )
where

import Control.Arrow (first, returnA, (>>>), (^>>), (|||))
import Control.Exception (IOException, onException, try)
import Control.Monad (when)
import Data.Foldable (for_)
import Data.Maybe (fromMaybe)
import Data.Word (Word8)
import Database.PostgreSQL.Errors (ConnectError (..))
import Database.PostgreSQL.Transport (Address (..), TcpOptions (..))
import Foreign.C.Error (Errno (..), eINPROGRESS, eINTR, errnoToIOError, getErrno, eAGAIN, eWOULDBLOCK)
import Foreign.C.Types (CInt (..), CSize (..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (Ptr)
import GHC.IO.Exception (IOException (..))
import Network.Socket
  ( AddrInfo (..),
    AddrInfoFlag (AI_NUMERICHOST, AI_NUMERICSERV),
    Family (AF_UNIX),
    NameInfoFlag (NI_NUMERICHOST),
    SockAddr (SockAddrUnix),
    Socket,
    SocketOption (KeepAlive, NoDelay, SoError, SockOpt, UserTimeout),
    SocketType (Stream),
    close,
    defaultHints,
    defaultProtocol,
    getAddrInfo,
    getNameInfo,
    getSocketOption,
    isSupportedSocketOption,
    openSocket,
    setSocketOption,
    socket,
    withFdSocket,
  )
import Network.Socket.Address (pokeSocketAddress, sizeOfSocketAddress)
import Network.Streaming.Classes (Block (..), Effect (..), Event (..), Exit (..), Runtime, Scope (..))
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Descriptors (fresh)
import Network.Streaming.Mode (Instrumented (bump))
import qualified System.Posix.Types as Posix

-- | Connects to the address, or says why it couldn't. The socket is closed if the part is cancelled while it
-- connects, or if it didn't connect.
connectSocket :: (Runtime arr) => TcpOptions -> arr Address (Either ConnectError Socket)
connectSocket options =
  (\case UnixAddress path -> Left path; TcpAddress host port -> Right (host, port)) ^>> (unix ||| tcp)
  where
    unix = (\path -> (socket AF_UNIX Stream defaultProtocol, SockAddrUnix path, UnreachableSocket path)) ^>> reaching
    tcp =
      now resolve
        >>> (now (pure . Left) ||| reaching)
    -- A numeric host is one address, which no lookup is needed for.
    resolve (host, port) =
      try (getAddrInfo (Just defaultHints {addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream}) (Just host) (Just (show port))) >>= \case
        Left failure -> pure (Left (UnresolvedHost host (ioe_description failure)))
        Right [] -> pure (Left (UnresolvedHost host "no address found"))
        Right (found : _) -> do
          numeric <- fromMaybe host . fst <$> getNameInfo [NI_NUMERICHOST] True False (addrAddress found)
          let opening = do
                sock <- openSocket found
                (setSocketOption sock NoDelay 1 *> setTcpOptions sock options) `onException` close sock
                pure sock
          pure (Right (opening, addrAddress found, \failure -> UnreachableHost host port [(numeric, failure)]))
    reaching = scope (now (\(opening, _, _) -> try (opening >>= \sock -> sock <$ fresh sock))) (now released) body
    released (opened, exit) = case exit of
      Done -> pure ()
      _ -> either (\(_ :: IOException) -> pure ()) close opened
    body =
      attempted
        ^>> (now (pure . Left) ||| (first reach >>> now (\(reached, (sock, wrap)) -> either (\failure -> Left (wrap failure) <$ close sock) (const (pure (Right sock))) reached)))
    attempted (opened, (_, address, wrap)) = either (Left . wrap) (\sock -> Right ((sock, address), (sock, wrap))) opened

-- | A connection that is made, or has failed, and why: the connect is started and, if it is under way, waited for
-- until the descriptor is writable.
reach :: (Runtime arr) => arr (Socket, SockAddr) (Either IOException ())
reach = now start >>> ((first block >>> now (\((), sock) -> confirm sock)) ||| returnA)
  where
    start (sock, address) = withFdSocket sock $ \fd ->
      allocaBytes (sizeOfSocketAddress address) $ \pointer -> do
        pokeSocketAddress pointer address
        c_connect fd pointer (fromIntegral (sizeOfSocketAddress address)) >>= \case
          0 -> pure (Right (Right ()))
          _ ->
            getErrno >>= \errno ->
              -- After an interrupt the connection goes on, and is waited for like one that is under way.
              if errno == eINPROGRESS || errno == eINTR
                then pure (Left (Writable (Posix.Fd fd), sock))
                else pure (Right (Left (errnoToIOError "connect" errno Nothing Nothing)))
    confirm sock =
      getSocketOption sock SoError >>= \case
        0 -> pure (Right ())
        code -> pure (Left (errnoToIOError "connect" (Errno (fromIntegral code)) Nothing Nothing))

foreign import ccall unsafe "connect"
  c_connect :: CInt -> Ptr a -> CInt -> IO CInt

-- | Whether the socket has nothing to read now, as a connection that waits for the next statement has: it has when
-- the server ended it, or said something that nobody asked for. It looks without taking the byte.
isQuiet :: forall m. (Instrumented m) => Socket -> IO Bool
isQuiet sock = withFdSocket sock $ \fd -> allocaBytes 1 $ \pointer ->
  bump @m Probes *> c_recv fd pointer 1 msgPeek >>= \case
    -1 -> (\errno -> errno == eAGAIN || errno == eWOULDBLOCK) <$> getErrno
    _ -> pure False
  where
    msgPeek = 2

foreign import ccall unsafe "recv"
  c_recv :: CInt -> Ptr Word8 -> CSize -> CInt -> IO Posix.CSsize

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

-- | TCP_KEEPIDLE (TCP_KEEPALIVE on macOS), TCP_KEEPINTVL and TCP_KEEPCNT, which the network package doesn't name,
-- at level IPPROTO_TCP.
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
