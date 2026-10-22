{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}

-- | Socket connection setup.
module Database.PostgreSQL.Internal.Socket
  ( connectSocket,
  )
where

import Control.Exception (bracketOnError, catch, throwIO, try)
import Control.Monad (when)
import Data.Foldable (for_)
import Data.Maybe (fromMaybe)
import Database.PostgreSQL.Transport (Address (..), ConnectError (..), TcpOptions (..))
import GHC.IO.Exception (IOException (..))
import Network.Socket
  ( AddrInfo (..),
    Family (AF_UNIX),
    NameInfoFlag (NI_NUMERICHOST),
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
  )

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
