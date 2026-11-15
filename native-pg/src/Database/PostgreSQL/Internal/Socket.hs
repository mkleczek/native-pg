{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Connecting a socket without waiting in IO: the @connect@ of the operating system, on a socket that doesn't
-- block, then a block of the arrow until the descriptor is writable, which says that the connection was made or has
-- failed, and @SO_ERROR@, which says which. A host that is given as a numeric address needs no lookup. A name is looked
-- up on a thread of its own, since resolving it waits for the network and would stall the core, and the part waits for
-- the thread at a ticket.
module Database.PostgreSQL.Internal.Socket
  ( connectSocket,
    isQuiet,
  )
where

import Control.Arrow (arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkIO)
import Control.Exception (IOException, SomeException, displayException, fromException, onException, try)
import Control.Monad (void, when)
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
    HostName,
    PortNumber,
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
import Network.Streaming.Classes (Block (..), Effect (..), Event (..), Exit (..), Promise, Reach (..), Runtime, Scope (..))
import Network.Streaming.Combinators (pattern Again, pattern Finish, deliver, done, iterateA, withTicket)
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Descriptors (fresh)
import Network.Streaming.Mode (Instrumented (bump))
import Network.Streaming.Threads (Threads, runThreads)
import qualified System.Posix.Types as Posix

-- | Connects to the address, or says why it couldn't. The socket is closed if the part is cancelled while it connects, or
-- if it didn't connect. A host name is looked up, and the addresses it has are tried in order; the first that connects
-- is the result, and if none does, each failure is in the 'UnreachableHost'.
connectSocket :: (Runtime arr) => TcpOptions -> arr Address (Either ConnectError Socket)
connectSocket options =
  (\case UnixAddress path -> Left path; TcpAddress host port -> Right (host, port)) ^>> (unix ||| tcp)
  where
    unix =
      (\path -> ((socket AF_UNIX Stream defaultProtocol, SockAddrUnix path), path))
        ^>> first attemptTo
        >>^ \(result, path) -> either (Left . UnreachableSocket path) Right result
    tcp = lookedUp >>> (arr Left ||| each)
    each = (\(host, port, found) -> Round host port found []) ^>> iterateA step
    -- The next address, which the loop tries, or the end of the addresses.
    step = choose ^>> (returnA ||| (first attemptTo >>> now concluded))
    choose (Round host port found failures) = case found of
      [] -> Left (Finish (Left (UnreachableHost host port (reverse failures))))
      address : others -> Right ((opening address, addrAddress address), (Round host port others failures, address))
    opening found = do
      sock <- openSocket found
      -- Messages are small and sent whole, so don't wait to coalesce them.
      (setSocketOption sock NoDelay 1 *> setTcpOptions sock options) `onException` close sock
      pure sock
    concluded (result, (Round host port others failures, address)) = case result of
      Right sock -> pure (Finish (Right sock))
      Left failure -> (\numeric -> Again (Round host port others ((numeric, failure) : failures))) . fromMaybe host . fst <$> getNameInfo [NI_NUMERICHOST] True False (addrAddress address)

-- | The addresses that a host name has for a port, in the order that they are to be tried.
type Found = (HostName, PortNumber, [AddrInfo])

-- | What the loop over the addresses of a host knows: the host and the port, the addresses that are left to try, and the
-- failure of each address tried, the latest first, with the address as numbers.
data Round = Round HostName PortNumber [AddrInfo] [(String, IOException)]

-- | The addresses of the host: a numeric one is one address, which no lookup is needed for, and a name is looked up on
-- a thread of its own, since resolving it waits for the network and would stall the core of the part.
lookedUp :: (Runtime arr) => arr (HostName, PortNumber) (Either ConnectError Found)
lookedUp = now numeric >>> (byName ||| arr Right)
  where
    numeric (host, port) =
      either (\(_ :: IOException) -> Left (host, port)) (Right . (host,port,))
        <$> try (getAddrInfo (Just defaultHints {addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream}) (Just host) (Just (show port)))

-- | A name's addresses, from a lookup that runs on a thread of its own and fulfils the ticket that the part waits at,
-- so that a deadline or a cancellation reaches the wait. A part that stopped waiting leaves the thread to end by itself:
-- it holds nothing.
byName :: (Runtime arr) => arr (HostName, PortNumber) (Either ConnectError Found)
byName = withTicket (now (\input -> void (forkIO (runThreads resolving input)))) done >>^ snd
  where
    resolving :: Threads (Promise 'Anywhere (Either ConnectError Found), (HostName, PortNumber)) ()
    resolving = deliver (now (uncurry resolve))
    resolve host port =
      try (getAddrInfo (Just defaultHints {addrFlags = [AI_NUMERICSERV], addrSocketType = Stream}) (Just host) (Just (show port))) >>= \case
        Left (failure :: SomeException) -> pure (Left (UnresolvedHost host (maybe (displayException failure) ioe_description (fromException failure))))
        Right [] -> pure (Left (UnresolvedHost host "no address found"))
        Right found -> pure (Right (host, port, found))

-- | One attempt: a socket, opened by the action and connected to the address, or why it couldn't be. The socket is
-- closed if the part is cancelled while it connects, or if it didn't connect.
attemptTo :: (Runtime arr) => arr (IO Socket, SockAddr) (Either IOException Socket)
attemptTo = scope (now (\(opening, _) -> try (opening >>= \sock -> sock <$ fresh sock))) (now released) body
  where
    released (opened, exit) = case exit of
      Done -> pure ()
      _ -> either (\(_ :: IOException) -> pure ()) close opened
    body =
      attempted
        ^>> (now (pure . Left) ||| (first reach >>> now (\(reached, sock) -> either (\failure -> Left failure <$ close sock) (const (pure (Right sock))) reached)))
    attempted (opened, (_, address)) = either Left (\sock -> Right ((sock, address), sock)) opened

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
