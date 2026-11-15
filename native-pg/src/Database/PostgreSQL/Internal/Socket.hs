{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Connecting a socket without waiting in IO: the @connect@ of the operating system, on a socket that doesn't
-- block, then a block of the arrow until the descriptor is writable, which says that the connection was made or has
-- failed, and @SO_ERROR@, which says which. A host that is given as a numeric address needs no lookup. A name is looked
-- up by the resolver of the settings, c-ares, which the part drives: it blocks on the sockets that the resolver waits for
-- and until its next timeout, and processes the resolver, which never waits, until the lookup has its answer.
module Database.PostgreSQL.Internal.Socket
  ( connectSocket,
    isQuiet,
  )
where

import Control.Arrow (arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, onException, try)
import Control.Monad (when)
import Data.Foldable (for_)
import Data.Maybe (fromMaybe)
import Data.Word (Word8)
import Database.PostgreSQL.Errors (ConnectError (..))
import Database.PostgreSQL.Internal.Cares (Lookup, Progress (..), Resolver, endLookup, process, progress, startLookup)
import Database.PostgreSQL.Transport (Address (..), TcpOptions (..))
import Foreign.C.Error (Errno (..), eINPROGRESS, eINTR, errnoToIOError, getErrno, eAGAIN, eWOULDBLOCK)
import Foreign.C.Types (CInt (..), CSize (..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (Ptr)
import Network.Socket
  ( AddrInfo (..),
    HostName,
    PortNumber,
    AddrInfoFlag (AI_NUMERICHOST, AI_NUMERICSERV),
    Family (AF_INET, AF_INET6, AF_UNIX),
    NameInfoFlag (NI_NUMERICHOST),
    SockAddr (SockAddrInet6, SockAddrUnix),
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
import Network.Streaming.Classes (Block (..), Effect (..), Event (..), Exit (..), Race (..), Runtime, Scope (..), Time (..), millis, plus)
import Network.Streaming.Combinators (pattern Again, pattern Finish, _2, done, iterateA, keeping, tap, through)
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Descriptors (fresh)
import Network.Streaming.Mode (Instrumented (bump))
import qualified System.Posix.Types as Posix

-- | Connects to the address, with the resolver for a host name and the TCP options that go with it, or says why it
-- couldn't. The socket is closed if the part is cancelled while it connects, or if it didn't connect. A host name is
-- looked up, and the addresses it has are tried in order; the first that connects is the result, and if none does, each
-- failure is in the 'UnreachableHost'.
connectSocket :: (Runtime arr) => arr (Maybe Resolver, (TcpOptions, Address)) (Either ConnectError Socket)
connectSocket =
  (\(resolver, (options, address)) -> case address of UnixAddress path -> Left path; TcpAddress host port -> Right (options, (resolver, (host, port)))) ^>> (unix ||| tcp)
  where
    unix =
      (\path -> ((socket AF_UNIX Stream defaultProtocol, SockAddrUnix path), path))
        ^>> first attemptTo
        >>^ \(result, path) -> either (Left . UnreachableSocket path) Right result
    tcp = (through _2 lookedUp >>^ \(options, looked) -> fmap (options,) looked) >>> (arr Left ||| each)
    each = (\(options, (host, port, found)) -> Round options host port found []) ^>> iterateA step
    -- The next address, which the loop tries, or the end of the addresses.
    step = choose ^>> (returnA ||| (first attemptTo >>> now concluded))
    choose (Round options host port found failures) = case found of
      [] -> Left (Finish (Left (UnreachableHost host port (reverse failures))))
      address : others -> Right ((opening options address, addrAddress address), (Round options host port others failures, address))
    opening options found = do
      sock <- openSocket found
      -- Messages are small and sent whole, so don't wait to coalesce them.
      (setSocketOption sock NoDelay 1 *> setTcpOptions sock options) `onException` close sock
      pure sock
    concluded (result, (Round options host port others failures, address)) = case result of
      Right sock -> pure (Finish (Right sock))
      Left failure -> (\numeric -> Again (Round options host port others ((numeric, failure) : failures))) . fromMaybe host . fst <$> getNameInfo [NI_NUMERICHOST] True False (addrAddress address)

-- | The addresses that a host name has for a port, in the order that they are to be tried.
type Found = (HostName, PortNumber, [AddrInfo])

-- | What the loop over the addresses of a host knows: the TCP options, the host and the port, the addresses that are left
-- to try, and the failure of each address tried, the latest first, with the address as numbers.
data Round = Round TcpOptions HostName PortNumber [AddrInfo] [(String, IOException)]

-- | The addresses of the host: a numeric one is one address, which no lookup is needed for, and a name is looked up by the
-- resolver.
lookedUp :: (Runtime arr) => arr (Maybe Resolver, (HostName, PortNumber)) (Either ConnectError Found)
lookedUp = now numeric >>> (byName ||| arr Right)
  where
    numeric (resolver, (host, port)) =
      either (\(_ :: IOException) -> Left (resolver, (host, port))) (Right . (host,port,))
        <$> try (getAddrInfo (Just defaultHints {addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream}) (Just host) (Just (show port)))

-- | A lookup, with the host and the port that it is for.
data Named = Named Lookup HostName PortNumber

-- | A name's addresses, from the resolver, which the part drives: the lookup is started, and then the part blocks on the
-- sockets that the resolver waits for, and until its next timeout, whichever comes first, and processes the resolver.
-- A deadline or a cancellation reaches the wait, and the lookup is ended however the part ends, so that the resolver
-- stops asking for what nobody waits for. Without a resolver a name can't be looked up.
byName :: (Runtime arr) => arr (Maybe Resolver, (HostName, PortNumber)) (Either ConnectError Found)
byName = resolvable ^>> (now (\host -> pure (Left (UnresolvedHost host "no resolver was given in the settings"))) ||| asked)
  where
    resolvable (resolver, (host, port)) = maybe (Left host) (\known -> Right (known, (host, port))) resolver
    asked = scope (now (\(resolver, (host, port)) -> startLookup resolver host port)) (now (endLookup . fst)) (named ^>> iterateA looking)
    named (started, (_, (host, port))) = Named started host port
    -- The answer, or the wait that goes before the next look.
    looking = now looked >>> (waited ||| arr Finish)
    looked current@(Named started host port) =
      progress started >>= \case
        Answered answer -> pure (Right (resolved host port answer))
        Waiting sockets delay -> pure (Left (current, (sockets, delay)))
    -- The wait is for the first two events of the resolver's sockets and for its timeout, whichever comes first, and then
    -- the resolver is processed for all of its sockets. A resolver with more than two events is looked at again after 50
    -- milliseconds at most, and one with no timeout after a second. A wait that fails does so for a socket that another
    -- lookup closed meanwhile, which the resolver has dropped by then: it is processed again.
    waited =
      (keeping clock >>^ events)
        >>> tap (tryA (race (first3 ^>> block) (race (second3 ^>> block) (third3 ^>> block))) >>> done)
        >>> now (\(current@(Named started _ _), _) -> Again current <$ process started)
    events (instant, (current, (sockets, delay))) =
      let limit = if length sockets > 2 then min (millis 50) else id
          deadline = Until (plus instant (limit (fromMaybe (millis 1000) delay)))
       in case sockets of
            one : two : _ -> (current, (one, two, deadline))
            [one] -> (current, (one, deadline, deadline))
            [] -> (current, (deadline, deadline, deadline))
    first3 (_, (one, _, _)) = one
    second3 (_, (_, two, _)) = two
    third3 (_, (_, _, deadline)) = deadline

-- | The addresses that a lookup gave, or why there are none.
resolved :: HostName -> PortNumber -> Either String [SockAddr] -> Either ConnectError Found
resolved host port = \case
  Left reason -> Left (UnresolvedHost host reason)
  Right [] -> Left (UnresolvedHost host "no address found")
  Right addresses -> Right (host, port, [AddrInfo [] (familyOf address) Stream defaultProtocol address Nothing | address <- addresses])
  where
    familyOf = \case SockAddrInet6 {} -> AF_INET6; SockAddrUnix _ -> AF_UNIX; _ -> AF_INET

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
