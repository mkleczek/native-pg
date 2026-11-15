{-# LANGUAGE LambdaCase #-}

-- | The resolver of host names, through c-ares in its mode without threads of its own: a lookup is started, and the
-- program asks the resolver which sockets it waits for and when it next has to be processed, waits for them in its own
-- way, and processes the resolver, which never waits, until the lookup has its answer. No thread and no blocking call
-- is needed, so a program waits for a name as it waits for anything else.
--
-- A resolver is made once, in IO, when a program starts, and every lookup of a name on any core uses it. c-ares reads
-- the system's configuration, @\/etc\/resolv.conf@ and the other files of the platform, when the channel is made, and
-- not again. Its hosts file is read when the channel is made too (by a lookup that nothing waits for), kept, and looked
-- at with one @stat@ for every lookup: a lookup reads it again only if its modification time says that it has changed
-- since. The environment variable @HOSTALIASES@, a file that c-ares would read for every lookup, is not honoured. The
-- channel is safe to use from every thread, since c-ares locks it for the length of each call, and none of the calls
-- waits.
module Database.PostgreSQL.Internal.Cares
  ( Resolver,
    ResolverOptions (..),
    defaultResolverOptions,
    newResolverWith,
    Lookup,
    startLookup,
    endLookup,
    Progress (..),
    progress,
    process,
  )
where

import Control.Exception (throwIO)
import Control.Monad (when)
import Data.Bits ((.&.))
import Foreign.C.String (CString, peekCString, withCString)
import Foreign.C.Types (CInt (..))
import qualified Foreign.Concurrent as Foreign
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (allocaArray, peekArray)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek)
import Network.Socket (HostName, PortNumber, SockAddr)
import Network.Socket.Address (peekSocketAddress)
import Network.Streaming.Classes (Duration, Event (..), millis)
import Network.Streaming.Watch (anyWatch, fdReadable, fdWritable)
import System.Posix.Types (Fd (..))

-- | What looks names up: a c-ares channel, freed with the last reference to it, and whether its lookups sort the addresses.
data Resolver = Resolver !(ForeignPtr Channel) !Bool

data Channel

-- | A lookup that is under way: the channel it is asked of, which it keeps alive, and what c-ares fills.
data Lookup = Lookup !(ForeignPtr Channel) !(Ptr Pending)

data Pending

data Node

-- | How a resolver is made: where its configuration comes from.
data ResolverOptions = ResolverOptions
  { -- | The name servers as a comma-separated list of addresses, each with a port if it isn't 53, in place of the
    -- system's.
    resolverServers :: Maybe String,
    -- | The milliseconds that each try of a query waits for an answer, in place of the system's.
    resolverTimeout :: Maybe Int,
    -- | How many times a query is tried, in place of the system's.
    resolverTries :: Maybe Int,
    -- | The hosts file, in place of the system's.
    resolverHostsFile :: Maybe FilePath,
    -- | Whether a name that isn't found is looked up with each of the system's search domains added.
    resolverSearchDomains :: Bool,
    -- | Whether the addresses of a name are put in the order that RFC 6724 gives, as @getaddrinfo@ puts them, which
    -- prefers the family and the address that a connection from here can use. Without it they are in the order that
    -- the server gave them.
    resolverSort :: Bool
  }

-- | The system's configuration, and the addresses sorted.
defaultResolverOptions :: ResolverOptions
defaultResolverOptions = ResolverOptions Nothing Nothing Nothing Nothing True True

-- | A resolver that is made as the options say. It reads the files of the system's configuration, so a program makes it
-- where it starts, and not where it connects. It fails with an 'IOError' if c-ares can't make one, or has no thread
-- support, which a resolver that every core uses needs.
newResolverWith :: ResolverOptions -> IO Resolver
newResolverWith options =
  optional (resolverServers options) $ \servers ->
    optional (resolverHostsFile options) $ \hosts ->
      alloca $ \status -> do
        channel <-
          c_new
            servers
            (maybe 0 fromIntegral (resolverTimeout options))
            (maybe 0 fromIntegral (resolverTries options))
            hosts
            (if resolverSearchDomains options then 1 else 0)
            status
        if channel /= nullPtr
          then (\pointer -> Resolver pointer (resolverSort options)) <$> Foreign.newForeignPtr channel (c_destroy channel)
          else do
            code <- peek status
            reason <- if code < 0 then pure "c-ares was built without thread support" else c_error code >>= peekCString
            throwIO (userError ("could not make a resolver: " <> reason))
  where
    optional value use = maybe (use nullPtr) (\present -> withCString present use) value

-- | Starts the lookup of a name, for a port. The answer may be there already, for a name in the hosts file.
startLookup :: Resolver -> HostName -> PortNumber -> IO Lookup
startLookup (Resolver channel sorted) host port =
  withCString host $ \name ->
    withCString (show port) $ \service ->
      withForeignPtr channel $ \pointer -> do
        pending <- c_lookup pointer name service (if sorted then 1 else 0)
        when (pending == nullPtr) (throwIO (userError "could not start a lookup: out of memory"))
        pure (Lookup channel pending)

-- | Ends the lookup, whether it has its answer or not: what c-ares still asks for it is given up, and the lookup is
-- freed. It never waits.
endLookup :: Lookup -> IO ()
endLookup (Lookup channel pending) = withForeignPtr channel $ \_ -> c_end pending

-- | Where a lookup is.
data Progress
  = -- | It has its answer: why there is none, or the addresses, in the order to try them.
    Answered (Either String [SockAddr])
  | -- | It waits for this event, if the resolver has sockets to wait for, and at most this long, if a query can time out.
    Waiting (Maybe Event) (Maybe Duration)

-- | Whether the lookup has its answer, and else what it waits for.
progress :: Lookup -> IO Progress
progress (Lookup channel pending) = withForeignPtr channel $ \pointer ->
  c_done pending >>= \case
    0 -> allocaArray sockets $ \fds -> allocaArray sockets $ \wants -> alloca $ \remaining -> do
      count <- fromIntegral <$> c_wanted pointer fds wants (fromIntegral sockets) remaining
      watched <- zip <$> peekArray count fds <*> peekArray count wants
      milliseconds <- peek remaining
      pure (Waiting (combined (concatMap watching watched)) (if milliseconds < 0 then Nothing else Just (millis (fromIntegral milliseconds))))
    _ -> Answered <$> answer
  where
    sockets = 16
    -- Each socket, for what the resolver wants of it: reading or writing, or both.
    watching (fd, wants) = [(False, Fd fd) | wants .&. 1 /= 0] <> [(True, Fd fd) | wants .&. 2 /= 0]
    combined = \case
      [] -> Nothing
      [(writing, fd)] -> Just (if writing then Writable fd else Readable fd)
      several -> Just (Watched (anyWatch [if writing then fdWritable fd else fdReadable fd | (writing, fd) <- several]))
    answer = do
      status <- c_status pending
      if status /= 0
        then Left <$> (c_error status >>= peekCString)
        else Right <$> (c_first pending >>= addresses)
    addresses node
      | node == nullPtr = pure []
      | otherwise = (:) <$> (c_address node >>= peekSocketAddress) <*> (c_next node >>= addresses)

-- | Does what the sockets that are ready, and the queries that have timed out, ask of the lookup's resolver. A lookup
-- that has its answer by then says so in its 'progress'. It never waits.
process :: Lookup -> IO ()
process (Lookup channel _) = withForeignPtr channel c_process

foreign import ccall unsafe "np_cares_new"
  c_new :: CString -> CInt -> CInt -> CString -> CInt -> Ptr CInt -> IO (Ptr Channel)

foreign import ccall unsafe "np_cares_destroy"
  c_destroy :: Ptr Channel -> IO ()

foreign import ccall unsafe "np_cares_error"
  c_error :: CInt -> IO CString

foreign import ccall unsafe "np_cares_lookup"
  c_lookup :: Ptr Channel -> CString -> CString -> CInt -> IO (Ptr Pending)

foreign import ccall unsafe "np_lookup_done"
  c_done :: Ptr Pending -> IO CInt

foreign import ccall unsafe "np_lookup_status"
  c_status :: Ptr Pending -> IO CInt

foreign import ccall unsafe "np_lookup_first"
  c_first :: Ptr Pending -> IO (Ptr Node)

foreign import ccall unsafe "np_node_next"
  c_next :: Ptr Node -> IO (Ptr Node)

foreign import ccall unsafe "np_node_address"
  c_address :: Ptr Node -> IO (Ptr SockAddr)

foreign import ccall unsafe "np_lookup_end"
  c_end :: Ptr Pending -> IO ()

foreign import ccall unsafe "np_cares_wanted"
  c_wanted :: Ptr Channel -> Ptr CInt -> Ptr CInt -> CInt -> Ptr CInt -> IO CInt

foreign import ccall unsafe "np_cares_process"
  c_process :: Ptr Channel -> IO ()
