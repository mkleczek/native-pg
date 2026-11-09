{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | The byte-stream interface implemented by transport links, and the raw
-- socket link.
module Network.Streaming.Link
  ( Link (..),
    Interest (..),
    Blocked (..),
    StreamLengthError (..),
  )
where

import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.Word (Word8)
import Foreign.Ptr (Ptr)
import Network.Socket (Socket, close)
import Network.Streaming.Watch (Watch (..), awaitFirst, awaitWatch, fdReadable, fdWritable)
import System.Posix.Types (Fd (..))
#if defined(mingw32_HOST_OS)
import qualified Data.ByteString as BS
import Network.Socket (recvBuf)
import Network.Socket.ByteString (sendMany)
#else
import Control.Concurrent (threadWaitRead, threadWaitWrite)
import Control.Monad (void, when)
import qualified Data.ByteString.Unsafe as BS
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CChar, CInt (..), CSize (..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (nullPtr)
import Foreign.Storable (pokeByteOff, sizeOf)
import GHC.Event (Event, FdKey, IOCallback, Lifetime (..), evtRead, evtWrite, getSystemEventManager, registerFd, unregisterFd)
import Network.Socket (touchSocket, withFdSocket)
import Network.Streaming.Watch (awaitCallback)
import qualified System.Posix.Types as Posix
#endif

-- | A stream of bytes to the server, such as a socket or TLS over it.
-- Reading, sending and flushing take what's ready without waiting.
-- Readiness watches register interest in an operation being retryable, and
-- fire as STM; they don't read or write, and don't guarantee progress. For
-- example, socket input may contain only part of a TLS record.
--
-- Callers must ensure exclusive access to each connection. Receiving,
-- sending, flushing, watching and closing must not overlap, whether called
-- on the link directly or through any @Channel@ built from it. Ownership
-- may pass to another thread between calls. Different connections may be
-- used concurrently.
class Link link where
  -- | Registers interest in receiving being retryable after 'receiveReady'
  -- returns 'Nothing', and fires once it can be. The caller releases the
  -- watch.
  watchReadable :: link -> IO Watch

  -- | Registers interest in sending or flushing being retryable after
  -- stalling, and fires once it can be. The caller releases the watch.
  watchWritable :: link -> IO Watch

  -- | Waits until any of what the interest includes may be retried.
  -- Readiness is a hint, so the caller retries every operation that is
  -- blocked, not only the one that fired. Returns at once if the interest is
  -- empty. The wait is interruptible and releases its watches when it returns
  -- or is interrupted. It waits on the watches, unless the link can wait more
  -- directly.
  awaitAny :: link -> Interest -> IO ()
  awaitAny link = awaitWatches (watchReadable link) (watchWritable link)

  -- | 'awaitAny' for input and for being writable, which says whether input
  -- may be retried, also when both may.
  awaitReadOrWrite :: link -> IO Bool
  awaitReadOrWrite link = awaitFirst [(watchReadable link, True), (watchWritable link, False)]

  -- | Reads what's ready into the buffer, up to its size, without waiting:
  -- 'Nothing' if nothing is, and 0 at the end of the stream.
  receiveReady :: link -> Ptr Word8 -> Int -> IO (Maybe Int)

  -- | Takes what it can of the pieces without waiting, and gives how many
  -- bytes it took. The socket link sends them with one @writev@, whose
  -- iovec limit it uses, so it takes at most 16 pieces at a time: a caller
  -- offers no more. It offers at least one piece, since an empty send fails
  -- with @EINVAL@ on macOS.
  sendReady :: link -> [ByteString] -> IO Int

  -- | Writes what it took but hasn't written yet, without waiting. True if
  -- nothing is left.
  flushReady :: link -> IO Bool

  -- | Closes the link and releases its underlying connection.
  closeLink :: link -> IO ()

-- | What a caller is blocked on, and so what 'awaitAny' waits for: input,
-- and what its output waits for.
data Interest = Interest
  { interestInput :: !Bool,
    interestOutput :: !Blocked
  }

-- | What output waits for.
data Blocked
  = -- | Nothing: none is left, or none is wanted.
    Done
  | -- | The link took only part of what it was offered, or nothing: wait for
    -- it to be writable.
    OnWritable

-- | Waits on the watches of what the interest includes, which the actions
-- register.
awaitWatches :: IO Watch -> IO Watch -> Interest -> IO ()
awaitWatches readable writable (Interest input output) = case output of
  Done
    | input -> awaitWatch readable
    | otherwise -> pure ()
  OnWritable
    | input -> awaitFirst [(readable, ()), (writable, ())]
    | otherwise -> awaitWatch writable
{-# NOINLINE awaitWatches #-}

-- | A parameter stream gave a different number of bytes than it declared.
-- The message it belongs to is incomplete, so the connection is unusable.
data StreamLengthError = StreamLengthError
  { declaredLength :: Int,
    -- | At least this many.
    deliveredLength :: Int
  }
  deriving (Show)

instance Exception StreamLengthError

-- | The raw socket link.
instance Link Socket where
  -- Registering a watch takes microseconds, so the watches aren't inlined,
  -- and nor are the waits that follow.
  watchReadable = socketWatch fdReadable
  {-# NOINLINE watchReadable #-}
  watchWritable = socketWatch fdWritable
  {-# NOINLINE watchWritable #-}
  -- Waiting for one event or for both needs no watch, and it is the same
  -- call every time, so it isn't inlined.
  awaitAny = socketAwait
  {-# NOINLINE awaitAny #-}
  awaitReadOrWrite = socketReadOrWrite
  {-# NOINLINE awaitReadOrWrite #-}
  receiveReady = tryReceive
  {-# INLINE receiveReady #-}
  sendReady = trySend
  {-# INLINE sendReady #-}
  flushReady _ = pure True
  {-# INLINE flushReady #-}
  closeLink = close
  {-# INLINE closeLink #-}

socketAwait :: Socket -> Interest -> IO ()
#if defined(mingw32_HOST_OS)
socketAwait sock = awaitWatches (watchReadable sock) (watchWritable sock)

socketReadOrWrite :: Socket -> IO Bool
socketReadOrWrite sock = awaitFirst [(watchReadable sock, True), (watchWritable sock, False)]
#else
socketAwait sock (Interest input output) = case output of
  Done
    | input -> withFdSocket sock (threadWaitRead . Fd)
    | otherwise -> pure ()
  OnWritable
    | input -> void (socketReadOrWrite sock)
    | otherwise -> withFdSocket sock (threadWaitWrite . Fd)

-- | Waits until the socket is readable or writable, as 'threadWaitRead' and
-- 'threadWaitWrite' wait for one of them, with one registration for both
-- events in the IO manager, and says whether it is readable. Without one, as
-- in the non-threaded runtime, it waits on both watches.
socketReadOrWrite :: Socket -> IO Bool
socketReadOrWrite sock =
  getSystemEventManager >>= \case
    Nothing -> awaitFirst [(watchReadable sock, True), (watchWritable sock, False)]
    Just manager -> do
      event <- withFdSocket sock $ \fd ->
        awaitCallback (registerBoth (registerFd manager) (Fd fd)) (unregisterFd manager)
      -- As in base, the descriptor was closed, which is reported as neither
      -- event, since 'GHC.Event' doesn't export the event for it.
      when (event <> bothEvents /= bothEvents) $
        ioError (Errno.errnoToIOError "threadWait" Errno.eBADF Nothing Nothing)
      pure (event <> evtWrite == bothEvents)
{-# NOINLINE socketReadOrWrite #-}

-- | Registers the callback for both events, with a function that registers a
-- callback. 'registerFd' is passed as a function and not applied, because it
-- is inlined where it is applied, which copies its body here.
registerBoth :: (IOCallback -> Fd -> Event -> Lifetime -> IO FdKey) -> Fd -> (Event -> IO ()) -> IO FdKey
registerBoth register fd callback = register (\_ -> callback) fd bothEvents OneShot
{-# NOINLINE registerBoth #-}

bothEvents :: Event
bothEvents = evtRead <> evtWrite
#endif

socketWatch :: (Fd -> IO Watch) -> Socket -> IO Watch
#if defined(mingw32_HOST_OS)
-- Sockets block on Windows, so receiving and sending never stall and nothing
-- needs a watch. A retry that is asked for anyway is allowed at once.
socketWatch _ _ = pure (Watch (pure ()) (pure ()))
#else
-- The watch keeps the socket open until it's released, as a wait that held
-- the descriptor would.
socketWatch watchFd sock = do
  watch <- withFdSocket sock (watchFd . Fd)
  pure watch {watchCancel = watchCancel watch *> touchSocket sock}
#endif

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
