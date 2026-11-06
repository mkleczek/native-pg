{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | The byte-stream interface implemented by transport links, and the raw
-- socket link.
module Network.Streaming.Link
  ( Link (..),
    Readiness (..),
    StreamLengthError (..),
  )
where

import Control.Concurrent (threadWaitRead, threadWaitReadSTM, threadWaitWrite, threadWaitWriteSTM)
import Control.Exception (Exception, bracket)
import Data.ByteString (ByteString)
import Data.Word (Word8)
import Foreign.Ptr (Ptr)
import GHC.Conc (atomically, orElse)
import Network.Socket (Socket, close, withFdSocket)
import System.Posix.Types (Fd (..))
#if defined(mingw32_HOST_OS)
import qualified Data.ByteString as BS
import Network.Socket (recvBuf)
import Network.Socket.ByteString (sendMany)
#else
import qualified Data.ByteString.Unsafe as BS
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CChar, CInt (..), CSize (..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (nullPtr)
import Foreign.Storable (pokeByteOff, sizeOf)
import qualified System.Posix.Types as Posix
#endif

-- | A stream of bytes to the server, such as a socket or TLS over it.
-- Reading, sending and flushing take what's ready without waiting.
-- Readiness waits suspend the calling thread until an operation can be
-- retried; they don't read or write, and don't guarantee progress. For
-- example, socket input may contain only part of a TLS record.
--
-- Callers must ensure exclusive access to each connection. Receiving,
-- sending, flushing, waiting and closing must not overlap, whether called
-- on the link directly or through any @Channel@ built from it. Ownership
-- may pass to another thread between calls. Different connections may be
-- used concurrently.
class Link link where
  -- | Waits until receiving can be retried after 'receiveReady' returns
  -- 'Nothing'. The wait must be interruptible.
  awaitReadable :: link -> IO ()

  -- | Waits until sending or flushing can be retried after stalling.
  -- The wait must be interruptible.
  awaitWritable :: link -> IO ()

  -- | Waits for either direction, so pending output can be sent while
  -- waiting for input. If both readiness events have been observed,
  -- prefers 'Readable'. The wait must be interruptible and release its
  -- registrations when it returns or is interrupted.
  awaitReadableOrWritable :: link -> IO Readiness

  -- | Reads what's ready into the buffer, up to its size, without waiting:
  -- 'Nothing' if nothing is, and 0 at the end of the stream.
  receiveReady :: link -> Ptr Word8 -> Int -> IO (Maybe Int)

  -- | Takes what it can of the pieces without waiting, and gives how many
  -- bytes it took.
  sendReady :: link -> [ByteString] -> IO Int

  -- | Writes what it took but hasn't written yet, without waiting. True if
  -- nothing is left.
  flushReady :: link -> IO Bool

  -- | Closes the link and releases its underlying connection.
  closeLink :: link -> IO ()

-- | Which operation to retry after waiting for either direction.
data Readiness = Readable | Writable
  deriving (Eq, Show)

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
  awaitReadable sock = withFdSocket sock (threadWaitRead . Fd)
  {-# INLINE awaitReadable #-}
  awaitWritable sock = withFdSocket sock (threadWaitWrite . Fd)
  {-# INLINE awaitWritable #-}
  awaitReadableOrWritable sock = withFdSocket sock $ \fd ->
    bracket (threadWaitReadSTM (Fd fd)) snd $ \(readable, _) ->
      bracket (threadWaitWriteSTM (Fd fd)) snd $ \(writable, _) ->
        atomically ((Readable <$ readable) `orElse` (Writable <$ writable))
  receiveReady = tryReceive
  {-# INLINE receiveReady #-}
  sendReady = trySend
  {-# INLINE sendReady #-}
  flushReady _ = pure True
  {-# INLINE flushReady #-}
  closeLink = close
  {-# INLINE closeLink #-}

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
