{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | The raw socket link.
module Database.PostgreSQL.Internal.Socket.Link () where

import Control.Concurrent (threadWaitRead, threadWaitReadSTM, threadWaitWrite, threadWaitWriteSTM)
import Control.Exception (bracket)
import Data.ByteString (ByteString)
import Data.Word (Word8)
import Database.PostgreSQL.Transport.Link
import GHC.Conc (atomically, orElse)
import Network.Socket (Socket, close, withFdSocket)
import System.Posix.Types (Fd (..))
#if defined(mingw32_HOST_OS)
import qualified Data.ByteString as BS
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
