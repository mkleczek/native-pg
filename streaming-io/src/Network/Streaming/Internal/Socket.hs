{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | A socket's own calls that never wait: what the socket's 'Network.Streaming.Classes.Link'
-- instance tries, and blocks on the socket's event while they find nothing
-- ready. They are the @recv@ and @writev@ of the operating system, on a
-- socket that doesn't block.
module Network.Streaming.Internal.Socket
  ( tryReceive,
    trySend,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Unsafe as BS
import Data.Word (Word8)
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CChar, CInt (..), CSize (..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (pokeByteOff, sizeOf)
import Network.Socket (Socket, withFdSocket)
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Mode (Instrumented (bump))
import qualified System.Posix.Types as Posix

-- | Reads what the socket has into the buffer, without waiting: 'Nothing' if
-- it has nothing, and 0 at the end of the stream.
tryReceive :: forall m. (Instrumented m) => Socket -> Ptr Word8 -> Int -> IO (Maybe Int)
tryReceive sock pointer size =
  withFdSocket sock $ \fd ->
    let attempt =
          bump @m Receives *> c_recv fd pointer (fromIntegral size) 0 >>= \case
            -1 ->
              Errno.getErrno >>= \errno ->
                if
                  | errno == Errno.eINTR -> attempt
                  | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> Nothing <$ bump @m ReceivesEmpty
                  | otherwise -> Errno.throwErrno "recv"
            received -> pure (Just (fromIntegral received))
     in attempt

foreign import ccall unsafe "recv"
  c_recv :: CInt -> Ptr Word8 -> CSize -> CInt -> IO Posix.CSsize

-- | Sends what the socket takes of the pieces, in one call, without waiting
-- for room in it, and gives how many bytes it took. It takes at most 16
-- pieces at a time, which is the iovec limit it uses: a caller offers no more.
-- It offers at least one piece, since an empty send fails with @EINVAL@ on
-- macOS.
trySend :: forall m. (Instrumented m) => Socket -> [ByteString] -> IO Int
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
            bump @m Sends *> c_writev fd iovecs (fromIntegral (length pieces)) >>= \case
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
