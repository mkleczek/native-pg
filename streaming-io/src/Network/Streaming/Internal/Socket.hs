{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | A socket's own calls that never wait: what the socket's 'Network.Streaming.Classes.Link'
-- instance tries, and blocks on the socket's event while they find nothing
-- ready. They are the @recv@, @send@ and @writev@ of the operating system, on a
-- socket that doesn't block.
module Network.Streaming.Internal.Socket
  ( tryReceive,
    receiveFirst,
    trySend,
    trySendBatch,
  )
where

import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as Bytes
import qualified Data.ByteString.Unsafe as BS
import Data.Word (Word8)
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CChar, CInt (..), CSize (..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (pokeByteOff, sizeOf)
import Network.Socket (Socket, withFdSocket)
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Descriptors (receiveNoted, receivesFirst, sendNoted, skipsReceive)
import Network.Streaming.Mode (Instrumented (bump))
import qualified System.Posix.Types as Posix

-- | Reads what the socket has into the buffer, without waiting: 'Nothing' if
-- it has nothing, and 0 at the end of the stream. A receive that gave fewer bytes than
-- the buffer holds drained the socket. If something was sent on the socket after that, the
-- other end can't have answered yet, so the call says 'Nothing' without reading: the caller
-- waits for input, which finds what has arrived by then, and reads once it has. Without a
-- send between, the rest of what the other end is sending may be on its way, and the call reads.
tryReceive :: forall m. (Instrumented m) => Socket -> Ptr Word8 -> Int -> IO (Maybe Int)
tryReceive sock pointer size =
  withFdSocket sock $ \fd -> do
    let descriptor = fromIntegral fd
        attempt =
          bump @m Receives *> c_recv fd pointer (fromIntegral size) 0 >>= \case
            -1 ->
              Errno.getErrno >>= \errno ->
                if
                  | errno == Errno.eINTR -> attempt
                  | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> receiveNoted descriptor size 0 *> (Nothing <$ bump @m ReceivesEmpty)
                  | otherwise -> Errno.throwErrno "recv"
            received -> do
              receiveNoted descriptor size (fromIntegral received)
              pure (Just (fromIntegral received))
    skip <- skipsReceive descriptor
    if skip then Nothing <$ bump @m ReceivesSkipped else attempt

-- Inlined so that a program specialised to its arrow gets the receive without a boxed result.
{-# INLINE tryReceive #-}

-- | The next receive on the socket reads before it waits: what the socket has was not read, or is not known to have been.
receiveFirst :: Socket -> IO ()
receiveFirst sock = withFdSocket sock (receivesFirst . fromIntegral)

foreign import ccall unsafe "recv"
  c_recv :: CInt -> Ptr Word8 -> CSize -> CInt -> IO Posix.CSsize

-- | Sends what the socket takes of at most 16 nonempty pieces, in one call,
-- without waiting for room in it, and gives how many bytes it took.
trySend :: forall m. (Instrumented m) => Socket -> [ByteString] -> IO Int
trySend sock pieces = offer @m sock pieces (\sent _ _ _ -> pure sent)
{-# INLINE trySend #-}

-- | Tries one offer, then runs the continuation for completion, more pieces,
-- or a blocked socket. A full write can use the tail found during preparation.
-- A short write finds the unsent suffix by walking the accepted bytes.
trySendBatch :: forall m r. (Instrumented m) => Socket -> [ByteString] -> IO r -> ([ByteString] -> IO r) -> ([ByteString] -> IO r) -> IO r
trySendBatch sock pieces finished more blocked = offer @m sock pieces $ \sent bytes following offered ->
  if sent == bytes
    then case nonempty following of
      [] -> finished
      rest -> more rest
    else blocked (dropSent sent offered)
  where
    dropSent _ [] = []
    dropSent sent remaining@(piece : rest)
      | sent == 0 = nonempty remaining
      | sent >= size = dropSent (sent - size) rest
      | otherwise = Bytes.drop sent piece : rest
      where
        size = Bytes.length piece
{-# INLINE trySendBatch #-}

-- Prepare the offer directly from the input. The nested CString uses keep
-- every piece alive through the system call, including its EINTR retries.
offer :: forall m r. (Instrumented m) => Socket -> [ByteString] -> (Int -> Int -> [ByteString] -> [ByteString] -> IO r) -> IO r
offer sock pieces reported = case nonempty pieces of
  [] -> reported 0 0 [] []
  offered@(first : rest) -> withFdSocket sock $ \fd -> case nonempty rest of
    [] -> BS.unsafeUseAsCStringLen first $ \(pointer, size) -> do
      sent <- sending @m fd "send" (c_send fd pointer (fromIntegral size) 0)
      reported sent size [] offered
    second : remaining ->
      let following = nonempty remaining
          -- The short gather is common for a head and body. Look ahead once
          -- and pass that same tail to the writer.
          capacity = case following of
            [] -> 2
            _ -> offerLimit
       in allocaBytes (capacity * iovecSize) $ \iovecs ->
        let place offset piece resume =
              BS.unsafeUseAsCStringLen piece $ \(pointer, size) -> do
                pokeByteOff iovecs offset pointer
                pokeByteOff iovecs (offset + sizeOf pointer) (fromIntegral size :: CSize)
                resume size
            describe !count !bytes piece others = place (count * iovecSize) piece $ \size -> do
              let next = count + 1
                  total = bytes + size
              if next == offerLimit
                then write next total others
                else case nonempty others of
                  [] -> write next total []
                  nextPiece : rest' -> describe next total nextPiece rest'
            write count bytes suffix = do
              sent <- sending @m fd "writev" (c_writev fd iovecs (fromIntegral count))
              reported sent bytes suffix offered
         in place 0 first (\size -> describe 1 size second following)
  where
    offerLimit = 16
    iovecSize = sizeOf (nullPtr :: Ptr CChar) + sizeOf (0 :: CSize)
{-# INLINE offer #-}

nonempty :: [ByteString] -> [ByteString]
nonempty = dropWhile Bytes.null
{-# INLINE nonempty #-}

sending :: forall m. (Instrumented m) => CInt -> String -> IO Posix.CSsize -> IO Int
sending fd operation call = attempt
  where
    attempt = bump @m Sends *> call >>= \case
      -1 -> Errno.getErrno >>= \errno ->
        if
          | errno == Errno.eINTR -> attempt
          | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> pure 0
          | otherwise -> Errno.throwErrno operation
      sent -> fromIntegral sent <$ when (sent > 0) (sendNoted (fromIntegral fd))
{-# INLINE sending #-}

foreign import ccall unsafe "send"
  c_send :: CInt -> Ptr CChar -> CSize -> CInt -> IO Posix.CSsize

-- network's sockets don't block, so a full socket fails with EAGAIN.
foreign import ccall unsafe "writev"
  c_writev :: CInt -> Ptr () -> CInt -> IO Posix.CSsize
