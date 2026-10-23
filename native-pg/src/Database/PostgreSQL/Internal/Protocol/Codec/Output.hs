{-# LANGUAGE LambdaCase #-}

-- | Serialized bytes with their length known before they are written, so a
-- length prefix can be written ahead of them.
module Database.PostgreSQL.Internal.Protocol.Codec.Output
  ( ByteStream (..),
    Output,
    Writer,
    WriteResult (..),
    outputWriter,
    runWriter,
    outputLength,
    outputCopied,
    Uncopied (..),
    writeOutput,
    outputByteString,
    outputs,
    word8,
    word16,
    int16,
    word32,
    int32,
    bytes,
    cstring,
    stream,
    borrowedStream,
  )
where

import Data.Bits (shiftR)
import Data.ByteString (ByteString)
import Network.Streaming (Source)
import Database.PostgreSQL.Streaming.Types (ByteStream (..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU
import Data.Int (Int16, Int32)
import Data.Word (Word16, Word32, Word8)
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import Foreign.Storable (pokeByteOff)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import System.IO.Unsafe (unsafeDupablePerformIO)

-- | The sizes are strict so length prefixes need not run the writer.
-- The writer has a tail continuation, so composition can fuse adjacent pokes
-- without allocating an instruction for every protocol field.
data Output = Output
  { outputLength :: {-# UNPACK #-} !Int,
    -- | The bytes 'writeOutput' copies to its buffer: all but long byte
    -- strings and streams.
    outputCopied :: {-# UNPACK #-} !Int,
    outputBuild :: Writer -> Writer
  }

-- | Write into the current buffer, stopping when a field needs more room or an
-- uncopied part is reached. Resuming 'NeedSpace' requires an empty buffer
-- with at least four bytes; its pointer and capacity may differ from before.
-- After 'Yield', resume at the same offset to retain contiguous copied bytes,
-- or at zero once the previous buffer's bytes have been accepted.
newtype Writer = Writer {runWriter :: Ptr Word8 -> Int -> Int -> IO WriteResult}

data WriteResult
  = Finished {-# UNPACK #-} !Int
  | NeedSpace {-# UNPACK #-} !Int Writer
  | Yield {-# UNPACK #-} !Int Uncopied Writer

outputWriter :: Output -> Writer
outputWriter Output {outputBuild} = outputBuild (Writer (\_ _ used -> pure (Finished used)))
{-# INLINE outputWriter #-}

-- | A part of an output that's sent from where it is, rather than copied.
data Uncopied
  = UncopiedBytes ByteString
  | UncopiedStream ByteStream
  | UncopiedBorrowedStream Int Source

instance Semigroup Output where
  Output leftLength leftCopied left <> Output rightLength rightCopied right =
    Output (leftLength + rightLength) (leftCopied + rightCopied) (left . right)
  {-# INLINE (<>) #-}

instance Monoid Output where
  mempty = Output 0 0 id
  {-# INLINE mempty #-}

-- | Writes the output's copied bytes to the memory, which has room for them,
-- and gives its parts in order: slices of the memory, and what isn't copied.
writeOutput :: ForeignPtr Word8 -> Output -> IO [Either ByteString Uncopied]
writeOutput memory output =
  withForeignPtr memory $ \buffer ->
    let copied start end parts
          | end > start = Left (BSI.BS (plusForeignPtr memory start) (end - start)) : parts
          | otherwise = parts
        go start offset writer parts =
          runWriter writer buffer (outputCopied output) offset >>= \case
            Finished end -> pure (reverse (copied start end parts))
            NeedSpace {} -> error "copied output exceeds its allocated size"
            Yield end part rest -> go end end rest (Right part : copied start end parts)
     in go 0 0 (outputWriter output) []

-- | The bytes of an output that has no streams.
outputByteString :: Output -> ByteString
outputByteString output = unsafeDupablePerformIO $ do
  parts <- (`writeOutput` output) =<< mallocPlainForeignPtrBytes (outputCopied output)
  pure . BS.concat $ map (either id uncopiedBytes) parts
  where
    uncopiedBytes (UncopiedBytes value) = value
    uncopiedBytes (UncopiedStream _) = error "an output with streams has no byte string"
    uncopiedBytes (UncopiedBorrowedStream _ _) = error "an output with streams has no byte string"

-- | Each value's output, one after another. Compute sizes without retaining
-- a serializer for every value; build their writers as the sender advances.
outputs :: (a -> Output) -> [a] -> Output
outputs output values = Output (total outputLength) (total outputCopied) build
  where
    total field = foldl' (\subtotal value -> subtotal + field (output value)) 0 values
    build rest = foldr (outputBuild . output) rest values
{-# INLINE outputs #-}

word8 :: Word8 -> Output
word8 value = Output 1 1 (scalar 1 (\pointer -> pokeByteOff pointer 0 value))
{-# INLINE word8 #-}

word16 :: Word16 -> Output
word16 value = Output 2 2 (scalar 2 (`pokeWord16` value))
{-# INLINE word16 #-}

int16 :: Int16 -> Output
int16 = word16 . fromIntegral
{-# INLINE int16 #-}

word32 :: Word32 -> Output
word32 value = Output 4 4 (scalar 4 (`pokeWord32` value))
{-# INLINE word32 #-}

int32 :: Int32 -> Output
int32 = word32 . fromIntegral
{-# INLINE int32 #-}

-- Keep the retry's pointer arguments inside its lambda: a resumed writer
-- must never retain the buffer that the transport has just accepted. The
-- empty replacement buffer has room for every scalar, avoiding another check.
scalar :: Int -> (Ptr Word8 -> IO ()) -> Writer -> Writer
scalar size poke rest = Writer $ \pointer capacity used ->
  if capacity - used < size
    then pure (NeedSpace used (writeScalar size poke rest))
    else runWriter (writeScalar size poke rest) pointer capacity used
{-# INLINE scalar #-}

writeScalar :: Int -> (Ptr Word8 -> IO ()) -> Writer -> Writer
writeScalar size poke rest = Writer $ \pointer capacity used ->
  poke (pointer `plusPtr` used) *> runWriter rest pointer capacity (used + size)
{-# INLINE writeScalar #-}

pokeWord16 :: Ptr Word8 -> Word16 -> IO ()
pokeWord16 pointer value = do
  pokeByteOff pointer 0 (fromIntegral (value `shiftR` 8) :: Word8)
  pokeByteOff pointer 1 (fromIntegral value :: Word8)
{-# INLINE pokeWord16 #-}

pokeWord32 :: Ptr Word8 -> Word32 -> IO ()
pokeWord32 pointer value = do
  pokeByteOff pointer 0 (fromIntegral (value `shiftR` 24) :: Word8)
  pokeByteOff pointer 1 (fromIntegral (value `shiftR` 16) :: Word8)
  pokeByteOff pointer 2 (fromIntegral (value `shiftR` 8) :: Word8)
  pokeByteOff pointer 3 (fromIntegral value :: Word8)
{-# INLINE pokeWord32 #-}

-- | Byte strings this long or longer are sent from where they are, without
-- being copied. Shorter ones are copied, so that they go in one send with the
-- bytes around them.
uncopiedLength :: Int
uncopiedLength = 8192

bytes :: ByteString -> Output
bytes value
  | size == 0 = mempty
  | otherwise = Output size (if size >= uncopiedLength then 0 else size) $ \rest ->
      Writer $ \pointer capacity used ->
        if size >= uncopiedLength || size > capacity
          then pure (Yield used (UncopiedBytes value) rest)
          else
            if size > capacity - used
              then pure (NeedSpace used (resumeBytes value rest))
              else runWriter (writeBytes value rest) pointer capacity used
  where
    size = BS.length value
{-# INLINE bytes #-}

-- A short byte string can outgrow a replacement buffer. Recheck its capacity
-- after suspension so it can still be sent directly without blocking progress.
resumeBytes :: ByteString -> Writer -> Writer
resumeBytes value rest = Writer $ \pointer capacity used ->
  if BS.length value > capacity
    then pure (Yield used (UncopiedBytes value) rest)
    else runWriter (writeBytes value rest) pointer capacity used
{-# INLINE resumeBytes #-}

writeBytes :: ByteString -> Writer -> Writer
writeBytes value rest = Writer $ \pointer capacity used -> do
  BSU.unsafeUseAsCStringLen value $ \(source, size) ->
    copyBytes (pointer `plusPtr` used) (castPtr source) size
  runWriter rest pointer capacity (used + BS.length value)
{-# INLINE writeBytes #-}

-- | A zero byte inside the value would end the string early on the server,
-- so it is rejected.
cstring :: ByteString -> Output
cstring value
  | BS.elem 0 value = error "a C string sent to the server must not contain a zero byte"
  | otherwise = bytes value <> word8 0
{-# INLINE cstring #-}

-- | Bytes the client supplies as they're sent.
stream :: ByteStream -> Output
stream source = Output (byteStreamLength source) 0 $ \rest ->
  Writer $ \_ _ used -> pure (Yield used (UncopiedStream source) rest)
{-# INLINE stream #-}

-- | The source owns each chunk through its synchronous callback.
borrowedStream :: Int -> Source -> Output
borrowedStream count source = Output count 0 $ \rest ->
  Writer $ \_ _ used -> pure (Yield used (UncopiedBorrowedStream count source) rest)
{-# INLINE borrowedStream #-}
