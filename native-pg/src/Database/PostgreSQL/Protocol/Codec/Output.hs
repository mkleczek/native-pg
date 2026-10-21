-- | Serialized bytes with their length known before they are written, so a
-- length prefix can be written ahead of them.
module Database.PostgreSQL.Protocol.Codec.Output
  ( ByteStream (..),
    Output,
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

import Control.Monad (when)
import Data.Bits (shiftR)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU
import Data.Functor (($>))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int16, Int32)
import Data.Word (Word16, Word32, Word8)
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr, minusPtr, plusPtr)
import Foreign.Storable (pokeByteOff)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import System.IO.Unsafe (unsafeDupablePerformIO)

-- | Bytes that arrive in chunks, with their total length known up front.
data ByteStream = ByteStream
  { byteStreamLength :: !Int,
    -- | The next chunk. An empty one means the end.
    byteStreamNext :: IO ByteString
  }

-- | Its length, how many of its bytes are copied when it's written, and how
-- to write it. Composing outputs composes their writes, so the fields of a
-- message become one function rather than a structure built at run time.
data Output = Output
  { outputLength :: {-# UNPACK #-} !Int,
    -- | The bytes 'writeOutput' copies to its buffer: all but long byte
    -- strings and streams.
    outputCopied :: {-# UNPACK #-} !Int,
    outputWrite :: Write
  }

-- | Writes the copied bytes at the pointer and gives where they end. A part
-- that isn't copied goes to the function, with where the copied bytes before
-- it end.
newtype Write = Write ((Ptr Word8 -> Uncopied -> IO ()) -> Ptr Word8 -> IO (Ptr Word8))

-- | A part of an output that's sent from where it is, rather than copied.
data Uncopied
  = UncopiedBytes ByteString
  | UncopiedStream ByteStream
  | UncopiedBorrowedStream ByteStream

instance Semigroup Output where
  Output leftLength leftCopied (Write left) <> Output rightLength rightCopied (Write right) =
    Output (leftLength + rightLength) (leftCopied + rightCopied) (Write (\uncopied pointer -> left uncopied pointer >>= right uncopied))
  {-# INLINE (<>) #-}

instance Monoid Output where
  mempty = Output 0 0 (Write (\_ pointer -> pure pointer))
  {-# INLINE mempty #-}

-- | Writes the output's copied bytes to the memory, which has room for them,
-- and gives its parts in order: slices of the memory, and what isn't copied.
writeOutput :: ForeignPtr Word8 -> Output -> IO [Either ByteString Uncopied]
writeOutput memory Output {outputWrite = Write write} =
  withForeignPtr memory $ \buffer -> do
    -- the parts so far, last first
    parts <- newIORef []
    start <- newIORef buffer
    let copiedUntil end = do
          from <- readIORef start
          writeIORef start end
          when (end > from) $
            modifyIORef' parts (Left (BSI.BS (plusForeignPtr memory (from `minusPtr` buffer)) (end `minusPtr` from)) :)
    end <- write (\end part -> copiedUntil end *> modifyIORef' parts (Right part :)) buffer
    copiedUntil end
    reverse <$> readIORef parts

-- | The bytes of an output that has no streams.
outputByteString :: Output -> ByteString
outputByteString output = unsafeDupablePerformIO $ do
  parts <- (`writeOutput` output) =<< mallocPlainForeignPtrBytes (outputCopied output)
  pure . BS.concat $ map (either id uncopiedBytes) parts
  where
    uncopiedBytes (UncopiedBytes value) = value
    uncopiedBytes (UncopiedStream _) = error "an output with streams has no byte string"
    uncopiedBytes (UncopiedBorrowedStream _) = error "an output with streams has no byte string"

-- | Each value's output, one after another. Unlike 'foldMap', it composes no
-- output per value: their lengths are summed, and one loop writes them.
outputs :: (a -> Output) -> [a] -> Output
outputs output values = Output (total outputLength) (total outputCopied) (Write write)
  where
    total field = foldl' (\subtotal value -> subtotal + field (output value)) 0 values
    write uncopied = go values
      where
        go [] pointer = pure pointer
        go (value : rest) pointer = case output value of
          Output _ _ (Write each) -> each uncopied pointer >>= go rest
{-# INLINE outputs #-}

fixed :: Int -> (Ptr Word8 -> IO ()) -> Output
fixed size poke = Output size size (Write (\_ pointer -> poke pointer $> (pointer `plusPtr` size)))
{-# INLINE fixed #-}

word8 :: Word8 -> Output
word8 value = fixed 1 (\pointer -> pokeByteOff pointer 0 value)
{-# INLINE word8 #-}

word16 :: Word16 -> Output
word16 value = fixed 2 $ \pointer -> do
  pokeByteOff pointer 0 (fromIntegral (value `shiftR` 8) :: Word8)
  pokeByteOff pointer 1 (fromIntegral value :: Word8)
{-# INLINE word16 #-}

int16 :: Int16 -> Output
int16 = word16 . fromIntegral
{-# INLINE int16 #-}

word32 :: Word32 -> Output
word32 value = fixed 4 $ \pointer -> do
  pokeByteOff pointer 0 (fromIntegral (value `shiftR` 24) :: Word8)
  pokeByteOff pointer 1 (fromIntegral (value `shiftR` 16) :: Word8)
  pokeByteOff pointer 2 (fromIntegral (value `shiftR` 8) :: Word8)
  pokeByteOff pointer 3 (fromIntegral value :: Word8)
{-# INLINE word32 #-}

int32 :: Int32 -> Output
int32 = word32 . fromIntegral
{-# INLINE int32 #-}

-- | Byte strings this long or longer are sent from where they are, without
-- being copied. Shorter ones are copied, so that they go in one send with the
-- bytes around them.
uncopiedLength :: Int
uncopiedLength = 8192

bytes :: ByteString -> Output
bytes value
  | size >= uncopiedLength = Output size 0 (Write (\uncopied pointer -> uncopied pointer (UncopiedBytes value) $> pointer))
  | otherwise =
      Output size size $
        Write $ \_ pointer ->
          BSU.unsafeUseAsCString value (\source -> copyBytes pointer (castPtr source) size) $> (pointer `plusPtr` size)
  where
    size = BS.length value
{-# INLINE bytes #-}

-- | A zero byte inside the value would end the string early on the server,
-- so it is rejected.
cstring :: ByteString -> Output
cstring value
  | BS.elem 0 value = error "a C string sent to the server must not contain a zero byte"
  | otherwise = bytes value <> word8 0
{-# INLINE cstring #-}

-- | Bytes the client supplies as they're sent.
stream :: ByteStream -> Output
stream source = Output (byteStreamLength source) 0 (Write (\uncopied pointer -> uncopied pointer (UncopiedStream source) $> pointer))
{-# INLINE stream #-}

-- | The source may overwrite a chunk on its next pull.
borrowedStream :: ByteStream -> Output
borrowedStream source = Output (byteStreamLength source) 0 (Write (\uncopied pointer -> uncopied pointer (UncopiedBorrowedStream source) $> pointer))
{-# INLINE borrowedStream #-}
