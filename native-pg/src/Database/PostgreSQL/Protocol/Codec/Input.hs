{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Blocking, pull-based input for the derived parsers.
--
-- A parser reads from a buffer of bytes that came from an 'InputSource' but
-- aren't consumed yet. Only when it needs more than the buffer holds does it
-- read from the source, which blocks until bytes arrive. One read may fill
-- the buffer's free space, but nothing else is read ahead.
module Database.PostgreSQL.Protocol.Codec.Input
  ( Input,
    InputBuffer,
    InputError (..),
    InputSource (..),
    MonadInput (..),
    emptyInputBuffer,
    inputPosition,
    readInputSome,
    runBodyParser,
    runInput,
    runInputUntil,
    skipInput,
  )
where

import Control.Exception (Exception, throwIO, try)
import Control.Monad (ap, liftM, when)
import Control.Monad.Except (MonadError (..))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peekByteOff)
import GHC.Exts (RealWorld, State#)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes, unsafeWithForeignPtr)
import GHC.IO (IO (..), unIO, unsafeDupablePerformIO)

class MonadError String m => MonadInput m where
  -- | The byte at the given offset from the current position, without
  -- consuming anything. Blocks until that many bytes are available.
  peekInputByteAt :: Int -> m Word8
  readInputExactly :: Int -> m ByteString
  readInputRemaining :: m ByteString
  readInputCStringBytes :: m ByteString
  withInputLimit :: Int -> m a -> m a

-- | Where input bytes come from.
data InputSource = InputSource
  { -- | Reads at most the given number of bytes to the address and returns how
    -- many it read, blocking until at least one is available. Zero means the
    -- input has ended.
    readInputInto :: Ptr Word8 -> Int -> IO Int,
    -- | The size of a new buffer. A read fills at most the free part of the
    -- current one.
    inputReadSize :: Int
  }

-- | Bytes read from the source but not consumed yet: the filled range from
-- 'bufferStart' to 'bufferEnd' of an allocation of 'bufferCapacity' bytes.
-- Reads only fill the rest of the allocation, so slices of consumed bytes
-- stay valid.
data InputBuffer = InputBuffer
  { bufferMemory :: {-# UNPACK #-} !(ForeignPtr Word8),
    bufferCapacity :: {-# UNPACK #-} !Int,
    bufferStart :: {-# UNPACK #-} !Int,
    bufferEnd :: {-# UNPACK #-} !Int,
    -- | How many bytes were consumed before 'bufferStart'.
    bufferPosition :: {-# UNPACK #-} !Int
  }

emptyInputBuffer :: InputBuffer
emptyInputBuffer = case BS.empty of
  BSI.BS memory _ -> InputBuffer memory 0 0 0 0

data InputEnv = InputEnv
  { envSource :: InputSource,
    -- | The position where the innermost bounded part ends.
    envLimit :: {-# UNPACK #-} !Int
  }

-- | The input is malformed, or ended early.
newtype InputError = InputError String
  deriving (Show)

instance Exception InputError

-- | Parsing in 'IO' with the buffer passed along in an unboxed tuple, so
-- inlined parsers keep it in registers.
newtype Input a = Input
  { unInput :: InputEnv -> InputBuffer -> State# RealWorld -> (# State# RealWorld, InputBuffer, a #)
  }

instance Functor Input where
  fmap = liftM
  {-# INLINE fmap #-}

instance Applicative Input where
  pure a = Input (\_ buffer s -> (# s, buffer, a #))
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}

instance Monad Input where
  Input m >>= k = Input $ \env buffer s ->
    case m env buffer s of
      (# s', buffer', a #) -> unInput (k a) env buffer' s'
  {-# INLINE (>>=) #-}

-- | Errors are exceptions; a parser that catches one continues from the
-- buffer as it was before the failed action.
instance MonadError String Input where
  throwError message = inputIO (throwIO (InputError message))
  {-# INLINE throwError #-}
  catchError (Input m) handler = Input $ \env buffer s ->
    case unIO (try (IO (\s1 -> case m env buffer s1 of (# s2, buffer', a #) -> (# s2, (buffer', a) #)))) s of
      (# s', Right (buffer', a) #) -> (# s', buffer', a #)
      (# s', Left (InputError message) #) -> unInput (handler message) env buffer s'

instance MonadInput Input where
  peekInputByteAt offset = do
    buffer <- ensureAvailable (offset + 1)
    inputIO $
      unsafeWithForeignPtr (bufferMemory buffer) $ \pointer ->
        peekByteOff pointer (bufferStart buffer + offset)
  {-# INLINE peekInputByteAt #-}

  readInputExactly count
    | count < 0 = throwError ("negative byte count " <> show count <> " is invalid")
    | otherwise = do
        buffer <- ensureAvailable count
        putBuffer (consume count buffer)
        pure (slice count buffer)
  {-# INLINE readInputExactly #-}

  readInputRemaining = do
    limit <- getLimit
    if limit == unbounded
      then throwError "RemainingBytesAtom needs an enclosing bounded part"
      else readInputExactly . (limit -) . bufferPosition =<< getBuffer
  {-# INLINE readInputRemaining #-}

  readInputCStringBytes = cstringFrom 0
  {-# INLINE readInputCStringBytes #-}

  withInputLimit count (Input action) = Input $ \env buffer s ->
    let end = bufferPosition buffer + count
     in if count < 0 || end > envLimit env
          then unInput (throwError ("a bounded part of " <> show count <> " bytes doesn't fit the enclosing one")) env buffer s
          else case action env {envLimit = end} buffer s of
            (# s', buffer', a #)
              | bufferPosition buffer' == end -> (# s', buffer', a #)
              | otherwise ->
                  unInput
                    (throwError ("bounded parser left " <> show (end - bufferPosition buffer') <> " unconsumed bytes"))
                    env
                    buffer'
                    s'
  {-# INLINE withInputLimit #-}

-- | Runs a parser on the given unconsumed bytes, and returns its result with
-- the bytes it left unconsumed. Malformed input throws 'InputError'.
runInput :: InputSource -> InputBuffer -> Input a -> IO (a, InputBuffer)
runInput source = runInputUntil source unbounded
{-# INLINE runInput #-}

-- | Like 'runInput', but the parser can't read past the given position.
runInputUntil :: InputSource -> Int -> InputBuffer -> Input a -> IO (a, InputBuffer)
runInputUntil source limit buffer (Input parser) = IO $ \s ->
  case parser (InputEnv source limit) buffer s of
    (# s', buffer', a #) -> (# s', (a, buffer') #)
{-# INLINE runInputUntil #-}

-- | How many bytes were consumed before the first unconsumed one.
inputPosition :: InputBuffer -> Int
inputPosition = bufferPosition

-- | Between one and the given number of bytes, without copying. It reads
-- until they're all there, or the buffer has no room for more, so that a
-- consumer that passes them on, e.g. to a socket, gets them in large chunks.
readInputSome :: Int -> Input ByteString
readInputSome count = do
  buffer <- ensureAvailable 1
  limit <- getLimit
  let size = minimum [count, bufferCapacity buffer - bufferStart buffer, limit - bufferPosition buffer]
  filled <- ensureAvailable size
  putBuffer (consume size filled)
  pure (slice size filled)
{-# INLINE readInputSome #-}

-- | Consumes the given number of bytes, reading as needed, without keeping
-- them together.
skipInput :: Int -> Input ()
skipInput count
  | count <= 0 = pure ()
  | otherwise = do
      chunk <- readInputSome count
      skipInput (count - BS.length chunk)

-- | Parses a complete input, without copying it, and returns the result with
-- the bytes left over.
runBodyParser :: ByteString -> Input a -> Either String (a, ByteString)
runBodyParser (BSI.BS memory size) parser =
  unsafeDupablePerformIO $ do
    result <- try (runInput endOfInput (InputBuffer memory size 0 size 0) parser)
    pure $ case result of
      Left (InputError message) -> Left message
      Right (a, buffer) -> Right (a, slice (bufferEnd buffer - bufferStart buffer) buffer)
  where
    endOfInput = InputSource (\_ _ -> pure 0) 0

inputIO :: IO a -> Input a
inputIO (IO m) = Input (\_ buffer s -> case m s of (# s', a #) -> (# s', buffer, a #))
{-# INLINE inputIO #-}

getBuffer :: Input InputBuffer
getBuffer = Input (\_ buffer s -> (# s, buffer, buffer #))
{-# INLINE getBuffer #-}

putBuffer :: InputBuffer -> Input ()
putBuffer buffer = Input (\_ _ s -> (# s, buffer, () #))
{-# INLINE putBuffer #-}

getLimit :: Input Int
getLimit = Input (\env buffer s -> (# s, buffer, envLimit env #))
{-# INLINE getLimit #-}

unbounded :: Int
unbounded = maxBound

-- | Makes the given number of unconsumed bytes available, reading from the
-- source as needed, and returns the buffer holding them.
ensureAvailable :: Int -> Input InputBuffer
ensureAvailable count = Input $ \env buffer s ->
  if bufferPosition buffer + count > envLimit env
    then unInput (throwError ("unexpected end of a bounded part: needed " <> show count <> " bytes")) env buffer s
    else
      if bufferEnd buffer - bufferStart buffer >= count
        then (# s, buffer, buffer #)
        else case unIO (fill (envSource env) count buffer) s of
          (# s', buffer' #) -> (# s', buffer', buffer' #)
{-# INLINE ensureAvailable #-}

-- | Reads until the given number of bytes is available. Reads go to the free
-- part of the current allocation if the bytes fit there, and otherwise to a
-- new allocation that the unconsumed bytes are copied to first.
fill :: InputSource -> Int -> InputBuffer -> IO InputBuffer
fill source count buffer
  | bufferStart buffer + count <= bufferCapacity buffer = readUntilAvailable buffer
  | otherwise = readUntilAvailable =<< reallocate
  where
    reallocate = do
      let unconsumed = bufferEnd buffer - bufferStart buffer
          capacity = max (inputReadSize source) count
      memory <- mallocPlainForeignPtrBytes capacity
      unsafeWithForeignPtr memory $ \target ->
        unsafeWithForeignPtr (bufferMemory buffer) $ \origin ->
          copyBytes target (origin `plusPtr` bufferStart buffer) unconsumed
      pure (InputBuffer memory capacity 0 unconsumed (bufferPosition buffer))
    readUntilAvailable current
      | bufferEnd current - bufferStart current >= count = pure current
      | otherwise = do
          received <-
            withForeignPtr (bufferMemory current) $ \pointer ->
              readInputInto source (pointer `plusPtr` bufferEnd current) (bufferCapacity current - bufferEnd current)
          when (received <= 0) (throwIO (InputError "unexpected end of input"))
          readUntilAvailable current {bufferEnd = bufferEnd current + received}
{-# NOINLINE fill #-}

-- | Reads a zero-terminated string whose first given number of bytes are
-- known not to be zero.
cstringFrom :: Int -> Input ByteString
cstringFrom scanned = do
  buffer <- getBuffer
  limit <- getLimit
  let available = min (bufferEnd buffer - bufferStart buffer) (limit - bufferPosition buffer)
  case BS.elemIndex 0 (BS.drop scanned (slice available buffer)) of
    Just index -> do
      let size = scanned + index
      putBuffer (consume (size + 1) buffer)
      pure (slice size buffer)
    Nothing
      | bufferPosition buffer + available >= limit -> throwError "unterminated C-string"
      | otherwise -> ensureAvailable (available + 1) *> cstringFrom available

consume :: Int -> InputBuffer -> InputBuffer
consume count buffer =
  buffer
    { bufferStart = bufferStart buffer + count,
      bufferPosition = bufferPosition buffer + count
    }
{-# INLINE consume #-}

-- | The given number of unconsumed bytes, without copying.
slice :: Int -> InputBuffer -> ByteString
slice count buffer = BSI.BS (bufferMemory buffer `plusForeignPtr` bufferStart buffer) count
{-# INLINE slice #-}
