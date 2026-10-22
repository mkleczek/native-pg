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
    reusableInputBuffer,
    attachInputBuffer,
    detachInputBuffer,
    withInputSome,
    inputPosition,
    readInputSome,
    readInputWord8,
    readInputWord16,
    readInputWord32,
    runBodyParser,
    runInput,
    runInputUntil,
    runInputFramedUntil,
    skipInput,
  )
where

import Control.Exception (Exception, evaluate, throwIO, try)
import Control.Monad (ap, liftM, when)
import Control.Monad.Except (MonadError (..))
import Data.Bits (shiftL, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Word (Word8, Word16, Word32)
import Database.PostgreSQL.Buffer (Buffer)
import qualified Database.PostgreSQL.Buffer as Buffer
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, moveBytes)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peekByteOff)
import GHC.Exts (RealWorld, State#)
import GHC.ForeignPtr (unsafeWithForeignPtr)
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
-- Allocating input preserves slices; reusable input compacts in place and
-- copies owned results before exposing them.
data InputBuffer = InputBuffer
  { bufferMemory :: {-# UNPACK #-} !(ForeignPtr Word8),
    bufferCapacity :: {-# UNPACK #-} !Int,
    bufferStart :: {-# UNPACK #-} !Int,
    bufferEnd :: {-# UNPACK #-} !Int,
    -- | How many bytes were consumed before 'bufferStart'.
    bufferPosition :: {-# UNPACK #-} !Int,
    bufferReusable :: !Bool
  }

emptyInputBuffer :: InputBuffer
emptyInputBuffer = case BS.empty of
  BSI.BS memory _ -> InputBuffer memory 0 0 0 0 False

-- | Empty input backed by storage lent by the caller.
reusableInputBuffer :: Buffer -> InputBuffer
reusableInputBuffer storage = InputBuffer (Buffer.bufferMemory storage) (Buffer.bufferCapacity storage) 0 0 0 True

-- | Moves unread bytes into caller-owned input storage, preserving position.
attachInputBuffer :: Buffer -> InputBuffer -> IO InputBuffer
attachInputBuffer storage previous = do
  when (bufferReusable previous) (throwIO (InputError "input storage is already borrowed"))
  let count = bufferEnd previous - bufferStart previous
      capacity = Buffer.bufferCapacity storage
  when (count > capacity) (throwIO (InputError "unread input exceeds supplied buffer capacity"))
  withForeignPtr (Buffer.bufferMemory storage) $ \target ->
    withForeignPtr (bufferMemory previous) $ \origin ->
      copyBytes target (origin `plusPtr` bufferStart previous) count
  pure (reusableInputBuffer storage) {bufferEnd = count, bufferPosition = bufferPosition previous}

-- | Ends a loan, retaining only an owned copy of unread bytes.
detachInputBuffer :: InputBuffer -> IO InputBuffer
detachInputBuffer buffer
  | not (bufferReusable buffer) = pure buffer
  | otherwise = do
      bytes <- evaluate (BS.copy (slice (bufferEnd buffer - bufferStart buffer) buffer))
      let BSI.BS memory size = bytes
      pure (InputBuffer memory size 0 size (bufferPosition buffer) False)

-- | Borrows one chunk until the callback returns, then consumes it.
-- The callback must not retain the bytes or read this input recursively.
withInputSome :: Int -> (ByteString -> IO a) -> Input a
withInputSome count use = do
  (size, buffer) <- someInput count
  result <- inputIO (use (slice size buffer))
  putBuffer (consume size buffer)
  pure result

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

-- | Allocating parsers recover from errors using their previous buffer
-- snapshot. Reusable parsers cannot recover: compaction may have overwritten
-- that snapshot, so errors remain terminal even under 'catchError'.
instance MonadError String Input where
  throwError message = inputIO (throwIO (InputError message))
  {-# INLINE throwError #-}
  catchError (Input m) handler = Input $ \env buffer s ->
    if bufferReusable buffer
      then m env buffer s
      else case unIO (try (IO (\s1 -> case m env buffer s1 of (# s2, buffer', a #) -> (# s2, (buffer', a) #)))) s of
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
        ownedSlice count buffer
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
-- | Like 'runInputUntil', batching refills within the current envelope while
-- preserving the next envelope for a later operation.
runInputFramedUntil :: InputSource -> Int -> InputBuffer -> Input a -> IO (a, InputBuffer)
runInputFramedUntil = runInputUntil

inputPosition :: InputBuffer -> Int
inputPosition = bufferPosition

-- | Between one and the given number of bytes. Reusable input copies this
-- owned result; use 'withInputSome' to borrow it instead. It reads
-- until they're all there, or the buffer has no room for more, so that a
-- consumer that passes them on, e.g. to a socket, gets them in large chunks.
readInputSome :: Int -> Input ByteString
readInputSome count = do
  (size, buffer) <- someInput count
  value <- ownedSlice size buffer
  putBuffer (consume size buffer)
  pure value
{-# INLINE readInputSome #-}

-- Scalars decode directly from the current storage instead of constructing
-- an owned ByteString that reusable input would have to copy.
readInputWord8 :: Input Word8
readInputWord8 = readScalar 1 $ \pointer -> peekByteOff pointer 0
{-# INLINE readInputWord8 #-}

readInputWord16 :: Input Word16
readInputWord16 = readScalar 2 $ \pointer -> do
  high <- peekByteOff pointer 0 :: IO Word8
  low <- peekByteOff pointer 1 :: IO Word8
  pure (shiftL (fromIntegral high) 8 .|. fromIntegral low)
{-# INLINE readInputWord16 #-}

readInputWord32 :: Input Word32
readInputWord32 = readScalar 4 $ \pointer -> do
  byte0 <- peekByteOff pointer 0 :: IO Word8
  byte1 <- peekByteOff pointer 1 :: IO Word8
  byte2 <- peekByteOff pointer 2 :: IO Word8
  byte3 <- peekByteOff pointer 3 :: IO Word8
  pure $
    shiftL (fromIntegral byte0) 24
      .|. shiftL (fromIntegral byte1) 16
      .|. shiftL (fromIntegral byte2) 8
      .|. fromIntegral byte3
{-# INLINE readInputWord32 #-}

readScalar :: Int -> (Ptr Word8 -> IO a) -> Input a
readScalar count decode = do
  buffer <- ensureAvailable count
  -- Force the result before consuming the bytes can release their loan.
  value <- inputIO $ unsafeWithForeignPtr (bufferMemory buffer) $ \pointer ->
    decode (pointer `plusPtr` bufferStart buffer) >>= evaluate
  putBuffer (consume count buffer)
  pure value
{-# INLINE readScalar #-}

someInput :: Int -> Input (Int, InputBuffer)
someInput count
  | count <= 0 = throwError "a streamed input request must be positive"
  | otherwise = do
      buffer <- ensureAvailable 1
      limit <- getLimit
      let size = minimum [count, bufferCapacity buffer - bufferStart buffer, limit - bufferPosition buffer]
      filled <- ensureAvailable size
      pure (size, filled)

-- | Consumes the given number of bytes, reading as needed, without keeping
-- them together.
skipInput :: Int -> Input ()
skipInput count
  | count <= 0 = pure ()
  | otherwise = do
      size <- withInputSome count (pure . BS.length)
      skipInput (count - size)

-- | Parses a complete input, without copying it, and returns the result with
-- the bytes left over.
runBodyParser :: ByteString -> Input a -> Either String (a, ByteString)
runBodyParser (BSI.BS memory size) parser =
  unsafeDupablePerformIO $ do
    result <- try (runInput endOfInput (InputBuffer memory size 0 size 0 False) parser)
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
-- new initialized allocation. Caller-owned input compacts in place instead.
fill :: InputSource -> Int -> InputBuffer -> IO InputBuffer
fill source count buffer
  | bufferReusable buffer && count > bufferCapacity buffer =
      throwIO (InputError ("contiguous input of " <> show count <> " bytes exceeds buffer capacity " <> show (bufferCapacity buffer)))
  | bufferStart buffer + count <= bufferCapacity buffer = readUntilAvailable buffer
  | bufferReusable buffer = readUntilAvailable =<< compact
  | otherwise = readUntilAvailable =<< reallocate
  where
    compact = do
      let unconsumed = bufferEnd buffer - bufferStart buffer
      withForeignPtr (bufferMemory buffer) $ \pointer ->
        moveBytes pointer (pointer `plusPtr` bufferStart buffer) unconsumed
      pure buffer {bufferStart = 0, bufferEnd = unconsumed}
    reallocate = do
      let unconsumed = bufferEnd buffer - bufferStart buffer
          capacity = max (inputReadSize source) count
      memory <- Buffer.bufferMemory <$> Buffer.newBuffer capacity
      unsafeWithForeignPtr memory $ \target ->
        unsafeWithForeignPtr (bufferMemory buffer) $ \origin ->
          copyBytes target (origin `plusPtr` bufferStart buffer) unconsumed
      pure (InputBuffer memory capacity 0 unconsumed (bufferPosition buffer) False)
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
      ownedSlice size buffer
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

-- Force the copy while this arena generation is still valid. A delayed pure
-- decoder may keep the returned ByteString long after subsequent reads.
ownedSlice :: Int -> InputBuffer -> Input ByteString
ownedSlice count buffer
  | bufferReusable buffer = inputIO (evaluate (BS.copy (slice count buffer)))
  | otherwise = pure (slice count buffer)
{-# INLINE ownedSlice #-}
