{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE PatternSynonyms #-}

-- | The reader of one direction: the link it receives from, a buffer of fixed
-- size that its caller gives it, and the bytes in hand, which are the part of
-- the buffer that was received and not taken yet. Parsers are arrows over it: each atom is one
-- 'now' that takes what is in hand, and only an atom that finds too little
-- receives, through the link, with the wait that the arrow gives it.
--
-- A reader belongs to one direction: its parts run one after the other, and
-- none but the part that streams a field takes bytes while it does.
module Network.Streaming.Reader
  ( Reader (..),
    newReader,
    readerMemory,
    readerCapacity,
    bytes,
    word8,
    word16,
    word32,
    stream,
    replicateA,

    -- * What cursors over a reader are built from
    mark,
    setMark,
    markStart,
    markEnd,
    markWanted,
    markLimit,
    markFrame,
    scalar,
    refill,
    receiveInto,
    roomAfter,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Monad (when)
import Data.Bits (shiftL, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI
import Data.Word (Word8, byteSwap16, byteSwap32, byteSwap64)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray, plusForeignPtr)
import Foreign.Marshal.Utils (copyBytes, moveBytes)
import Foreign.Ptr (plusPtr)
import Foreign.Storable (peekByteOff, peekElemOff, pokeElemOff)
import GHC.ByteOrder (ByteOrder (..), targetByteOrder)
import GHC.Exts (Int (I#), readWord8OffAddrAsWord16#, readWord8OffAddrAsWord32#, readWord8OffAddrAsWord64#)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import GHC.IO (IO (..))
import GHC.Ptr (Ptr (Ptr))
import GHC.Word (Word16 (W16#), Word32 (W32#), Word64 (W64#))
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes hiding (Stream (..))
import Network.Streaming.Combinators (pattern Again, pattern Finish, iterateA, iterateWith)
import Network.Streaming.With (With (..), asks, lift, with)

-- | One direction's reader.
data Reader link = Reader
  { readerLink :: !link,
    readerBuffer :: !Buffer,
    -- | Where the bytes in hand start and end, how many bytes the atom that
    -- receives, or the stream, still wants, where the body that cursors stand
    -- in ends, and what each level of a cursor has left: unboxed, so that
    -- taking bytes allocates nothing.
    readerMarks :: !(ForeignPtr Int)
  }

-- | The memory of the reader's buffer, and its size.
readerMemory :: Reader link -> ForeignPtr Word8
readerMemory = bufferMemory . readerBuffer
{-# INLINE readerMemory #-}

readerCapacity :: Reader link -> Int
readerCapacity = bufferCapacity . readerBuffer
{-# INLINE readerCapacity #-}

-- | A reader of the link, in the buffer, which no other reader or direction
-- uses meanwhile.
newReader :: link -> Buffer -> IO (Reader link)
newReader link buffer = do
  marks <- mallocForeignPtrArray markCount
  unsafeWithForeignPtr marks (\pointer -> mapM_ (\index -> pokeElemOff pointer index 0) [0 .. markCount - 1])
  pure (Reader link buffer marks)

-- | Where the bytes in hand start, where they end, how many are wanted, and
-- where the body ends that cursors read in. The last is a place in the buffer
-- like the others, which the receives keep when they move the bytes in hand,
-- and which is behind the start once the body is read.
markStart, markEnd, markWanted, markLimit :: Int
markStart = 0
markEnd = 1
markWanted = 2
markLimit = 3

-- | What the cursor at this depth has left: elements or bytes.
markFrame :: Int -> Int
markFrame depth = 4 + depth

-- | How deep cursors can go.
maxDepth :: Int
maxDepth = 8

markCount :: Int
markCount = markFrame maxDepth

mark :: Reader link -> Int -> IO Int
mark reader index = unsafeWithForeignPtr (readerMarks reader) (`peekElemOff` index)
{-# INLINE mark #-}

setMark :: Reader link -> Int -> Int -> IO ()
setMark reader index value = unsafeWithForeignPtr (readerMarks reader) (\pointer -> pokeElemOff pointer index value)
{-# INLINE setMark #-}

-- | The next n bytes, copied: they stay valid when the buffer is refilled. It
-- receives only when fewer than n are in hand. An n larger than the buffer
-- fails.
bytes :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => arr (Reader link, Int) ByteString
bytes = atom (\reader from size -> unsafeWithForeignPtr (readerMemory reader) (\memory -> BSI.create size (\target -> copyBytes target (memory `plusPtr` from) size)))
{-# INLINABLE bytes #-}

-- | The next byte, as a number.
word8 :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => arr (Reader link) Int
word8 = (,1) ^>> atom (\reader from _ -> scalar reader from 1)
{-# INLINABLE word8 #-}

-- | The next two bytes, as a big-endian number.
word16 :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => arr (Reader link) Int
word16 = (,2) ^>> atom (\reader from _ -> scalar reader from 2)
{-# INLINABLE word16 #-}

-- | The next four bytes, as a big-endian number.
word32 :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => arr (Reader link) Int
word32 = (,4) ^>> atom (\reader from _ -> scalar reader from 4)
{-# INLINABLE word32 #-}

-- | The big-endian number of the bytes from the offset in the buffer.
scalar :: Reader link -> Int -> Int -> IO Int
scalar reader from size =
  unsafeWithForeignPtr (readerMemory reader) $ \memory -> case size of
    1 -> fromIntegral <$> (peekByteOff memory from :: IO Word8)
    2 -> peekWord16BE memory from
    4 -> peekWord32BE memory from
    8 -> peekWord64BE memory from
    _ -> go memory 0 0
  where
    -- The number is built as a word and converted at the end, which lets GHC
    -- keep it unboxed through the loop.
    go :: Ptr Word8 -> Int -> Word -> IO Int
    go !memory !index !value
      | index == size = pure (fromIntegral value)
      | otherwise = peekByteOff memory (from + index) >>= \byte -> go memory (index + 1) (value `shiftL` 8 .|. fromIntegral (byte :: Word8))
{-# INLINE scalar #-}

-- The offset is in bytes. In particular, a length after a message tag
-- need not be aligned even when the beginning of the buffer is.
peekWord16BE :: Ptr Word8 -> Int -> IO Int
peekWord16BE (Ptr addr) (I# offset) = IO $ \state ->
  case readWord8OffAddrAsWord16# addr offset state of
    (# state', value #) -> (# state', fromIntegral (fromBigEndian byteSwap16 (W16# value)) #)
{-# INLINE peekWord16BE #-}

peekWord32BE :: Ptr Word8 -> Int -> IO Int
peekWord32BE (Ptr addr) (I# offset) = IO $ \state ->
  case readWord8OffAddrAsWord32# addr offset state of
    (# state', value #) -> (# state', fromIntegral (fromBigEndian byteSwap32 (W32# value)) #)
{-# INLINE peekWord32BE #-}

peekWord64BE :: Ptr Word8 -> Int -> IO Int
peekWord64BE (Ptr addr) (I# offset) = IO $ \state ->
  case readWord8OffAddrAsWord64# addr offset state of
    (# state', value #) -> (# state', fromIntegral (fromBigEndian byteSwap64 (W64# value)) #)
{-# INLINE peekWord64BE #-}

fromBigEndian :: (a -> a) -> a -> a
fromBigEndian swap value = case targetByteOrder of
  LittleEndian -> swap value
  BigEndian -> value
{-# INLINE fromBigEndian #-}

-- | When the bytes are in hand, an atom takes them and advances the start in
-- the same 'now'. Only the refill path stores the wanted size across receives.
atom :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => (Reader link -> Int -> Int -> IO a) -> arr (Reader link, Int) a
atom take' = with (With (now ready) >>> ((With refill >>> asks (now refilled)) ||| returnA))
  where
    ready (reader, size) =
      unsafeWithForeignPtr (readerMarks reader) $ \marks -> do
        from <- peekElemOff marks markStart
        end <- peekElemOff marks markEnd
        if size >= 0 && end - from >= size
          then Right <$> takeBytes marks reader from size
          else pure (Left size)
    refilled reader =
      unsafeWithForeignPtr (readerMarks reader) $ \marks -> do
        from <- peekElemOff marks markStart
        size <- peekElemOff marks markWanted
        takeBytes marks reader from size
    takeBytes marks reader from size = do
      value <- take' reader from size
      value `seq` pokeElemOff marks markStart (from + size)
      pure value
{-# INLINE atom #-}

-- | Moves the bytes in hand to the buffer's start, and receives after them
-- until as many bytes as the input says are there.
refill :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => arr (Reader link, Int) ()
refill = with (With (now prepare) >>> iterateA (With receiveInto >>> With (now arrived)))
  where
    prepare (reader, size) = do
      when (size < 0 || size > readerCapacity reader) $
        ioError (userError ("a read of " <> show size <> " bytes doesn't fit a buffer of " <> show (readerCapacity reader)))
      from <- mark reader markStart
      end <- mark reader markEnd
      let held = end - from
      when (from > 0) $ unsafeWithForeignPtr (readerMemory reader) (\memory -> moveBytes memory (memory `plusPtr` from) held)
      limit <- mark reader markLimit
      setMark reader markLimit (limit - from)
      setMark reader markStart 0
      setMark reader markEnd held
      setMark reader markWanted size
      pure (roomAfter reader held)
    arrived (reader, count) = do
      when (count <= 0) $ ioError (userError "the stream ended inside a value")
      end <- (+ count) <$> mark reader markEnd
      setMark reader markEnd end
      wanted <- mark reader markWanted
      pure (if end >= wanted then Finish () else Again (roomAfter reader end))
{-# INLINABLE refill #-}

-- | Receives from the reader's link into the room.
receiveInto :: (Arrow arr, Link arr link) => arr (Reader link, Room) Int
receiveInto = first readerLink ^>> receive
{-# INLINE receiveInto #-}

-- | The buffer after the bytes in it, to receive into.
roomAfter :: Reader link -> Int -> Room
roomAfter reader held = Room (readerBuffer reader) held (readerCapacity reader - held)
{-# INLINE roomAfter #-}

-- | The next n bytes, as the pieces that arrive: each is a view of the buffer,
-- not a copy, valid until the consumer ends, and no larger than the buffer.
-- The consumer must not read from the reader. The bytes in hand go first, and
-- a receive is made only when none are.
stream :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => arr ByteString () -> arr (Reader link, Int) ()
stream consumer = with (With (now begin) >>> iterateWith (asks (now next)) (more ||| lift consumer))
  where
    begin (reader, size) = do
      when (size < 0) $ ioError (userError ("a stream of " <> show size <> " bytes"))
      setMark reader markWanted size
    next reader = do
      wanted <- mark reader markWanted
      if wanted == 0
        then pure (Finish ())
        else do
          from <- mark reader markStart
          end <- mark reader markEnd
          if end == from
            then pure (Again (Left ()))
            else do
              let size = min wanted (end - from)
              setMark reader markStart (from + size)
              setMark reader markWanted (wanted - size)
              pure (Again (Right (BSI.BS (readerMemory reader `plusForeignPtr` from) size)))
    -- Nothing is in hand, so the receive has the whole buffer.
    more =
      asks
        ( now
            ( \reader -> do
                end <- mark reader markEnd
                limit <- mark reader markLimit
                setMark reader markLimit (limit - end)
                setMark reader markStart 0
                setMark reader markEnd 0
                pure (roomAfter reader 0)
            )
        )
        >>> With receiveInto
        >>> With
          ( now
              ( \(reader, count) -> do
                  when (count <= 0) $ ioError (userError "the stream ended inside a field")
                  setMark reader markEnd count
              )
          )
{-# INLINABLE stream #-}

-- | The action, as many times as the input says, and what it gave each time,
-- in order.
replicateA :: (ArrowChoice arr, Annotate arr) => arr () b -> arr Int [b]
replicateA action = (,[]) ^>> iterateWith (arr leftToDo) (startOne ^>> first action >>^ gather) >>^ reverse
  where
    leftToDo (count, values) = if count <= 0 then Finish values else Again (count, values)
    startOne (count, values) = ((), (count - 1, values))
    gather (value, (count, values)) = (count, value : values)
{-# INLINABLE replicateA #-}
