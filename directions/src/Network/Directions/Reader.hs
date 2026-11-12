{-# LANGUAGE BangPatterns #-}

-- | The reader of one direction: the link it receives from, a buffer of fixed
-- size, and the bytes in hand, which are the part of the buffer that was
-- received and not taken yet. Parsers are arrows over it: each atom is one
-- 'now' that takes what is in hand, and only an atom that finds too little
-- receives, through the link, with the wait that the arrow gives it.
--
-- A reader belongs to one direction: its parts run one after the other, and
-- none but the part that streams a field takes bytes while it does.
module Network.Directions.Reader
  ( Reader (..),
    newReader,
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
    roomAfter,
  )
where

import Control.Arrow (ArrowChoice, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Monad (when)
import Data.Bits (shiftL, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray, plusForeignPtr)
import Foreign.Marshal.Utils (copyBytes, moveBytes)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peekByteOff, peekElemOff, pokeElemOff)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes, unsafeWithForeignPtr)
import Network.Directions.Classes

-- | One direction's reader.
data Reader link = Reader
  { readerLink :: !link,
    readerMemory :: !(ForeignPtr Word8),
    readerCapacity :: !Int,
    -- | Where the bytes in hand start and end, how many bytes the atom that
    -- receives, or the stream, still wants, where the body that cursors stand
    -- in ends, and what each level of a cursor has left: unboxed, so that
    -- taking bytes allocates nothing.
    readerMarks :: !(ForeignPtr Int)
  }

-- | A reader of the link, with a buffer of this many bytes.
newReader :: link -> Int -> IO (Reader link)
newReader link capacity
  | capacity <= 0 = ioError (userError "a reader's buffer needs a positive capacity")
  | otherwise = do
      memory <- mallocPlainForeignPtrBytes capacity
      marks <- mallocForeignPtrArray markCount
      unsafeWithForeignPtr marks (\pointer -> mapM_ (\index -> pokeElemOff pointer index 0) [0 .. markCount - 1])
      pure (Reader link memory capacity marks)

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
bytes :: (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr Int ByteString
bytes reader = atom reader (\from size -> unsafeWithForeignPtr (readerMemory reader) (\memory -> BSI.create size (\target -> copyBytes target (memory `plusPtr` from) size)))
{-# INLINABLE bytes #-}

-- | The next byte, as a number.
word8 :: (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr () Int
word8 reader = const 1 ^>> atom reader (\from _ -> scalar reader from 1)
{-# INLINABLE word8 #-}

-- | The next two bytes, as a big-endian number.
word16 :: (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr () Int
word16 reader = const 2 ^>> atom reader (\from _ -> scalar reader from 2)
{-# INLINABLE word16 #-}

-- | The next four bytes, as a big-endian number.
word32 :: (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr () Int
word32 reader = const 4 ^>> atom reader (\from _ -> scalar reader from 4)
{-# INLINABLE word32 #-}

-- | The big-endian number of the bytes from the offset in the buffer.
scalar :: Reader link -> Int -> Int -> IO Int
scalar reader from size = unsafeWithForeignPtr (readerMemory reader) (\memory -> go memory 0 0)
  where
    -- The number is built as a word and converted at the end, which lets GHC
    -- keep it unboxed through the loop.
    go :: Ptr Word8 -> Int -> Word -> IO Int
    go !memory !index !value
      | index == size = pure (fromIntegral value)
      | otherwise = peekByteOff memory (from + index) >>= \byte -> go memory (index + 1) (value `shiftL` 8 .|. fromIntegral (byte :: Word8))
{-# INLINE scalar #-}

-- | An atom of the size it is given: it takes the bytes in hand, which is one
-- 'now', and only if there are too few, it receives the rest, and takes them
-- then. The taking is given where the bytes start, and its result is
-- evaluated before the bytes are given up.
atom :: (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> (Int -> Int -> IO a) -> arr Int a
atom reader take' = now attempt >>> (retry ||| returnA)
  where
    attempt size = do
      from <- mark reader markStart
      end <- mark reader markEnd
      if size >= 0 && end - from >= size
        then Right <$> taken size from
        else pure (Left size)
    taken size from = do
      value <- take' from size
      value `seq` setMark reader markStart (from + size)
      pure value
    -- The receive made room and wanted the size, so the bytes are in hand.
    retry = refill reader >>> now (\() -> mark reader markStart >>= \from -> mark reader markWanted >>= \size -> taken size from)
{-# INLINE atom #-}

-- | Moves the bytes in hand to the buffer's start, and receives after them
-- until as many bytes as the input says are there.
refill :: (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr Int ()
refill reader = now prepare >>> loop
  where
    prepare size = do
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
    loop = receive (readerLink reader) >>> now arrived >>> (loop ||| returnA)
    arrived count = do
      when (count <= 0) $ ioError (userError "the stream ended inside a value")
      end <- (+ count) <$> mark reader markEnd
      setMark reader markEnd end
      wanted <- mark reader markWanted
      pure (if end >= wanted then Right () else Left (roomAfter reader end))
{-# INLINABLE refill #-}

-- | The buffer after the bytes in it, to receive into.
roomAfter :: Reader link -> Int -> Room
roomAfter reader held = Room (readerMemory reader `plusForeignPtr` held) (readerCapacity reader - held)
{-# INLINE roomAfter #-}

-- | The next n bytes, as the pieces that arrive: each is a view of the buffer,
-- not a copy, valid until the consumer ends, and no larger than the buffer.
-- The consumer must not read from the reader. The bytes in hand go first, and
-- a receive is made only when none are.
stream :: (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr ByteString () -> arr Int ()
stream reader consumer = now begin >>> loop
  where
    begin size = do
      when (size < 0) $ ioError (userError ("a stream of " <> show size <> " bytes"))
      setMark reader markWanted size
    loop = now next >>> (returnA ||| (more ||| (consumer >>> loop)))
    next () = do
      left <- mark reader markWanted
      if left == 0
        then pure (Left ())
        else do
          from <- mark reader markStart
          end <- mark reader markEnd
          if end == from
            then pure (Right (Left ()))
            else do
              let size = min left (end - from)
              setMark reader markStart (from + size)
              setMark reader markWanted (left - size)
              pure (Right (Right (BSI.BS (readerMemory reader `plusForeignPtr` from) size)))
    -- Nothing is in hand, so the receive has the whole buffer.
    more =
      now
        ( \() -> do
            end <- mark reader markEnd
            limit <- mark reader markLimit
            setMark reader markLimit (limit - end)
            setMark reader markStart 0
            setMark reader markEnd 0
            pure (roomAfter reader 0)
        )
        >>> receive (readerLink reader)
        >>> now
          ( \count -> do
              when (count <= 0) $ ioError (userError "the stream ended inside a field")
              setMark reader markEnd count
          )
        >>> loop
{-# INLINABLE stream #-}

-- | The action, as many times as the input says, and what it gave each time,
-- in order.
replicateA :: (ArrowChoice arr) => arr () b -> arr Int [b]
replicateA action = (,[]) ^>> loop >>^ reverse
  where
    loop = (\(count, done) -> if count <= 0 then Right done else Left (count, done)) ^>> (step ||| returnA)
    step = (\(count, done) -> ((), (count - 1, done))) ^>> first action >>> (\(value, (count, done)) -> (count, value : done)) ^>> loop
{-# INLINABLE replicateA #-}
