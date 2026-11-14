{-# LANGUAGE BangPatterns #-}
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
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray, plusForeignPtr)
import Foreign.Marshal.Utils (copyBytes, moveBytes)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peekByteOff, peekElemOff, pokeElemOff)
import GHC.ForeignPtr (unsafeWithForeignPtr)
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
-- then. The taking is given the reader and where the bytes start, and its
-- result is evaluated before the bytes are given up. The steps take the reader
-- from the environment, and the sum between them has nothing in it, so that
-- nothing is built for the choice: the size is kept where the receive keeps it.
atom :: (ArrowChoice arr, Effect arr, Annotate arr, Link arr link) => (Reader link -> Int -> Int -> IO a) -> arr (Reader link, Int) a
atom take' = with (With (now ready) >>> (With refill ||| returnA) >>> asks (now taken))
  where
    ready (reader, size) = do
      from <- mark reader markStart
      end <- mark reader markEnd
      if size >= 0 && end - from >= size
        then Right () <$ setMark reader markWanted size
        else pure (Left size)
    -- The bytes are in hand, either way: the size is what the atom wanted.
    taken reader = do
      from <- mark reader markStart
      size <- mark reader markWanted
      value <- take' reader from size
      value `seq` setMark reader markStart (from + size)
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
replicateA action = (,[]) ^>> iterateWith (arr leftToDo) (startOne ^>> first action >>^ collect) >>^ reverse
  where
    leftToDo (count, values) = if count <= 0 then Finish values else Again (count, values)
    startOne (count, values) = ((), (count - 1, values))
    collect (value, (count, values)) = (count, value : values)
{-# INLINABLE replicateA #-}
