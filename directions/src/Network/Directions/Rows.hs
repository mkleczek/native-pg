{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PatternSynonyms #-}

-- | PostgreSQL-shaped messages, and parsers for them written as arrows over a
-- 'Reader': a stream of DataRow messages ended by a CommandComplete, and one
-- DataRow whose only field is as large as a whole response body. They are what
-- "Network.Streaming.Reader" is measured with.
module Network.Directions.Rows
  ( -- * The wire
    rowsWire,
    rowsChecksum,
    rowFields,
    streamWire,

    -- * The parsers
    Message (..),
    message,
    foldRows,
    streamMessages,

    -- * The same, as resumable parsers
    rowsResumable,
    streamResumable,

    -- * Results
    bigEndian,
    checksum,
    Counter,
    newCounter,
    counting,
    adding,
    counted,
  )
where

import Control.Arrow (ArrowChoice, (>>>), (>>^), (^>>), (|||))
import Control.Monad (replicateM, replicateM_, void)
import Data.Bits (shiftL, (.&.), (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Builder (Builder, byteString, int16BE, int32BE, int64BE, toLazyByteString)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int64)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray)
import Foreign.Storable (peek, poke)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Directions.Resumable
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, Step, done, iterateA, iterateWith, keeping)
import Network.Streaming.Reader
import Network.Streaming.With

-- | A stream of this many DataRow messages, each of five fields, then a
-- CommandComplete. Row i has an int4 (i), an int8, a text of 16 bytes, a text
-- of 40 bytes and a bool.
rowsWire :: Int -> ByteString
rowsWire count = strict (foldMap encoded [0 .. count - 1] <> commandComplete count)
  where
    encoded i =
      Builder.word8 68
        <> int32BE (4 + 2 + 8 + 12 + 20 + 44 + 5)
        <> int16BE 5
        <> (int32BE 4 <> int32BE (fromIntegral i))
        <> (int32BE 8 <> int64BE (fromIntegral (int8Of i)))
        <> (int32BE 16 <> int64BE (fromIntegral i) <> int64BE sixteenTail)
        <> (int32BE 40 <> foldMap (int64BE . fromIntegral . (+ i)) [1 .. 5])
        <> (int32BE 1 <> Builder.word8 (fromIntegral (i .&. 1)))

-- | What 'checksum' of every row of the stream of this many adds up to: the
-- ints, the lengths of the texts, and the bools.
rowsChecksum :: Int -> Int
rowsChecksum count = sum [i + int8Of i + 16 + 40 + (i .&. 1) | i <- [0 .. count - 1]]

-- | The fields that row i of the stream has, one by one.
rowFields :: Int -> [ByteString]
rowFields i =
  strict
    <$> [ int32BE (fromIntegral i),
          int64BE (fromIntegral (int8Of i)),
          int64BE (fromIntegral i) <> int64BE sixteenTail,
          foldMap (int64BE . fromIntegral . (+ i)) [1 .. 5],
          Builder.word8 (fromIntegral (i .&. 1))
        ]

int8Of :: Int -> Int
int8Of i = i * 4099 + 1

sixteenTail :: Int64
sixteenTail = 0x6162636465666768

-- | One DataRow with one field, of the given bytes, then a CommandComplete.
streamWire :: ByteString -> ByteString
streamWire field =
  BS.concat
    [ strict (Builder.word8 68 <> int32BE (fromIntegral (4 + 2 + 4 + size)) <> int16BE 1 <> int32BE (fromIntegral size)),
      field,
      strict (commandComplete 1)
    ]
  where
    size = BS.length field

commandComplete :: Int -> Builder
commandComplete count = Builder.word8 67 <> int32BE (fromIntegral (4 + BS.length tag)) <> byteString tag
  where
    tag = BS8.pack ("SELECT " <> show count <> "\0")

strict :: Builder -> ByteString
strict = BL.toStrict . toLazyByteString

-- | A message of the stream: a row with its fields as bytes, or the end.
data Message = DataRow [ByteString] | CommandComplete ByteString

-- | The next message: its tag decides which it is.
message :: (ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => arr (Reader link) Message
message = withOnly readMessage
{-# INLINABLE message #-}

readMessage :: (ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => With (Reader link) arr () Message
readMessage = asks word8 >>> tagged ^>> (row ||| (complete ||| unknown))
  where
    row = asks word32 >>> done >>> dataRow >>^ DataRow
    complete = asks word32 >>> subtract 4 ^>> With bytes >>^ CommandComplete
    unknown = now (\tag -> ioError (userError ("a message with the tag " <> show tag)))
{-# INLINABLE readMessage #-}

-- | Which message a tag starts: a DataRow, after which the stream goes on, or a CommandComplete or another tag, which
-- end it.
tagged :: Int -> Step () (Either () Int)
tagged 68 = Again ()
tagged 67 = Finish (Left ())
tagged other = Finish (Right other)

-- | A DataRow's body: how many fields, and each one's length and bytes.
dataRow :: (ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => With (Reader link) arr () [ByteString]
dataRow = asks word16 >>> replicateA field
  where
    field = asks word32 >>> With bytes
{-# INLINABLE dataRow #-}

-- | Folds the rows of the stream, until its CommandComplete.
foldRows :: (ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => (acc -> [ByteString] -> acc) -> arr (Reader link, acc) acc
foldRows add = with (iterateA (keeping readMessage >>^ step))
  where
    step (DataRow fields, acc) = Again $! add acc fields
    step (CommandComplete _, acc) = Finish acc
{-# INLINABLE foldRows #-}

-- | The stream's messages, until its CommandComplete, with the bytes of every
-- field given to the consumer as pieces that arrive.
streamMessages :: (ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => arr ByteString () -> arr (Reader link) ()
streamMessages consumer = withOnly (iterateWith (asks word8 >>^ tagged) row >>> (complete ||| unknown))
  where
    row = asks word32 >>> asks word16 >>> replicateA (asks word32 >>> With (stream consumer)) >>> done
    complete = asks word32 >>> subtract 4 ^>> With bytes >>> done
    unknown = now (\tag -> ioError (userError ("a message with the tag " <> show tag)))
{-# INLINABLE streamMessages #-}

-- | The rows' checksum, as 'foldRows' gives it, until the CommandComplete.
rowsResumable :: Parser Int
rowsResumable = loop 0
  where
    loop !total =
      takeWord 1 >>= \tag -> case tag of
        68 -> takeWord 4 *> row >>= \fields -> loop (checksum total fields)
        67 -> takeWord 4 >>= \size -> total <$ takeBytes (size - 4)
        _ -> error ("a message with the tag " <> show tag)
    row = takeWord 2 >>= \count -> replicateM count (takeWord 4 >>= takeBytes)

-- | The stream's messages, with the bytes of every field yielded as pieces.
streamResumable :: Parser ()
streamResumable = loop
  where
    loop =
      takeWord 1 >>= \tag -> case tag of
        68 -> takeWord 4 *> takeWord 2 >>= \count -> replicateM_ count (takeWord 4 >>= streamBytes) *> loop
        67 -> takeWord 4 >>= \size -> void (takeBytes (size - 4))
        _ -> error ("a message with the tag " <> show tag)

-- | The number that the bytes are, taken as big-endian.
bigEndian :: ByteString -> Int
bigEndian = BS.foldl' (\value byte -> value `shiftL` 8 .|. fromIntegral byte) 0

-- | Adds a row of the stream to the sum: its ints, the lengths of its texts,
-- and its bool.
checksum :: Int -> [ByteString] -> Int
checksum total [int4, int8, text16, text40, bool] = total + bigEndian int4 + bigEndian int8 + BS.length text16 + BS.length text40 + bigEndian bool
checksum _ fields = error ("a row of " <> show (length fields) <> " fields")

-- | A count of bytes, unboxed.
newtype Counter = Counter (ForeignPtr Int)

newCounter :: IO Counter
newCounter = do
  total <- mallocForeignPtrArray 1
  unsafeWithForeignPtr total (`poke` 0)
  pure (Counter total)

-- | Counts the bytes of the pieces it is given.
counting :: (Effect arr) => Counter -> arr ByteString ()
counting (Counter total) = now (addTo total . BS.length)
{-# INLINABLE counting #-}

-- | Adds the numbers it is given.
adding :: (Effect arr) => Counter -> arr Int ()
adding (Counter total) = now (addTo total)
{-# INLINABLE adding #-}

counted :: Counter -> IO Int
counted (Counter total) = unsafeWithForeignPtr total peek

-- | Adds the number to the count, in place.
addTo :: ForeignPtr Int -> Int -> IO ()
addTo total number = unsafeWithForeignPtr total (\pointer -> peek pointer >>= \sofar -> poke pointer $! sofar + number)
{-# INLINE addTo #-}
