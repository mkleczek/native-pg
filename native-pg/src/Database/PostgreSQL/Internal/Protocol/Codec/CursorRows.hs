{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PatternSynonyms #-}

-- | The programs over cursors that the tests and the benchmark run: a stream
-- of DataRow messages ended by a CommandComplete, as "Network.Directions.Rows"
-- builds it, read with the schema's cursors.
module Database.PostgreSQL.Internal.Protocol.Codec.CursorRows
  ( ResultStream,
    foldRowsCursors,
    streamCursors,
    column,
  )
where

import Control.Arrow (ArrowChoice, returnA, (>>>), (>>^), (^>>))
import Control.Exception (throwIO)
import Control.Monad (when)
import Data.Bool (bool)
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Database.PostgreSQL.Internal.Protocol.Codec.Cursor
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Network.Directions.Rows (Counter, adding)
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, boolA, done, iterateA, iterateWith)
import Network.Streaming.Reader (Reader)
import Network.Streaming.With (With (..), asks, withOnly)

-- | What the result of a query is: rows, ended by a command's completion.
type ResultStream =
  'PayloadChoice
    '[ FramedMessage 'D' ('PayloadCounted16 'MaybeBytes32Atom),
       FramedMessage 'C' 'CStringAtom
     ]

-- | The checksum of the rows' values, as 'Network.Directions.Rows.checksum'
-- adds them up into the counter: the ints, the lengths of the texts and the
-- bools, each decoded in place or skipped, with no list of the fields and no
-- copy.
foldRowsCursors :: forall arr link. (ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => Counter -> arr (Reader link) ()
foldRowsCursors total = (messages :: Reader link -> At ResultStream link) ^>> withOnly loop
  where
    loop :: With (At ResultStream link) arr () ()
    loop = iterateA (asks message >>> With (cases (row :> complete :> NoCases)))
    row = columns >>^ const (Again ())
    complete = cstring >>^ const (Finish ())
    -- The columns of a row, which each takes the next element of the body.
    columns :: arr (At ('PayloadCounted16 'MaybeBytes32Atom) link) ()
    columns =
      withOnly $
        asks count
          >>> expectFive
          >>> added (fromIntegral :: Int32 -> Int)
          >>> added (fromIntegral :: Int64 -> Int)
          >>> added (\(Length size) -> size)
          >>> added (\(Length size) -> size)
          >>> added (fromEnum :: Bool -> Int)
    added :: (InPlace a) => (a -> Int) -> With (At ('PayloadCounted16 'MaybeBytes32Atom) link) arr x ()
    added number = asks column >>> number ^>> adding total
    {-# INLINE added #-}
    expectFive = now (\fields -> when (fields /= 5) (throwIO (CursorError ("a row of " <> show fields <> " fields"))))
{-# INLINABLE foldRowsCursors #-}

-- | The bytes of every value of the stream's rows, given to the consumer as the
-- pieces that arrive, up to the CommandComplete.
streamCursors :: forall arr link. (ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => arr ByteString () -> arr (Reader link) ()
streamCursors consumer = (messages :: Reader link -> At ResultStream link) ^>> withOnly loop
  where
    loop :: With (At ResultStream link) arr () ()
    loop = iterateA (asks message >>> With (cases (row :> complete :> NoCases)))
    row = withOnly (asks (count >>> done) >>> elements) >>^ const (Again ())
    complete = cstring >>^ const (Finish ())
    -- The values of the body, one after the other.
    elements :: With (At ('PayloadCounted16 'MaybeBytes32Atom) link) arr () ()
    elements = iterateWith (asks advance >>^ bool (Finish ()) (Again ())) (asks (child ^>> value))
    value = withOnly (asks valueLength >>> (< 0) ^>> boolA (asks (eachPiece consumer)) returnA)
{-# INLINABLE streamCursors #-}

-- | The next element of the body, decoded in place; its absence, and a NULL, are
-- errors.
column :: (InPlace a, ArrowChoice arr, Annotate arr, Effect arr, Link arr link) => arr (At ('PayloadCounted16 'MaybeBytes32Atom) link) a
column = withOnly (asks advance >>> boolA (missing "fewer fields") value)
  where
    value = asks (child ^>> valueLength) >>> (< 0) ^>> boolA (asks (child ^>> decode)) (missing "a NULL")
    -- The environment is looked at (as 'child' does) on the failing path too, so that every path is strict in the cursor,
    -- and it is passed unboxed through the joins of the sums, not built once for each row.
    missing what = With (now (\(cursor, ()) -> child cursor `seq` throwIO (CursorError ("a row with " <> what))))
{-# INLINE column #-}
