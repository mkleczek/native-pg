{-# LANGUAGE DataKinds #-}

-- | The programs over cursors that the tests and the benchmark run: a stream
-- of DataRow messages ended by a CommandComplete, as "Network.Directions.Rows"
-- builds it, read with the schema's cursors.
module Database.PostgreSQL.Internal.Protocol.Codec.CursorRows
  ( ResultStream,
    foldRowsCursors,
    streamCursors,
    column,
    absent,
    present,
    isNull,
  )
where

import Control.Arrow (ArrowChoice, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (throwIO)
import Control.Monad (when)
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Database.PostgreSQL.Internal.Protocol.Codec.Cursor
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Network.Directions.Classes
import Network.Directions.Reader (Reader)
import Network.Directions.Rows (Counter, adding)

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
foldRowsCursors :: forall arr link. (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> Counter -> arr () ()
foldRowsCursors reader total = loop
  where
    result = messages reader :: At ResultStream link
    loop = message result >>> (,()) ^>> cases result (row :> complete :> NoCases) >>> (loop ||| returnA)
    row body =
      (count body >>> expectFive)
        >>> column body
        >>> (\value -> fromIntegral (value :: Int32))
        ^>> adding total
        >>> column body
        >>> (\value -> fromIntegral (value :: Int64))
        ^>> adding total
        >>> column body
        >>> (\(Length size) -> size)
        ^>> adding total
        >>> column body
        >>> (\(Length size) -> size)
        ^>> adding total
        >>> column body
        >>> (\value -> fromEnum (value :: Bool))
        ^>> adding total
        >>^ Left
    complete body = cstring body >>^ Right . const ()
    expectFive = now (\fields -> when (fields /= 5) (throwIO (CursorError ("a row of " <> show fields <> " fields"))))
{-# INLINABLE foldRowsCursors #-}

-- | The bytes of every value of the stream's rows, given to the consumer as the
-- pieces that arrive, up to the CommandComplete.
streamCursors :: forall arr link. (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr ByteString () -> arr () ()
streamCursors reader consumer = loop
  where
    result = messages reader :: At ResultStream link
    loop = message result >>> (,()) ^>> cases result (row :> complete :> NoCases) >>> (loop ||| returnA)
    row body = count body >>> const () ^>> elements body >>^ Left
    complete body = cstring body >>^ Right . const ()
    elements body = element body >>> (absent ^>> ((value (child body) >>> elements body) ||| returnA))
    value cursor = valueLength cursor >>> (isNull ^>> (eachPiece cursor consumer ||| returnA))
{-# INLINABLE streamCursors #-}

-- | The next element of the body, decoded in place; its absence, and a NULL, are
-- errors.
column :: (InPlace a, ArrowChoice arr, Effect arr, Link arr link) => At ('PayloadCounted16 'MaybeBytes32Atom) link -> arr () a
column body = element body >>> (present ^>> (missing "fewer fields" ||| (valueLength cursor >>> (isNull ^>> (decode cursor ||| missing "a NULL")))))
  where
    cursor = child body
    missing what = now (\() -> throwIO (CursorError ("a row with " <> what)))
{-# INLINE column #-}

-- | A length that is NULL is 'Right'. The sums of these are the same constants
-- whichever way they go, so whatever follows them can share its code.
isNull :: Int -> Either () ()
isNull size = if size < 0 then Right () else Left ()

-- | Nothing is 'Right', as in a loop that goes on with something and ends with
-- nothing.
absent :: Maybe a -> Either () ()
absent = maybe (Right ()) (const (Left ()))

-- | Something is 'Right', and nothing is what has no alternative.
present :: Maybe a -> Either () ()
present = maybe (Left ()) (const (Right ()))
