{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Streaming cursors derived from the 'Payload' schema: a result is a stream
-- of messages, a message's body is a counted payload of values, and a value is
-- a stream of bytes. A cursor stands at a payload of some shape and is pulled
-- one step at a time; the steps are arrows over a 'Reader' of the directions
-- prototype, so they run in any arrow that has a link for it.
--
-- Where the cursors are is in the reader's unboxed marks: the bytes in hand,
-- the end of the message body that is being read, and what each level has
-- left (elements, or bytes of a value). A step makes sure that the bytes it
-- needs are in hand, which is a 'now' that gives a sum with nothing in it but
-- what a receive needs, and takes them, which is another 'now'; only a step
-- that finds too few in hand receives. A step that waits for nothing allocates
-- nothing in the framework. Nothing is copied, no value or message is held
-- whole: the pieces of a value are views of the buffer, and a value that is
-- decoded in place is decoded from the buffer.
--
-- A cursor is a run-time value, so it is an input of a step, and not an
-- argument: what a step does when it has to wait is an arrow, which is built
-- once for any reader, and a cursor is an index into the reader. 'element' gives
-- the cursor of an element, and 'cases' hands the cursor of the body that
-- 'message' chose to the arrow for its alternative; 'messages' and 'child' only
-- compute a cursor from another, and are functions. A step that tells what to do
-- next does it with a sum that has nothing in it ('advance', 'message'), and the
-- cursor comes from the environment, because a sum that carries a cursor is
-- built, and allocates, each time it is chosen.
--
-- What the schema derives, by instances, is the shape of the stream: which
-- messages an alternative has ('Framed', 'Messages': which of them a tag chooses
-- and the cases that go on from it), how a payload is begun ('Cursor') and left
-- before its end ('Skippable'), and how a type is decoded ('InPlace'). The
-- steps of a shape ('count', 'element', 'valueLength', 'piece', 'decode',
-- 'cstring', 'fixed') are written for the atoms and the counted payload.
module Database.PostgreSQL.Internal.Protocol.Codec.Cursor
  ( -- * Cursors
    At,
    CursorError (..),
    Cursor (..),
    Skippable (..),

    -- * A stream of messages
    FramedMessage,
    Framed (..),
    Messages (..),
    Cases (..),
    messages,
    message,
    cases,

    -- * Counted payloads
    child,
    count,
    advance,
    element,

    -- * A value that is a length and bytes
    valueLength,
    valueSize,
    piece,
    eachPiece,
    skipValue,
    InPlace (..),
    Decoder (..),
    Length (..),
    decode,

    -- * Atoms inside a message
    fixed,
    cstring,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (Exception, throwIO)
import Control.Monad (when)
import Data.Bits (shiftL, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Char (ord)
import Data.Int (Int32, Int64)
import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import Data.Void (Void, absurd)
import Data.Word (Word8)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Foreign.ForeignPtr (plusForeignPtr)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peekByteOff)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import GHC.TypeLits (KnownChar, KnownNat, Nat, charVal, natVal)
import Network.Streaming.Classes hiding (Stream (..))
import Network.Streaming.Reader
import Network.Streaming.With (With (..), asks, lift, with, withOnly)

-- | A message's frame: its tag byte and the length-prefixed body, as
-- native-pg's own.
type FramedMessage tag payload =
  'PayloadStruct
    '[ 'PayloadConst 1 tag,
       'PayloadBounded payload
     ]

-- | A cursor standing at a payload of this shape: the reader, and the depth
-- whose marks hold what this level has left.
data At (p :: Payload) link = At !(Reader link) !Int

-- | What went wrong reading a stream of messages: a message that is not what
-- its frame says, a read past the end of a body, or something that doesn't fit
-- the buffer.
newtype CursorError = CursorError String
  deriving (Show)

instance Exception CursorError

failing :: String -> IO a
failing = throwIO . CursorError

-- The errors that depend on the numbers are out of line, so that the steps that
-- go well don't build them.

shortMessage :: Int -> IO a
shortMessage size = failing ("a message of " <> show size <> " bytes, which can't hold its length")
{-# NOINLINE shortMessage #-}

pastBody :: Int -> IO a
pastBody size = failing ("a read of " <> show size <> " bytes reaches past the end of its message's body")
{-# NOINLINE pastBody #-}

valueBeyond :: Int -> IO a
valueBeyond size = failing ("a value of " <> show size <> " bytes reaches past the end of its message")
{-# NOINLINE valueBeyond #-}

wrongSize :: Int -> Int -> IO a
wrongSize size wire = failing ("a value of " <> show size <> " bytes, where its type has " <> show wire)
{-# NOINLINE wrongSize #-}

unfitting :: String -> Int -> Int -> IO a
unfitting what size capacity = failing (what <> " of " <> show size <> " bytes doesn't fit a buffer of " <> show capacity)
{-# NOINLINE unfitting #-}

-- | The shapes a cursor can stand at. A cursor is begun when the one above it
-- hands it out.
class Cursor (p :: Payload) where
  -- | Begins a payload of this shape at the depth: nothing of it is read.
  begin :: Reader link -> Int -> IO ()

-- | A value that has not been given its length.
instance Cursor 'MaybeBytes32Atom where
  begin reader depth = setMark reader (markFrame depth) (-1)
  {-# INLINE begin #-}

instance Cursor 'CStringAtom where
  begin _ _ = pure ()
  {-# INLINE begin #-}

-- | The count is not read, so there are no elements, and none of them is begun.
instance Cursor ('PayloadCounted16 e) where
  begin reader depth = do
    setMark reader (markFrame depth) 0
    setMark reader (markFrame (depth + 1)) 0
  {-# INLINE begin #-}

-- | A message without a body has nothing to read.
instance Cursor ('PayloadStruct '[]) where
  begin _ _ = pure ()
  {-# INLINE begin #-}

instance (KnownNat n) => Cursor ('FixedBytesAtom n) where
  begin reader depth = setMark reader (markFrame depth) (natAsInt @n)
  {-# INLINE begin #-}

-- | The shapes that can be left before they are read to the end: what a
-- counted payload does with the element it gave before.
class (Cursor p) => Skippable (p :: Payload) where
  -- | Skips what is unread of the payload the cursor stands at, receiving if
  -- the bytes are not in hand.
  skipRest :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At p link) ()

-- | A value whose length was not read is read, to skip it.
instance Skippable 'MaybeBytes32Atom where
  skipRest = withOnly (asks (now unread) >>> ((asks valueLength >>> asks skipValue) ||| asks skipValue))
    where
      unread (At reader depth) = (\left -> if left < 0 then Left () else Right ()) <$> mark reader (markFrame depth)
  {-# INLINE skipRest #-}

instance (KnownNat n) => Skippable ('FixedBytesAtom n) where
  skipRest = skipLeft >>^ const ()
  {-# INLINE skipRest #-}

-- | The messages of a stream, told apart by the tag of their frame.
class Framed (alternative :: Payload) where
  -- | What the body of the message is.
  type BodyOf alternative :: Payload

  frameTag :: Int

instance (KnownChar tag) => Framed (FramedMessage (tag :: Char) body) where
  type BodyOf (FramedMessage (tag :: Char) body) = body
  frameTag = ord (charVal (Proxy @tag))
  {-# INLINE frameTag #-}

instance (KnownNat tag) => Framed (FramedMessage (tag :: Nat) body) where
  type BodyOf (FramedMessage (tag :: Nat) body) = body
  frameTag = natAsInt @tag
  {-# INLINE frameTag #-}

-- | One arrow for each alternative of a stream of messages, given the cursor of
-- the alternative's body, and all giving the same result.
data Cases (alternatives :: [Payload]) link (arr :: Type -> Type -> Type) r where
  NoCases :: Cases '[] link arr r
  (:>) :: arr (At (BodyOf alternative) link) r -> Cases alternatives link arr r -> Cases (alternative ': alternatives) link arr r

infixr 5 :>

-- | The alternatives of a stream of messages: which one the frame of the next
-- message tells, as a sum that has nothing in it but which, so that no cursor is
-- built to tell it, and the arrow that goes on from each, which is given the
-- cursor of the alternative's body.
class Messages (alternatives :: [Payload]) where
  type Chosen alternatives :: Type

  -- | The alternative that has the tag, with its body begun at the depth below.
  select :: Reader link -> Int -> Int -> IO (Chosen alternatives)

  -- | Goes on from the alternative that was chosen, with the case for it, in the
  -- environment of the cursor of the stream.
  branches :: (ArrowChoice arr) => Cases alternatives link arr r -> With (At p link) arr (Chosen alternatives) r

-- | No alternative has the tag.
instance Messages '[] where
  type Chosen '[] = Void
  select _ _ tag = failing ("a message with the tag " <> show tag)
  {-# INLINE select #-}
  branches NoCases = arr absurd
  {-# INLINE branches #-}

instance (Framed alternative, Cursor (BodyOf alternative), Messages alternatives) => Messages (alternative ': alternatives) where
  type Chosen (alternative ': alternatives) = Either () (Chosen alternatives)
  select reader depth tag
    | tag == frameTag @alternative = Left () <$ begin @(BodyOf alternative) reader (depth + 1)
    | otherwise = Right <$> select @alternatives reader depth tag
  {-# INLINE select #-}
  branches (handler :> others) = (asks (arr (\(At reader depth) -> At reader (depth + 1))) >>> lift handler) ||| branches others
  {-# INLINE branches #-}

-- | The cursor of a stream of messages that the reader starts: nothing of the
-- stream is read, and the reader has been used for nothing else.
messages :: Reader link -> At ('PayloadChoice alternatives) link
messages reader = At reader 0
{-# INLINE messages #-}

-- | The next message. It skips what is unread of the previous message's body,
-- takes the frame, the tag and the length, with one check that these five bytes
-- are in hand, and tells which alternative the tag chooses, with its body
-- begun. It never asks for more than the frame says is there: five bytes, and
-- then the body's own.
message :: forall alternatives arr link. (Messages alternatives, ArrowChoice arr, Effect arr, Link arr link) => arr (At ('PayloadChoice alternatives) link) (Chosen alternatives)
message = withOnly (asks (now ready) >>> (slow ||| returnA) >>> asks (now framed))
  where
    -- The frame starts where the previous body ends, if it is in hand.
    ready (At reader _) = do
      from <- mark reader markStart
      limit <- mark reader markLimit
      end <- mark reader markEnd
      pure (if end - max from limit >= 5 then Right () else Left ())
    -- The unread bytes of the body are skipped, and the frame is received.
    slow =
      asks (now (\(At reader _) -> (\from limit -> max 0 (limit - from)) <$> mark reader markStart <*> mark reader markLimit))
        >>> onReader skipBytes
        >>> asks (now (\(At reader _) -> (\from end -> if end - from >= 5 then Right () else Left 5) <$> mark reader markStart <*> mark reader markEnd))
        >>> (onReader refill ||| returnA)
    framed (At reader depth) = do
      from <- mark reader markStart
      limit <- mark reader markLimit
      let !at = max from limit
      tag <- scalar reader at 1
      size <- scalar reader (at + 1) 4
      when (size < 4) $ shortMessage size
      setMark reader markStart (at + 5)
      setMark reader markLimit (at + 1 + size)
      select @alternatives reader depth tag
{-# INLINE message #-}

-- | Goes on from the alternative that 'message' chose, with the case for it: its
-- inputs are the cursor of the stream and the alternative.
cases :: (Messages alternatives, ArrowChoice arr) => Cases alternatives link arr r -> arr (At ('PayloadChoice alternatives) link, Chosen alternatives) r
cases handlers = with (branches handlers)
{-# INLINE cases #-}

-- | The cursor of the elements of a counted payload.
child :: At ('PayloadCounted16 e) link -> At e link
child (At reader depth) = At reader (depth + 1)
{-# INLINE child #-}

-- | The number of elements, which is read before them.
count :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At ('PayloadCounted16 e) link) Int
count =
  inBody 2 $ \(At reader depth) from -> do
    elements <- scalar reader from 2
    when (elements > 0x7fff) $ failing "a negative element count"
    setMark reader (markFrame depth) elements
    setMark reader (markFrame (depth + 1)) 0
    pure elements
{-# INLINE count #-}

-- | Moves on to the next element, with what is unread of the one before
-- skipped, and says whether there is one. Its cursor is the 'child' of this
-- one, which is begun. It gives no cursor, so nothing is built for it.
advance :: forall e arr link. (Skippable e, ArrowChoice arr, Effect arr, Link arr link) => arr (At ('PayloadCounted16 e) link) Bool
advance = withOnly (asks (now ready) >>> (slow ||| returnA) >>> asks (now next))
  where
    -- Nothing is unread of the one before.
    ready (At reader depth) = (\before -> if before == 0 then Right () else Left ()) <$> mark reader (markFrame (depth + 1))
    slow = asks (child ^>> skipRest @e)
    next (At reader depth) = do
      left <- mark reader (markFrame depth)
      if left == 0
        then pure False
        else do
          setMark reader (markFrame depth) (left - 1)
          begin @e reader (depth + 1)
          pure True
{-# INLINE advance #-}

-- | The next element, or 'Nothing' after the last, with what is unread of the
-- one before skipped. The element is a cursor that is begun, standing at it.
element :: forall e arr link. (Skippable e, ArrowChoice arr, Effect arr, Link arr link) => arr (At ('PayloadCounted16 e) link) (Maybe (At e link))
element = advance &&& returnA >>^ \(found, cursor) -> if found then Just (child cursor) else Nothing
{-# INLINE element #-}

-- | The length of the value, which is read before its bytes, and -1 for NULL.
-- A value that reaches past the end of its message is an error. This is the
-- step that allocates nothing: it gives a number, where 'valueSize' gives a
-- 'Maybe', which is built for whatever follows it when that is large.
valueLength :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At 'MaybeBytes32Atom link) Int
valueLength =
  inBody 4 $ \(At reader depth) from -> do
    raw <- scalar reader from 4
    limit <- mark reader markLimit
    if raw == 0xffffffff
      then (-1) <$ setMark reader (markFrame depth) 0
      else do
        when (raw > 0x7fffffff) $ failing "a negative value length"
        when (raw > limit - from - 4) $ valueBeyond raw
        setMark reader (markFrame depth) raw
        pure raw
{-# INLINE valueLength #-}

-- | The length of the value, or 'Nothing' for NULL.
valueSize :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At 'MaybeBytes32Atom link) (Maybe Int)
valueSize = valueLength >>^ \size -> if size < 0 then Nothing else Just size
{-# INLINE valueSize #-}

-- | The next piece of the value, as a view of the buffer that is valid until
-- the next pull: the bytes in hand go first, and a receive is made only when
-- none are. 'Nothing' at the end of the value.
piece :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At 'MaybeBytes32Atom link) (Maybe ByteString)
piece = withOnly (asks (now attempt) >>> (more ||| returnA))
  where
    attempt (At reader depth) = do
      left <- mark reader (markFrame depth)
      if left == 0
        then pure (Right Nothing)
        else do
          when (left < 0) $ failing "the length of a value was not read"
          from <- mark reader markStart
          end <- mark reader markEnd
          if end == from then pure (Left ()) else Right . Just <$> taking reader depth left from (end - from)
    taking reader depth !left !from !held = do
      let !size = min left held
      setMark reader markStart (from + size)
      setMark reader (markFrame depth) (left - size)
      pure (BSI.BS (readerMemory reader `plusForeignPtr` from) size)
    {-# INLINE taking #-}
    -- Nothing is in hand, so the receive has the whole buffer.
    more =
      asks
        ( now
            ( \(At reader _) -> do
                end <- mark reader markEnd
                limit <- mark reader markLimit
                setMark reader markLimit (limit - end)
                setMark reader markStart 0
                setMark reader markEnd 0
                pure (roomAfter reader 0)
            )
        )
        >>> receiveAt
        >>> With
          ( now
              ( \(At reader depth, received) -> do
                  when (received <= 0) $ ioError (userError "the stream ended inside a value")
                  setMark reader markEnd received
                  left <- mark reader (markFrame depth)
                  Just <$> taking reader depth left 0 received
              )
          )
{-# INLINE piece #-}

-- | The pieces of the value, one after the other, given to the consumer.
eachPiece :: (ArrowChoice arr, Effect arr, Link arr link) => arr ByteString () -> arr (At 'MaybeBytes32Atom link) ()
eachPiece consumer = withOnly loop
  where
    loop = asks piece >>> (maybe (Left ()) Right ^>> (returnA ||| (lift consumer >>> loop)))
{-# INLINE eachPiece #-}

-- | Skips what is unread of the value, whose length was read. The size of what
-- was skipped.
skipLeft :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At p link) Int
skipLeft = withOnly (asks (now starting) >>> (\size -> (size, size)) ^>> first (onReader skipBytes) >>^ snd)
  where
    starting (At reader depth) = do
      size <- mark reader (markFrame depth)
      when (size < 0) $ failing "the length of a value was not read"
      setMark reader (markFrame depth) 0
      pure size
{-# INLINE skipLeft #-}

-- | Skips what is unread of the value, whose length was read.
skipValue :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At 'MaybeBytes32Atom link) ()
skipValue = skipLeft >>^ const ()
{-# INLINE skipValue #-}

-- | How a type is decoded from the bytes of a value.
data Decoder a
  = -- | From a value of exactly this many bytes, in hand, in place.
    Peek !Int (Ptr Word8 -> IO a)
  | -- | From a value of any size that fits the buffer, in hand, in place.
    PeekWhole (Int -> Ptr Word8 -> IO a)
  | -- | From the size only: the bytes are skipped, and need not be in hand.
    Skip (Int -> a)

-- | The types that are decoded from a view of the bytes, without copying them.
class InPlace a where
  decoder :: Decoder a

instance InPlace Int32 where
  decoder = Peek 4 (fmap fromIntegral . bigEndian 4)
  {-# INLINE decoder #-}

instance InPlace Int64 where
  decoder = Peek 8 (fmap fromIntegral . bigEndian 8)
  {-# INLINE decoder #-}

instance InPlace Bool where
  decoder = Peek 1 (\pointer -> (/= (0 :: Word8)) <$> peekByteOff pointer 0)
  {-# INLINE decoder #-}

-- | The length of a value, which is all that is taken of it.
newtype Length = Length Int
  deriving (Eq, Show)

instance InPlace Length where
  decoder = Skip Length
  {-# INLINE decoder #-}

-- | The value, decoded in place: with all of its bytes in hand, which a receive
-- makes if it has to. Its length must have been read. A value that is larger
-- than the buffer, or that the type has another size for, is an error.
decode :: forall a arr link. (InPlace a, ArrowChoice arr, Effect arr, Link arr link) => arr (At 'MaybeBytes32Atom link) a
decode = case decoder @a of
  Peek wire peek -> inValue wire (const peek)
  PeekWhole peek -> inValue (-1) peek
  Skip make -> skipLeft >>^ make
{-# INLINE decode #-}

-- | The value's bytes in hand, and what is made of them: how many bytes the
-- value must have, unless that is negative.
inValue :: (ArrowChoice arr, Effect arr, Link arr link) => Int -> (Int -> Ptr Word8 -> IO a) -> arr (At 'MaybeBytes32Atom link) a
inValue wire peek = withOnly (asks (now ready) >>> (onReader refill ||| returnA) >>> asks (now taken))
  where
    ready (At reader depth) = do
      size <- mark reader (markFrame depth)
      when (size < 0) $ failing "the length of a value was not read"
      when (wire >= 0 && size /= wire) $ wrongSize size wire
      from <- mark reader markStart
      end <- mark reader markEnd
      if end - from >= size
        then pure (Right ())
        else do
          when (size > readerCapacity reader) $ unfitting "a value" size (readerCapacity reader)
          pure (Left size)
    taken (At reader depth) = do
      size <- mark reader (markFrame depth)
      from <- mark reader markStart
      value <- unsafeWithForeignPtr (readerMemory reader) (\memory -> peek size (memory `plusPtr` from))
      value `seq` setMark reader markStart (from + size)
      setMark reader (markFrame depth) 0
      pure value
{-# INLINE inValue #-}

-- | A fixed number of bytes, up to eight, as a big-endian number.
fixed :: forall n arr link. (KnownNat n, ArrowChoice arr, Effect arr, Link arr link) => arr (At ('FixedBytesAtom n) link) Int
fixed =
  inBody (natAsInt @n) $ \(At reader depth) from -> do
    setMark reader (markFrame depth) 0
    scalar reader from (natAsInt @n)
{-# INLINE fixed #-}

-- | The C string that is the whole body of a control message: a view of the
-- buffer, without its terminator. A message that doesn't fit the buffer is an
-- error, since the string is in hand at once.
cstring :: (ArrowChoice arr, Effect arr, Link arr link) => arr (At 'CStringAtom link) ByteString
cstring = withOnly (asks (now ready) >>> (onReader refill ||| returnA) >>> asks (now found))
  where
    ready (At reader _) = do
      from <- mark reader markStart
      limit <- mark reader markLimit
      end <- mark reader markEnd
      let !size = limit - from
      when (size > readerCapacity reader) $ unfitting "a control message" size (readerCapacity reader)
      pure (if end - from >= size then Right () else Left size)
    found (At reader _) = do
      from <- mark reader markStart
      limit <- mark reader markLimit
      let body = BSI.BS (readerMemory reader `plusForeignPtr` from) (limit - from)
      case BS.elemIndex 0 body of
        Nothing -> failing "a C string without its terminator"
        Just index -> do
          setMark reader markStart (from + index + 1)
          pure (BS.take index body)
{-# INLINE cstring #-}

-- | An atom inside the body, of the size it is given: it makes sure that the
-- bytes are in hand, which is one 'now', and only if there are too few, it
-- receives the rest; then it takes them, which is the other. It is an error to
-- reach past the end of the body, which is checked before anything is received,
-- so that nothing waits for bytes that the message does not have.
inBody :: (ArrowChoice arr, Effect arr, Link arr link) => Int -> (At p link -> Int -> IO a) -> arr (At p link) a
inBody size take' = withOnly (asks (now ready) >>> (onReader refill ||| returnA) >>> asks (now taken))
  where
    ready (At reader _) = do
      from <- mark reader markStart
      limit <- mark reader markLimit
      when (size > limit - from) $ pastBody size
      end <- mark reader markEnd
      if end - from >= size
        then pure (Right ())
        else do
          when (size > readerCapacity reader) $ unfitting "a read" size (readerCapacity reader)
          pure (Left size)
    taken cursor@(At reader _) = do
      from <- mark reader markStart
      value <- take' cursor from
      value `seq` setMark reader markStart (from + size)
      pure value
{-# INLINE inBody #-}

-- | Skips this many bytes: the ones in hand, and then the ones that arrive.
skipBytes :: (ArrowChoice arr, Effect arr, Link arr link) => arr (Reader link, Int) ()
skipBytes = with (With (now attempt) >>> (more ||| returnA))
  where
    attempt (reader, size) = do
      from <- mark reader markStart
      end <- mark reader markEnd
      if end - from >= size
        then Right () <$ setMark reader markStart (from + size)
        else Left (size - (end - from)) <$ setMark reader markStart end
    more = With (now (uncurry (`setMark` markWanted))) >>> loop
    -- Nothing is in hand, so the receive has the whole buffer.
    loop =
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
              ( \(reader, received) -> do
                  when (received <= 0) $ ioError (userError "the stream ended inside a message")
                  wanted <- mark reader markWanted
                  if received >= wanted
                    then Right () <$ (setMark reader markStart wanted *> setMark reader markEnd received)
                    else do
                      limit <- mark reader markLimit
                      setMark reader markLimit (limit - received)
                      setMark reader markWanted (wanted - received)
                      pure (Left ())
              )
          )
        >>> (loop ||| returnA)
{-# INLINE skipBytes #-}

-- | A step of the reader's that takes a size, on the reader of the cursor in the environment.
onReader :: (Arrow arr) => arr (Reader link, Int) () -> With (At p link) arr Int ()
onReader step = With ((\(At reader _, size) -> (reader, size)) ^>> step)
{-# INLINE onReader #-}

-- | Receives from the link of the cursor's reader, into the room.
receiveAt :: (Arrow arr, Link arr link) => With (At p link) arr Room Int
receiveAt = With ((\(At reader _, room) -> (reader, room)) ^>> receiveInto)
{-# INLINE receiveAt #-}

-- | The big-endian number of the bytes at the address.
bigEndian :: Int -> Ptr Word8 -> IO Word
bigEndian size pointer = go 0 0
  where
    go :: Int -> Word -> IO Word
    go !index !value
      | index == size = pure value
      | otherwise = peekByteOff pointer index >>= \byte -> go (index + 1) (value `shiftL` 8 .|. fromIntegral (byte :: Word8))
{-# INLINE bigEndian #-}

natAsInt :: forall n. (KnownNat n) => Int
natAsInt = fromInteger (natVal (Proxy @n))
{-# INLINE natAsInt #-}
