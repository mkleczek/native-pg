{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Parsers and serializers derived from the type-level 'Payload' schema.
--
-- Everything is resolved by instance search over the schema and every method
-- is inlined, so a message's parser or serializer specializes into
-- straight-line code with its constants folded in.
module Database.PostgreSQL.Protocol.Codec
  ( Alternatives (..),
    ByteStream (..),
    HasConstructor (..),
    HasPayload,
    Input,
    InputBuffer,
    InputError (..),
    InputSource (..),
    MonadInput (..),
    Output,
    Parsable (..),
    ParseConstructor (..),
    PayloadPrefix (..),
    Serializable (..),
    SerializeConstructor (..),
    emptyInputBuffer,
    inputPosition,
    outputBuilder,
    outputByteString,
    outputLength,
    outputStreams,
    parseChoice,
    readInputSome,
    runBodyParser,
    runInput,
    runInputUntil,
    skipInput,
  )
where

import Control.Monad (replicateM, unless)
import Control.Monad.Except (MonadError (..))
import Data.Bits (shiftL, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Char (ord)
import Data.Functor (($>))
import Data.Int (Int16, Int32)
import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import Data.Word (Word16, Word32, Word8)
import Database.PostgreSQL.Protocol.Codec.Input
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
import Database.PostgreSQL.Protocol.Codec.Output
  ( ByteStream (..),
    Output,
    bytes,
    cstring,
    int16,
    int32,
    outputBuilder,
    outputByteString,
    outputLength,
    outputStreams,
    word16,
    word32,
    word8,
  )
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Payload (..),
  )
import GHC.TypeLits (KnownChar, KnownNat, Nat, charVal, natVal)

class HasPayload (singleton :: k) (payload :: Payload) | singleton -> payload where

instance HasPayload (singleton :: s (payload :: Payload)) payload where

instance HasPayload singleton payload => HasPayload (wrapper singleton) payload where

-- | Converts between a singleton's payload fields and a semantic value.
--
-- @ConstructorOf singleton semantic r@ is a curried function from the
-- payload's fields to @r@; for a 'PayloadChoice' it is one such function per
-- alternative. With @r ~ semantic@ it is the constructor parsers apply to the
-- fields they read. 'destructorOf' feeds a value's fields to one with any
-- other @r@, which is how serializers take a value apart.
class HasConstructor (singleton :: k) (semantic :: Type) where
  type ConstructorOf singleton semantic (r :: Type) :: Type
  constructorOf :: ConstructorOf singleton semantic semantic
  destructorOf :: semantic -> ConstructorOf singleton semantic r -> r

-- | One function per alternative of a 'PayloadChoice', all producing @r@.
data Alternatives (functions :: [Type]) (r :: Type) where
  NoAlternatives :: Alternatives '[] r
  (:|:) :: function -> Alternatives functions r -> Alternatives (function ': functions) r

infixr 5 :|:

class Parsable (a :: Type) s where
  parser :: forall m. MonadInput m => m a

class Serializable (a :: Type) s where
  serialize :: a -> Output

class ParseConstructor (payload :: Payload) f a | payload f -> a where
  parseWithConstructor :: forall m. MonadInput m => f -> m a

-- | The mirror of 'ParseConstructor': builds the function that takes the
-- payload's fields and passes their serialization on.
class SerializeConstructor (payload :: Payload) k r | payload k -> r where
  serializeWith :: (Output -> r) -> k

-- | Checks a payload's leading constants against the input without consuming
-- anything. It peeks only at constants, so it never waits for bytes after the
-- last one, and it stops at the first part whose size isn't fixed.
class PayloadPrefix (payload :: Payload) where
  matchPrefix ::
    forall m r.
    MonadInput m =>
    -- | Offset of the payload in the input.
    Int ->
    -- | A constant differs.
    m r ->
    -- | The constants match up to a part whose size isn't fixed.
    m r ->
    -- | The whole payload has a fixed size and matches; it ends at this offset.
    (Int -> m r) ->
    m r

-- | Parses whichever alternative the input matches and passes it to that
-- alternative's function.
parseChoice ::
  forall alternatives functions r m.
  ( ParseConstructor ('PayloadChoice alternatives) (Alternatives functions r) r,
    MonadInput m
  ) =>
  Alternatives functions r ->
  m r
parseChoice = parseWithConstructor @('PayloadChoice alternatives)
{-# INLINE parseChoice #-}

-- We keep these instances explicit instead of using a generic `Storable` one,
-- because that would also admit platform-sized numerics like `Int` and `Word`.
instance Parsable Word8 ('FixedBytesAtom 1) where
  parser = (`BSU.unsafeIndex` 0) <$> readInputExactly 1
  {-# INLINE parser #-}

instance Parsable Word16 ('FixedBytesAtom 2) where
  parser = bigEndianWord16 <$> readInputExactly 2
  {-# INLINE parser #-}

instance Parsable Int16 ('FixedBytesAtom 2) where
  parser = fromIntegral <$> parser @Word16 @('FixedBytesAtom 2)
  {-# INLINE parser #-}

instance Parsable Word32 ('FixedBytesAtom 4) where
  parser = bigEndianWord32 <$> readInputExactly 4
  {-# INLINE parser #-}

instance Parsable Int32 ('FixedBytesAtom 4) where
  parser = fromIntegral <$> parser @Word32 @('FixedBytesAtom 4)
  {-# INLINE parser #-}

instance KnownNat n => Parsable ByteString ('FixedBytesAtom n) where
  parser = readInputExactly (natAsInt @n)
  {-# INLINE parser #-}

instance Parsable ByteString 'CStringAtom where
  parser = readInputCStringBytes
  {-# INLINE parser #-}

instance Parsable ByteString 'RemainingBytesAtom where
  parser = readInputRemaining
  {-# INLINE parser #-}

instance Parsable (Maybe ByteString) 'MaybeBytes32Atom where
  parser = do
    lengthOrMissing <- parser @Int32 @('FixedBytesAtom 4)
    case compare lengthOrMissing (-1) of
      EQ -> pure Nothing
      GT -> Just <$> readInputExactly (fromIntegral lengthOrMissing)
      LT -> throwError ("negative length " <> show lengthOrMissing <> " is invalid for MaybeBytes32Atom")
  {-# INLINE parser #-}

instance KnownChar value => Parsable () ('PayloadConst 1 (value :: Char)) where
  parser = expectConstant (charByte @value) =<< parser @Word8 @('FixedBytesAtom 1)
  {-# INLINE parser #-}

instance KnownNat value => Parsable () ('PayloadConst 1 (value :: Nat)) where
  parser = expectConstant (natConstant @value) =<< parser @Word8 @('FixedBytesAtom 1)
  {-# INLINE parser #-}

instance KnownNat value => Parsable () ('PayloadConst 2 (value :: Nat)) where
  parser = expectConstant (natConstant @value) =<< parser @Word16 @('FixedBytesAtom 2)
  {-# INLINE parser #-}

instance KnownNat value => Parsable () ('PayloadConst 4 (value :: Nat)) where
  parser = expectConstant (natConstant @value) =<< parser @Word32 @('FixedBytesAtom 4)
  {-# INLINE parser #-}

instance forall a (payload :: Payload). Parsable a payload => Parsable a ('PayloadBounded payload) where
  parser = withBoundedInput (parser @a @payload)
  {-# INLINE parser #-}

instance Parsable a field => Parsable a ('PayloadField field) where
  parser = parser @a @field
  {-# INLINE parser #-}

instance forall a (payload :: Payload). Parsable a payload => Parsable [a] ('PayloadZeroTerminated payload) where
  parser = go []
    where
      go acc = do
        end <- (== 0) <$> peekInputByteAt 0
        if end
          then readInputExactly 1 $> reverse acc
          else go . (: acc) =<< parser @a @payload
  {-# INLINE parser #-}

instance forall a (payload :: Payload). Parsable a payload => Parsable [a] ('PayloadCounted16 payload) where
  parser = do
    count <- parser @Int16 @('FixedBytesAtom 2)
    unless
      (count >= 0)
      (throwError ("negative element count " <> show count <> " is invalid for PayloadCounted16"))
    replicateM (fromIntegral count) (parser @a @payload)
  {-# INLINE parser #-}

instance forall a (payload :: Payload). Parsable a payload => Parsable [a] ('PayloadCounted32 payload) where
  parser = do
    count <- parser @Int32 @('FixedBytesAtom 4)
    unless
      (count >= 0)
      (throwError ("negative element count " <> show count <> " is invalid for PayloadCounted32"))
    replicateM (fromIntegral count) (parser @a @payload)
  {-# INLINE parser #-}

instance {-# OVERLAPPABLE #-}
  ( HasPayload singleton payload,
    HasConstructor singleton a,
    ParseConstructor payload (ConstructorOf singleton a a) a
  ) =>
  Parsable a singleton
  where
  parser = parseWithConstructor @payload (constructorOf @_ @singleton @a)
  {-# INLINE parser #-}

instance Parsable a 'CStringAtom => ParseConstructor 'CStringAtom (a -> r) r where
  parseWithConstructor constructor = constructor <$> parser @a @'CStringAtom
  {-# INLINE parseWithConstructor #-}

instance Parsable a ('FixedBytesAtom n) => ParseConstructor ('FixedBytesAtom n) (a -> r) r where
  parseWithConstructor constructor = constructor <$> parser @a @('FixedBytesAtom n)
  {-# INLINE parseWithConstructor #-}

instance Parsable a 'RemainingBytesAtom => ParseConstructor 'RemainingBytesAtom (a -> r) r where
  parseWithConstructor constructor = constructor <$> parser @a @'RemainingBytesAtom
  {-# INLINE parseWithConstructor #-}

instance Parsable a 'MaybeBytes32Atom => ParseConstructor 'MaybeBytes32Atom (a -> r) r where
  parseWithConstructor constructor = constructor <$> parser @a @'MaybeBytes32Atom
  {-# INLINE parseWithConstructor #-}

instance Parsable a field => ParseConstructor ('PayloadField field) (a -> r) r where
  parseWithConstructor constructor = constructor <$> parser @a @field
  {-# INLINE parseWithConstructor #-}

instance Parsable [a] ('PayloadZeroTerminated payload) => ParseConstructor ('PayloadZeroTerminated payload) ([a] -> r) r where
  parseWithConstructor constructor = constructor <$> parser @[a] @('PayloadZeroTerminated payload)
  {-# INLINE parseWithConstructor #-}

instance Parsable [a] ('PayloadCounted16 payload) => ParseConstructor ('PayloadCounted16 payload) ([a] -> r) r where
  parseWithConstructor constructor = constructor <$> parser @[a] @('PayloadCounted16 payload)
  {-# INLINE parseWithConstructor #-}

instance Parsable [a] ('PayloadCounted32 payload) => ParseConstructor ('PayloadCounted32 payload) ([a] -> r) r where
  parseWithConstructor constructor = constructor <$> parser @[a] @('PayloadCounted32 payload)
  {-# INLINE parseWithConstructor #-}

instance Parsable () ('PayloadConst bytes value) => ParseConstructor ('PayloadConst bytes value) r r where
  parseWithConstructor constructor = parser @() @('PayloadConst bytes value) $> constructor
  {-# INLINE parseWithConstructor #-}

instance ParseConstructor payload f a => ParseConstructor ('PayloadBounded payload) f a where
  parseWithConstructor constructor = withBoundedInput (parseWithConstructor @payload constructor)
  {-# INLINE parseWithConstructor #-}

instance ParseConstructor ('PayloadStruct '[]) r r where
  parseWithConstructor = pure
  {-# INLINE parseWithConstructor #-}

instance
  ( ParseConstructor field r0 r1,
    ParseConstructor ('PayloadStruct fields) r1 r2
  ) =>
  ParseConstructor ('PayloadStruct (field ': fields)) r0 r2
  where
  parseWithConstructor constructor =
    parseWithConstructor @field constructor >>= parseWithConstructor @('PayloadStruct fields)
  {-# INLINE parseWithConstructor #-}

instance ParseConstructor ('PayloadChoice '[]) (Alternatives '[] r) r where
  parseWithConstructor NoAlternatives = do
    first <- peekInputByteAt 0
    throwError ("no alternative matches input starting with byte " <> show first)
  {-# INLINE parseWithConstructor #-}

instance
  ( PayloadPrefix alternative,
    ParseConstructor alternative function r,
    ParseConstructor ('PayloadChoice alternatives) (Alternatives functions r) r
  ) =>
  ParseConstructor ('PayloadChoice (alternative ': alternatives)) (Alternatives (function ': functions) r) r
  where
  parseWithConstructor (function :|: functions) =
    matchPrefix @alternative 0 others this (const this)
    where
      this = parseWithConstructor @alternative function
      others = parseWithConstructor @('PayloadChoice alternatives) functions
  {-# INLINE parseWithConstructor #-}

instance Serializable Word8 ('FixedBytesAtom 1) where
  serialize = word8
  {-# INLINE serialize #-}

instance Serializable Word16 ('FixedBytesAtom 2) where
  serialize = word16
  {-# INLINE serialize #-}

instance Serializable Int16 ('FixedBytesAtom 2) where
  serialize = int16
  {-# INLINE serialize #-}

instance Serializable Word32 ('FixedBytesAtom 4) where
  serialize = word32
  {-# INLINE serialize #-}

instance Serializable Int32 ('FixedBytesAtom 4) where
  serialize = int32
  {-# INLINE serialize #-}

instance KnownNat n => Serializable ByteString ('FixedBytesAtom n) where
  serialize value
    | BS.length value == natAsInt @n = bytes value
    | otherwise =
        error $
          "expected "
            <> show (natAsInt @n)
            <> " bytes for FixedBytesAtom, got "
            <> show (BS.length value)
  {-# INLINE serialize #-}

instance Serializable ByteString 'CStringAtom where
  serialize = cstring
  {-# INLINE serialize #-}

instance Serializable ByteString 'RemainingBytesAtom where
  serialize = bytes
  {-# INLINE serialize #-}

instance Serializable (Maybe ByteString) 'MaybeBytes32Atom where
  serialize = maybe (int32 (-1)) (\value -> int32 (lengthAsInt32 (BS.length value)) <> bytes value)
  {-# INLINE serialize #-}

instance KnownChar value => Serializable () ('PayloadConst 1 (value :: Char)) where
  serialize () = word8 (charByte @value)
  {-# INLINE serialize #-}

instance KnownNat value => Serializable () ('PayloadConst 1 (value :: Nat)) where
  serialize () = word8 (natConstant @value)
  {-# INLINE serialize #-}

instance KnownNat value => Serializable () ('PayloadConst 2 (value :: Nat)) where
  serialize () = word16 (natConstant @value)
  {-# INLINE serialize #-}

instance KnownNat value => Serializable () ('PayloadConst 4 (value :: Nat)) where
  serialize () = word32 (natConstant @value)
  {-# INLINE serialize #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable a ('PayloadBounded payload) where
  serialize = withLengthPrefix . serialize @a @payload
  {-# INLINE serialize #-}

instance Serializable a field => Serializable a ('PayloadField field) where
  serialize = serialize @a @field
  {-# INLINE serialize #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable [a] ('PayloadZeroTerminated payload) where
  serialize values = foldMap (serialize @a @payload) values <> word8 0
  {-# INLINE serialize #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable [a] ('PayloadCounted16 payload) where
  serialize values = int16 (countAsInt16 values) <> foldMap (serialize @a @payload) values
  {-# INLINE serialize #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable [a] ('PayloadCounted32 payload) where
  serialize values = int32 (lengthAsInt32 (length values)) <> foldMap (serialize @a @payload) values
  {-# INLINE serialize #-}

instance {-# OVERLAPPABLE #-}
  ( HasPayload singleton payload,
    HasConstructor singleton a,
    SerializeConstructor payload (ConstructorOf singleton a Output) Output
  ) =>
  Serializable a singleton
  where
  serialize value = destructorOf @_ @singleton @a value (serializeWith @payload id)
  {-# INLINE serialize #-}

instance Serializable a 'CStringAtom => SerializeConstructor 'CStringAtom (a -> r) r where
  serializeWith continue = continue . serialize @a @'CStringAtom
  {-# INLINE serializeWith #-}

instance Serializable a ('FixedBytesAtom n) => SerializeConstructor ('FixedBytesAtom n) (a -> r) r where
  serializeWith continue = continue . serialize @a @('FixedBytesAtom n)
  {-# INLINE serializeWith #-}

instance Serializable a 'RemainingBytesAtom => SerializeConstructor 'RemainingBytesAtom (a -> r) r where
  serializeWith continue = continue . serialize @a @'RemainingBytesAtom
  {-# INLINE serializeWith #-}

instance Serializable a 'MaybeBytes32Atom => SerializeConstructor 'MaybeBytes32Atom (a -> r) r where
  serializeWith continue = continue . serialize @a @'MaybeBytes32Atom
  {-# INLINE serializeWith #-}

instance Serializable a field => SerializeConstructor ('PayloadField field) (a -> r) r where
  serializeWith continue = continue . serialize @a @field
  {-# INLINE serializeWith #-}

instance Serializable [a] ('PayloadZeroTerminated payload) => SerializeConstructor ('PayloadZeroTerminated payload) ([a] -> r) r where
  serializeWith continue = continue . serialize @[a] @('PayloadZeroTerminated payload)
  {-# INLINE serializeWith #-}

instance Serializable [a] ('PayloadCounted16 payload) => SerializeConstructor ('PayloadCounted16 payload) ([a] -> r) r where
  serializeWith continue = continue . serialize @[a] @('PayloadCounted16 payload)
  {-# INLINE serializeWith #-}

instance Serializable [a] ('PayloadCounted32 payload) => SerializeConstructor ('PayloadCounted32 payload) ([a] -> r) r where
  serializeWith continue = continue . serialize @[a] @('PayloadCounted32 payload)
  {-# INLINE serializeWith #-}

instance Serializable () ('PayloadConst bytes value) => SerializeConstructor ('PayloadConst bytes value) r r where
  serializeWith continue = continue (serialize @() @('PayloadConst bytes value) ())
  {-# INLINE serializeWith #-}

instance SerializeConstructor payload k r => SerializeConstructor ('PayloadBounded payload) k r where
  serializeWith continue = serializeWith @payload (continue . withLengthPrefix)
  {-# INLINE serializeWith #-}

instance SerializeConstructor ('PayloadStruct '[]) r r where
  serializeWith continue = continue mempty
  {-# INLINE serializeWith #-}

instance
  ( SerializeConstructor field k0 k1,
    SerializeConstructor ('PayloadStruct fields) k1 r
  ) =>
  SerializeConstructor ('PayloadStruct (field ': fields)) k0 r
  where
  serializeWith continue =
    serializeWith @field $ \first ->
      serializeWith @('PayloadStruct fields) $ \rest ->
        continue (first <> rest)
  {-# INLINE serializeWith #-}

instance SerializeConstructor ('PayloadChoice '[]) (Alternatives '[] r) r where
  serializeWith _ = NoAlternatives
  {-# INLINE serializeWith #-}

instance
  ( SerializeConstructor alternative k r,
    SerializeConstructor ('PayloadChoice alternatives) (Alternatives ks r) r
  ) =>
  SerializeConstructor ('PayloadChoice (alternative ': alternatives)) (Alternatives (k ': ks) r) r
  where
  serializeWith continue =
    serializeWith @alternative continue :|: serializeWith @('PayloadChoice alternatives) continue
  {-# INLINE serializeWith #-}

instance PayloadPrefix 'CStringAtom where
  matchPrefix _ _ matched _ = matched
  {-# INLINE matchPrefix #-}

instance KnownNat n => PayloadPrefix ('FixedBytesAtom n) where
  matchPrefix offset _ _ continue = continue (offset + natAsInt @n)
  {-# INLINE matchPrefix #-}

instance PayloadPrefix 'RemainingBytesAtom where
  matchPrefix _ _ matched _ = matched
  {-# INLINE matchPrefix #-}

instance PayloadPrefix 'MaybeBytes32Atom where
  matchPrefix _ _ matched _ = matched
  {-# INLINE matchPrefix #-}

instance (HasPayload field payload, PayloadPrefix payload) => PayloadPrefix ('PayloadField field) where
  matchPrefix = matchPrefix @payload
  {-# INLINE matchPrefix #-}

instance PayloadPrefix ('PayloadZeroTerminated payload) where
  matchPrefix _ _ matched _ = matched
  {-# INLINE matchPrefix #-}

instance PayloadPrefix ('PayloadCounted16 payload) where
  matchPrefix _ _ matched _ = matched
  {-# INLINE matchPrefix #-}

instance PayloadPrefix ('PayloadCounted32 payload) where
  matchPrefix _ _ matched _ = matched
  {-# INLINE matchPrefix #-}

instance PayloadPrefix ('PayloadStruct '[]) where
  matchPrefix offset _ _ continue = continue offset
  {-# INLINE matchPrefix #-}

instance (PayloadPrefix field, PayloadPrefix ('PayloadStruct fields)) => PayloadPrefix ('PayloadStruct (field ': fields)) where
  matchPrefix offset mismatch matched continue =
    matchPrefix @field offset mismatch matched $ \next ->
      matchPrefix @('PayloadStruct fields) next mismatch matched continue
  {-# INLINE matchPrefix #-}

-- Constants inside a nested choice don't take part in telling the
-- alternatives of an outer choice apart.
instance PayloadPrefix ('PayloadChoice alternatives) where
  matchPrefix _ _ matched _ = matched
  {-# INLINE matchPrefix #-}

instance KnownChar value => PayloadPrefix ('PayloadConst 1 (value :: Char)) where
  matchPrefix offset mismatch _ continue = do
    actual <- peekInputByteAt offset
    if actual == charByte @value then continue (offset + 1) else mismatch
  {-# INLINE matchPrefix #-}

instance KnownNat value => PayloadPrefix ('PayloadConst 1 (value :: Nat)) where
  matchPrefix offset mismatch _ continue = do
    actual <- peekInputByteAt offset
    if actual == natConstant @value then continue (offset + 1) else mismatch
  {-# INLINE matchPrefix #-}

instance KnownNat value => PayloadPrefix ('PayloadConst 2 (value :: Nat)) where
  matchPrefix offset mismatch _ continue = do
    actual <- peekBigEndianWord16At offset
    if actual == natConstant @value then continue (offset + 2) else mismatch
  {-# INLINE matchPrefix #-}

instance KnownNat value => PayloadPrefix ('PayloadConst 4 (value :: Nat)) where
  matchPrefix offset mismatch _ continue = do
    actual <- peekBigEndianWord32At offset
    if actual == natConstant @value then continue (offset + 4) else mismatch
  {-# INLINE matchPrefix #-}

-- The length prefix is skipped; its value isn't a constant.
instance PayloadPrefix payload => PayloadPrefix ('PayloadBounded payload) where
  matchPrefix offset = matchPrefix @payload (offset + 4)
  {-# INLINE matchPrefix #-}

-- | The length prefix counts its own four bytes.
withBoundedInput :: MonadInput m => m a -> m a
withBoundedInput inner = do
  totalLength <- parser @Int32 @('FixedBytesAtom 4)
  unless
    (totalLength >= 4)
    (throwError ("length " <> show totalLength <> " is invalid for PayloadBounded"))
  withInputLimit (fromIntegral totalLength - 4) inner
{-# INLINE withBoundedInput #-}

withLengthPrefix :: Output -> Output
withLengthPrefix inner = int32 (lengthAsInt32 (outputLength inner + 4)) <> inner
{-# INLINE withLengthPrefix #-}

expectConstant :: (MonadError String m, Eq a, Show a) => a -> a -> m ()
expectConstant expected actual =
  unless
    (actual == expected)
    (throwError ("unexpected constant " <> show actual <> ", expected " <> show expected))
{-# INLINE expectConstant #-}

peekBigEndianWord16At :: MonadInput m => Int -> m Word16
peekBigEndianWord16At offset = do
  low <- peekInputByteAt (offset + 1)
  high <- peekInputByteAt offset
  pure (shiftL (fromIntegral high) 8 .|. fromIntegral low)
{-# INLINE peekBigEndianWord16At #-}

peekBigEndianWord32At :: MonadInput m => Int -> m Word32
peekBigEndianWord32At offset = do
  byte3 <- peekInputByteAt (offset + 3)
  byte0 <- peekInputByteAt offset
  byte1 <- peekInputByteAt (offset + 1)
  byte2 <- peekInputByteAt (offset + 2)
  pure $
    shiftL (fromIntegral byte0) 24
      .|. shiftL (fromIntegral byte1) 16
      .|. shiftL (fromIntegral byte2) 8
      .|. fromIntegral byte3
{-# INLINE peekBigEndianWord32At #-}

-- Callers have checked the length.
bigEndianWord16 :: ByteString -> Word16
bigEndianWord16 input =
  shiftL (fromIntegral (BSU.unsafeIndex input 0)) 8
    .|. fromIntegral (BSU.unsafeIndex input 1)
{-# INLINE bigEndianWord16 #-}

-- Callers have checked the length.
bigEndianWord32 :: ByteString -> Word32
bigEndianWord32 input =
  shiftL (fromIntegral (BSU.unsafeIndex input 0)) 24
    .|. shiftL (fromIntegral (BSU.unsafeIndex input 1)) 16
    .|. shiftL (fromIntegral (BSU.unsafeIndex input 2)) 8
    .|. fromIntegral (BSU.unsafeIndex input 3)
{-# INLINE bigEndianWord32 #-}

lengthAsInt32 :: Int -> Int32
lengthAsInt32 count
  | count <= fromIntegral (maxBound @Int32) = fromIntegral count
  | otherwise = error ("length " <> show count <> " does not fit in an Int32")
{-# INLINE lengthAsInt32 #-}

countAsInt16 :: [a] -> Int16
countAsInt16 values
  | count <= fromIntegral (maxBound @Int16) = fromIntegral count
  | otherwise = error ("element count " <> show count <> " does not fit in an Int16")
  where
    count = length values
{-# INLINE countAsInt16 #-}

natAsInt :: forall n. KnownNat n => Int
natAsInt = fromInteger (natVal (Proxy @n))
{-# INLINE natAsInt #-}

natConstant :: forall n a. (KnownNat n, Bounded a, Integral a) => a
natConstant
  | value <= toInteger (maxBound @a) = fromInteger value
  | otherwise = error ("payload constant " <> show value <> " does not fit its size")
  where
    value = natVal (Proxy @n)
{-# INLINE natConstant #-}

charByte :: forall value. KnownChar value => Word8
charByte
  | code <= 255 = fromIntegral code
  | otherwise = error ("payload constant " <> show (charVal (Proxy @value)) <> " does not fit in 1 byte")
  where
    code = ord (charVal (Proxy @value))
{-# INLINE charByte #-}
