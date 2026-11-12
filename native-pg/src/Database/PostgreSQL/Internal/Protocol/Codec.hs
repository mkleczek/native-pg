{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Serializers derived from the type-level 'Payload' schema.
--
-- Everything is resolved by instance search over the schema and every method
-- is inlined, so a message's serializer specializes into straight-line code
-- with its constants folded in.
module Database.PostgreSQL.Internal.Protocol.Codec
  ( Alternatives (..),
    ByteStream (..),
    HasConstructor (..),
    HasPayload,
    Output,
    Serializable (..),
    SerializeConstructor (..),
    Uncopied (..),
    outputCopied,
    outputLength,
    writeOutput,
  )
where

import Data.ByteString (ByteString)
import Database.PostgreSQL.Types (Parameter (..))
import qualified Data.ByteString as BS
import Data.Char (ord)
import Data.Int (Int16, Int32)
import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import Data.Word (Word16, Word32, Word8)
import Database.PostgreSQL.Internal.Protocol.Codec.Output
  ( ByteStream (..),
    Output,
    Uncopied (..),
    bytes,
    cstring,
    int16,
    int32,
    outputCopied,
    outputLength,
    outputs,
    word16,
    word32,
    word8,
    writeOutput,
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
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
-- alternative. With @r ~ semantic@ it is the constructor. 'destructorOf' feeds
-- a value's fields to one with any other @r@, which is how serializers take a
-- value apart.
class HasConstructor (singleton :: k) (semantic :: Type) where
  type ConstructorOf singleton semantic (r :: Type) :: Type
  constructorOf :: ConstructorOf singleton semantic semantic
  destructorOf :: semantic -> ConstructorOf singleton semantic r -> r

-- | One function per alternative of a 'PayloadChoice', all producing @r@.
data Alternatives (functions :: [Type]) (r :: Type) where
  NoAlternatives :: Alternatives '[] r
  (:|:) :: function -> Alternatives functions r -> Alternatives (function ': functions) r

infixr 5 :|:

-- A primitive value can have several wire encodings, so its schema stays explicit.
class Serializable (a :: Type) s where
  serializeField :: a -> Output

-- | Builds the function that takes the payload's fields and passes their
-- serialization on.
class SerializeConstructor (payload :: Payload) k r | payload k -> r where
  serializeWith :: (Output -> r) -> k

instance Serializable Word8 ('FixedBytesAtom 1) where
  serializeField = word8
  {-# INLINE serializeField #-}

instance Serializable Word16 ('FixedBytesAtom 2) where
  serializeField = word16
  {-# INLINE serializeField #-}

instance Serializable Int16 ('FixedBytesAtom 2) where
  serializeField = int16
  {-# INLINE serializeField #-}

instance Serializable Word32 ('FixedBytesAtom 4) where
  serializeField = word32
  {-# INLINE serializeField #-}

instance Serializable Int32 ('FixedBytesAtom 4) where
  serializeField = int32
  {-# INLINE serializeField #-}

instance KnownNat n => Serializable ByteString ('FixedBytesAtom n) where
  serializeField value
    | BS.length value == natAsInt @n = bytes value
    | otherwise =
        error $
          "expected "
            <> show (natAsInt @n)
            <> " bytes for FixedBytesAtom, got "
            <> show (BS.length value)
  {-# INLINE serializeField #-}

instance Serializable ByteString 'CStringAtom where
  serializeField = cstring
  {-# INLINE serializeField #-}

instance Serializable ByteString 'RemainingBytesAtom where
  serializeField = bytes
  {-# INLINE serializeField #-}

instance Serializable (Maybe ByteString) 'MaybeBytes32Atom where
  serializeField = maybe (int32 (-1)) (\value -> int32 (lengthAsInt32 (BS.length value)) <> bytes value)
  {-# INLINE serializeField #-}

instance KnownChar value => Serializable () ('PayloadConst 1 (value :: Char)) where
  serializeField () = word8 (charByte @value)
  {-# INLINE serializeField #-}

instance KnownNat value => Serializable () ('PayloadConst 1 (value :: Nat)) where
  serializeField () = word8 (natConstant @value)
  {-# INLINE serializeField #-}

instance KnownNat value => Serializable () ('PayloadConst 2 (value :: Nat)) where
  serializeField () = word16 (natConstant @value)
  {-# INLINE serializeField #-}

instance KnownNat value => Serializable () ('PayloadConst 4 (value :: Nat)) where
  serializeField () = word32 (natConstant @value)
  {-# INLINE serializeField #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable a ('PayloadBounded payload) where
  serializeField = withLengthPrefix . serializeField @a @payload
  {-# INLINE serializeField #-}

instance Serializable a field => Serializable a ('PayloadField field) where
  serializeField = serializeField @a @field
  {-# INLINE serializeField #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable [a] ('PayloadZeroTerminated payload) where
  serializeField values = outputs (serializeField @a @payload) values <> word8 0
  {-# INLINE serializeField #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable [a] ('PayloadCounted16 payload) where
  serializeField values = int16 (countAsInt16 values) <> outputs (serializeField @a @payload) values
  {-# INLINE serializeField #-}

instance forall a (payload :: Payload). Serializable a payload => Serializable [a] ('PayloadCounted32 payload) where
  serializeField values = int32 (countAsInt32 values) <> outputs (serializeField @a @payload) values
  {-# INLINE serializeField #-}

instance {-# OVERLAPPABLE #-}
  ( HasPayload singleton payload,
    HasConstructor singleton a,
    SerializeConstructor payload (ConstructorOf singleton a Output) Output
  ) =>
  Serializable a singleton
  where
  serializeField value = destructorOf @_ @singleton @a value (serializeWith @payload id)
  {-# INLINE serializeField #-}

instance Serializable a 'CStringAtom => SerializeConstructor 'CStringAtom (a -> r) r where
  serializeWith continue = continue . serializeField @a @'CStringAtom
  {-# INLINE serializeWith #-}

instance Serializable a ('FixedBytesAtom n) => SerializeConstructor ('FixedBytesAtom n) (a -> r) r where
  serializeWith continue = continue . serializeField @a @('FixedBytesAtom n)
  {-# INLINE serializeWith #-}

instance Serializable a 'RemainingBytesAtom => SerializeConstructor 'RemainingBytesAtom (a -> r) r where
  serializeWith continue = continue . serializeField @a @'RemainingBytesAtom
  {-# INLINE serializeWith #-}

instance Serializable a 'MaybeBytes32Atom => SerializeConstructor 'MaybeBytes32Atom (a -> r) r where
  serializeWith continue = continue . serializeField @a @'MaybeBytes32Atom
  {-# INLINE serializeWith #-}

instance Serializable a field => SerializeConstructor ('PayloadField field) (a -> r) r where
  serializeWith continue = continue . serializeField @a @field
  {-# INLINE serializeWith #-}

instance Serializable [a] ('PayloadZeroTerminated payload) => SerializeConstructor ('PayloadZeroTerminated payload) ([a] -> r) r where
  serializeWith continue = continue . serializeField @[a] @('PayloadZeroTerminated payload)
  {-# INLINE serializeWith #-}

instance Serializable [a] ('PayloadCounted16 payload) => SerializeConstructor ('PayloadCounted16 payload) ([a] -> r) r where
  serializeWith continue = continue . serializeField @[a] @('PayloadCounted16 payload)
  {-# INLINE serializeWith #-}

instance Serializable [a] ('PayloadCounted32 payload) => SerializeConstructor ('PayloadCounted32 payload) ([a] -> r) r where
  serializeWith continue = continue . serializeField @[a] @('PayloadCounted32 payload)
  {-# INLINE serializeWith #-}

instance Serializable () ('PayloadConst bytes value) => SerializeConstructor ('PayloadConst bytes value) r r where
  serializeWith continue = continue (serializeField @() @('PayloadConst bytes value) ())
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

withLengthPrefix :: Output -> Output
withLengthPrefix inner = int32 (lengthAsInt32 (outputLength inner + 4)) <> inner
{-# INLINE withLengthPrefix #-}

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

countAsInt32 :: [a] -> Int32
countAsInt32 values
  | count <= fromIntegral (maxBound @Int32) = fromIntegral count
  | otherwise = error ("element count " <> show count <> " does not fit in an Int32")
  where
    count = length values
{-# INLINE countAsInt32 #-}

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

instance Serializable (Maybe Parameter) 'MaybeBytes32Atom where
  serializeField Nothing = int32 (-1)
  serializeField (Just (ParameterBytes value)) = int32 (parameterLengthAsInt32 (BS.length value)) <> bytes value
  serializeField (Just (ParameterChunks chunks)) = int32 (parameterLengthAsInt32 (sum (map BS.length chunks))) <> outputs bytes chunks
  {-# INLINE serializeField #-}

parameterLengthAsInt32 :: Int -> Int32
parameterLengthAsInt32 count
  | count <= fromIntegral (maxBound @Int32) = fromIntegral count
  | otherwise = error ("a parameter of " <> show count <> " bytes doesn't fit in an Int32 length")
