{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.PostgreSQL.Protocol.Codec
  ( InputSource (..),
    MonadInput (..),
    HasPayload,
    HasConstructor (..),
    Parsable (..),
  )
where

import Control.Monad (replicateM, unless)
import Control.Monad.Except (MonadError (..))
import Data.Char (ord)
import Data.Bits (Bits, (.|.), shiftL)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Functor (($>))
import Data.Int (Int16, Int32)
import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import Foreign.Storable (Storable, sizeOf)
import Data.Word (Word8, Word32)
import Database.PostgreSQL.Protocol.Codec.Input
  ( InputSource (..),
    MonadInput (..)
  )
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Empty,
    Payload (..)
  )
import GHC.TypeLits (KnownChar, KnownNat, charVal, natVal)

class HasPayload (singleton :: k) (payload :: Payload) | singleton -> payload where

class HasConstructor (singleton :: k) (semantic :: Type) where
  type ConstructorOf singleton semantic :: Type
  constructorOf :: ConstructorOf singleton semantic

class Parsable (a :: Type) s where
  parser :: forall m. MonadInput m => m a

class ParseConstructor (payload :: Payload) f a | payload f -> a where
  parseWithConstructor :: forall m. MonadInput m => f -> m a

instance HasPayload (singleton :: s (payload :: Payload)) payload where

instance HasPayload singleton payload => HasPayload (wrapper singleton) payload where

-- We keep these instances explicit instead of using a generic `Storable` one,
-- because that would also admit platform-sized numerics like `Int` and `Word`.
instance KnownNat n => Parsable Word8 (FixedBytesAtom n) where
  parser = parseShiftedIntegral @n @Word8

instance KnownNat n => Parsable Int16 (FixedBytesAtom n) where
  parser = parseShiftedIntegral @n @Int16

instance KnownNat n => Parsable Int32 (FixedBytesAtom n) where
  parser = parseShiftedIntegral @n @Int32

instance KnownNat n => Parsable Word32 (FixedBytesAtom n) where
  parser = parseShiftedIntegral @n @Word32

instance Parsable ByteString CStringAtom where
  parser = readInputCStringBytes

instance KnownNat n => Parsable ByteString (FixedBytesAtom n) where
  parser = readInputExactly (natAsInt @n)

instance Parsable ByteString RemainingBytesAtom where
  parser = readInputRemaining

instance Parsable (Maybe ByteString) MaybeBytes32Atom where
  parser = do
    lengthOrMissing <- parser @Int32 @('FixedBytesAtom 4)
    unless
      (lengthOrMissing >= (-1))
      (throwError ("negative length " <> show lengthOrMissing <> " is invalid for MaybeBytes32Atom"))
    if lengthOrMissing == -1
      then pure Nothing
      else Just <$> readInputExactly (fromIntegral lengthOrMissing)

instance KnownChar value => Parsable () (PayloadConst 1 value) where
  parser = do
    actual <- parser @Word8 @(FixedBytesAtom 1)
    let expected = ord (charVal (Proxy @value))
    if expected <= fromIntegral (maxBound :: Word8)
      then
        unless
          (actual == fromIntegral expected)
          (throwError ("unexpected constant " <> show actual <> ", expected " <> show expected))
      else
        error $
          "payload constant "
            <> show (charVal (Proxy @value))
            <> " does not fit in 1 byte"

instance KnownNat value => Parsable () (PayloadConst 4 value) where
  parser = do
    let expected = natVal (Proxy @value)
    if expected <= toInteger (maxBound :: Word32)
      then do
        actual <- parseShiftedIntegral @4 @Word32
        unless
          (actual == fromInteger expected)
          (throwError ("unexpected constant " <> show actual <> ", expected " <> show expected))
      else
        error $
          "payload constant "
            <> show expected
            <> " does not fit in 4 bytes"

instance forall a (payload :: Payload). Parsable a payload => Parsable a ('PayloadBounded payload) where
  parser = do
    length' <- parser @Int32 @('FixedBytesAtom 4)
    unless
      (length' >= 0)
      (throwError ("negative length " <> show length' <> " is invalid for PayloadBounded"))
    withInputLimit (fromIntegral length') (parser @a @payload)

instance Parsable a CStringAtom => ParseConstructor CStringAtom (a -> result) result where
  parseWithConstructor constructor = constructor <$> parser @a @CStringAtom

instance (KnownNat n, Parsable a (FixedBytesAtom n)) => ParseConstructor (FixedBytesAtom n) (a -> result) result where
  parseWithConstructor constructor = constructor <$> parser @a @(FixedBytesAtom n)

instance Parsable a RemainingBytesAtom => ParseConstructor RemainingBytesAtom (a -> result) result where
  parseWithConstructor constructor = constructor <$> parser @a @RemainingBytesAtom

instance Parsable a MaybeBytes32Atom => ParseConstructor MaybeBytes32Atom (a -> result) result where
  parseWithConstructor constructor = constructor <$> parser @a @MaybeBytes32Atom

instance Parsable () (PayloadConst bytes value) => ParseConstructor (PayloadConst bytes value) result result where
  parseWithConstructor constructor = parser @() @(PayloadConst bytes value) $> constructor

instance ParseConstructor payload f a => ParseConstructor ('PayloadBounded payload) f a where
  parseWithConstructor constructor = do
    length' <- parser @Int32 @('FixedBytesAtom 4)
    unless
      (length' >= 0)
      (throwError ("negative length " <> show length' <> " is invalid for PayloadBounded"))
    withInputLimit (fromIntegral length') (parseWithConstructor @payload constructor)

instance Parsable a field => ParseConstructor ('PayloadField field) (a -> result) result where
  parseWithConstructor constructor = constructor <$> parser @a @field

instance Parsable [a] ('PayloadZeroTerminated payload) => ParseConstructor ('PayloadZeroTerminated payload) ([a] -> result) result where
  parseWithConstructor constructor = constructor <$> parser @[a] @('PayloadZeroTerminated payload)

instance Parsable [a] ('PayloadCounted16 payload) => ParseConstructor ('PayloadCounted16 payload) ([a] -> result) result where
  parseWithConstructor constructor = constructor <$> parser @[a] @('PayloadCounted16 payload)

instance ParseConstructor Empty result result where
  parseWithConstructor = pure

instance
  ( ParseConstructor field result0 result1,
    ParseConstructor ('PayloadStruct fields) result1 result2
  ) =>
  ParseConstructor ('PayloadStruct (field ': fields)) result0 result2
  where
  parseWithConstructor constructor =
    parseWithConstructor @field constructor >>= parseWithConstructor @('PayloadStruct fields)

instance Parsable a field => Parsable a (PayloadField field) where
  parser = parser @a @field

instance forall a (payload :: Payload). Parsable a payload => Parsable [a] ('PayloadZeroTerminated payload) where
  parser = go []
    where
      go acc = do
        end <- (== 0) <$> peekInputByte
        if end
          then readInputExactly 1 $> reverse acc
          else go . (: acc) =<< parser @a @payload

instance forall a (payload :: Payload). Parsable a payload => Parsable [a] (PayloadCounted16 payload) where
  parser = do
    count <- parser @Int16 @('FixedBytesAtom 2)
    unless
      (count >= 0)
      (throwError ("negative element count " <> show count <> " is invalid for PayloadCounted16"))
    replicateM (fromIntegral count) (parser @a @payload)

instance {-# OVERLAPPABLE #-}
  ( HasPayload singleton payload,
    HasConstructor singleton a,
    ParseConstructor payload (ConstructorOf singleton a) a
  ) =>
  Parsable a singleton
  where
  parser = parseWithConstructor @payload (constructorOf @_ @singleton @a)

natAsInt :: forall n. KnownNat n => Int
natAsInt = fromInteger (natVal (Proxy @n))

parseShiftedIntegral :: forall n a m. (KnownNat n, Storable a, Bits a, Integral a, MonadInput m) => m a
parseShiftedIntegral =
  if sizeOf (undefined :: a) == natAsInt @n
    then
      BS.foldl' (\value byte -> shiftL value 8 .|. fromIntegral byte) 0
        <$> readInputExactly (natAsInt @n)
    else
      error $
        "fixed-width parser size mismatch: expected "
          <> show (natAsInt @n)
          <> " bytes, actual "
          <> show (sizeOf (undefined :: a))
