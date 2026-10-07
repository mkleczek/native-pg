{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Decoding values from PostgreSQL's binary format, chosen by their Haskell
-- types. The decoders for single values come from postgresql-binary.
--
-- Fixed-size types require values of exactly their size, so a column of
-- another type is reported instead of misread: an @int4@ value doesn't
-- decode as 'Int64'.
module Database.PostgreSQL.Decoding
  ( DecodeField (..),
    DecodeValue (..),
    DecodingError (..),
    Json (..),
    Jsonb (..),
    composite,
    field,
  )
where

import Control.Monad (replicateM)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Int (Int16, Int32, Int64)
import Data.Scientific (Scientific)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Time (Day, DiffTime, LocalTime, TimeOfDay, UTCTime)
import Data.UUID (UUID)
import Database.PostgreSQL.Protocol.Codec (ByteStream)
import GHC.TypeLits (ErrorMessage (..), TypeError)
import qualified PostgreSQL.Binary.Decoding as Binary

-- | A value that isn't NULL, decoded from its bytes.
class DecodeValue a where
  valueDecoder :: Binary.Value a

-- | A value that may be NULL: a column, an array element or a field of a
-- composite. 'Maybe' accepts NULL; any other type rejects it.
class DecodeField a where
  decodeField :: Maybe ByteString -> Either Text a
  arrayElement :: Binary.Array a
  compositeField :: Binary.Composite a

instance {-# OVERLAPPABLE #-} DecodeValue a => DecodeField a where
  decodeField = maybe (Left "unexpected NULL") (Binary.valueParser valueDecoder)
  {-# INLINE decodeField #-}
  arrayElement = Binary.valueArray valueDecoder
  {-# INLINE arrayElement #-}
  compositeField = Binary.valueComposite valueDecoder
  {-# INLINE compositeField #-}

instance DecodeValue a => DecodeField (Maybe a) where
  decodeField = traverse (Binary.valueParser valueDecoder)
  {-# INLINE decodeField #-}
  arrayElement = Binary.nullableValueArray valueDecoder
  {-# INLINE arrayElement #-}
  compositeField = Binary.nullableValueComposite valueDecoder
  {-# INLINE compositeField #-}

data DecodingError
  = -- | The row has a different number of columns than were decoded.
    ColumnCountMismatch
      { expectedColumns :: Int,
        actualColumns :: Int
      }
  | -- | A column's value doesn't decode. Columns count from 1.
    ColumnDecodingFailed Int Text
  deriving (Eq, Show)

-- | A @json@ value's text, as a slice of the read buffer.
newtype Json = Json ByteString
  deriving (Eq, Show)

-- | A @jsonb@ value's text, as a slice of the read buffer.
newtype Jsonb = Jsonb ByteString
  deriving (Eq, Show)

-- | A composite value, from its fields in order, e.g.
-- @composite (Pair \<$\> field \<*\> field)@.
composite :: Binary.Composite a -> Binary.Value a
composite = Binary.composite
{-# INLINE composite #-}

-- | A composite's next field, decoded by its type.
field :: DecodeField a => Binary.Composite a
field = compositeField
{-# INLINE field #-}

instance DecodeValue Bool where
  valueDecoder = exactly 1 "bool" Binary.bool
  {-# INLINE valueDecoder #-}

instance DecodeValue Int16 where
  valueDecoder = exactly 2 "int2" Binary.int
  {-# INLINE valueDecoder #-}

instance DecodeValue Int32 where
  valueDecoder = exactly 4 "int4" Binary.int
  {-# INLINE valueDecoder #-}

instance DecodeValue Int64 where
  valueDecoder = exactly 8 "int8" Binary.int
  {-# INLINE valueDecoder #-}

instance DecodeValue Float where
  valueDecoder = exactly 4 "float4" Binary.float4
  {-# INLINE valueDecoder #-}

instance DecodeValue Double where
  valueDecoder = exactly 8 "float8" Binary.float8
  {-# INLINE valueDecoder #-}

instance DecodeValue Scientific where
  valueDecoder = Binary.numeric
  {-# INLINE valueDecoder #-}

-- | A single character, e.g. of type @char@ or @"char"@.
instance DecodeValue Char where
  valueDecoder = Binary.char
  {-# INLINE valueDecoder #-}

-- | Any text type: @text@, @varchar@, @char(n)@ or @name@.
instance DecodeValue Text where
  valueDecoder = Binary.text_strict
  {-# INLINE valueDecoder #-}

-- | A @bytea@ value, as a slice of the read buffer.
instance DecodeValue ByteString where
  valueDecoder = Binary.bytea_strict
  {-# INLINE valueDecoder #-}

instance DecodeValue Json where
  valueDecoder = Binary.json_bytes (Right . Json)
  {-# INLINE valueDecoder #-}

-- | Without the format version that starts a @jsonb@ value's bytes.
instance DecodeValue Jsonb where
  valueDecoder = Binary.jsonb_bytes (Right . Jsonb)
  {-# INLINE valueDecoder #-}

instance DecodeValue UUID where
  valueDecoder = exactly 16 "uuid" Binary.uuid
  {-# INLINE valueDecoder #-}

instance DecodeValue Day where
  valueDecoder = exactly 4 "date" Binary.date
  {-# INLINE valueDecoder #-}

instance DecodeValue TimeOfDay where
  valueDecoder = exactly 8 "time" Binary.time_int
  {-# INLINE valueDecoder #-}

instance DecodeValue LocalTime where
  valueDecoder = exactly 8 "timestamp" Binary.timestamp_int
  {-# INLINE valueDecoder #-}

instance DecodeValue UTCTime where
  valueDecoder = exactly 8 "timestamptz" Binary.timestamptz_int
  {-# INLINE valueDecoder #-}

instance DecodeValue DiffTime where
  valueDecoder = exactly 16 "interval" Binary.interval_int
  {-# INLINE valueDecoder #-}

-- | A one-dimensional array. Elements of type 'Maybe' may be NULL.
instance DecodeField a => DecodeValue [a] where
  valueDecoder = Binary.array (Binary.dimensionArray replicateM arrayElement)
  {-# INLINE valueDecoder #-}

instance
  TypeError ('Text "A ByteStream column isn't decoded: stream the row's last column with decodeRowStreaming") =>
  DecodeValue ByteStream
  where
  valueDecoder = error "unreachable"

-- | Requires the value to have exactly the given size.
exactly :: Int -> Text -> Binary.Value a -> Binary.Value a
exactly size name decoder =
  Binary.fn $ \bytes ->
    if BS.length bytes == size
      then Binary.valueParser decoder bytes
      else
        Left $
          name
            <> " needs "
            <> Text.pack (show size)
            <> " bytes, got "
            <> Text.pack (show (BS.length bytes))
{-# INLINE exactly #-}
