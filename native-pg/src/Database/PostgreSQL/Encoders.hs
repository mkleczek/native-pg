{-# LANGUAGE RankNTypes #-}

-- | Encoders of parameter values, with hasql's names. Each encoder sends the
-- type OID and the format hasql's does, with bytes from the same
-- postgresql-binary encoding, so a parameter goes on the wire as hasql sends
-- it.
--
-- Bytes can also be sent as they are, which lets them stream.
module Database.PostgreSQL.Encoders
  ( -- * Values
    Value,
    NullableOrNot,
    nonNullable,
    nullable,
    param,

    -- * Encoders
    unknown,
    bytea,
    text,
    jsonLazyBytes,
    jsonbLazyBytes,
    foldableArray,

    -- * Bytes as they are
    Parameter (..),
    Raw,
    rawParam,
    rawJson,
    rawJsonb,
    rawBytea,
    rawUnknown,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LBS
import Data.Text (Text)
import Data.Word (Word32)
import Database.PostgreSQL.Types (FormatCode (..))
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import Database.PostgreSQL.Transaction (Param (..))
import qualified PostgreSQL.Binary.Encoding as Binary

-- | How a value of type @a@ is encoded: its type's OID, the OID of the
-- arrays of that type, its format, and its bytes.
data Value a = Value Word32 Word32 FormatCode (a -> Binary.Encoding)

-- | A value that can be NULL, or one that can't.
data NullableOrNot a = NullableOrNot Word32 Word32 FormatCode (a -> Maybe Binary.Encoding)

nonNullable :: Value a -> NullableOrNot a
nonNullable (Value oid arrayOid format encode) = NullableOrNot oid arrayOid format (Just . encode)

nullable :: Value a -> NullableOrNot (Maybe a)
nullable (Value oid arrayOid format encode) = NullableOrNot oid arrayOid format (fmap encode)

-- | The parameter for the value.
param :: NullableOrNot a -> a -> Param
param (NullableOrNot oid _ format encode) value =
  Param
    { paramType = Just oid,
      paramFormat = format,
      paramValue = ParameterBytes . Binary.encodingBytes <$> encode value
    }

-- | Bytes of PostgreSQL's @unknown@ type, which leaves inferring their type
-- to the server.
unknown :: Value ByteString
unknown = Value 705 705 TextFormat Binary.bytea_strict

bytea :: Value ByteString
bytea = Value 17 1001 BinaryFormat Binary.bytea_strict

text :: Value Text
text = Value 25 1009 BinaryFormat Binary.text_strict

jsonLazyBytes :: Value LBS.ByteString
jsonLazyBytes = Value 114 199 BinaryFormat Binary.json_bytes_lazy

jsonbLazyBytes :: Value LBS.ByteString
jsonbLazyBytes = Value 3802 3807 BinaryFormat Binary.jsonb_bytes_lazy

-- | A one-dimensional array of the elements.
foldableArray :: Foldable foldable => NullableOrNot element -> Value (foldable element)
foldableArray (NullableOrNot oid arrayOid _ encode) =
  Value arrayOid arrayOid BinaryFormat $
    Binary.array oid . Binary.dimensionArray foldl' (maybe Binary.nullArray Binary.encodingArray . encode)

-- | Bytes sent as a value of a type as they are, in the format in which they
-- are its encoding. Their parameter can stream.
data Raw = Raw Word32 FormatCode

-- | The parameter of the bytes, at hand or streamed, or NULL.
rawParam :: Raw -> Maybe Parameter -> Param
rawParam (Raw oid format) value =
  Param
    { paramType = Just oid,
      paramFormat = format,
      paramValue = value
    }

-- | JSON text, in json's binary format, which is the text.
rawJson :: Raw
rawJson = Raw 114 BinaryFormat

-- | JSON text, in jsonb's text format. Its binary format begins with a
-- version.
rawJsonb :: Raw
rawJsonb = Raw 3802 TextFormat

rawBytea :: Raw
rawBytea = Raw 17 BinaryFormat

-- | Bytes of PostgreSQL's @unknown@ type, as 'unknown' sends them.
rawUnknown :: Raw
rawUnknown = Raw 705 TextFormat
