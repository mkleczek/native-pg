{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.PostgreSQL.Protocol.Messages.Types
  ( Scalar (..),
    ValueType
      ( OidType,
        ProcessIdType,
        SecretKeyType,
        ProtocolVersionType,
        StartupParameterType,
        FormatCodeType,
        CopyFormatType,
        NamedObjectType,
        ErrorNoticeFieldType,
        ParameterStatusValueType,
        TransactionStatusType
      ),
    StructuredValueType
      ( BackendKeyType,
        BindBodyType,
        CopyResponseType,
        NotificationType,
        RowFieldType
      ),
    Oid,
    ProcessId,
    SecretKey,
    BackendKey,
    ProtocolVersion (..),
    StartupParameter,
    FormatCode (..),
    CopyFormat (..),
    NamedObject (..),
    BindBody (..),
    CopyResponse,
    ErrorNoticeField,
    Notification (..),
    ParameterStatusValue,
    TransactionStatus (..),
    RowField (..),
    cancelRequestCode,
    gssEncRequestCode,
    protocolVersion3_2,
    protocolVersionCode,
    sslRequestCode,
  )
where

import Data.Bits ((.|.), shiftL)
import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)
import Data.Kind (Type)
import Data.Text (Text)
import Data.Word (Word16, Word32)
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Atom (..),
    DenotationOf,
    Payload (..),
    PayloadValue,
  )

type ScalarValue :: k -> Type
type family ScalarValue (t :: k) :: Type where
  ScalarValue (singleton :: sing (payload :: Payload)) = PayloadValue payload

type Scalar :: k -> Type
newtype Scalar (t :: k) = Scalar {unScalar :: ScalarValue t}

deriving instance Eq (ScalarValue t) => Eq (Scalar t)
deriving instance Ord (ScalarValue t) => Ord (Scalar t)
deriving instance Show (ScalarValue t) => Show (Scalar t)

type ValueType :: Payload -> Type
data ValueType payload where
  OidType :: ValueType (PayloadField 'Word32Atom)
  ProcessIdType :: ValueType (PayloadField 'Int32Atom)
  SecretKeyType :: ValueType (PayloadField ('FixedBytesAtom 4))
  ProtocolVersionType :: ValueType (PayloadField 'Word32Atom)
  StartupParameterType ::
    ValueType
      (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'CStringAtom])
  FormatCodeType :: ValueType (PayloadField 'Int16Atom)
  CopyFormatType :: ValueType (PayloadField 'ByteAtom)
  NamedObjectType :: ValueType (PayloadField 'ByteAtom)
  ErrorNoticeFieldType ::
    ValueType
      (PayloadStruct '[PayloadField 'ByteAtom, PayloadField 'CStringAtom])
  ParameterStatusValueType ::
    ValueType
      (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'CStringAtom])
  TransactionStatusType :: ValueType (PayloadField 'ByteAtom)

type StructuredValueType :: Payload -> Type
data StructuredValueType payload where
  BackendKeyType ::
    StructuredValueType
      (PayloadStruct '[PayloadField 'ProcessIdType, PayloadField 'SecretKeyType])
  BindBodyType ::
    StructuredValueType
      ( PayloadStruct
          '[ PayloadField 'CStringAtom,
             PayloadField 'CStringAtom,
             PayloadCounted16 ('PayloadField 'FormatCodeType),
             PayloadCounted16 ('PayloadField 'MaybeBytes32Atom),
             PayloadCounted16 ('PayloadField 'FormatCodeType)
           ]
      )
  CopyResponseType ::
    StructuredValueType
      (PayloadStruct '[PayloadField 'CopyFormatType, PayloadCounted16 ('PayloadField 'FormatCodeType)])
  NotificationType ::
    StructuredValueType
      (PayloadStruct '[PayloadField 'ProcessIdType, PayloadField 'CStringAtom, PayloadField 'CStringAtom])
  RowFieldType ::
    StructuredValueType
      ( PayloadStruct
          '[ PayloadField 'CStringAtom,
             PayloadField 'Word32Atom,
             PayloadField 'Int16Atom,
             PayloadField 'OidType,
             PayloadField 'Int16Atom,
             PayloadField 'Int32Atom,
             PayloadField 'FormatCodeType
           ]
      )

type Oid :: Type
type Oid = Scalar 'OidType

type instance DenotationOf ('OidType :: ValueType (PayloadField 'Word32Atom)) = Oid

type ProcessId :: Type
type ProcessId = Scalar 'ProcessIdType

type instance DenotationOf ('ProcessIdType :: ValueType (PayloadField 'Int32Atom)) = ProcessId

type SecretKey :: Type
type SecretKey = Scalar 'SecretKeyType

type instance DenotationOf ('SecretKeyType :: ValueType (PayloadField ('FixedBytesAtom 4))) = SecretKey

type BackendKey :: Type
type BackendKey = Scalar 'BackendKeyType

type instance DenotationOf ('BackendKeyType :: StructuredValueType (PayloadStruct '[PayloadField 'ProcessIdType, PayloadField 'SecretKeyType])) = BackendKey

data ProtocolVersion (t :: ValueType payload) = ProtocolVersion
  { protocolVersionMajor :: Word16,
    protocolVersionMinor :: Word16
  }

deriving instance Eq (ProtocolVersion valueType)
deriving instance Ord (ProtocolVersion valueType)
deriving instance Show (ProtocolVersion valueType)

type instance DenotationOf ('ProtocolVersionType :: ValueType (PayloadField 'Word32Atom)) = ProtocolVersion 'ProtocolVersionType

protocolVersionCode :: ProtocolVersion 'ProtocolVersionType -> Word32
protocolVersionCode version =
  shiftL (fromIntegral (protocolVersionMajor version)) 16
    .|. fromIntegral (protocolVersionMinor version)

protocolVersion3_2 :: ProtocolVersion 'ProtocolVersionType
protocolVersion3_2 = ProtocolVersion 3 2

cancelRequestCode :: Word32
cancelRequestCode = 80877102

sslRequestCode :: Word32
sslRequestCode = 80877103

gssEncRequestCode :: Word32
gssEncRequestCode = 80877104

type StartupParameter :: Type
type StartupParameter = Scalar 'StartupParameterType

type instance DenotationOf ('StartupParameterType :: ValueType (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'CStringAtom])) = StartupParameter

data FormatCode (t :: ValueType payload)
  = TextFormat
  | BinaryFormat

deriving instance Eq (FormatCode valueType)
deriving instance Ord (FormatCode valueType)
deriving instance Show (FormatCode valueType)

type instance DenotationOf ('FormatCodeType :: ValueType (PayloadField 'Int16Atom)) = FormatCode 'FormatCodeType

data CopyFormat (t :: ValueType payload)
  = CopyText
  | CopyBinary

deriving instance Eq (CopyFormat valueType)
deriving instance Ord (CopyFormat valueType)
deriving instance Show (CopyFormat valueType)

type instance DenotationOf ('CopyFormatType :: ValueType (PayloadField 'ByteAtom)) = CopyFormat 'CopyFormatType

data NamedObject (t :: ValueType payload)
  = PreparedStatement
  | Portal

deriving instance Eq (NamedObject valueType)
deriving instance Ord (NamedObject valueType)
deriving instance Show (NamedObject valueType)

type instance DenotationOf ('NamedObjectType :: ValueType (PayloadField 'ByteAtom)) = NamedObject 'NamedObjectType

data BindBody (t :: StructuredValueType payload) = BindBody
  { bindPortalName :: Text,
    bindStatementName :: Text,
    bindParameterFormats :: [FormatCode 'FormatCodeType],
    bindParameterValues :: [Maybe ByteString],
    bindResultFormats :: [FormatCode 'FormatCodeType]
  }

deriving instance Show (BindBody valueType)
deriving instance Eq (BindBody valueType)
deriving instance Ord (BindBody valueType)

type instance DenotationOf ('BindBodyType :: StructuredValueType (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'CStringAtom, PayloadCounted16 ('PayloadField 'FormatCodeType), PayloadCounted16 ('PayloadField 'MaybeBytes32Atom), PayloadCounted16 ('PayloadField 'FormatCodeType)])) = BindBody 'BindBodyType

type CopyResponse :: Type
type CopyResponse = Scalar 'CopyResponseType

type instance DenotationOf ('CopyResponseType :: StructuredValueType (PayloadStruct '[PayloadField 'CopyFormatType, PayloadCounted16 ('PayloadField 'FormatCodeType)])) = CopyResponse

type ErrorNoticeField :: Type
type ErrorNoticeField = Scalar 'ErrorNoticeFieldType

type instance DenotationOf ('ErrorNoticeFieldType :: ValueType (PayloadStruct '[PayloadField 'ByteAtom, PayloadField 'CStringAtom])) = ErrorNoticeField

data Notification (t :: StructuredValueType payload) = Notification
  { notificationProcessId :: ProcessId,
    notificationChannel :: Text,
    notificationPayload :: Text
  }

deriving instance Eq (Notification valueType)
deriving instance Ord (Notification valueType)
deriving instance Show (Notification valueType)

type instance DenotationOf ('NotificationType :: StructuredValueType (PayloadStruct '[PayloadField 'ProcessIdType, PayloadField 'CStringAtom, PayloadField 'CStringAtom])) = Notification 'NotificationType

type ParameterStatusValue :: Type
type ParameterStatusValue = Scalar 'ParameterStatusValueType

type instance DenotationOf ('ParameterStatusValueType :: ValueType (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'CStringAtom])) = ParameterStatusValue

data TransactionStatus (t :: ValueType payload)
  = TxIdle
  | TxInTransaction
  | TxFailedTransaction

deriving instance Eq (TransactionStatus valueType)
deriving instance Ord (TransactionStatus valueType)
deriving instance Show (TransactionStatus valueType)

type instance DenotationOf ('TransactionStatusType :: ValueType (PayloadField 'ByteAtom)) = TransactionStatus 'TransactionStatusType

data RowField (t :: StructuredValueType payload) = RowField
  { rowFieldName :: Text,
    rowFieldTableOid :: Maybe Oid,
    rowFieldColumnAttributeNumber :: Maybe Int16,
    rowFieldTypeOid :: Oid,
    rowFieldTypeSize :: Int16,
    rowFieldTypeModifier :: Int32,
    rowFieldFormat :: FormatCode 'FormatCodeType
  }

deriving instance Eq (RowField valueType)
deriving instance Ord (RowField valueType)
deriving instance Show (RowField valueType)

type instance DenotationOf ('RowFieldType :: StructuredValueType (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'Word32Atom, PayloadField 'Int16Atom, PayloadField 'OidType, PayloadField 'Int16Atom, PayloadField 'Int32Atom, PayloadField 'FormatCodeType])) = RowField 'RowFieldType
