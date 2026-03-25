{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Protocol.Messages.Types
  ( Envelope (..),
    SomeEnvelope,
    Scalar (..),
    OidType (OidType),
    ProcessIdType (ProcessIdType),
    SecretKeyType (SecretKeyType),
    BackendKeyType (BackendKeyType),
    ProtocolVersionType (ProtocolVersionType),
    StartupParameterType (StartupParameterType),
    FormatCodeType (FormatCodeType),
    CopyFormatType (CopyFormatType),
    NamedObjectType (NamedObjectType),
    BindBodyType (BindBodyType),
    CopyResponseType (CopyResponseType),
    ErrorNoticeFieldType (ErrorNoticeFieldType),
    NotificationType (NotificationType),
    ParameterStatusValueType (ParameterStatusValueType),
    TransactionStatusType (TransactionStatusType),
    RowFieldType (RowFieldType),
    OidEnvelope,
    ProcessIdEnvelope,
    SecretKeyEnvelope,
    BackendKeyEnvelope,
    ProtocolVersionEnvelope,
    StartupParameterEnvelope,
    FormatCodeEnvelope,
    CopyFormatEnvelope,
    NamedObjectEnvelope,
    BindBodyEnvelope,
    CopyResponseEnvelope,
    ErrorNoticeFieldEnvelope,
    NotificationEnvelope,
    ParameterStatusValueEnvelope,
    TransactionStatusEnvelope,
    RowFieldEnvelope,
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
    toSomeEnvelope,
    withSomeEnvelope,
  )
where

import Data.Bits ((.|.), shiftL, shiftR)
import Data.Int (Int16, Int32)
import Data.Kind (Type)
import Data.Word (Word16, Word32, Word8)
import Database.PostgreSQL.Protocol.Codec
  ( HasConstructor (..) )
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Payload (..),
  )
import GHC.Exts (Any)
import Unsafe.Coerce (unsafeCoerce)

newtype Envelope a (s :: k) = Envelope {unEnvelope :: a}

deriving instance Eq a => Eq (Envelope a s)
deriving instance Ord a => Ord (Envelope a s)
deriving instance Show a => Show (Envelope a s)

newtype SomeEnvelope k = SomeEnvelope Any

toSomeEnvelope :: Envelope a s -> SomeEnvelope k
toSomeEnvelope = SomeEnvelope . unsafeCoerce

withSomeEnvelope :: SomeEnvelope f -> (forall k (a :: Type) (s :: k). Envelope a s -> r) -> r
withSomeEnvelope (SomeEnvelope x) k = k (unsafeCoerce x)

newtype Scalar a = Scalar {unScalar :: a}

deriving instance Eq a => Eq (Scalar a)
deriving instance Ord a => Ord (Scalar a)
deriving instance Show a => Show (Scalar a)

data OidType (payload :: Payload) where
  OidType :: OidType ('FixedBytesAtom 4)

data ProcessIdType (payload :: Payload) where
  ProcessIdType :: ProcessIdType ('FixedBytesAtom 4)

data SecretKeyType (payload :: Payload) where
  SecretKeyType :: SecretKeyType ('FixedBytesAtom 4)

data BackendKeyType (payload :: Payload) where
  BackendKeyType ::
    BackendKeyType
      (PayloadStruct '[PayloadField 'ProcessIdType, PayloadField 'SecretKeyType])

data ProtocolVersionType (payload :: Payload) where
  ProtocolVersionType :: ProtocolVersionType ('FixedBytesAtom 4)

data StartupParameterType (payload :: Payload) where
  StartupParameterType ::
    StartupParameterType
      (PayloadStruct '[ 'CStringAtom, 'CStringAtom])

data FormatCodeType (payload :: Payload) where
  FormatCodeType :: FormatCodeType ('FixedBytesAtom 2)

data CopyFormatType (payload :: Payload) where
  CopyFormatType :: CopyFormatType ('FixedBytesAtom 1)

data NamedObjectType (payload :: Payload) where
  NamedObjectType :: NamedObjectType ('FixedBytesAtom 1)

data BindBodyType (payload :: Payload) where
  BindBodyType ::
    BindBodyType
      ( PayloadStruct
          '[ 'CStringAtom,
             'CStringAtom,
             PayloadCounted16 ('PayloadField 'FormatCodeType),
             PayloadCounted16 'MaybeBytes32Atom,
             PayloadCounted16 ('PayloadField 'FormatCodeType)
           ]
      )

data CopyResponseType (payload :: Payload) where
  CopyResponseType ::
    CopyResponseType
      (PayloadStruct '[PayloadField 'CopyFormatType, PayloadCounted16 ('PayloadField 'FormatCodeType)])

data ErrorNoticeFieldType (payload :: Payload) where
  ErrorNoticeFieldType ::
    ErrorNoticeFieldType
      (PayloadStruct '[ 'FixedBytesAtom 1, 'CStringAtom])

data NotificationType (payload :: Payload) where
  NotificationType ::
    NotificationType
      (PayloadStruct '[PayloadField 'ProcessIdType, 'CStringAtom, 'CStringAtom])

data ParameterStatusValueType (payload :: Payload) where
  ParameterStatusValueType ::
    ParameterStatusValueType
      (PayloadStruct '[ 'CStringAtom, 'CStringAtom])

data TransactionStatusType (payload :: Payload) where
  TransactionStatusType :: TransactionStatusType ('FixedBytesAtom 1)

data RowFieldType (payload :: Payload) where
  RowFieldType ::
    RowFieldType
      ( PayloadStruct
          '[ 'CStringAtom,
             'FixedBytesAtom 4,
             'FixedBytesAtom 2,
             PayloadField 'OidType,
             'FixedBytesAtom 2,
             'FixedBytesAtom 4,
             PayloadField 'FormatCodeType
           ]
      )

type Oid = Scalar Word32

type OidEnvelope =
  Envelope
    Oid
    'OidType

type ProcessId = Scalar Int32

type ProcessIdEnvelope =
  Envelope
    ProcessId
    'ProcessIdType

type SecretKey bytes = Scalar bytes

type SecretKeyEnvelope bytes =
  Envelope
    (SecretKey bytes)
    'SecretKeyType

type BackendKey bytes = Scalar (ProcessId, SecretKey bytes)

type BackendKeyEnvelope bytes =
  Envelope
    (BackendKey bytes)
    'BackendKeyType

instance HasConstructor 'ProcessIdType ProcessId where
  type ConstructorOf 'ProcessIdType ProcessId = Int32 -> ProcessId
  constructorOf = Scalar

instance HasConstructor 'SecretKeyType (SecretKey bytes) where
  type ConstructorOf 'SecretKeyType (SecretKey bytes) = bytes -> SecretKey bytes
  constructorOf = Scalar

instance HasConstructor 'BackendKeyType (BackendKey bytes) where
  type ConstructorOf 'BackendKeyType (BackendKey bytes) = ProcessId -> SecretKey bytes -> BackendKey bytes
  constructorOf processId = Scalar . (processId,)

instance HasConstructor 'ProtocolVersionType ProtocolVersion where
  type ConstructorOf 'ProtocolVersionType ProtocolVersion = Word32 -> ProtocolVersion
  constructorOf code =
    ProtocolVersion
      (fromIntegral (shiftR code 16))
      (fromIntegral code)

instance HasConstructor 'StartupParameterType (StartupParameter bytes) where
  type ConstructorOf 'StartupParameterType (StartupParameter bytes) = bytes -> bytes -> StartupParameter bytes
  constructorOf name value = Scalar (name, value)

data ProtocolVersion = ProtocolVersion
  { protocolVersionMajor :: Word16,
    protocolVersionMinor :: Word16
  }

deriving instance Eq ProtocolVersion
deriving instance Ord ProtocolVersion
deriving instance Show ProtocolVersion

type ProtocolVersionEnvelope =
  Envelope
    ProtocolVersion
    'ProtocolVersionType

protocolVersionCode :: ProtocolVersion -> Word32
protocolVersionCode version =
  shiftL (fromIntegral (protocolVersionMajor version)) 16
    .|. fromIntegral (protocolVersionMinor version)

protocolVersion3_2 :: ProtocolVersion
protocolVersion3_2 = ProtocolVersion 3 2

cancelRequestCode :: Word32
cancelRequestCode = 80877102

sslRequestCode :: Word32
sslRequestCode = 80877103

gssEncRequestCode :: Word32
gssEncRequestCode = 80877104

type StartupParameter bytes = Scalar (bytes, bytes)

type StartupParameterEnvelope bytes =
  Envelope
    (StartupParameter bytes)
    'StartupParameterType

data FormatCode
  = TextFormat
  | BinaryFormat

deriving instance Eq FormatCode
deriving instance Ord FormatCode
deriving instance Show FormatCode

type FormatCodeEnvelope =
  Envelope
    FormatCode
    'FormatCodeType

data CopyFormat
  = CopyText
  | CopyBinary

deriving instance Eq CopyFormat
deriving instance Ord CopyFormat
deriving instance Show CopyFormat

type CopyFormatEnvelope =
  Envelope
    CopyFormat
    'CopyFormatType

data NamedObject
  = PreparedStatement
  | Portal

deriving instance Eq NamedObject
deriving instance Ord NamedObject
deriving instance Show NamedObject

type NamedObjectEnvelope =
  Envelope
    NamedObject
    'NamedObjectType

data BindBody bytes = BindBody
  { bindPortalName :: bytes,
    bindStatementName :: bytes,
    bindParameterFormats :: [FormatCode],
    bindParameterValues :: [Maybe bytes],
    bindResultFormats :: [FormatCode]
  }

deriving instance Show bytes => Show (BindBody bytes)
deriving instance Eq bytes => Eq (BindBody bytes)
deriving instance Ord bytes => Ord (BindBody bytes)

type BindBodyEnvelope bytes =
  Envelope
    (BindBody bytes)
    'BindBodyType

type CopyResponse = Scalar (CopyFormat, [FormatCode])

type CopyResponseEnvelope =
  Envelope
    CopyResponse
    'CopyResponseType

type ErrorNoticeField bytes = Scalar (Word8, bytes)

type ErrorNoticeFieldEnvelope bytes =
  Envelope
    (ErrorNoticeField bytes)
    'ErrorNoticeFieldType

data Notification bytes = Notification
  { notificationProcessId :: ProcessId,
    notificationChannel :: bytes,
    notificationPayload :: bytes
  }

deriving instance Eq bytes => Eq (Notification bytes)
deriving instance Ord bytes => Ord (Notification bytes)
deriving instance Show bytes => Show (Notification bytes)

type NotificationEnvelope bytes =
  Envelope
    (Notification bytes)
    'NotificationType

type ParameterStatusValue bytes = Scalar (bytes, bytes)

type ParameterStatusValueEnvelope bytes =
  Envelope
    (ParameterStatusValue bytes)
    'ParameterStatusValueType

data TransactionStatus
  = TxIdle
  | TxInTransaction
  | TxFailedTransaction

deriving instance Eq TransactionStatus
deriving instance Ord TransactionStatus
deriving instance Show TransactionStatus

type TransactionStatusEnvelope =
  Envelope
    TransactionStatus
    'TransactionStatusType

data RowField bytes = RowField
  { rowFieldName :: bytes,
    rowFieldTableOid :: Maybe Oid,
    rowFieldColumnAttributeNumber :: Maybe Int16,
    rowFieldTypeOid :: Oid,
    rowFieldTypeSize :: Int16,
    rowFieldTypeModifier :: Int32,
    rowFieldFormat :: FormatCode
  }

deriving instance Eq bytes => Eq (RowField bytes)
deriving instance Ord bytes => Ord (RowField bytes)
deriving instance Show bytes => Show (RowField bytes)

type RowFieldEnvelope bytes =
  Envelope
    (RowField bytes)
    'RowFieldType
