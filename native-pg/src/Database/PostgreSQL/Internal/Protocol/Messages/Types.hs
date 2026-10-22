{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.Types
  ( Envelope (..),
    SomeEnvelope,
    Scalar (..),
    OidType (OidType),
    OptionalOidType (OptionalOidType),
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
    OptionalOidEnvelope,
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
    OptionalOid,
    ProcessId,
    SecretKey,
    BackendKey,
    ProtocolVersion (..),
    StartupParameter,
    FormatCode (..),
    CopyFormat (..),
    NamedObject (..),
    BindBody (..),
    Parameter (..),
    CopyResponse,
    ErrorNoticeField,
    Notification (..),
    ParameterStatusValue,
    TransactionStatus (..),
    RowField (..),
    cancelRequestCode,
    gssEncRequestCode,
    protocolVersion3_0,
    protocolVersion3_2,
    protocolVersionCode,
    sslRequestCode,
    toSomeEnvelope,
    withSomeEnvelope,
  )
where

import Data.Bits ((.|.), shiftL, shiftR)
import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)
import Data.Kind (Type)
import Data.Maybe (fromMaybe)
import Data.Word (Word32, Word8)
import Database.PostgreSQL.Types
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import Database.PostgreSQL.Internal.Protocol.Codec
  ( Alternatives (..),
    HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
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

-- | An OID where zero means none.
data OptionalOidType (payload :: Payload) where
  OptionalOidType :: OptionalOidType ('FixedBytesAtom 4)

data ProcessIdType (payload :: Payload) where
  ProcessIdType :: ProcessIdType ('FixedBytesAtom 4)

-- | Four bytes in protocol 3.0 and up to 256 in 3.2. It always ends its
-- message, so its length is whatever the message has left.
data SecretKeyType (payload :: Payload) where
  SecretKeyType :: SecretKeyType 'RemainingBytesAtom

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
  FormatCodeType ::
    FormatCodeType
      ('PayloadChoice '[ 'PayloadConst 2 0, 'PayloadConst 2 1])

data CopyFormatType (payload :: Payload) where
  CopyFormatType ::
    CopyFormatType
      ('PayloadChoice '[ 'PayloadConst 1 0, 'PayloadConst 1 1])

data NamedObjectType (payload :: Payload) where
  NamedObjectType ::
    NamedObjectType
      ('PayloadChoice '[ 'PayloadConst 1 'S', 'PayloadConst 1 'P'])

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
  TransactionStatusType ::
    TransactionStatusType
      ('PayloadChoice '[ 'PayloadConst 1 'I', 'PayloadConst 1 'T', 'PayloadConst 1 'E'])

data RowFieldType (payload :: Payload) where
  RowFieldType ::
    RowFieldType
      ( PayloadStruct
          '[ 'CStringAtom,
             PayloadField 'OptionalOidType,
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

instance HasConstructor 'OidType Oid where
  type ConstructorOf 'OidType Oid r = Word32 -> r
  constructorOf = Scalar
  destructorOf (Scalar oid) k = k oid

type OptionalOid = Maybe Oid

type OptionalOidEnvelope =
  Envelope
    OptionalOid
    'OptionalOidType

instance HasConstructor 'OptionalOidType OptionalOid where
  type ConstructorOf 'OptionalOidType OptionalOid r = Word32 -> r
  constructorOf 0 = Nothing
  constructorOf oid = Just (Scalar oid)
  destructorOf oid k = k (maybe 0 unScalar oid)

type ProcessId = Scalar Int32

type ProcessIdEnvelope =
  Envelope
    ProcessId
    'ProcessIdType

instance HasConstructor 'ProcessIdType ProcessId where
  type ConstructorOf 'ProcessIdType ProcessId r = Int32 -> r
  constructorOf = Scalar
  destructorOf (Scalar processId) k = k processId

type SecretKey bytes = Scalar bytes

type SecretKeyEnvelope bytes =
  Envelope
    (SecretKey bytes)
    'SecretKeyType

instance HasConstructor 'SecretKeyType (SecretKey bytes) where
  type ConstructorOf 'SecretKeyType (SecretKey bytes) r = bytes -> r
  constructorOf = Scalar
  destructorOf (Scalar secretKey) k = k secretKey

type BackendKey bytes = Scalar (ProcessId, SecretKey bytes)

type BackendKeyEnvelope bytes =
  Envelope
    (BackendKey bytes)
    'BackendKeyType

instance HasConstructor 'BackendKeyType (BackendKey bytes) where
  type ConstructorOf 'BackendKeyType (BackendKey bytes) r = ProcessId -> SecretKey bytes -> r
  constructorOf processId = Scalar . (processId,)
  destructorOf (Scalar (processId, secretKey)) k = k processId secretKey

type ProtocolVersionEnvelope =
  Envelope
    ProtocolVersion
    'ProtocolVersionType

instance HasConstructor 'ProtocolVersionType ProtocolVersion where
  type ConstructorOf 'ProtocolVersionType ProtocolVersion r = Word32 -> r
  constructorOf code =
    ProtocolVersion
      (fromIntegral (shiftR code 16))
      (fromIntegral code)
  destructorOf version k = k (protocolVersionCode version)

protocolVersionCode :: ProtocolVersion -> Word32
protocolVersionCode version =
  shiftL (fromIntegral (protocolVersionMajor version)) 16
    .|. fromIntegral (protocolVersionMinor version)

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

instance HasConstructor 'StartupParameterType (StartupParameter bytes) where
  type ConstructorOf 'StartupParameterType (StartupParameter bytes) r = bytes -> bytes -> r
  constructorOf name value = Scalar (name, value)
  destructorOf (Scalar (name, value)) k = k name value

type FormatCodeEnvelope =
  Envelope
    FormatCode
    'FormatCodeType

instance HasConstructor 'FormatCodeType FormatCode where
  type ConstructorOf 'FormatCodeType FormatCode r = Alternatives '[r, r] r
  constructorOf = TextFormat :|: BinaryFormat :|: NoAlternatives
  destructorOf format (text :|: binary :|: NoAlternatives) =
    case format of
      TextFormat -> text
      BinaryFormat -> binary

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

instance HasConstructor 'CopyFormatType CopyFormat where
  type ConstructorOf 'CopyFormatType CopyFormat r = Alternatives '[r, r] r
  constructorOf = CopyText :|: CopyBinary :|: NoAlternatives
  destructorOf format (text :|: binary :|: NoAlternatives) =
    case format of
      CopyText -> text
      CopyBinary -> binary

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

instance HasConstructor 'NamedObjectType NamedObject where
  type ConstructorOf 'NamedObjectType NamedObject r = Alternatives '[r, r] r
  constructorOf = PreparedStatement :|: Portal :|: NoAlternatives
  destructorOf object (preparedStatement :|: portal :|: NoAlternatives) =
    case object of
      PreparedStatement -> preparedStatement
      Portal -> portal

-- | Parameter values are 'ByteString's when parsed, and 'Parameter's, which
-- can be streams, when sent.
data BindBody value = BindBody
  { bindPortalName :: ByteString,
    bindStatementName :: ByteString,
    bindParameterFormats :: [FormatCode],
    bindParameterValues :: [Maybe value],
    bindResultFormats :: [FormatCode]
  }

deriving instance Show value => Show (BindBody value)
deriving instance Eq value => Eq (BindBody value)
deriving instance Ord value => Ord (BindBody value)

type BindBodyEnvelope value =
  Envelope
    (BindBody value)
    'BindBodyType

instance HasConstructor 'BindBodyType (BindBody value) where
  type
    ConstructorOf 'BindBodyType (BindBody value) r =
      ByteString -> ByteString -> [FormatCode] -> [Maybe value] -> [FormatCode] -> r
  constructorOf = BindBody
  destructorOf (BindBody portalName statementName parameterFormats parameterValues resultFormats) k =
    k portalName statementName parameterFormats parameterValues resultFormats

type CopyResponse = Scalar (CopyFormat, [FormatCode])

type CopyResponseEnvelope =
  Envelope
    CopyResponse
    'CopyResponseType

instance HasConstructor 'CopyResponseType CopyResponse where
  type ConstructorOf 'CopyResponseType CopyResponse r = CopyFormat -> [FormatCode] -> r
  constructorOf format columnFormats = Scalar (format, columnFormats)
  destructorOf (Scalar (format, columnFormats)) k = k format columnFormats

type ErrorNoticeField bytes = Scalar (Word8, bytes)

type ErrorNoticeFieldEnvelope bytes =
  Envelope
    (ErrorNoticeField bytes)
    'ErrorNoticeFieldType

instance HasConstructor 'ErrorNoticeFieldType (ErrorNoticeField bytes) where
  type ConstructorOf 'ErrorNoticeFieldType (ErrorNoticeField bytes) r = Word8 -> bytes -> r
  constructorOf code value = Scalar (code, value)
  destructorOf (Scalar (code, value)) k = k code value

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

instance HasConstructor 'NotificationType (Notification bytes) where
  type ConstructorOf 'NotificationType (Notification bytes) r = ProcessId -> bytes -> bytes -> r
  constructorOf = Notification
  destructorOf (Notification processId channel payload) k = k processId channel payload

type ParameterStatusValue bytes = Scalar (bytes, bytes)

type ParameterStatusValueEnvelope bytes =
  Envelope
    (ParameterStatusValue bytes)
    'ParameterStatusValueType

instance HasConstructor 'ParameterStatusValueType (ParameterStatusValue bytes) where
  type ConstructorOf 'ParameterStatusValueType (ParameterStatusValue bytes) r = bytes -> bytes -> r
  constructorOf name value = Scalar (name, value)
  destructorOf (Scalar (name, value)) k = k name value

type TransactionStatusEnvelope =
  Envelope
    TransactionStatus
    'TransactionStatusType

instance HasConstructor 'TransactionStatusType TransactionStatus where
  type ConstructorOf 'TransactionStatusType TransactionStatus r = Alternatives '[r, r, r] r
  constructorOf = TxIdle :|: TxInTransaction :|: TxFailedTransaction :|: NoAlternatives
  destructorOf status (idle :|: inTransaction :|: failedTransaction :|: NoAlternatives) =
    case status of
      TxIdle -> idle
      TxInTransaction -> inTransaction
      TxFailedTransaction -> failedTransaction

data RowField bytes = RowField
  { rowFieldName :: bytes,
    rowFieldTableOid :: OptionalOid,
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

instance HasConstructor 'RowFieldType (RowField bytes) where
  type
    ConstructorOf 'RowFieldType (RowField bytes) r =
      bytes -> OptionalOid -> Int16 -> Oid -> Int16 -> Int32 -> FormatCode -> r
  constructorOf name tableOid attributeNumber =
    RowField
      name
      tableOid
      (if attributeNumber == 0 then Nothing else Just attributeNumber)
  destructorOf (RowField name tableOid attributeNumber typeOid typeSize typeModifier format) k =
    k name tableOid (fromMaybe 0 attributeNumber) typeOid typeSize typeModifier format
