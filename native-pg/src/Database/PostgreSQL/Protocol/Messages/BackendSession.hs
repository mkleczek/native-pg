{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Protocol.Messages.BackendSession
  ( BackendKeyDataType (BackendKeyDataType),
    BindCompleteType (BindCompleteType),
    CloseCompleteType (CloseCompleteType),
    CommandCompleteType (CommandCompleteType),
    CopyInResponseType (CopyInResponseType),
    CopyOutResponseType (CopyOutResponseType),
    CopyBothResponseType (CopyBothResponseType),
    DataRowType (DataRowType),
    DataRowHeaderType (DataRowHeaderType),
    EmptyQueryResponseType (EmptyQueryResponseType),
    ErrorResponseType (ErrorResponseType),
    FunctionCallResponseType (FunctionCallResponseType),
    NegotiateProtocolVersionType (NegotiateProtocolVersionType),
    NoDataType (NoDataType),
    NoticeResponseType (NoticeResponseType),
    NotificationResponseType (NotificationResponseType),
    ParameterDescriptionType (ParameterDescriptionType),
    ParameterStatusType (ParameterStatusType),
    ParseCompleteType (ParseCompleteType),
    PortalSuspendedType (PortalSuspendedType),
    ReadyForQueryType (ReadyForQueryType),
    RowDescriptionType (RowDescriptionType),
    BackendKeyDataEnvelope,
    BindCompleteEnvelope,
    CloseCompleteEnvelope,
    CommandCompleteEnvelope,
    CopyInResponseEnvelope,
    CopyOutResponseEnvelope,
    CopyBothResponseEnvelope,
    DataRowEnvelope,
    DataRowHeaderEnvelope,
    EmptyQueryResponseEnvelope,
    ErrorResponseEnvelope,
    FunctionCallResponseEnvelope,
    NegotiateProtocolVersionEnvelope,
    NoDataEnvelope,
    NoticeResponseEnvelope,
    NotificationResponseEnvelope,
    ParameterDescriptionEnvelope,
    ParameterStatusEnvelope,
    ParseCompleteEnvelope,
    PortalSuspendedEnvelope,
    ReadyForQueryEnvelope,
    RowDescriptionEnvelope,
    BackendKeyData (BackendKeyData),
    BindComplete (BindComplete),
    CloseComplete (CloseComplete),
    CommandComplete (CommandComplete),
    CopyInResponse (CopyInResponse),
    CopyOutResponse (CopyOutResponse),
    CopyBothResponse (CopyBothResponse),
    DataRow (DataRow),
    DataRowHeader (..),
    EmptyQueryResponse (EmptyQueryResponse),
    ErrorResponse (ErrorResponse),
    FunctionCallResponse (FunctionCallResponse),
    NegotiateProtocolVersion (NegotiateProtocolVersion),
    NoData (NoData),
    NoticeResponse (NoticeResponse),
    NotificationResponse (NotificationResponse),
    ParameterDescription (ParameterDescription),
    ParameterStatus (ParameterStatus),
    ParseComplete (ParseComplete),
    PortalSuspended (PortalSuspended),
    ReadyForQuery (ReadyForQuery),
    RowDescription (RowDescription),
  )
where

import Data.Int (Int16, Int32)
import Database.PostgreSQL.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
import Database.PostgreSQL.Protocol.Messages.Types
  ( BackendKey,
    BackendKeyType (BackendKeyType),
    CopyResponse,
    CopyResponseType (CopyResponseType),
    Envelope,
    ErrorNoticeField,
    ErrorNoticeFieldType (ErrorNoticeFieldType),
    Notification,
    NotificationType (NotificationType),
    Oid,
    OidType (OidType),
    ParameterStatusValue,
    ParameterStatusValueType (ParameterStatusValueType),
    ProtocolVersion,
    ProtocolVersionType (ProtocolVersionType),
    RowField,
    RowFieldType (RowFieldType),
    TransactionStatus,
    TransactionStatusType (TransactionStatusType),
  )

type BackendSessionMessage tag payload =
  'PayloadStruct
    '[ 'PayloadConst 1 tag,
       'PayloadBounded payload
     ]

data BackendKeyDataType (payload :: Payload) where
  BackendKeyDataType ::
    BackendKeyDataType
      (BackendSessionMessage 'K' ('PayloadField 'BackendKeyType))

data BindCompleteType (payload :: Payload) where
  BindCompleteType ::
    BindCompleteType
      (BackendSessionMessage '2' Empty)

data CloseCompleteType (payload :: Payload) where
  CloseCompleteType ::
    CloseCompleteType
      (BackendSessionMessage '3' Empty)

data CommandCompleteType (payload :: Payload) where
  CommandCompleteType ::
    CommandCompleteType
      (BackendSessionMessage 'C' 'CStringAtom)

data CopyInResponseType (payload :: Payload) where
  CopyInResponseType ::
    CopyInResponseType
      (BackendSessionMessage 'G' ('PayloadField 'CopyResponseType))

data CopyOutResponseType (payload :: Payload) where
  CopyOutResponseType ::
    CopyOutResponseType
      (BackendSessionMessage 'H' ('PayloadField 'CopyResponseType))

data CopyBothResponseType (payload :: Payload) where
  CopyBothResponseType ::
    CopyBothResponseType
      (BackendSessionMessage 'W' ('PayloadField 'CopyResponseType))

data DataRowType (payload :: Payload) where
  DataRowType ::
    DataRowType
      (BackendSessionMessage 'D' (PayloadCounted16 'MaybeBytes32Atom))

-- | Only the start of a DataRow, so that its columns can be pulled one at a
-- time after it.
data DataRowHeaderType (payload :: Payload) where
  DataRowHeaderType ::
    DataRowHeaderType
      ('PayloadStruct '[ 'PayloadConst 1 'D', 'FixedBytesAtom 4, 'FixedBytesAtom 2])

data EmptyQueryResponseType (payload :: Payload) where
  EmptyQueryResponseType ::
    EmptyQueryResponseType
      (BackendSessionMessage 'I' Empty)

data ErrorResponseType (payload :: Payload) where
  ErrorResponseType ::
    ErrorResponseType
      (BackendSessionMessage 'E' (PayloadZeroTerminated ('PayloadField 'ErrorNoticeFieldType)))

data FunctionCallResponseType (payload :: Payload) where
  FunctionCallResponseType ::
    FunctionCallResponseType
      (BackendSessionMessage 'V' 'MaybeBytes32Atom)

data NegotiateProtocolVersionType (payload :: Payload) where
  NegotiateProtocolVersionType ::
    NegotiateProtocolVersionType
      (BackendSessionMessage 'v' ('PayloadStruct '[PayloadField 'ProtocolVersionType, PayloadCounted32 'CStringAtom]))

data NoDataType (payload :: Payload) where
  NoDataType ::
    NoDataType
      (BackendSessionMessage 'n' Empty)

data NoticeResponseType (payload :: Payload) where
  NoticeResponseType ::
    NoticeResponseType
      (BackendSessionMessage 'N' (PayloadZeroTerminated ('PayloadField 'ErrorNoticeFieldType)))

data NotificationResponseType (payload :: Payload) where
  NotificationResponseType ::
    NotificationResponseType
      (BackendSessionMessage 'A' ('PayloadField 'NotificationType))

data ParameterDescriptionType (payload :: Payload) where
  ParameterDescriptionType ::
    ParameterDescriptionType
      (BackendSessionMessage 't' (PayloadCounted16 ('PayloadField 'OidType)))

data ParameterStatusType (payload :: Payload) where
  ParameterStatusType ::
    ParameterStatusType
      (BackendSessionMessage 'S' ('PayloadField 'ParameterStatusValueType))

data ParseCompleteType (payload :: Payload) where
  ParseCompleteType ::
    ParseCompleteType
      (BackendSessionMessage '1' Empty)

data PortalSuspendedType (payload :: Payload) where
  PortalSuspendedType ::
    PortalSuspendedType
      (BackendSessionMessage 's' Empty)

data ReadyForQueryType (payload :: Payload) where
  ReadyForQueryType ::
    ReadyForQueryType
      (BackendSessionMessage 'Z' ('PayloadField 'TransactionStatusType))

data RowDescriptionType (payload :: Payload) where
  RowDescriptionType ::
    RowDescriptionType
      (BackendSessionMessage 'T' (PayloadCounted16 ('PayloadField 'RowFieldType)))

newtype BackendKeyData bytes = BackendKeyData (BackendKey bytes)

deriving instance Eq bytes => Eq (BackendKeyData bytes)
deriving instance Show bytes => Show (BackendKeyData bytes)

type BackendKeyDataEnvelope bytes =
  Envelope
    (BackendKeyData bytes)
    'BackendKeyDataType

instance HasConstructor 'BackendKeyDataType (BackendKeyData bytes) where
  type ConstructorOf 'BackendKeyDataType (BackendKeyData bytes) r = BackendKey bytes -> r
  constructorOf = BackendKeyData
  destructorOf (BackendKeyData key) k = k key

data BindComplete = BindComplete

deriving instance Eq BindComplete
deriving instance Show BindComplete

type BindCompleteEnvelope =
  Envelope
    BindComplete
    'BindCompleteType

instance HasConstructor 'BindCompleteType BindComplete where
  type ConstructorOf 'BindCompleteType BindComplete r = r
  constructorOf = BindComplete
  destructorOf BindComplete k = k

data CloseComplete = CloseComplete

deriving instance Eq CloseComplete
deriving instance Show CloseComplete

type CloseCompleteEnvelope =
  Envelope
    CloseComplete
    'CloseCompleteType

instance HasConstructor 'CloseCompleteType CloseComplete where
  type ConstructorOf 'CloseCompleteType CloseComplete r = r
  constructorOf = CloseComplete
  destructorOf CloseComplete k = k

newtype CommandComplete bytes = CommandComplete bytes

deriving instance Eq bytes => Eq (CommandComplete bytes)
deriving instance Show bytes => Show (CommandComplete bytes)

type CommandCompleteEnvelope bytes =
  Envelope
    (CommandComplete bytes)
    'CommandCompleteType

instance HasConstructor 'CommandCompleteType (CommandComplete bytes) where
  type ConstructorOf 'CommandCompleteType (CommandComplete bytes) r = bytes -> r
  constructorOf = CommandComplete
  destructorOf (CommandComplete tag) k = k tag

newtype CopyInResponse = CopyInResponse CopyResponse

deriving instance Eq CopyInResponse
deriving instance Show CopyInResponse

type CopyInResponseEnvelope =
  Envelope
    CopyInResponse
    'CopyInResponseType

instance HasConstructor 'CopyInResponseType CopyInResponse where
  type ConstructorOf 'CopyInResponseType CopyInResponse r = CopyResponse -> r
  constructorOf = CopyInResponse
  destructorOf (CopyInResponse response) k = k response

newtype CopyOutResponse = CopyOutResponse CopyResponse

deriving instance Eq CopyOutResponse
deriving instance Show CopyOutResponse

type CopyOutResponseEnvelope =
  Envelope
    CopyOutResponse
    'CopyOutResponseType

instance HasConstructor 'CopyOutResponseType CopyOutResponse where
  type ConstructorOf 'CopyOutResponseType CopyOutResponse r = CopyResponse -> r
  constructorOf = CopyOutResponse
  destructorOf (CopyOutResponse response) k = k response

newtype CopyBothResponse = CopyBothResponse CopyResponse

deriving instance Eq CopyBothResponse
deriving instance Show CopyBothResponse

type CopyBothResponseEnvelope =
  Envelope
    CopyBothResponse
    'CopyBothResponseType

instance HasConstructor 'CopyBothResponseType CopyBothResponse where
  type ConstructorOf 'CopyBothResponseType CopyBothResponse r = CopyResponse -> r
  constructorOf = CopyBothResponse
  destructorOf (CopyBothResponse response) k = k response

newtype DataRow bytes = DataRow [Maybe bytes]

deriving instance Eq bytes => Eq (DataRow bytes)
deriving instance Show bytes => Show (DataRow bytes)

type DataRowEnvelope bytes =
  Envelope
    (DataRow bytes)
    'DataRowType

instance HasConstructor 'DataRowType (DataRow bytes) where
  type ConstructorOf 'DataRowType (DataRow bytes) r = [Maybe bytes] -> r
  constructorOf = DataRow
  destructorOf (DataRow values) k = k values

data DataRowHeader = DataRowHeader
  { -- | Counts itself, the column count and the columns.
    dataRowLength :: Int32,
    dataRowColumnCount :: Int16
  }

deriving instance Eq DataRowHeader
deriving instance Show DataRowHeader

type DataRowHeaderEnvelope =
  Envelope
    DataRowHeader
    'DataRowHeaderType

instance HasConstructor 'DataRowHeaderType DataRowHeader where
  type ConstructorOf 'DataRowHeaderType DataRowHeader r = Int32 -> Int16 -> r
  constructorOf = DataRowHeader
  destructorOf (DataRowHeader rowLength columnCount) k = k rowLength columnCount

data EmptyQueryResponse = EmptyQueryResponse

deriving instance Eq EmptyQueryResponse
deriving instance Show EmptyQueryResponse

type EmptyQueryResponseEnvelope =
  Envelope
    EmptyQueryResponse
    'EmptyQueryResponseType

instance HasConstructor 'EmptyQueryResponseType EmptyQueryResponse where
  type ConstructorOf 'EmptyQueryResponseType EmptyQueryResponse r = r
  constructorOf = EmptyQueryResponse
  destructorOf EmptyQueryResponse k = k

newtype ErrorResponse bytes = ErrorResponse [ErrorNoticeField bytes]

deriving instance Eq bytes => Eq (ErrorResponse bytes)
deriving instance Show bytes => Show (ErrorResponse bytes)

type ErrorResponseEnvelope bytes =
  Envelope
    (ErrorResponse bytes)
    'ErrorResponseType

instance HasConstructor 'ErrorResponseType (ErrorResponse bytes) where
  type ConstructorOf 'ErrorResponseType (ErrorResponse bytes) r = [ErrorNoticeField bytes] -> r
  constructorOf = ErrorResponse
  destructorOf (ErrorResponse fields) k = k fields

newtype FunctionCallResponse bytes =
  FunctionCallResponse (Maybe bytes)

deriving instance Eq bytes => Eq (FunctionCallResponse bytes)
deriving instance Show bytes => Show (FunctionCallResponse bytes)

type FunctionCallResponseEnvelope bytes =
  Envelope
    (FunctionCallResponse bytes)
    'FunctionCallResponseType

instance HasConstructor 'FunctionCallResponseType (FunctionCallResponse bytes) where
  type ConstructorOf 'FunctionCallResponseType (FunctionCallResponse bytes) r = Maybe bytes -> r
  constructorOf = FunctionCallResponse
  destructorOf (FunctionCallResponse result) k = k result

data NegotiateProtocolVersion bytes =
  NegotiateProtocolVersion
    ProtocolVersion
    [bytes]

deriving instance Eq bytes => Eq (NegotiateProtocolVersion bytes)
deriving instance Show bytes => Show (NegotiateProtocolVersion bytes)

type NegotiateProtocolVersionEnvelope bytes =
  Envelope
    (NegotiateProtocolVersion bytes)
    'NegotiateProtocolVersionType

instance HasConstructor 'NegotiateProtocolVersionType (NegotiateProtocolVersion bytes) where
  type ConstructorOf 'NegotiateProtocolVersionType (NegotiateProtocolVersion bytes) r = ProtocolVersion -> [bytes] -> r
  constructorOf = NegotiateProtocolVersion
  destructorOf (NegotiateProtocolVersion version unrecognizedOptions) k = k version unrecognizedOptions

data NoData = NoData

deriving instance Eq NoData
deriving instance Show NoData

type NoDataEnvelope =
  Envelope
    NoData
    'NoDataType

instance HasConstructor 'NoDataType NoData where
  type ConstructorOf 'NoDataType NoData r = r
  constructorOf = NoData
  destructorOf NoData k = k

newtype NoticeResponse bytes = NoticeResponse [ErrorNoticeField bytes]

deriving instance Eq bytes => Eq (NoticeResponse bytes)
deriving instance Show bytes => Show (NoticeResponse bytes)

type NoticeResponseEnvelope bytes =
  Envelope
    (NoticeResponse bytes)
    'NoticeResponseType

instance HasConstructor 'NoticeResponseType (NoticeResponse bytes) where
  type ConstructorOf 'NoticeResponseType (NoticeResponse bytes) r = [ErrorNoticeField bytes] -> r
  constructorOf = NoticeResponse
  destructorOf (NoticeResponse fields) k = k fields

newtype NotificationResponse bytes =
  NotificationResponse (Notification bytes)

deriving instance Eq bytes => Eq (NotificationResponse bytes)
deriving instance Show bytes => Show (NotificationResponse bytes)

type NotificationResponseEnvelope bytes =
  Envelope
    (NotificationResponse bytes)
    'NotificationResponseType

instance HasConstructor 'NotificationResponseType (NotificationResponse bytes) where
  type ConstructorOf 'NotificationResponseType (NotificationResponse bytes) r = Notification bytes -> r
  constructorOf = NotificationResponse
  destructorOf (NotificationResponse notification) k = k notification

newtype ParameterDescription = ParameterDescription [Oid]

deriving instance Eq ParameterDescription
deriving instance Show ParameterDescription

type ParameterDescriptionEnvelope =
  Envelope
    ParameterDescription
    'ParameterDescriptionType

instance HasConstructor 'ParameterDescriptionType ParameterDescription where
  type ConstructorOf 'ParameterDescriptionType ParameterDescription r = [Oid] -> r
  constructorOf = ParameterDescription
  destructorOf (ParameterDescription types) k = k types

newtype ParameterStatus bytes = ParameterStatus (ParameterStatusValue bytes)

deriving instance Eq bytes => Eq (ParameterStatus bytes)
deriving instance Show bytes => Show (ParameterStatus bytes)

type ParameterStatusEnvelope bytes =
  Envelope
    (ParameterStatus bytes)
    'ParameterStatusType

instance HasConstructor 'ParameterStatusType (ParameterStatus bytes) where
  type ConstructorOf 'ParameterStatusType (ParameterStatus bytes) r = ParameterStatusValue bytes -> r
  constructorOf = ParameterStatus
  destructorOf (ParameterStatus status) k = k status

data ParseComplete = ParseComplete

deriving instance Eq ParseComplete
deriving instance Show ParseComplete

type ParseCompleteEnvelope =
  Envelope
    ParseComplete
    'ParseCompleteType

instance HasConstructor 'ParseCompleteType ParseComplete where
  type ConstructorOf 'ParseCompleteType ParseComplete r = r
  constructorOf = ParseComplete
  destructorOf ParseComplete k = k

data PortalSuspended = PortalSuspended

deriving instance Eq PortalSuspended
deriving instance Show PortalSuspended

type PortalSuspendedEnvelope =
  Envelope
    PortalSuspended
    'PortalSuspendedType

instance HasConstructor 'PortalSuspendedType PortalSuspended where
  type ConstructorOf 'PortalSuspendedType PortalSuspended r = r
  constructorOf = PortalSuspended
  destructorOf PortalSuspended k = k

newtype ReadyForQuery =
  ReadyForQuery TransactionStatus

deriving instance Eq ReadyForQuery
deriving instance Show ReadyForQuery

type ReadyForQueryEnvelope =
  Envelope
    ReadyForQuery
    'ReadyForQueryType

instance HasConstructor 'ReadyForQueryType ReadyForQuery where
  type ConstructorOf 'ReadyForQueryType ReadyForQuery r = TransactionStatus -> r
  constructorOf = ReadyForQuery
  destructorOf (ReadyForQuery status) k = k status

newtype RowDescription bytes = RowDescription [RowField bytes]

deriving instance Eq bytes => Eq (RowDescription bytes)
deriving instance Show bytes => Show (RowDescription bytes)

type RowDescriptionEnvelope bytes =
  Envelope
    (RowDescription bytes)
    'RowDescriptionType

instance HasConstructor 'RowDescriptionType (RowDescription bytes) where
  type ConstructorOf 'RowDescriptionType (RowDescription bytes) r = [RowField bytes] -> r
  constructorOf = RowDescription
  destructorOf (RowDescription fields) k = k fields
