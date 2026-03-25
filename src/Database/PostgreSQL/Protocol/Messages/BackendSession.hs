{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}

module Database.PostgreSQL.Protocol.Messages.BackendSession
  ( BackendKeyDataType (BackendKeyDataType),
    BindCompleteType (BindCompleteType),
    CloseCompleteType (CloseCompleteType),
    CommandCompleteType (CommandCompleteType),
    CopyInResponseType (CopyInResponseType),
    CopyOutResponseType (CopyOutResponseType),
    CopyBothResponseType (CopyBothResponseType),
    DataRowType (DataRowType),
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
      (BackendSessionMessage 'v' ('PayloadStruct '[PayloadField 'ProtocolVersionType, PayloadZeroTerminated 'CStringAtom]))

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

data BindComplete = BindComplete

deriving instance Eq BindComplete
deriving instance Show BindComplete

type BindCompleteEnvelope =
  Envelope
    BindComplete
    'BindCompleteType

data CloseComplete = CloseComplete

deriving instance Eq CloseComplete
deriving instance Show CloseComplete

type CloseCompleteEnvelope =
  Envelope
    CloseComplete
    'CloseCompleteType

newtype CommandComplete bytes = CommandComplete bytes

deriving instance Eq bytes => Eq (CommandComplete bytes)
deriving instance Show bytes => Show (CommandComplete bytes)

type CommandCompleteEnvelope bytes =
  Envelope
    (CommandComplete bytes)
    'CommandCompleteType

newtype CopyInResponse = CopyInResponse CopyResponse

deriving instance Eq CopyInResponse
deriving instance Show CopyInResponse

type CopyInResponseEnvelope =
  Envelope
    CopyInResponse
    'CopyInResponseType

newtype CopyOutResponse = CopyOutResponse CopyResponse

deriving instance Eq CopyOutResponse
deriving instance Show CopyOutResponse

type CopyOutResponseEnvelope =
  Envelope
    CopyOutResponse
    'CopyOutResponseType

newtype CopyBothResponse = CopyBothResponse CopyResponse

deriving instance Eq CopyBothResponse
deriving instance Show CopyBothResponse

type CopyBothResponseEnvelope =
  Envelope
    CopyBothResponse
    'CopyBothResponseType

newtype DataRow bytes = DataRow [Maybe bytes]

deriving instance Eq bytes => Eq (DataRow bytes)
deriving instance Show bytes => Show (DataRow bytes)

type DataRowEnvelope bytes =
  Envelope
    (DataRow bytes)
    'DataRowType

data EmptyQueryResponse = EmptyQueryResponse

deriving instance Eq EmptyQueryResponse
deriving instance Show EmptyQueryResponse

type EmptyQueryResponseEnvelope =
  Envelope
    EmptyQueryResponse
    'EmptyQueryResponseType

newtype ErrorResponse bytes = ErrorResponse [ErrorNoticeField bytes]

deriving instance Eq bytes => Eq (ErrorResponse bytes)
deriving instance Show bytes => Show (ErrorResponse bytes)

type ErrorResponseEnvelope bytes =
  Envelope
    (ErrorResponse bytes)
    'ErrorResponseType

newtype FunctionCallResponse bytes =
  FunctionCallResponse (Maybe bytes)

deriving instance Eq bytes => Eq (FunctionCallResponse bytes)
deriving instance Show bytes => Show (FunctionCallResponse bytes)

type FunctionCallResponseEnvelope bytes =
  Envelope
    (FunctionCallResponse bytes)
    'FunctionCallResponseType

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

data NoData = NoData

deriving instance Eq NoData
deriving instance Show NoData

type NoDataEnvelope =
  Envelope
    NoData
    'NoDataType

newtype NoticeResponse bytes = NoticeResponse [ErrorNoticeField bytes]

deriving instance Eq bytes => Eq (NoticeResponse bytes)
deriving instance Show bytes => Show (NoticeResponse bytes)

type NoticeResponseEnvelope bytes =
  Envelope
    (NoticeResponse bytes)
    'NoticeResponseType

newtype NotificationResponse bytes =
  NotificationResponse (Notification bytes)

deriving instance Eq bytes => Eq (NotificationResponse bytes)
deriving instance Show bytes => Show (NotificationResponse bytes)

type NotificationResponseEnvelope bytes =
  Envelope
    (NotificationResponse bytes)
    'NotificationResponseType

newtype ParameterDescription = ParameterDescription [Oid]

deriving instance Eq ParameterDescription
deriving instance Show ParameterDescription

type ParameterDescriptionEnvelope =
  Envelope
    ParameterDescription
    'ParameterDescriptionType

newtype ParameterStatus bytes = ParameterStatus (ParameterStatusValue bytes)

deriving instance Eq bytes => Eq (ParameterStatus bytes)
deriving instance Show bytes => Show (ParameterStatus bytes)

type ParameterStatusEnvelope bytes =
  Envelope
    (ParameterStatus bytes)
    'ParameterStatusType

data ParseComplete = ParseComplete

deriving instance Eq ParseComplete
deriving instance Show ParseComplete

type ParseCompleteEnvelope =
  Envelope
    ParseComplete
    'ParseCompleteType

data PortalSuspended = PortalSuspended

deriving instance Eq PortalSuspended
deriving instance Show PortalSuspended

type PortalSuspendedEnvelope =
  Envelope
    PortalSuspended
    'PortalSuspendedType

newtype ReadyForQuery =
  ReadyForQuery TransactionStatus

deriving instance Eq ReadyForQuery
deriving instance Show ReadyForQuery

type ReadyForQueryEnvelope =
  Envelope
    ReadyForQuery
    'ReadyForQueryType

newtype RowDescription bytes = RowDescription [RowField bytes]

deriving instance Eq bytes => Eq (RowDescription bytes)
deriving instance Show bytes => Show (RowDescription bytes)

type RowDescriptionEnvelope bytes =
  Envelope
    (RowDescription bytes)
    'RowDescriptionType
