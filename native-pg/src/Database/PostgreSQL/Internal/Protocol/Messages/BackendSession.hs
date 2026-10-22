{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.BackendSession
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
    DataRowHeaderEnvelope,
    BackendKeyData,
    pattern BackendKeyData,
    BindComplete,
    pattern BindComplete,
    CloseComplete,
    pattern CloseComplete,
    CommandComplete,
    pattern CommandComplete,
    CopyInResponse,
    pattern CopyInResponse,
    CopyOutResponse,
    pattern CopyOutResponse,
    CopyBothResponse,
    pattern CopyBothResponse,
    DataRow,
    pattern DataRow,
    DataRowHeader (..),
    EmptyQueryResponse,
    pattern EmptyQueryResponse,
    ErrorResponse,
    pattern ErrorResponse,
    FunctionCallResponse,
    pattern FunctionCallResponse,
    NegotiateProtocolVersion,
    pattern NegotiateProtocolVersion,
    NoData,
    pattern NoData,
    NoticeResponse,
    pattern NoticeResponse,
    NotificationResponse,
    pattern NotificationResponse,
    ParameterDescription,
    pattern ParameterDescription,
    ParameterStatus,
    pattern ParameterStatus,
    ParseComplete,
    pattern ParseComplete,
    PortalSuspended,
    pattern PortalSuspended,
    ReadyForQuery,
    pattern ReadyForQuery,
    RowDescription,
    pattern RowDescription,
  )
where

import Data.Int (Int16, Int32)
import Database.PostgreSQL.Internal.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types
  ( BackendKey,
    BackendKeyType (BackendKeyType),
    CopyResponse,
    CopyResponseType (CopyResponseType),
    Envelope (Envelope),
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

newtype BackendKeyDataPayload bytes = BackendKeyDataPayload (BackendKey bytes)

deriving instance Eq bytes => Eq (BackendKeyDataPayload bytes)
deriving instance Show bytes => Show (BackendKeyDataPayload bytes)

type BackendKeyData bytes = Envelope (BackendKeyDataPayload bytes) 'BackendKeyDataType

pattern BackendKeyData :: BackendKey bytes -> BackendKeyData bytes
pattern BackendKeyData key = Envelope (BackendKeyDataPayload key)

{-# COMPLETE BackendKeyData #-}

instance HasConstructor 'BackendKeyDataType (BackendKeyData bytes) where
  type ConstructorOf 'BackendKeyDataType (BackendKeyData bytes) r = BackendKey bytes -> r
  constructorOf = BackendKeyData
  destructorOf (BackendKeyData key) k = k key

data BindCompletePayload = BindCompletePayload

deriving instance Eq BindCompletePayload
deriving instance Show BindCompletePayload

type BindComplete = Envelope BindCompletePayload 'BindCompleteType

pattern BindComplete :: BindComplete
pattern BindComplete = Envelope BindCompletePayload

{-# COMPLETE BindComplete #-}

instance HasConstructor 'BindCompleteType BindComplete where
  type ConstructorOf 'BindCompleteType BindComplete r = r
  constructorOf = BindComplete
  destructorOf BindComplete k = k

data CloseCompletePayload = CloseCompletePayload

deriving instance Eq CloseCompletePayload
deriving instance Show CloseCompletePayload

type CloseComplete = Envelope CloseCompletePayload 'CloseCompleteType

pattern CloseComplete :: CloseComplete
pattern CloseComplete = Envelope CloseCompletePayload

{-# COMPLETE CloseComplete #-}

instance HasConstructor 'CloseCompleteType CloseComplete where
  type ConstructorOf 'CloseCompleteType CloseComplete r = r
  constructorOf = CloseComplete
  destructorOf CloseComplete k = k

newtype CommandCompletePayload bytes = CommandCompletePayload bytes

deriving instance Eq bytes => Eq (CommandCompletePayload bytes)
deriving instance Show bytes => Show (CommandCompletePayload bytes)

type CommandComplete bytes = Envelope (CommandCompletePayload bytes) 'CommandCompleteType

pattern CommandComplete :: bytes -> CommandComplete bytes
pattern CommandComplete tag = Envelope (CommandCompletePayload tag)

{-# COMPLETE CommandComplete #-}

instance HasConstructor 'CommandCompleteType (CommandComplete bytes) where
  type ConstructorOf 'CommandCompleteType (CommandComplete bytes) r = bytes -> r
  constructorOf = CommandComplete
  destructorOf (CommandComplete tag) k = k tag

newtype CopyInResponsePayload = CopyInResponsePayload CopyResponse

deriving instance Eq CopyInResponsePayload
deriving instance Show CopyInResponsePayload

type CopyInResponse = Envelope CopyInResponsePayload 'CopyInResponseType

pattern CopyInResponse :: CopyResponse -> CopyInResponse
pattern CopyInResponse response = Envelope (CopyInResponsePayload response)

{-# COMPLETE CopyInResponse #-}

instance HasConstructor 'CopyInResponseType CopyInResponse where
  type ConstructorOf 'CopyInResponseType CopyInResponse r = CopyResponse -> r
  constructorOf = CopyInResponse
  destructorOf (CopyInResponse response) k = k response

newtype CopyOutResponsePayload = CopyOutResponsePayload CopyResponse

deriving instance Eq CopyOutResponsePayload
deriving instance Show CopyOutResponsePayload

type CopyOutResponse = Envelope CopyOutResponsePayload 'CopyOutResponseType

pattern CopyOutResponse :: CopyResponse -> CopyOutResponse
pattern CopyOutResponse response = Envelope (CopyOutResponsePayload response)

{-# COMPLETE CopyOutResponse #-}

instance HasConstructor 'CopyOutResponseType CopyOutResponse where
  type ConstructorOf 'CopyOutResponseType CopyOutResponse r = CopyResponse -> r
  constructorOf = CopyOutResponse
  destructorOf (CopyOutResponse response) k = k response

newtype CopyBothResponsePayload = CopyBothResponsePayload CopyResponse

deriving instance Eq CopyBothResponsePayload
deriving instance Show CopyBothResponsePayload

type CopyBothResponse = Envelope CopyBothResponsePayload 'CopyBothResponseType

pattern CopyBothResponse :: CopyResponse -> CopyBothResponse
pattern CopyBothResponse response = Envelope (CopyBothResponsePayload response)

{-# COMPLETE CopyBothResponse #-}

instance HasConstructor 'CopyBothResponseType CopyBothResponse where
  type ConstructorOf 'CopyBothResponseType CopyBothResponse r = CopyResponse -> r
  constructorOf = CopyBothResponse
  destructorOf (CopyBothResponse response) k = k response

newtype DataRowPayload bytes = DataRowPayload [Maybe bytes]

deriving instance Eq bytes => Eq (DataRowPayload bytes)
deriving instance Show bytes => Show (DataRowPayload bytes)

type DataRow bytes = Envelope (DataRowPayload bytes) 'DataRowType

pattern DataRow :: [Maybe bytes] -> DataRow bytes
pattern DataRow values = Envelope (DataRowPayload values)

{-# COMPLETE DataRow #-}

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

data EmptyQueryResponsePayload = EmptyQueryResponsePayload

deriving instance Eq EmptyQueryResponsePayload
deriving instance Show EmptyQueryResponsePayload

type EmptyQueryResponse = Envelope EmptyQueryResponsePayload 'EmptyQueryResponseType

pattern EmptyQueryResponse :: EmptyQueryResponse
pattern EmptyQueryResponse = Envelope EmptyQueryResponsePayload

{-# COMPLETE EmptyQueryResponse #-}

instance HasConstructor 'EmptyQueryResponseType EmptyQueryResponse where
  type ConstructorOf 'EmptyQueryResponseType EmptyQueryResponse r = r
  constructorOf = EmptyQueryResponse
  destructorOf EmptyQueryResponse k = k

newtype ErrorResponsePayload bytes = ErrorResponsePayload [ErrorNoticeField bytes]

deriving instance Eq bytes => Eq (ErrorResponsePayload bytes)
deriving instance Show bytes => Show (ErrorResponsePayload bytes)

type ErrorResponse bytes = Envelope (ErrorResponsePayload bytes) 'ErrorResponseType

pattern ErrorResponse :: [ErrorNoticeField bytes] -> ErrorResponse bytes
pattern ErrorResponse fields = Envelope (ErrorResponsePayload fields)

{-# COMPLETE ErrorResponse #-}

instance HasConstructor 'ErrorResponseType (ErrorResponse bytes) where
  type ConstructorOf 'ErrorResponseType (ErrorResponse bytes) r = [ErrorNoticeField bytes] -> r
  constructorOf = ErrorResponse
  destructorOf (ErrorResponse fields) k = k fields

newtype FunctionCallResponsePayload bytes =
  FunctionCallResponsePayload (Maybe bytes)

deriving instance Eq bytes => Eq (FunctionCallResponsePayload bytes)
deriving instance Show bytes => Show (FunctionCallResponsePayload bytes)

type FunctionCallResponse bytes = Envelope (FunctionCallResponsePayload bytes) 'FunctionCallResponseType

pattern FunctionCallResponse :: Maybe bytes -> FunctionCallResponse bytes
pattern FunctionCallResponse result = Envelope (FunctionCallResponsePayload result)

{-# COMPLETE FunctionCallResponse #-}

instance HasConstructor 'FunctionCallResponseType (FunctionCallResponse bytes) where
  type ConstructorOf 'FunctionCallResponseType (FunctionCallResponse bytes) r = Maybe bytes -> r
  constructorOf = FunctionCallResponse
  destructorOf (FunctionCallResponse result) k = k result

data NegotiateProtocolVersionPayload bytes =
  NegotiateProtocolVersionPayload
    ProtocolVersion
    [bytes]

deriving instance Eq bytes => Eq (NegotiateProtocolVersionPayload bytes)
deriving instance Show bytes => Show (NegotiateProtocolVersionPayload bytes)

type NegotiateProtocolVersion bytes = Envelope (NegotiateProtocolVersionPayload bytes) 'NegotiateProtocolVersionType

pattern NegotiateProtocolVersion :: ProtocolVersion -> [bytes] -> NegotiateProtocolVersion bytes
pattern NegotiateProtocolVersion version unrecognizedOptions = Envelope (NegotiateProtocolVersionPayload version unrecognizedOptions)

{-# COMPLETE NegotiateProtocolVersion #-}

instance HasConstructor 'NegotiateProtocolVersionType (NegotiateProtocolVersion bytes) where
  type ConstructorOf 'NegotiateProtocolVersionType (NegotiateProtocolVersion bytes) r = ProtocolVersion -> [bytes] -> r
  constructorOf = NegotiateProtocolVersion
  destructorOf (NegotiateProtocolVersion version unrecognizedOptions) k = k version unrecognizedOptions

data NoDataPayload = NoDataPayload

deriving instance Eq NoDataPayload
deriving instance Show NoDataPayload

type NoData = Envelope NoDataPayload 'NoDataType

pattern NoData :: NoData
pattern NoData = Envelope NoDataPayload

{-# COMPLETE NoData #-}

instance HasConstructor 'NoDataType NoData where
  type ConstructorOf 'NoDataType NoData r = r
  constructorOf = NoData
  destructorOf NoData k = k

newtype NoticeResponsePayload bytes = NoticeResponsePayload [ErrorNoticeField bytes]

deriving instance Eq bytes => Eq (NoticeResponsePayload bytes)
deriving instance Show bytes => Show (NoticeResponsePayload bytes)

type NoticeResponse bytes = Envelope (NoticeResponsePayload bytes) 'NoticeResponseType

pattern NoticeResponse :: [ErrorNoticeField bytes] -> NoticeResponse bytes
pattern NoticeResponse fields = Envelope (NoticeResponsePayload fields)

{-# COMPLETE NoticeResponse #-}

instance HasConstructor 'NoticeResponseType (NoticeResponse bytes) where
  type ConstructorOf 'NoticeResponseType (NoticeResponse bytes) r = [ErrorNoticeField bytes] -> r
  constructorOf = NoticeResponse
  destructorOf (NoticeResponse fields) k = k fields

newtype NotificationResponsePayload bytes =
  NotificationResponsePayload (Notification bytes)

deriving instance Eq bytes => Eq (NotificationResponsePayload bytes)
deriving instance Show bytes => Show (NotificationResponsePayload bytes)

type NotificationResponse bytes = Envelope (NotificationResponsePayload bytes) 'NotificationResponseType

pattern NotificationResponse :: Notification bytes -> NotificationResponse bytes
pattern NotificationResponse notification = Envelope (NotificationResponsePayload notification)

{-# COMPLETE NotificationResponse #-}

instance HasConstructor 'NotificationResponseType (NotificationResponse bytes) where
  type ConstructorOf 'NotificationResponseType (NotificationResponse bytes) r = Notification bytes -> r
  constructorOf = NotificationResponse
  destructorOf (NotificationResponse notification) k = k notification

newtype ParameterDescriptionPayload = ParameterDescriptionPayload [Oid]

deriving instance Eq ParameterDescriptionPayload
deriving instance Show ParameterDescriptionPayload

type ParameterDescription = Envelope ParameterDescriptionPayload 'ParameterDescriptionType

pattern ParameterDescription :: [Oid] -> ParameterDescription
pattern ParameterDescription types = Envelope (ParameterDescriptionPayload types)

{-# COMPLETE ParameterDescription #-}

instance HasConstructor 'ParameterDescriptionType ParameterDescription where
  type ConstructorOf 'ParameterDescriptionType ParameterDescription r = [Oid] -> r
  constructorOf = ParameterDescription
  destructorOf (ParameterDescription types) k = k types

newtype ParameterStatusPayload bytes = ParameterStatusPayload (ParameterStatusValue bytes)

deriving instance Eq bytes => Eq (ParameterStatusPayload bytes)
deriving instance Show bytes => Show (ParameterStatusPayload bytes)

type ParameterStatus bytes = Envelope (ParameterStatusPayload bytes) 'ParameterStatusType

pattern ParameterStatus :: ParameterStatusValue bytes -> ParameterStatus bytes
pattern ParameterStatus status = Envelope (ParameterStatusPayload status)

{-# COMPLETE ParameterStatus #-}

instance HasConstructor 'ParameterStatusType (ParameterStatus bytes) where
  type ConstructorOf 'ParameterStatusType (ParameterStatus bytes) r = ParameterStatusValue bytes -> r
  constructorOf = ParameterStatus
  destructorOf (ParameterStatus status) k = k status

data ParseCompletePayload = ParseCompletePayload

deriving instance Eq ParseCompletePayload
deriving instance Show ParseCompletePayload

type ParseComplete = Envelope ParseCompletePayload 'ParseCompleteType

pattern ParseComplete :: ParseComplete
pattern ParseComplete = Envelope ParseCompletePayload

{-# COMPLETE ParseComplete #-}

instance HasConstructor 'ParseCompleteType ParseComplete where
  type ConstructorOf 'ParseCompleteType ParseComplete r = r
  constructorOf = ParseComplete
  destructorOf ParseComplete k = k

data PortalSuspendedPayload = PortalSuspendedPayload

deriving instance Eq PortalSuspendedPayload
deriving instance Show PortalSuspendedPayload

type PortalSuspended = Envelope PortalSuspendedPayload 'PortalSuspendedType

pattern PortalSuspended :: PortalSuspended
pattern PortalSuspended = Envelope PortalSuspendedPayload

{-# COMPLETE PortalSuspended #-}

instance HasConstructor 'PortalSuspendedType PortalSuspended where
  type ConstructorOf 'PortalSuspendedType PortalSuspended r = r
  constructorOf = PortalSuspended
  destructorOf PortalSuspended k = k

newtype ReadyForQueryPayload =
  ReadyForQueryPayload TransactionStatus

deriving instance Eq ReadyForQueryPayload
deriving instance Show ReadyForQueryPayload

type ReadyForQuery = Envelope ReadyForQueryPayload 'ReadyForQueryType

pattern ReadyForQuery :: TransactionStatus -> ReadyForQuery
pattern ReadyForQuery status = Envelope (ReadyForQueryPayload status)

{-# COMPLETE ReadyForQuery #-}

instance HasConstructor 'ReadyForQueryType ReadyForQuery where
  type ConstructorOf 'ReadyForQueryType ReadyForQuery r = TransactionStatus -> r
  constructorOf = ReadyForQuery
  destructorOf (ReadyForQuery status) k = k status

newtype RowDescriptionPayload bytes = RowDescriptionPayload [RowField bytes]

deriving instance Eq bytes => Eq (RowDescriptionPayload bytes)
deriving instance Show bytes => Show (RowDescriptionPayload bytes)

type RowDescription bytes = Envelope (RowDescriptionPayload bytes) 'RowDescriptionType

pattern RowDescription :: [RowField bytes] -> RowDescription bytes
pattern RowDescription fields = Envelope (RowDescriptionPayload fields)

{-# COMPLETE RowDescription #-}

instance HasConstructor 'RowDescriptionType (RowDescription bytes) where
  type ConstructorOf 'RowDescriptionType (RowDescription bytes) r = [RowField bytes] -> r
  constructorOf = RowDescription
  destructorOf (RowDescription fields) k = k fields
