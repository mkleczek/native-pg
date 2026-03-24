{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Protocol.Messages
  ( StartupPacket (..),
    SomeStartupPacket (..),
    startupPacketType,
    startupPacketName,
    startupPacketCode,
    AuthFrontendMessage (..),
    SomeAuthFrontendMessage (..),
    authFrontendMessageType,
    authFrontendMessageName,
    authFrontendMessageCode,
    AuthBackendMessage (..),
    SomeAuthBackendMessage (..),
    authBackendMessageType,
    authBackendMessageName,
    authBackendMessageCode,
    CopyMessage (..),
    SomeCopyMessage (..),
    copyMessageType,
    copyMessageName,
    copyMessageCode,
    FrontendSessionMessage (..),
    SomeFrontendSessionMessage (..),
    frontendSessionMessageType,
    frontendSessionMessageName,
    frontendSessionMessageCode,
    BackendSessionMessage (..),
    SomeBackendSessionMessage (..),
    backendSessionMessageType,
    backendSessionMessageName,
    backendSessionMessageCode,
    module Database.PostgreSQL.Protocol.Messages.MessageType,
    module Database.PostgreSQL.Protocol.Messages.Types,
  )
where

import Data.ByteString (ByteString)
import Data.Int (Int32)
import Data.Kind (Type)
import Data.Text (Text)
import Data.Word (Word32)
import Database.PostgreSQL.Protocol.Codec.Payload (DenotationOf)
import Database.PostgreSQL.Protocol.Messages.MessageType
import Database.PostgreSQL.Protocol.Messages.Types

-- | The protocol no longer exposes one giant message GADT.
--
-- PostgreSQL traffic is already phase-structured, and keeping that split in
-- the types makes the API read much closer to the protocol itself:
--
-- * startup/request packets
-- * frontend authentication replies
-- * backend authentication challenges/results
-- * copy-stream messages shared by both peers
-- * ordinary frontend session traffic
-- * ordinary backend session traffic
--
-- Each family also gets its own singleton kind, so the type-level
-- discriminators now follow the same phase boundaries as the message AST.

type StartupPacket :: forall payload. StartupPacketType payload -> Type
data StartupPacket messageType where
  StartupMessage ::
    ProtocolVersion 'ProtocolVersionType ->
    [StartupParameter] ->
    StartupPacket 'StartupMessageType
  SSLRequest :: StartupPacket 'SSLRequestType
  GSSENCRequest :: StartupPacket 'GSSENCRequestType
  CancelRequest :: BackendKey -> StartupPacket 'CancelRequestType

deriving instance Eq (StartupPacket messageType)
deriving instance Show (StartupPacket messageType)

data SomeStartupPacket where
  SomeStartupPacket :: StartupPacket messageType -> SomeStartupPacket

deriving instance Show SomeStartupPacket

type instance DenotationOf (messageType :: StartupPacketType payload) = StartupPacket messageType

startupPacketType ::
  StartupPacket (messageType :: StartupPacketType payload) ->
  StartupPacketType payload
startupPacketType packet = case packet of
  StartupMessage {} -> StartupMessageType
  SSLRequest -> SSLRequestType
  GSSENCRequest -> GSSENCRequestType
  CancelRequest {} -> CancelRequestType

startupPacketName :: StartupPacket messageType -> String
startupPacketName packet = case packet of
  StartupMessage {} -> "StartupMessage"
  SSLRequest -> "SSLRequest"
  GSSENCRequest -> "GSSENCRequest"
  CancelRequest {} -> "CancelRequest"

startupPacketCode :: StartupPacket messageType -> Word32
startupPacketCode packet = case packet of
  StartupMessage protocolVersion _ -> protocolVersionCode protocolVersion
  SSLRequest -> sslRequestCode
  GSSENCRequest -> gssEncRequestCode
  CancelRequest {} -> cancelRequestCode

type AuthFrontendMessage :: forall payload. AuthFrontendType payload -> Type
data AuthFrontendMessage messageType where
  PasswordMessage :: Text -> AuthFrontendMessage 'PasswordMessageType
  GSSResponse :: ByteString -> AuthFrontendMessage 'GSSResponseType
  SASLInitialResponse :: Text -> Maybe ByteString -> AuthFrontendMessage 'SASLInitialResponseType
  SASLResponse :: ByteString -> AuthFrontendMessage 'SASLResponseType

deriving instance Eq (AuthFrontendMessage messageType)
deriving instance Show (AuthFrontendMessage messageType)

data SomeAuthFrontendMessage where
  SomeAuthFrontendMessage :: AuthFrontendMessage messageType -> SomeAuthFrontendMessage

deriving instance Show SomeAuthFrontendMessage

type instance DenotationOf (messageType :: AuthFrontendType payload) = AuthFrontendMessage messageType

authFrontendMessageType ::
  AuthFrontendMessage (messageType :: AuthFrontendType payload) ->
  AuthFrontendType payload
authFrontendMessageType message = case message of
  PasswordMessage _ -> PasswordMessageType
  GSSResponse _ -> GSSResponseType
  SASLInitialResponse {} -> SASLInitialResponseType
  SASLResponse _ -> SASLResponseType

authFrontendMessageName :: AuthFrontendMessage messageType -> String
authFrontendMessageName message = case message of
  PasswordMessage _ -> "PasswordMessage"
  GSSResponse _ -> "GSSResponse"
  SASLInitialResponse {} -> "SASLInitialResponse"
  SASLResponse _ -> "SASLResponse"

authFrontendMessageCode :: AuthFrontendMessage messageType -> Char
authFrontendMessageCode _ = 'p'

type AuthBackendMessage :: forall subcode payload. AuthBackendType subcode payload -> Type
data AuthBackendMessage messageType where
  AuthenticationOk :: AuthBackendMessage 'AuthenticationOkType
  AuthenticationKerberosV5 :: AuthBackendMessage 'AuthenticationKerberosV5Type
  AuthenticationCleartextPassword :: AuthBackendMessage 'AuthenticationCleartextPasswordType
  AuthenticationMD5Password :: ByteString -> AuthBackendMessage 'AuthenticationMD5PasswordType
  AuthenticationGSS :: AuthBackendMessage 'AuthenticationGSSType
  AuthenticationGSSContinue :: ByteString -> AuthBackendMessage 'AuthenticationGSSContinueType
  AuthenticationSSPI :: AuthBackendMessage 'AuthenticationSSPIType
  AuthenticationSASL :: [Text] -> AuthBackendMessage 'AuthenticationSASLType
  AuthenticationSASLContinue :: ByteString -> AuthBackendMessage 'AuthenticationSASLContinueType
  AuthenticationSASLFinal :: ByteString -> AuthBackendMessage 'AuthenticationSASLFinalType

deriving instance Eq (AuthBackendMessage messageType)
deriving instance Show (AuthBackendMessage messageType)

data SomeAuthBackendMessage where
  SomeAuthBackendMessage :: AuthBackendMessage messageType -> SomeAuthBackendMessage

deriving instance Show SomeAuthBackendMessage

type instance DenotationOf (messageType :: AuthBackendType subcode payload) = AuthBackendMessage messageType

authBackendMessageType ::
  AuthBackendMessage (messageType :: AuthBackendType subcode payload) ->
  AuthBackendType subcode payload
authBackendMessageType message = case message of
  AuthenticationOk -> AuthenticationOkType
  AuthenticationKerberosV5 -> AuthenticationKerberosV5Type
  AuthenticationCleartextPassword -> AuthenticationCleartextPasswordType
  AuthenticationMD5Password _ -> AuthenticationMD5PasswordType
  AuthenticationGSS -> AuthenticationGSSType
  AuthenticationGSSContinue _ -> AuthenticationGSSContinueType
  AuthenticationSSPI -> AuthenticationSSPIType
  AuthenticationSASL _ -> AuthenticationSASLType
  AuthenticationSASLContinue _ -> AuthenticationSASLContinueType
  AuthenticationSASLFinal _ -> AuthenticationSASLFinalType

authBackendMessageName :: AuthBackendMessage messageType -> String
authBackendMessageName message = case message of
  AuthenticationOk -> "AuthenticationOk"
  AuthenticationKerberosV5 -> "AuthenticationKerberosV5"
  AuthenticationCleartextPassword -> "AuthenticationCleartextPassword"
  AuthenticationMD5Password _ -> "AuthenticationMD5Password"
  AuthenticationGSS -> "AuthenticationGSS"
  AuthenticationGSSContinue _ -> "AuthenticationGSSContinue"
  AuthenticationSSPI -> "AuthenticationSSPI"
  AuthenticationSASL _ -> "AuthenticationSASL"
  AuthenticationSASLContinue _ -> "AuthenticationSASLContinue"
  AuthenticationSASLFinal _ -> "AuthenticationSASLFinal"

authBackendMessageCode :: AuthBackendMessage messageType -> Char
authBackendMessageCode _ = 'R'

type CopyMessage :: forall tag payload. CopyMessageType tag payload -> Type
data CopyMessage messageType where
  CopyData :: ByteString -> CopyMessage 'CopyDataType
  CopyDone :: CopyMessage 'CopyDoneType
  CopyFail :: Text -> CopyMessage 'CopyFailType

deriving instance Eq (CopyMessage messageType)
deriving instance Show (CopyMessage messageType)

data SomeCopyMessage where
  SomeCopyMessage :: CopyMessage messageType -> SomeCopyMessage

deriving instance Show SomeCopyMessage

type instance DenotationOf (messageType :: CopyMessageType tag payload) = CopyMessage messageType

copyMessageType ::
  CopyMessage (messageType :: CopyMessageType tag payload) ->
  CopyMessageType tag payload
copyMessageType message = case message of
  CopyData _ -> CopyDataType
  CopyDone -> CopyDoneType
  CopyFail _ -> CopyFailType

copyMessageName :: CopyMessage messageType -> String
copyMessageName message = case message of
  CopyData _ -> "CopyData"
  CopyDone -> "CopyDone"
  CopyFail _ -> "CopyFail"

copyMessageCode :: CopyMessage messageType -> Char
copyMessageCode message = case message of
  CopyData _ -> 'd'
  CopyDone -> 'c'
  CopyFail _ -> 'f'

type FrontendSessionMessage :: forall tag payload. FrontendSessionType tag payload -> Type
data FrontendSessionMessage messageType where
  Bind :: BindBody 'BindBodyType -> FrontendSessionMessage 'BindType
  Close :: NamedObject 'NamedObjectType -> Text -> FrontendSessionMessage 'CloseType
  Describe :: NamedObject 'NamedObjectType -> Text -> FrontendSessionMessage 'DescribeType
  Execute :: Text -> Int32 -> FrontendSessionMessage 'ExecuteType
  Flush :: FrontendSessionMessage 'FlushType
  FunctionCall ::
    Oid ->
    [FormatCode 'FormatCodeType] ->
    [Maybe ByteString] ->
    FormatCode 'FormatCodeType ->
    FrontendSessionMessage 'FunctionCallType
  Parse :: Text -> Text -> [Maybe Oid] -> FrontendSessionMessage 'ParseType
  Query :: Text -> FrontendSessionMessage 'QueryType
  Sync :: FrontendSessionMessage 'SyncType
  Terminate :: FrontendSessionMessage 'TerminateType

deriving instance Eq (FrontendSessionMessage messageType)
deriving instance Show (FrontendSessionMessage messageType)

data SomeFrontendSessionMessage where
  SomeFrontendSessionMessage :: FrontendSessionMessage messageType -> SomeFrontendSessionMessage

deriving instance Show SomeFrontendSessionMessage

type instance DenotationOf (messageType :: FrontendSessionType tag payload) = FrontendSessionMessage messageType

frontendSessionMessageType ::
  FrontendSessionMessage (messageType :: FrontendSessionType tag payload) ->
  FrontendSessionType tag payload
frontendSessionMessageType message = case message of
  Bind _ -> BindType
  Close {} -> CloseType
  Describe {} -> DescribeType
  Execute {} -> ExecuteType
  Flush -> FlushType
  FunctionCall {} -> FunctionCallType
  Parse {} -> ParseType
  Query _ -> QueryType
  Sync -> SyncType
  Terminate -> TerminateType

frontendSessionMessageName :: FrontendSessionMessage messageType -> String
frontendSessionMessageName message = case message of
  Bind _ -> "Bind"
  Close {} -> "Close"
  Describe {} -> "Describe"
  Execute {} -> "Execute"
  Flush -> "Flush"
  FunctionCall {} -> "FunctionCall"
  Parse {} -> "Parse"
  Query _ -> "Query"
  Sync -> "Sync"
  Terminate -> "Terminate"

frontendSessionMessageCode :: FrontendSessionMessage messageType -> Char
frontendSessionMessageCode message = case message of
  Bind _ -> 'B'
  Close {} -> 'C'
  Describe {} -> 'D'
  Execute {} -> 'E'
  Flush -> 'H'
  FunctionCall {} -> 'F'
  Parse {} -> 'P'
  Query _ -> 'Q'
  Sync -> 'S'
  Terminate -> 'X'

type BackendSessionMessage :: forall tag payload. BackendSessionType tag payload -> Type
data BackendSessionMessage messageType where
  BackendKeyData :: BackendKey -> BackendSessionMessage 'BackendKeyDataType
  BindComplete :: BackendSessionMessage 'BindCompleteType
  CloseComplete :: BackendSessionMessage 'CloseCompleteType
  CommandComplete :: Text -> BackendSessionMessage 'CommandCompleteType
  CopyInResponse :: CopyResponse -> BackendSessionMessage 'CopyInResponseType
  CopyOutResponse :: CopyResponse -> BackendSessionMessage 'CopyOutResponseType
  CopyBothResponse :: CopyResponse -> BackendSessionMessage 'CopyBothResponseType
  DataRow :: [Maybe ByteString] -> BackendSessionMessage 'DataRowType
  EmptyQueryResponse :: BackendSessionMessage 'EmptyQueryResponseType
  ErrorResponse :: [ErrorNoticeField] -> BackendSessionMessage 'ErrorResponseType
  FunctionCallResponse :: Maybe ByteString -> BackendSessionMessage 'FunctionCallResponseType
  NegotiateProtocolVersion ::
    ProtocolVersion 'ProtocolVersionType ->
    [Text] ->
    BackendSessionMessage 'NegotiateProtocolVersionType
  NoData :: BackendSessionMessage 'NoDataType
  NoticeResponse :: [ErrorNoticeField] -> BackendSessionMessage 'NoticeResponseType
  NotificationResponse :: Notification 'NotificationType -> BackendSessionMessage 'NotificationResponseType
  ParameterDescription :: [Oid] -> BackendSessionMessage 'ParameterDescriptionType
  ParameterStatus ::
    ParameterStatusValue ->
    BackendSessionMessage 'ParameterStatusType
  ParseComplete :: BackendSessionMessage 'ParseCompleteType
  PortalSuspended :: BackendSessionMessage 'PortalSuspendedType
  ReadyForQuery :: TransactionStatus 'TransactionStatusType -> BackendSessionMessage 'ReadyForQueryType
  RowDescription :: [RowField 'RowFieldType] -> BackendSessionMessage 'RowDescriptionType

deriving instance Eq (BackendSessionMessage messageType)
deriving instance Show (BackendSessionMessage messageType)

data SomeBackendSessionMessage where
  SomeBackendSessionMessage :: BackendSessionMessage messageType -> SomeBackendSessionMessage

deriving instance Show SomeBackendSessionMessage

type instance DenotationOf (messageType :: BackendSessionType tag payload) = BackendSessionMessage messageType

backendSessionMessageType ::
  BackendSessionMessage (messageType :: BackendSessionType tag payload) ->
  BackendSessionType tag payload
backendSessionMessageType message = case message of
  BackendKeyData _ -> BackendKeyDataType
  BindComplete -> BindCompleteType
  CloseComplete -> CloseCompleteType
  CommandComplete _ -> CommandCompleteType
  CopyInResponse _ -> CopyInResponseType
  CopyOutResponse _ -> CopyOutResponseType
  CopyBothResponse _ -> CopyBothResponseType
  DataRow _ -> DataRowType
  EmptyQueryResponse -> EmptyQueryResponseType
  ErrorResponse _ -> ErrorResponseType
  FunctionCallResponse _ -> FunctionCallResponseType
  NegotiateProtocolVersion {} -> NegotiateProtocolVersionType
  NoData -> NoDataType
  NoticeResponse _ -> NoticeResponseType
  NotificationResponse _ -> NotificationResponseType
  ParameterDescription _ -> ParameterDescriptionType
  ParameterStatus _ -> ParameterStatusType
  ParseComplete -> ParseCompleteType
  PortalSuspended -> PortalSuspendedType
  ReadyForQuery _ -> ReadyForQueryType
  RowDescription _ -> RowDescriptionType

backendSessionMessageName :: BackendSessionMessage messageType -> String
backendSessionMessageName message = case message of
  BackendKeyData _ -> "BackendKeyData"
  BindComplete -> "BindComplete"
  CloseComplete -> "CloseComplete"
  CommandComplete _ -> "CommandComplete"
  CopyInResponse _ -> "CopyInResponse"
  CopyOutResponse _ -> "CopyOutResponse"
  CopyBothResponse _ -> "CopyBothResponse"
  DataRow _ -> "DataRow"
  EmptyQueryResponse -> "EmptyQueryResponse"
  ErrorResponse _ -> "ErrorResponse"
  FunctionCallResponse _ -> "FunctionCallResponse"
  NegotiateProtocolVersion {} -> "NegotiateProtocolVersion"
  NoData -> "NoData"
  NoticeResponse _ -> "NoticeResponse"
  NotificationResponse _ -> "NotificationResponse"
  ParameterDescription _ -> "ParameterDescription"
  ParameterStatus _ -> "ParameterStatus"
  ParseComplete -> "ParseComplete"
  PortalSuspended -> "PortalSuspended"
  ReadyForQuery _ -> "ReadyForQuery"
  RowDescription _ -> "RowDescription"

backendSessionMessageCode :: BackendSessionMessage messageType -> Char
backendSessionMessageCode message = case message of
  BackendKeyData _ -> 'K'
  BindComplete -> '2'
  CloseComplete -> '3'
  CommandComplete _ -> 'C'
  CopyInResponse _ -> 'G'
  CopyOutResponse _ -> 'H'
  CopyBothResponse _ -> 'W'
  DataRow _ -> 'D'
  EmptyQueryResponse -> 'I'
  ErrorResponse _ -> 'E'
  FunctionCallResponse _ -> 'V'
  NegotiateProtocolVersion {} -> 'v'
  NoData -> 'n'
  NoticeResponse _ -> 'N'
  NotificationResponse _ -> 'A'
  ParameterDescription _ -> 't'
  ParameterStatus _ -> 'S'
  ParseComplete -> '1'
  PortalSuspended -> 's'
  ReadyForQuery _ -> 'Z'
  RowDescription _ -> 'T'
