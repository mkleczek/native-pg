{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE StandaloneKindSignatures #-}

module Database.PostgreSQL.Protocol.Messages.MessageType
  ( StartupPacketType (..),
    AuthFrontendType (..),
    AuthBackendType (..),
    CopyMessageType (..),
    FrontendSessionType (..),
    BackendSessionType (..),
  )
where

import Data.Kind (Type)
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Atom (..),
    Payload (..),
  )
import Database.PostgreSQL.Protocol.Messages.Types
  ( StructuredValueType
      ( BackendKeyType,
        BindBodyType,
        CopyResponseType,
        NotificationType,
        RowFieldType
      ),
    ValueType
      ( FormatCodeType,
        NamedObjectType,
        OidType,
        ParameterStatusValueType,
        ProtocolVersionType,
        TransactionStatusType
      ),
  )
import GHC.TypeLits (Nat)

type StartupPacketType :: Payload -> Type
data StartupPacketType payload where
  StartupMessageType ::
    StartupPacketType
      (PayloadField 'CStringPairListZeroTerminatedAtom)
  SSLRequestType :: StartupPacketType (PayloadStruct '[])
  GSSENCRequestType :: StartupPacketType (PayloadStruct '[])
  CancelRequestType ::
    StartupPacketType
      (PayloadField 'BackendKeyType)

deriving instance Eq (StartupPacketType payload)
deriving instance Show (StartupPacketType payload)

type AuthFrontendType :: Payload -> Type
data AuthFrontendType payload where
  PasswordMessageType ::
    AuthFrontendType
      (PayloadField 'CStringAtom)
  GSSResponseType ::
    AuthFrontendType
      (PayloadField 'RemainingBytesAtom)
  SASLInitialResponseType ::
    AuthFrontendType
      (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'MaybeBytes32Atom])
  SASLResponseType ::
    AuthFrontendType
      (PayloadField 'RemainingBytesAtom)

deriving instance Eq (AuthFrontendType payload)
deriving instance Show (AuthFrontendType payload)

type AuthBackendType :: Nat -> Payload -> Type
data AuthBackendType subcode payload where
  AuthenticationOkType :: AuthBackendType 0 (PayloadStruct '[])
  AuthenticationKerberosV5Type :: AuthBackendType 2 (PayloadStruct '[])
  AuthenticationCleartextPasswordType :: AuthBackendType 3 (PayloadStruct '[])
  AuthenticationMD5PasswordType ::
    AuthBackendType
      5
      (PayloadField ('FixedBytesAtom 4))
  AuthenticationGSSType :: AuthBackendType 7 (PayloadStruct '[])
  AuthenticationGSSContinueType ::
    AuthBackendType
      8
      (PayloadField 'RemainingBytesAtom)
  AuthenticationSSPIType :: AuthBackendType 9 (PayloadStruct '[])
  AuthenticationSASLType ::
    AuthBackendType
      10
      (PayloadField 'CStringListAtom)
  AuthenticationSASLContinueType ::
    AuthBackendType
      11
      (PayloadField 'RemainingBytesAtom)
  AuthenticationSASLFinalType ::
    AuthBackendType
      12
      (PayloadField 'RemainingBytesAtom)

deriving instance Eq (AuthBackendType subcode payload)
deriving instance Show (AuthBackendType subcode payload)

type CopyMessageType :: Nat -> Payload -> Type
data CopyMessageType tag payload where
  CopyDataType ::
    CopyMessageType
      100
      (PayloadField 'RemainingBytesAtom)
  CopyDoneType :: CopyMessageType 99 (PayloadStruct '[])
  CopyFailType ::
    CopyMessageType
      102
      (PayloadField 'CStringAtom)

deriving instance Eq (CopyMessageType tag payload)
deriving instance Show (CopyMessageType tag payload)

type FrontendSessionType :: Nat -> Payload -> Type
data FrontendSessionType tag payload where
  BindType ::
    FrontendSessionType
      66
      (PayloadField 'BindBodyType)
  CloseType ::
    FrontendSessionType
      67
      (PayloadStruct '[PayloadField 'NamedObjectType, PayloadField 'CStringAtom])
  DescribeType ::
    FrontendSessionType
      68
      (PayloadStruct '[PayloadField 'NamedObjectType, PayloadField 'CStringAtom])
  ExecuteType ::
    FrontendSessionType
      69
      (PayloadStruct '[PayloadField 'CStringAtom, PayloadField 'Int32Atom])
  FlushType :: FrontendSessionType 72 (PayloadStruct '[])
  FunctionCallType ::
    FrontendSessionType
      70
      ( PayloadStruct
          '[ PayloadField 'OidType,
             PayloadCounted16 ('PayloadField 'FormatCodeType),
             PayloadCounted16 ('PayloadField 'MaybeBytes32Atom),
             PayloadField 'FormatCodeType
           ]
      )
  ParseType ::
    FrontendSessionType
      80
      ( PayloadStruct
          '[ PayloadField 'CStringAtom,
             PayloadField 'CStringAtom,
             PayloadCounted16 ('PayloadField 'Word32Atom)
           ]
      )
  QueryType ::
    FrontendSessionType
      81
      (PayloadField 'CStringAtom)
  SyncType :: FrontendSessionType 83 (PayloadStruct '[])
  TerminateType :: FrontendSessionType 88 (PayloadStruct '[])

deriving instance Eq (FrontendSessionType tag payload)
deriving instance Show (FrontendSessionType tag payload)

type BackendSessionType :: Nat -> Payload -> Type
data BackendSessionType tag payload where
  BackendKeyDataType ::
    BackendSessionType
      75
      (PayloadField 'BackendKeyType)
  BindCompleteType :: BackendSessionType 50 (PayloadStruct '[])
  CloseCompleteType :: BackendSessionType 51 (PayloadStruct '[])
  CommandCompleteType ::
    BackendSessionType
      67
      (PayloadField 'CStringAtom)
  CopyInResponseType ::
    BackendSessionType
      71
      (PayloadField 'CopyResponseType)
  CopyOutResponseType ::
    BackendSessionType
      72
      (PayloadField 'CopyResponseType)
  CopyBothResponseType ::
    BackendSessionType
      87
      (PayloadField 'CopyResponseType)
  DataRowType ::
    BackendSessionType
      68
      (PayloadCounted16 ('PayloadField 'MaybeBytes32Atom))
  EmptyQueryResponseType :: BackendSessionType 73 (PayloadStruct '[])
  ErrorResponseType ::
    BackendSessionType
      69
      (PayloadField 'ByteCStringPairListZeroTerminatedAtom)
  FunctionCallResponseType ::
    BackendSessionType
      86
      (PayloadField 'MaybeBytes32Atom)
  NegotiateProtocolVersionType ::
    BackendSessionType
      118
      (PayloadStruct '[PayloadField 'ProtocolVersionType, PayloadField 'CStringListAtom])
  NoDataType :: BackendSessionType 110 (PayloadStruct '[])
  NoticeResponseType ::
    BackendSessionType
      78
      (PayloadField 'ByteCStringPairListZeroTerminatedAtom)
  NotificationResponseType ::
    BackendSessionType
      65
      (PayloadField 'NotificationType)
  ParameterDescriptionType ::
    BackendSessionType
      116
      (PayloadCounted16 ('PayloadField 'OidType))
  ParameterStatusType ::
    BackendSessionType
      83
      (PayloadField 'ParameterStatusValueType)
  ParseCompleteType :: BackendSessionType 49 (PayloadStruct '[])
  PortalSuspendedType :: BackendSessionType 115 (PayloadStruct '[])
  ReadyForQueryType ::
    BackendSessionType
      90
      (PayloadField 'TransactionStatusType)
  RowDescriptionType ::
    BackendSessionType
      84
      (PayloadCounted16 ('PayloadField 'RowFieldType))

deriving instance Eq (BackendSessionType tag payload)
deriving instance Show (BackendSessionType tag payload)
