{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeAbstractions #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | A phase-oriented codec prototype for PostgreSQL protocol messages.
--
-- The first iteration of this module kept one global semantic 'Message' GADT
-- and then reconstructed protocol phases with lightweight wrappers in the
-- codec. That worked, but the structure was upside down: the protocol is
-- phase-oriented already, so the message layer should say so directly.
--
-- The current design therefore assumes:
--
-- * the core message AST is already split into startup/auth/copy/session
--   families in [Messages.hs](/Users/michal/supabase/Development/haskell-native-pg/src/Database/PostgreSQL/Protocol/Messages.hs)
-- * the outer connection state machine chooses which parser family is active
-- * the codec only needs to solve the irregularities /inside/ each family
--
-- That gives us six small surfaces instead of one giant dispatch problem:
--
-- * 'StartupPacket' for startup/request traffic with 32-bit selectors
-- * 'AuthFrontendMessage' for frontend @'p'@ authentication replies
-- * 'AuthBackendMessage' for backend @'R'@ authentication responses
-- * 'CopyMessage' for copy-stream traffic shared by both peers
-- * 'FrontendSessionMessage' for ordinary tagged frontend session messages
-- * 'BackendSessionMessage' for ordinary tagged backend session messages
--
-- The payload DSL is still useful for the message families whose bodies really
-- are typed products. Startup packets are intentionally handled with dedicated
-- code because their outer framing shape is genuinely different.
module Database.PostgreSQL.Protocol.Codec
  ( Atom (..),
    Payload (..),
    PayloadOf,
    DenotationOf,
    PayloadCodec (..),
    WireFrame (..),
    PasswordTagContext (..),
    serializeStartupPacket,
    parseStartupPacket,
    serializeAuthFrontendMessage,
    parseAuthFrontendMessage,
    serializeAuthBackendMessage,
    parseAuthBackendMessage,
    serializeCopyMessage,
    parseCopyMessage,
    serializeFrontendSessionMessage,
    parseFrontendSessionMessage,
    serializeBackendSessionMessage,
    parseBackendSessionMessage,
    renderWireFrame,
  )
where

import Data.Bits ((.|.), shiftL, shiftR)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as LBS
import Control.Monad (replicateM)
import Data.Int (Int16, Int32)
import GHC.Exts (proxy#)
import Data.Kind (Type)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding (decodeUtf8', encodeUtf8)
import Data.Word (Word32, Word8)
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Atom (..),
    DenotationOf,
    Payload (..),
    PayloadValue,
  )
import Database.PostgreSQL.Protocol.Messages
  ( AuthBackendMessage (..),
    AuthFrontendMessage (..),
    BackendSessionMessage (..),
    CopyMessage (..),
    FrontendSessionMessage (..),
    SomeAuthBackendMessage (..),
    SomeAuthFrontendMessage (..),
    SomeBackendSessionMessage (..),
    SomeCopyMessage (..),
    SomeFrontendSessionMessage (..),
    SomeStartupPacket (..),
    StartupPacket (..),
    backendSessionMessageName,
    frontendSessionMessageName,
  )
import Database.PostgreSQL.Protocol.Messages.MessageType
import Database.PostgreSQL.Protocol.Messages.Types
  ( BackendKey,
    BindBody (..),
    CopyFormat (..),
    FormatCode (..),
    NamedObject (..),
    Notification (..),
    ParameterStatusValue,
    ProcessId,
    ProtocolVersion (..),
    RowField (..),
    Scalar (..),
    StructuredValueType
      ( BackendKeyType,
        BindBodyType,
        CopyResponseType,
        NotificationType,
        RowFieldType
      ),
    StartupParameter,
    TransactionStatus (..),
    ValueType
      ( CopyFormatType,
        ErrorNoticeFieldType,
        FormatCodeType,
        NamedObjectType,
        OidType,
        ParameterStatusValueType,
        ProcessIdType,
        ProtocolVersionType,
        SecretKeyType,
        StartupParameterType,
        TransactionStatusType
      ),
    cancelRequestCode,
    gssEncRequestCode,
    protocolVersionCode,
    sslRequestCode,
  )
import GHC.TypeLits (KnownNat, natVal')
import Data.Maybe

-- | A raw framed PostgreSQL packet without the outer length field.
--
-- Keeping the header and body split lets us focus on message encoding and
-- decoding separately from socket framing. The parser/serializer functions in
-- this module work with 'WireFrame', and 'renderWireFrame' turns that structure
-- back into exact protocol bytes.
data WireFrame
  = TaggedFrame Word8 ByteString
  | StartupFrame Word32 ByteString
  deriving (Eq, Show)

-- | Render a 'WireFrame' into exact PostgreSQL wire bytes, including the
-- length field that comes after the optional tag byte.
renderWireFrame :: WireFrame -> ByteString
renderWireFrame wireFrame =
  LBS.toStrict $
    BB.toLazyByteString $
      case wireFrame of
        TaggedFrame tag body ->
          BB.word8 tag
            <> BB.int32BE (fromIntegral (BS.length body + 4))
            <> BB.byteString body
        StartupFrame code body ->
          BB.int32BE (fromIntegral (BS.length body + 8))
            <> BB.word32BE code
            <> BB.byteString body

-- | The frontend @'p'@ tag is ambiguous on purpose. PostgreSQL reuses it for
-- several authentication steps, so the parser must be told which step the
-- connection state machine expects next.
data PasswordTagContext
  = ExpectPasswordMessage
  | ExpectGssResponse
  | ExpectSaslInitialResponse
  | ExpectSaslResponse
  deriving (Eq, Show)

-- | Recover the interpreted Haskell payload type from a payload-indexed
-- singleton or from the semantic value/message indexed by that singleton.
--
-- The key idea is the same one explored in
-- [Copy.hs](/Users/michal/supabase/Development/haskell-native-pg/src/Database/PostgreSQL/Protocol/Codec/Copy.hs):
-- the singleton already /carries/ the payload schema, so the codec classes do
-- not need a separate payload parameter in their own API.

type PayloadOf :: k -> Type
type family PayloadOf a where
  PayloadOf (payload :: Payload) = PayloadValue payload
  PayloadOf (singleton :: sing (payload :: Payload)) = PayloadValue payload
  PayloadOf (semantic (singleton :: k)) = PayloadOf singleton

type PayloadConstructor :: Payload -> Type -> Type
type family PayloadConstructor (payload :: Payload) result where
  PayloadConstructor ('PayloadField atom) result = DenotationOf atom -> result
  PayloadConstructor ('PayloadCounted16 payload) result = [PayloadValue payload] -> result
  PayloadConstructor ('PayloadStruct '[]) result = result
  PayloadConstructor ('PayloadStruct '[payload]) result = PayloadConstructor payload result
  PayloadConstructor ('PayloadStruct (left ': right ': payloads)) result =
    PayloadValue left -> PayloadConstructor ('PayloadStruct (right ': payloads)) result

type ScalarConstructorOf :: k -> Type
type family ScalarConstructorOf singleton where
  ScalarConstructorOf (singleton :: sing payload) = PayloadConstructor payload (Scalar singleton)

type ConstructorOf :: k -> Type
type family ConstructorOf singleton where
  ConstructorOf 'BindBodyType =
    Text ->
    Text ->
    [FormatCode 'FormatCodeType] ->
    [Maybe ByteString] ->
    [FormatCode 'FormatCodeType] ->
    BindBody 'BindBodyType
  ConstructorOf 'NotificationType =
    ProcessId -> Text -> Text -> Notification 'NotificationType
  ConstructorOf 'AuthenticationOkType = AuthBackendMessage 'AuthenticationOkType
  ConstructorOf 'AuthenticationKerberosV5Type = AuthBackendMessage 'AuthenticationKerberosV5Type
  ConstructorOf 'AuthenticationCleartextPasswordType = AuthBackendMessage 'AuthenticationCleartextPasswordType
  ConstructorOf 'AuthenticationMD5PasswordType =
    ByteString -> AuthBackendMessage 'AuthenticationMD5PasswordType
  ConstructorOf 'AuthenticationGSSType = AuthBackendMessage 'AuthenticationGSSType
  ConstructorOf 'AuthenticationGSSContinueType =
    ByteString -> AuthBackendMessage 'AuthenticationGSSContinueType
  ConstructorOf 'AuthenticationSSPIType = AuthBackendMessage 'AuthenticationSSPIType
  ConstructorOf 'AuthenticationSASLType =
    [Text] -> AuthBackendMessage 'AuthenticationSASLType
  ConstructorOf 'AuthenticationSASLContinueType =
    ByteString -> AuthBackendMessage 'AuthenticationSASLContinueType
  ConstructorOf 'AuthenticationSASLFinalType =
    ByteString -> AuthBackendMessage 'AuthenticationSASLFinalType
  ConstructorOf 'PasswordMessageType = Text -> AuthFrontendMessage 'PasswordMessageType
  ConstructorOf 'GSSResponseType = ByteString -> AuthFrontendMessage 'GSSResponseType
  ConstructorOf 'SASLInitialResponseType =
    Text -> Maybe ByteString -> AuthFrontendMessage 'SASLInitialResponseType
  ConstructorOf 'SASLResponseType = ByteString -> AuthFrontendMessage 'SASLResponseType
  ConstructorOf 'CopyDataType = ByteString -> CopyMessage 'CopyDataType
  ConstructorOf 'CopyDoneType = CopyMessage 'CopyDoneType
  ConstructorOf 'CopyFailType = Text -> CopyMessage 'CopyFailType
  ConstructorOf 'QueryType = Text -> FrontendSessionMessage 'QueryType
  ConstructorOf 'SyncType = FrontendSessionMessage 'SyncType
  ConstructorOf 'TerminateType = FrontendSessionMessage 'TerminateType
  ConstructorOf 'ParameterStatusType =
    ParameterStatusValue -> BackendSessionMessage 'ParameterStatusType
  ConstructorOf 'ReadyForQueryType =
    TransactionStatus 'TransactionStatusType -> BackendSessionMessage 'ReadyForQueryType
  ConstructorOf singleton = ScalarConstructorOf singleton

class HasConstructor (singleton :: k) where
  constructorOf :: ConstructorOf singleton

class BuildPayloadConstructor (payload :: Payload) result where
  buildPayloadConstructor :: (PayloadValue payload -> result) -> PayloadConstructor payload result

instance BuildPayloadConstructor ('PayloadField atom) result where
  buildPayloadConstructor = id

instance BuildPayloadConstructor ('PayloadCounted16 payload) result where
  buildPayloadConstructor = id

instance BuildPayloadConstructor ('PayloadStruct '[]) result where
  buildPayloadConstructor finish = finish ()

instance BuildPayloadConstructor payload result => BuildPayloadConstructor ('PayloadStruct '[payload]) result where
  buildPayloadConstructor = buildPayloadConstructor @payload

instance
  BuildPayloadConstructor ('PayloadStruct (right ': payloads)) result =>
  BuildPayloadConstructor ('PayloadStruct (left ': right ': payloads)) result
  where
  buildPayloadConstructor finish left =
    buildPayloadConstructor @('PayloadStruct (right ': payloads)) (\rest -> finish (left, rest))

class ApplyConstructor (payload :: Payload) f where
  type AppliedConstructor payload f :: Type
  applyConstructor :: f -> PayloadValue payload -> AppliedConstructor payload f

class ApplySingletonConstructor (singleton :: k) f where
  type AppliedSingletonConstructor singleton f :: Type
  applySingletonConstructor :: f -> PayloadOf singleton -> AppliedSingletonConstructor singleton f

instance ApplyConstructor ('PayloadStruct '[]) r where
  type AppliedConstructor ('PayloadStruct '[]) r = r
  applyConstructor = const

instance
  ( argument ~ DenotationOf atom
  ) =>
  ApplyConstructor ('PayloadField atom) (argument -> result)
  where
  type AppliedConstructor ('PayloadField atom) (argument -> result) = result
  applyConstructor constructor = constructor

instance
  ( elements ~ [PayloadValue payload]
  ) =>
  ApplyConstructor ('PayloadCounted16 payload) (elements -> result)
  where
  type AppliedConstructor ('PayloadCounted16 payload) (elements -> result) = result
  applyConstructor constructor = constructor

instance ApplyConstructor payload f => ApplyConstructor ('PayloadStruct '[payload]) f where
  type AppliedConstructor ('PayloadStruct '[payload]) f = AppliedConstructor payload f
  applyConstructor = applyConstructor @payload

instance
  ( argument ~ PayloadValue left,
    ApplyConstructor ('PayloadStruct (right ': payloads)) f
  ) =>
  ApplyConstructor ('PayloadStruct (left ': right ': payloads)) (argument -> f)
  where
  type AppliedConstructor ('PayloadStruct (left ': right ': payloads)) (argument -> f) =
    AppliedConstructor ('PayloadStruct (right ': payloads)) f
  applyConstructor constructor (argument, rest) =
    applyConstructor @('PayloadStruct (right ': payloads)) (constructor argument) rest

instance ApplyConstructor payload f => ApplySingletonConstructor (singleton :: sing payload) f where
  type AppliedSingletonConstructor (singleton :: sing payload) f = AppliedConstructor payload f
  applySingletonConstructor = applyConstructor @payload

class HasScalarConstructor (singleton :: k)

instance HasScalarConstructor 'OidType
instance HasScalarConstructor 'ProcessIdType
instance HasScalarConstructor 'SecretKeyType
instance HasScalarConstructor 'BackendKeyType
instance HasScalarConstructor 'StartupParameterType
instance HasScalarConstructor 'CopyResponseType
instance HasScalarConstructor 'ErrorNoticeFieldType
instance HasScalarConstructor 'ParameterStatusValueType

instance
  {-# OVERLAPPABLE #-}
  forall sing (payload :: Payload) (singleton :: sing payload).
  ( HasScalarConstructor singleton,
    ConstructorOf singleton ~ ScalarConstructorOf singleton,
    BuildPayloadConstructor payload (Scalar singleton)
  ) =>
  HasConstructor singleton
  where
  constructorOf =
    buildPayloadConstructor @payload (\value -> Scalar value :: Scalar singleton)

instance HasConstructor 'BindBodyType where
  constructorOf = BindBody

instance HasConstructor 'NotificationType where
  constructorOf = Notification

instance HasConstructor 'AuthenticationOkType where
  constructorOf = AuthenticationOk

instance HasConstructor 'AuthenticationKerberosV5Type where
  constructorOf = AuthenticationKerberosV5

instance HasConstructor 'AuthenticationCleartextPasswordType where
  constructorOf = AuthenticationCleartextPassword

instance HasConstructor 'AuthenticationMD5PasswordType where
  constructorOf = AuthenticationMD5Password

instance HasConstructor 'AuthenticationGSSType where
  constructorOf = AuthenticationGSS

instance HasConstructor 'AuthenticationGSSContinueType where
  constructorOf = AuthenticationGSSContinue

instance HasConstructor 'AuthenticationSSPIType where
  constructorOf = AuthenticationSSPI

instance HasConstructor 'AuthenticationSASLType where
  constructorOf = AuthenticationSASL

instance HasConstructor 'AuthenticationSASLContinueType where
  constructorOf = AuthenticationSASLContinue

instance HasConstructor 'AuthenticationSASLFinalType where
  constructorOf = AuthenticationSASLFinal

instance HasConstructor 'PasswordMessageType where
  constructorOf = PasswordMessage

instance HasConstructor 'GSSResponseType where
  constructorOf = GSSResponse

instance HasConstructor 'SASLInitialResponseType where
  constructorOf = SASLInitialResponse

instance HasConstructor 'SASLResponseType where
  constructorOf = SASLResponse

instance HasConstructor 'CopyDataType where
  constructorOf = CopyData

instance HasConstructor 'CopyDoneType where
  constructorOf = CopyDone

instance HasConstructor 'CopyFailType where
  constructorOf = CopyFail

instance HasConstructor 'QueryType where
  constructorOf = Query

instance HasConstructor 'SyncType where
  constructorOf = Sync

instance HasConstructor 'TerminateType where
  constructorOf = Terminate

instance HasConstructor 'ParameterStatusType where
  constructorOf = ParameterStatus

instance HasConstructor 'ReadyForQueryType where
  constructorOf = ReadyForQuery

class
  ( InterpretPayload singleton
  ) =>
  PayloadCodec (singleton :: k)
  where
  toPayloadValue :: DenotationOf singleton -> PayloadOf singleton
  fromPayloadValue :: PayloadOf singleton -> Either String (DenotationOf singleton)
  default fromPayloadValue ::
    ( HasConstructor singleton,
      ApplySingletonConstructor singleton (ConstructorOf singleton),
      AppliedSingletonConstructor singleton (ConstructorOf singleton) ~ DenotationOf singleton
    ) =>
    PayloadOf singleton -> Either String (DenotationOf singleton)
  fromPayloadValue = Right . applySingletonConstructor @k @singleton (constructorOf @k @singleton)

instance PayloadCodec 'OidType where
  toPayloadValue = unScalar

instance PayloadCodec 'ProcessIdType where
  toPayloadValue = unScalar

instance PayloadCodec 'SecretKeyType where
  toPayloadValue = unScalar

instance PayloadCodec 'BackendKeyType where
  toPayloadValue = unScalar

instance PayloadCodec 'ProtocolVersionType where
  toPayloadValue = protocolVersionCode
  fromPayloadValue = Right . protocolVersionFromCode

instance PayloadCodec 'StartupParameterType where
  toPayloadValue = unScalar

instance PayloadCodec 'FormatCodeType where
  toPayloadValue = encodeFormatCode
  fromPayloadValue = decodeFormatCode

instance PayloadCodec 'CopyFormatType where
  toPayloadValue = encodeCopyFormat
  fromPayloadValue = decodeCopyFormat

instance PayloadCodec 'NamedObjectType where
  toPayloadValue = encodeNamedObject
  fromPayloadValue = decodeNamedObject

instance PayloadCodec 'BindBodyType where
  toPayloadValue bindBody =
    ( bindPortalName bindBody,
      ( bindStatementName bindBody,
        ( bindParameterFormats bindBody,
          (bindParameterValues bindBody, bindResultFormats bindBody)
        )
      )
    )

instance PayloadCodec 'CopyResponseType where
  toPayloadValue = unScalar

instance PayloadCodec 'ErrorNoticeFieldType where
  toPayloadValue = unScalar
  fromPayloadValue = Right . Scalar

instance PayloadCodec 'NotificationType where
  toPayloadValue notification =
    ( notificationProcessId notification,
      (notificationChannel notification, notificationPayload notification)
    )

instance PayloadCodec 'ParameterStatusValueType where
  toPayloadValue = unScalar

instance PayloadCodec 'TransactionStatusType where
  toPayloadValue = encodeTransactionStatus
  fromPayloadValue = decodeTransactionStatus

instance PayloadCodec 'RowFieldType where
  toPayloadValue rowField =
    ( rowFieldName rowField,
      ( maybe 0 unScalar (rowFieldTableOid rowField),
        ( fromMaybe 0 (rowFieldColumnAttributeNumber rowField),
          ( rowFieldTypeOid rowField,
            ( rowFieldTypeSize rowField,
              (rowFieldTypeModifier rowField, rowFieldFormat rowField)
            )
          )
        )
      )
    )
  fromPayloadValue (name, (tableOid, (columnAttributeNumber, (typeOid, (typeSize, (typeModifier, formatCode)))))) =
    Right
      RowField
        { rowFieldName = name,
          rowFieldTableOid =
            if tableOid == 0 then Nothing else Just (Scalar tableOid),
          rowFieldColumnAttributeNumber =
            if columnAttributeNumber == 0 then Nothing else Just columnAttributeNumber,
          rowFieldTypeOid = typeOid,
          rowFieldTypeSize = typeSize,
          rowFieldTypeModifier = typeModifier,
          rowFieldFormat = formatCode
        }

instance PayloadCodec 'AuthenticationOkType where
  toPayloadValue AuthenticationOk = ()

instance PayloadCodec 'AuthenticationKerberosV5Type where
  toPayloadValue AuthenticationKerberosV5 = ()

instance PayloadCodec 'AuthenticationCleartextPasswordType where
  toPayloadValue AuthenticationCleartextPassword = ()

instance PayloadCodec 'AuthenticationMD5PasswordType where
  toPayloadValue (AuthenticationMD5Password salt) = salt

instance PayloadCodec 'AuthenticationGSSType where
  toPayloadValue AuthenticationGSS = ()

instance PayloadCodec 'AuthenticationGSSContinueType where
  toPayloadValue (AuthenticationGSSContinue payload) = payload

instance PayloadCodec 'AuthenticationSSPIType where
  toPayloadValue AuthenticationSSPI = ()

instance PayloadCodec 'AuthenticationSASLType where
  toPayloadValue (AuthenticationSASL mechanisms) = mechanisms

instance PayloadCodec 'AuthenticationSASLContinueType where
  toPayloadValue (AuthenticationSASLContinue payload) = payload

instance PayloadCodec 'AuthenticationSASLFinalType where
  toPayloadValue (AuthenticationSASLFinal payload) = payload

instance PayloadCodec 'PasswordMessageType where
  toPayloadValue (PasswordMessage password) = password

instance PayloadCodec 'GSSResponseType where
  toPayloadValue (GSSResponse bytes) = bytes

instance PayloadCodec 'SASLInitialResponseType where
  toPayloadValue (SASLInitialResponse mechanism initialResponse) =
    (mechanism, initialResponse)

instance PayloadCodec 'SASLResponseType where
  toPayloadValue (SASLResponse bytes) = bytes

instance PayloadCodec 'CopyDataType where
  toPayloadValue (CopyData bytes) = bytes

instance PayloadCodec 'CopyDoneType where
  toPayloadValue CopyDone = ()

instance PayloadCodec 'CopyFailType where
  toPayloadValue (CopyFail message) = message

instance PayloadCodec 'QueryType where
  toPayloadValue (Query sql) = sql

instance PayloadCodec 'SyncType where
  toPayloadValue Sync = ()

instance PayloadCodec 'TerminateType where
  toPayloadValue Terminate = ()

instance PayloadCodec 'ParameterStatusType where
  toPayloadValue (ParameterStatus status) = status

instance PayloadCodec 'ReadyForQueryType where
  toPayloadValue (ReadyForQuery status) = status

-- | Serialize a startup-family packet.
--
-- Startup traffic is the one place where we /do not/ try to reuse the generic
-- tagged-message machinery. There is no tag byte at all, and the selector
-- lives in the 32-bit startup code field instead.
serializeStartupPacket :: StartupPacket messageType -> Either String WireFrame
serializeStartupPacket startupPacket =
  case startupPacket of
    StartupMessage protocolVersion parameters ->
      StartupFrame (protocolVersionCode protocolVersion)
        <$> serializeStartupParameters parameters
    SSLRequest ->
      pure (StartupFrame sslRequestCode BS.empty)
    GSSENCRequest ->
      pure (StartupFrame gssEncRequestCode BS.empty)
    CancelRequest backendKey ->
      StartupFrame cancelRequestCode
        <$> serializeCancelRequestBody backendKey

parseStartupPacket :: WireFrame -> Either String SomeStartupPacket
parseStartupPacket wireFrame =
  case wireFrame of
    TaggedFrame actualTag _ ->
      Left $
        "expected startup frame, got tagged frame with tag "
          <> show actualTag
    StartupFrame code body
      | code == sslRequestCode -> do
          ensureEmptyBody "SSLRequest" body
          pure (SomeStartupPacket SSLRequest)
      | code == gssEncRequestCode -> do
          ensureEmptyBody "GSSENCRequest" body
          pure (SomeStartupPacket GSSENCRequest)
      | code == cancelRequestCode -> do
          backendKey <- parseCancelRequestBody body
          pure (SomeStartupPacket (CancelRequest backendKey))
      | otherwise -> do
          parameters <- parseStartupParameters body
          pure
            ( SomeStartupPacket
                (StartupMessage (protocolVersionFromCode code) parameters)
            )

-- | Serialize a frontend authentication reply. The outer tag is always @'p'@.
serializeAuthFrontendMessage ::
  forall payload (messageType :: AuthFrontendType payload).
  PayloadCodec messageType =>
  AuthFrontendMessage messageType ->
  Either String WireFrame
serializeAuthFrontendMessage =
  serializeTaggedPayload @_ @messageType authFrontendTag

parseAuthFrontendMessage ::
  PasswordTagContext ->
  WireFrame ->
  Either String SomeAuthFrontendMessage
parseAuthFrontendMessage passwordTagContext wireFrame =
  case passwordTagContext of
    ExpectPasswordMessage ->
      SomeAuthFrontendMessage
        <$> parseTaggedPayload @_ @'PasswordMessageType authFrontendTag wireFrame
    ExpectGssResponse ->
      SomeAuthFrontendMessage
        <$> parseTaggedPayload @_ @'GSSResponseType authFrontendTag wireFrame
    ExpectSaslInitialResponse ->
      SomeAuthFrontendMessage
        <$> parseTaggedPayload @_ @'SASLInitialResponseType authFrontendTag wireFrame
    ExpectSaslResponse ->
      SomeAuthFrontendMessage
        <$> parseTaggedPayload @_ @'SASLResponseType authFrontendTag wireFrame

-- | Serialize a backend authentication message.
--
-- Backend auth responses are irregular in a different way: every branch uses
-- outer tag @'R'@, and the actual selector is the first Int32 of the body.
serializeAuthBackendMessage ::
  forall subcode payload (messageType :: AuthBackendType subcode payload).
  ( KnownNat subcode,
    PayloadCodec messageType
  ) =>
  AuthBackendMessage messageType ->
  Either String WireFrame
serializeAuthBackendMessage =
  serializeTaggedInt32PrefixedPayload @_ @messageType
    authBackendTag
    (natAsInt32Literal @subcode)

parseAuthBackendMessage :: WireFrame -> Either String SomeAuthBackendMessage
parseAuthBackendMessage wireFrame = do
  authBody <- expectTaggedBody authBackendTag wireFrame
  (subcode, payloadBody) <- runBodyParser parseInt32 authBody
  case subcode of
    0 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationOkType payloadBody
    2 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationKerberosV5Type payloadBody
    3 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationCleartextPasswordType payloadBody
    5 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationMD5PasswordType payloadBody
    7 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationGSSType payloadBody
    8 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationGSSContinueType payloadBody
    9 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationSSPIType payloadBody
    10 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationSASLType payloadBody
    11 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationSASLContinueType payloadBody
    12 ->
      SomeAuthBackendMessage
        <$> parseKnownMessage @_ @'AuthenticationSASLFinalType payloadBody
    _ ->
      Left ("unknown authentication subcode: " <> show subcode)

-- | Copy-stream traffic is split out as its own family because its constructors
-- are shared by both peers and do not fit naturally inside either ordinary
-- session family.
serializeCopyMessage ::
  forall tag payload (messageType :: CopyMessageType tag payload).
  ( KnownNat tag,
    PayloadCodec messageType
  ) =>
  CopyMessage messageType ->
  Either String WireFrame
serializeCopyMessage =
  serializeTaggedPayload @_ @messageType (natAsWord8 @tag)

parseCopyMessage :: WireFrame -> Either String SomeCopyMessage
parseCopyMessage wireFrame =
  case wireFrame of
    TaggedFrame 100 _ ->
      SomeCopyMessage
        <$> parseTaggedPayload @_ @'CopyDataType (natAsWord8 @100) wireFrame
    TaggedFrame 99 _ ->
      SomeCopyMessage
        <$> parseTaggedPayload @_ @'CopyDoneType (natAsWord8 @99) wireFrame
    TaggedFrame 102 _ ->
      SomeCopyMessage
        <$> parseTaggedPayload @_ @'CopyFailType (natAsWord8 @102) wireFrame
    TaggedFrame actualTag _ ->
      Left ("unknown copy message tag: " <> show actualTag)
    StartupFrame {} ->
      Left "expected tagged copy message, got startup frame"

-- | Serialize an ordinary frontend session message.
--
-- This prototype intentionally implements only a representative subset of the
-- frontend session family. The split into phase-specific message types still
-- pays off even when codec coverage is incremental, because unsupported cases
-- are now local and explicit.
serializeFrontendSessionMessage ::
  FrontendSessionMessage messageType ->
  Either String WireFrame
serializeFrontendSessionMessage sessionMessage =
  case sessionMessage of
    message@Query {} ->
      serializeFrontendSessionTagged message
    message@Sync ->
      serializeFrontendSessionTagged message
    message@Terminate ->
      serializeFrontendSessionTagged message
    unsupported ->
      Left $
        "serializeFrontendSessionMessage: "
          <> frontendSessionMessageName unsupported
          <> " is not implemented in this codec prototype"

parseFrontendSessionMessage :: WireFrame -> Either String SomeFrontendSessionMessage
parseFrontendSessionMessage wireFrame =
  case wireFrame of
    TaggedFrame 81 _ ->
      SomeFrontendSessionMessage
        <$> parseTaggedPayload @_ @'QueryType (natAsWord8 @81) wireFrame
    TaggedFrame 83 _ ->
      SomeFrontendSessionMessage
        <$> parseTaggedPayload @_ @'SyncType (natAsWord8 @83) wireFrame
    TaggedFrame 88 _ ->
      SomeFrontendSessionMessage
        <$> parseTaggedPayload @_ @'TerminateType (natAsWord8 @88) wireFrame
    TaggedFrame actualTag _ ->
      Left ("unknown frontend session tag: " <> show actualTag)
    StartupFrame {} ->
      Left "expected tagged frontend session message, got startup frame"

serializeBackendSessionMessage ::
  BackendSessionMessage messageType ->
  Either String WireFrame
serializeBackendSessionMessage sessionMessage =
  case sessionMessage of
    message@ParameterStatus {} ->
      serializeBackendSessionTagged message
    message@ReadyForQuery {} ->
      serializeBackendSessionTagged message
    unsupported ->
      Left $
        "serializeBackendSessionMessage: "
          <> backendSessionMessageName unsupported
          <> " is not implemented in this codec prototype"

parseBackendSessionMessage :: WireFrame -> Either String SomeBackendSessionMessage
parseBackendSessionMessage wireFrame =
  case wireFrame of
    TaggedFrame 83 _ ->
      SomeBackendSessionMessage
        <$> parseTaggedPayload @_ @'ParameterStatusType (natAsWord8 @83) wireFrame
    TaggedFrame 90 _ ->
      SomeBackendSessionMessage
        <$> parseTaggedPayload @_ @'ReadyForQueryType (natAsWord8 @90) wireFrame
    TaggedFrame actualTag _ ->
      Left ("unknown backend session tag: " <> show actualTag)
    StartupFrame {} ->
      Left "expected tagged backend session message, got startup frame"

encodeTransactionStatus :: TransactionStatus 'TransactionStatusType -> Word8
encodeTransactionStatus = \case
  TxIdle -> 73
  TxInTransaction -> 84
  TxFailedTransaction -> 69

decodeTransactionStatus :: Word8 -> Either String (TransactionStatus 'TransactionStatusType)
decodeTransactionStatus = \case
  73 -> Right TxIdle
  84 -> Right TxInTransaction
  69 -> Right TxFailedTransaction
  other ->
    Left ("unknown ReadyForQuery status byte: " <> show other)

encodeNamedObject :: NamedObject 'NamedObjectType -> Word8
encodeNamedObject = \case
  PreparedStatement -> 83
  Portal -> 80

decodeNamedObject :: Word8 -> Either String (NamedObject 'NamedObjectType)
decodeNamedObject = \case
  83 -> Right PreparedStatement
  80 -> Right Portal
  other ->
    Left ("unknown named object tag: " <> show other)

encodeFormatCode :: FormatCode 'FormatCodeType -> Int16
encodeFormatCode = \case
  TextFormat -> 0
  BinaryFormat -> 1

decodeFormatCode :: Int16 -> Either String (FormatCode 'FormatCodeType)
decodeFormatCode = \case
  0 -> Right TextFormat
  1 -> Right BinaryFormat
  other ->
    Left ("unknown format code: " <> show other)

encodeCopyFormat :: CopyFormat 'CopyFormatType -> Word8
encodeCopyFormat = \case
  CopyText -> 0
  CopyBinary -> 1

decodeCopyFormat :: Word8 -> Either String (CopyFormat 'CopyFormatType)
decodeCopyFormat = \case
  0 -> Right CopyText
  1 -> Right CopyBinary
  other ->
    Left ("unknown copy format code: " <> show other)

authFrontendTag :: Word8
authFrontendTag = natAsWord8 @112

authBackendTag :: Word8
authBackendTag = natAsWord8 @82

serializeFrontendSessionTagged ::
  forall tag payload (messageType :: FrontendSessionType tag payload).
  ( KnownNat tag,
    PayloadCodec messageType
  ) =>
  FrontendSessionMessage messageType ->
  Either String WireFrame
serializeFrontendSessionTagged =
  serializeTaggedPayload @_ @messageType (natAsWord8 @tag)

serializeBackendSessionTagged ::
  forall tag payload (messageType :: BackendSessionType tag payload).
  ( KnownNat tag,
    PayloadCodec messageType
  ) =>
  BackendSessionMessage messageType ->
  Either String WireFrame
serializeBackendSessionTagged =
  serializeTaggedPayload @_ @messageType (natAsWord8 @tag)

class InterpretAtom (atom :: Atom) where
  parseAtom :: BodyParser (DenotationOf atom)
  serializeAtom :: DenotationOf atom -> Either String ByteString

class InterpretLeaf (a :: k) where
  parseLeaf :: BodyParser (DenotationOf a)
  serializeLeaf :: DenotationOf a -> Either String ByteString

instance InterpretAtom 'Int16Atom where
  parseAtom = parseInt16
  serializeAtom value =
    Right (LBS.toStrict (BB.toLazyByteString (BB.int16BE value)))

instance InterpretAtom 'Int32Atom where
  parseAtom = parseInt32
  serializeAtom value =
    Right (LBS.toStrict (BB.toLazyByteString (BB.int32BE value)))

instance InterpretAtom 'Word32Atom where
  parseAtom = parseWord32
  serializeAtom value =
    Right (LBS.toStrict (BB.toLazyByteString (BB.word32BE value)))

instance InterpretAtom 'ByteAtom where
  parseAtom = parseByte
  serializeAtom value = Right (BS.singleton value)

instance InterpretAtom 'CStringAtom where
  parseAtom = parseCString
  serializeAtom value = Right (encodeUtf8 value <> BS.singleton 0)

instance InterpretAtom 'CStringListAtom where
  parseAtom = parseCStringList
  serializeAtom values = do
    mapM_ rejectEmpty values
    pure $
      BS.concat (map (\value -> encodeUtf8 value <> BS.singleton 0) values)
        <> BS.singleton 0
    where
      rejectEmpty value
        | T.null value =
            Left "CStringListAtom cannot encode empty elements because the empty string is the list terminator"
        | otherwise =
            Right ()

instance KnownNat n => InterpretAtom ('FixedBytesAtom n) where
  parseAtom = do
    expectedLength <- parserNatAsInt @n
    takeExactly expectedLength
  serializeAtom value = do
    expectedLength <- natAsInt @n
    if BS.length value == expectedLength
      then Right value
      else
        Left $
          "expected exactly "
            <> show expectedLength
            <> " bytes, got "
            <> show (BS.length value)

instance InterpretAtom 'RemainingBytesAtom where
  parseAtom = takeRemaining
  serializeAtom = Right

instance InterpretAtom 'MaybeBytes32Atom where
  parseAtom = do
    lengthOrMissing <- parseInt32
    case compare lengthOrMissing (-1) of
      EQ -> pure Nothing
      LT ->
        failParser $
          "negative length " <> show lengthOrMissing <> " is invalid for MaybeBytes32Atom"
      GT -> do
        bytes <- takeExactly (fromIntegral lengthOrMissing)
        pure (Just bytes)
  serializeAtom Nothing =
    Right (LBS.toStrict (BB.toLazyByteString (BB.int32BE (-1))))
  serializeAtom (Just value) =
    Right $
      LBS.toStrict
        ( BB.toLazyByteString $
            BB.int32BE (fromIntegral (BS.length value))
              <> BB.byteString value
        )

instance {-# OVERLAPPING #-} InterpretAtom atom => InterpretLeaf (atom :: Atom) where
  parseLeaf = parseAtom @atom
  serializeLeaf = serializeAtom @atom

instance {-# OVERLAPPABLE #-} PayloadCodec singleton => InterpretLeaf singleton where
  parseLeaf =
    parsePayloadOf @singleton >>= either failParser pure . fromPayloadValue @_ @singleton

  serializeLeaf =
    encodePayloadOf @singleton . toPayloadValue @_ @singleton

instance InterpretAtom 'CStringPairListZeroTerminatedAtom where
  parseAtom = parseCStringPairsZeroTerminated
  serializeAtom = serializeCStringPairsZeroTerminated

instance InterpretAtom 'ByteCStringPairListZeroTerminatedAtom where
  parseAtom = parseByteCStringPairsZeroTerminated
  serializeAtom = serializeByteCStringPairsZeroTerminated

class InterpretPayload t where
  parsePayload :: BodyParser (PayloadOf t)
  encodePayload :: PayloadOf t -> Either String ByteString

instance InterpretPayload payload => InterpretPayload (singleton :: sing (payload :: Payload)) where
  parsePayload = parsePayload @payload
  encodePayload = encodePayload @payload

instance forall k (atom :: k). InterpretLeaf atom => InterpretPayload ('PayloadField atom) where
  parsePayload = parseLeaf @k @atom
  encodePayload = serializeLeaf @k @atom

instance InterpretPayload payload => InterpretPayload ('PayloadCounted16 payload) where
  parsePayload = do
    count <- parseInt16
    if count < 0
      then failParser ("negative element count " <> show count <> " is invalid for PayloadCounted16")
      else replicateM (fromIntegral count) (parsePayload @payload)

  encodePayload values =
    if length values > fromIntegral (maxBound :: Int16)
      then
        Left $
          "PayloadCounted16 cannot encode "
            <> show (length values)
            <> " elements because the Int16 count would overflow"
      else do
        encodedValues <- traverse (encodePayload @payload) values
        pure $
          LBS.toStrict
            (BB.toLazyByteString (BB.int16BE (fromIntegral (length values))))
            <> BS.concat encodedValues

instance InterpretPayload ('PayloadStruct '[]) where
  parsePayload = pure ()
  encodePayload = const (Right BS.empty)

instance InterpretPayload payload => InterpretPayload ('PayloadStruct '[payload]) where
  parsePayload = parsePayload @payload
  encodePayload = encodePayload @payload

instance
  ( InterpretPayload left,
    InterpretPayload ('PayloadStruct (right ': payloads))
  ) =>
  InterpretPayload ('PayloadStruct (left ': right ': payloads))
  where
  parsePayload =
    (,) <$> parsePayload @left <*> parsePayload @('PayloadStruct (right ': payloads))

  encodePayload (fieldValue, restValue) =
    (<>) <$> encodePayload @left fieldValue <*> encodePayload @('PayloadStruct (right ': payloads)) restValue

newtype BodyParser a = BodyParser
  { runBodyParser :: ByteString -> Either String (a, ByteString)
  }

instance Functor BodyParser where
  fmap f parser =
    BodyParser $ \input -> do
      (value, rest) <- runBodyParser parser input
      pure (f value, rest)

instance Applicative BodyParser where
  pure value = BodyParser (\input -> Right (value, input))
  parserF <*> parserA =
    BodyParser $ \input -> do
      (f, rest) <- runBodyParser parserF input
      (a, finalRest) <- runBodyParser parserA rest
      pure (f a, finalRest)

instance Monad BodyParser where
  parser >>= f =
    BodyParser $ \input -> do
      (value, rest) <- runBodyParser parser input
      runBodyParser (f value) rest

failParser :: String -> BodyParser a
failParser message = BodyParser (\_ -> Left message)

parsePayloadOf ::
  forall t.
  InterpretPayload t =>
  BodyParser (PayloadOf t)
parsePayloadOf = parsePayload @t

encodePayloadOf ::
  forall t.
  InterpretPayload t =>
  PayloadOf t ->
  Either String ByteString
encodePayloadOf = encodePayload @t

serializeTaggedPayload ::
  forall k (messageType :: k).
  PayloadCodec messageType =>
  Word8 ->
  DenotationOf messageType ->
  Either String WireFrame
serializeTaggedPayload tag message =
  TaggedFrame tag <$> encodePayloadOf @messageType (toPayloadValue @_ @messageType message)

serializeTaggedInt32PrefixedPayload ::
  forall k (messageType :: k).
  PayloadCodec messageType =>
  Word8 ->
  Int32 ->
  DenotationOf messageType ->
  Either String WireFrame
serializeTaggedInt32PrefixedPayload tag discriminator message = do
  payload <- encodePayloadOf @messageType (toPayloadValue @_ @messageType message)
  pure $
    TaggedFrame
      tag
      ( LBS.toStrict (BB.toLazyByteString (BB.int32BE discriminator))
          <> payload
      )

parseKnownMessage ::
  forall k (messageType :: k).
  PayloadCodec messageType =>
  ByteString ->
  Either String (DenotationOf messageType)
parseKnownMessage body = do
  (payloadValue, rest) <- runBodyParser (parsePayloadOf @messageType) body
  if BS.null rest
    then fromPayloadValue @_ @messageType payloadValue
    else
      Left $
        "message parser left "
          <> show (BS.length rest)
          <> " unconsumed bytes"

parseTaggedPayload ::
  forall k (messageType :: k).
  PayloadCodec messageType =>
  Word8 ->
  WireFrame ->
  Either String (DenotationOf messageType)
parseTaggedPayload expectedTag wireFrame =
  parseKnownMessage @_ @messageType =<< expectTaggedBody expectedTag wireFrame

expectTaggedBody :: Word8 -> WireFrame -> Either String ByteString
expectTaggedBody expectedTag wireFrame =
  case wireFrame of
    TaggedFrame actualTag body
      | actualTag == expectedTag ->
          Right body
      | otherwise ->
          Left $
            "expected tag "
              <> show expectedTag
              <> ", got "
              <> show actualTag
    StartupFrame {} ->
      Left "expected tagged frame, got startup frame"

ensureEmptyBody :: String -> ByteString -> Either String ()
ensureEmptyBody label body
  | BS.null body = Right ()
  | otherwise =
      Left $
        label
          <> " must not contain a body, but got "
          <> show (BS.length body)
          <> " bytes"

protocolVersionFromCode :: Word32 -> ProtocolVersion 'ProtocolVersionType
protocolVersionFromCode code =
  ProtocolVersion
    { protocolVersionMajor = fromIntegral (shiftR code 16),
      protocolVersionMinor = fromIntegral code
    }

serializeStartupParameters ::
  [StartupParameter] ->
  Either String ByteString
serializeStartupParameters parameters =
  Right $
    BS.concat
      ( map
          ( \(Scalar (name, value)) ->
              encodeUtf8 name
                <> BS.singleton 0
                <> encodeUtf8 value
                <> BS.singleton 0
          )
          parameters
      )
      <> BS.singleton 0

parseStartupParameters ::
  ByteString ->
  Either String [StartupParameter]
parseStartupParameters input = do
  (parameters, rest) <- runBodyParser (go []) input
  if BS.null rest
    then Right parameters
    else
      Left $
        "startup parameter parser left "
          <> show (BS.length rest)
          <> " unconsumed bytes"
  where
    go acc = do
      rawName <- takeCStringBytes
      if BS.null rawName
        then pure (reverse acc)
        else do
          rawValue <- takeCStringBytes
          name <- decodeUtf8Atom "startup parameter name" rawName
          value <- decodeUtf8Atom "startup parameter value" rawValue
          go (Scalar (name, value) : acc)

serializeCancelRequestBody ::
  BackendKey ->
  Either String ByteString
serializeCancelRequestBody (Scalar (processId, secretKey)) = do
  secretKeyBytes <-
    requireExactLength 4 "CancelRequest secret key" (unScalar secretKey)
  pure $
    LBS.toStrict $
      BB.toLazyByteString $
        BB.int32BE (unScalar processId)
          <> BB.byteString secretKeyBytes

parseCancelRequestBody ::
  ByteString ->
  Either String BackendKey
parseCancelRequestBody input = do
  (processId, restAfterProcessId) <- runBodyParser parseInt32 input
  (secretKeyBytes, rest) <- runBodyParser (takeExactly 4) restAfterProcessId
  if BS.null rest
    then Right (Scalar (Scalar processId, Scalar secretKeyBytes))
    else
      Left $
        "CancelRequest parser left "
          <> show (BS.length rest)
          <> " unconsumed bytes"

requireExactLength :: Int -> String -> ByteString -> Either String ByteString
requireExactLength expectedLength label bytes
  | BS.length bytes == expectedLength =
      Right bytes
  | otherwise =
      Left $
        label
          <> " must be exactly "
          <> show expectedLength
          <> " bytes, got "
          <> show (BS.length bytes)

parseInt16 :: BodyParser Int16
parseInt16 = do
  bytes <- takeExactly 2
  let b0 = fromIntegral (BS.index bytes 0) :: Word32
      b1 = fromIntegral (BS.index bytes 1) :: Word32
  pure (fromIntegral (shiftL b0 8 .|. b1))

parseInt32 :: BodyParser Int32
parseInt32 = do
  bytes <- takeExactly 4
  let b0 = fromIntegral (BS.index bytes 0) :: Word32
      b1 = fromIntegral (BS.index bytes 1) :: Word32
      b2 = fromIntegral (BS.index bytes 2) :: Word32
      b3 = fromIntegral (BS.index bytes 3) :: Word32
      word =
        shiftL b0 24
          .|. shiftL b1 16
          .|. shiftL b2 8
          .|. b3
  pure (fromIntegral word)

parseWord32 :: BodyParser Word32
parseWord32 = do
  bytes <- takeExactly 4
  let b0 = fromIntegral (BS.index bytes 0) :: Word32
      b1 = fromIntegral (BS.index bytes 1) :: Word32
      b2 = fromIntegral (BS.index bytes 2) :: Word32
      b3 = fromIntegral (BS.index bytes 3) :: Word32
  pure $
    shiftL b0 24
      .|. shiftL b1 16
      .|. shiftL b2 8
      .|. b3

parseByte :: BodyParser Word8
parseByte =
  BodyParser $ \input ->
    case BS.uncons input of
      Nothing -> Left "unexpected end of input while reading ByteAtom"
      Just (byte, rest) -> Right (byte, rest)

parseCString :: BodyParser Text
parseCString = decodeUtf8Atom "C-string" =<< takeCStringBytes

parseCStringList :: BodyParser [Text]
parseCStringList = go []
  where
    go acc = do
      raw <- takeCStringBytes
      if BS.null raw
        then pure (reverse acc)
        else do
          value <- decodeUtf8Atom "C-string list element" raw
          go (value : acc)

parseCStringPairsZeroTerminated :: BodyParser [(Text, Text)]
parseCStringPairsZeroTerminated = go []
  where
    go acc = do
      rawFirst <- takeCStringBytes
      if BS.null rawFirst
        then pure (reverse acc)
        else do
          rawSecond <- takeCStringBytes
          first <- decodeUtf8Atom "zero-terminated pair first field" rawFirst
          second <- decodeUtf8Atom "zero-terminated pair second field" rawSecond
          go ((first, second) : acc)

serializeCStringPairsZeroTerminated :: [(Text, Text)] -> Either String ByteString
serializeCStringPairsZeroTerminated pairs = do
  mapM_ rejectEmptyFirst pairs
  pure $
    BS.concat
      ( map
          (\(first, second) -> encodeUtf8 first <> BS.singleton 0 <> encodeUtf8 second <> BS.singleton 0)
          pairs
      )
      <> BS.singleton 0
  where
    rejectEmptyFirst (first, _)
      | T.null first =
          Left "CStringPairListZeroTerminatedAtom cannot encode an empty first field because it is the list terminator"
      | otherwise =
          Right ()

parseByteCStringPairsZeroTerminated :: BodyParser [(Word8, Text)]
parseByteCStringPairsZeroTerminated = go []
  where
    go acc = do
      code <- parseByte
      if code == 0
        then pure (reverse acc)
        else do
          value <- parseCString
          go ((code, value) : acc)

serializeByteCStringPairsZeroTerminated :: [(Word8, Text)] -> Either String ByteString
serializeByteCStringPairsZeroTerminated pairs = do
  mapM_ rejectZeroCode pairs
  encodedPairs <-
    traverse
      (\(code, value) -> (BS.singleton code <>) <$> serializeAtom @'CStringAtom value)
      pairs
  pure (BS.concat encodedPairs <> BS.singleton 0)
  where
    rejectZeroCode (code, _)
      | code == 0 =
          Left "ByteCStringPairListZeroTerminatedAtom cannot encode a zero code because it is the list terminator"
      | otherwise =
          Right ()

decodeUtf8Atom :: String -> ByteString -> BodyParser Text
decodeUtf8Atom label raw =
  case decodeUtf8' raw of
    Left err ->
      failParser ("invalid UTF-8 in " <> label <> ": " <> show err)
    Right text ->
      pure text

takeCStringBytes :: BodyParser ByteString
takeCStringBytes =
  BodyParser $ \input ->
    case BS.elemIndex 0 input of
      Nothing ->
        Left "unterminated C-string"
      Just index ->
        let (value, restWithTerminator) = BS.splitAt index input
         in Right (value, BS.drop 1 restWithTerminator)

takeExactly :: Int -> BodyParser ByteString
takeExactly count =
  BodyParser $ \input ->
    if BS.length input < count
      then
        Left $
          "unexpected end of input: needed "
            <> show count
            <> " bytes, but only "
            <> show (BS.length input)
            <> " remain"
      else
        Right (BS.take count input, BS.drop count input)

takeRemaining :: BodyParser ByteString
takeRemaining =
  BodyParser $ \input -> Right (input, BS.empty)

natAsInt :: forall n. KnownNat n => Either String Int
natAsInt =
  let value = natVal' @n proxy#
   in if value > toInteger (maxBound :: Int)
        then
          Left $
            "type-level literal "
              <> show value
              <> " does not fit into Int"
        else
          Right (fromInteger value)

parserNatAsInt :: forall n. KnownNat n => BodyParser Int
parserNatAsInt =
  case natAsInt @n of
    Left err -> failParser err
    Right value -> pure value

natAsWord8 :: forall n. KnownNat n => Word8
natAsWord8 =
  let value = natVal' @n proxy#
   in if value > toInteger (maxBound :: Word8)
        then
          error $
            "type-level literal "
              <> show value
              <> " does not fit into Word8"
        else
          fromInteger value

natAsInt32Literal :: forall n. KnownNat n => Int32
natAsInt32Literal =
  let value = natVal' @n proxy#
   in if value > toInteger (maxBound :: Int32)
        then
          error $
            "type-level literal "
              <> show value
              <> " does not fit into Int32"
        else
          fromInteger value
