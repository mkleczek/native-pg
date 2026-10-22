{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.Startup
  ( StartupMessageType (StartupMessageType),
    SSLRequestType (SSLRequestType),
    GSSENCRequestType (GSSENCRequestType),
    CancelRequestType (CancelRequestType),
    StartupMessageEnvelope,
    SSLRequestEnvelope,
    GSSENCRequestEnvelope,
    CancelRequestEnvelope,
    StartupMessage (..),
    SSLRequest (SSLRequest),
    GSSENCRequest (GSSENCRequest),
    CancelRequest (CancelRequest),
    parseStartupMessage,
    parseCancelRequest,
  )
where

import Data.ByteString (ByteString)
import Database.PostgreSQL.Internal.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
  ( Payload (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Parse
  ( parseEnvelope,
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types
  ( BackendKey,
    BackendKeyType (BackendKeyType),
    Envelope,
    ProtocolVersion,
    ProtocolVersionType (ProtocolVersionType),
    StartupParameter,
    StartupParameterType (StartupParameterType),
  )

data StartupMessageType (payload :: Payload) where
  StartupMessageType ::
    StartupMessageType
      ( PayloadBounded
          ( PayloadStruct
              '[ PayloadField 'ProtocolVersionType,
                 PayloadZeroTerminated ('PayloadField 'StartupParameterType)
               ]
          )
      )

data SSLRequestType (payload :: Payload) where
  SSLRequestType ::
    SSLRequestType
      (PayloadBounded ('PayloadConst 4 80877103))

data GSSENCRequestType (payload :: Payload) where
  GSSENCRequestType ::
    GSSENCRequestType
      (PayloadBounded ('PayloadConst 4 80877104))

data CancelRequestType (payload :: Payload) where
  CancelRequestType ::
    CancelRequestType
      ( PayloadBounded
          ( PayloadStruct
              '[ 'PayloadConst 4 80877102,
                 PayloadField 'BackendKeyType
               ]
          )
      )

data StartupMessage bytes = StartupMessage
  { startupMessageProtocolVersion :: ProtocolVersion,
    startupMessageParameters :: [StartupParameter bytes]
  }

deriving instance Eq bytes => Eq (StartupMessage bytes)
deriving instance Show bytes => Show (StartupMessage bytes)

type StartupMessageEnvelope bytes =
  Envelope
    (StartupMessage bytes)
    'StartupMessageType

instance HasConstructor 'StartupMessageType (StartupMessage bytes) where
  type ConstructorOf 'StartupMessageType (StartupMessage bytes) r = ProtocolVersion -> [StartupParameter bytes] -> r
  constructorOf = StartupMessage
  destructorOf (StartupMessage version parameters) k = k version parameters

data SSLRequest = SSLRequest

deriving instance Eq SSLRequest
deriving instance Show SSLRequest

type SSLRequestEnvelope =
  Envelope
    SSLRequest
    'SSLRequestType

instance HasConstructor 'SSLRequestType SSLRequest where
  type ConstructorOf 'SSLRequestType SSLRequest r = r
  constructorOf = SSLRequest
  destructorOf SSLRequest k = k

data GSSENCRequest = GSSENCRequest

deriving instance Eq GSSENCRequest
deriving instance Show GSSENCRequest

type GSSENCRequestEnvelope =
  Envelope
    GSSENCRequest
    'GSSENCRequestType

instance HasConstructor 'GSSENCRequestType GSSENCRequest where
  type ConstructorOf 'GSSENCRequestType GSSENCRequest r = r
  constructorOf = GSSENCRequest
  destructorOf GSSENCRequest k = k

newtype CancelRequest bytes = CancelRequest (BackendKey bytes)

deriving instance Eq bytes => Eq (CancelRequest bytes)
deriving instance Show bytes => Show (CancelRequest bytes)

type CancelRequestEnvelope bytes =
  Envelope
    (CancelRequest bytes)
    'CancelRequestType

instance HasConstructor 'CancelRequestType (CancelRequest bytes) where
  type ConstructorOf 'CancelRequestType (CancelRequest bytes) r = BackendKey bytes -> r
  constructorOf = CancelRequest
  destructorOf (CancelRequest key) k = k key

parseStartupMessage :: ByteString -> Either String (StartupMessageEnvelope ByteString)
parseStartupMessage = parseEnvelope @(StartupMessage ByteString) @'StartupMessageType

parseCancelRequest :: ByteString -> Either String (CancelRequestEnvelope ByteString)
parseCancelRequest = parseEnvelope @(CancelRequest ByteString) @'CancelRequestType
