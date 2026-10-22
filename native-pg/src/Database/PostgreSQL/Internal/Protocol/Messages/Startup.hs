{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.Startup
  ( StartupMessageType (StartupMessageType),
    SSLRequestType (SSLRequestType),
    GSSENCRequestType (GSSENCRequestType),
    CancelRequestType (CancelRequestType),
    StartupMessage,
    pattern StartupMessage,
    startupMessageProtocolVersion,
    startupMessageParameters,
    SSLRequest,
    pattern SSLRequest,
    GSSENCRequest,
    pattern GSSENCRequest,
    CancelRequest,
    pattern CancelRequest,
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
  ( parseMessage,
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types
  ( BackendKey,
    BackendKeyType (BackendKeyType),
    Envelope (..),
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

data StartupMessagePayload bytes = StartupMessagePayload ProtocolVersion [StartupParameter bytes]

deriving instance Eq bytes => Eq (StartupMessagePayload bytes)
deriving instance Show bytes => Show (StartupMessagePayload bytes)

type StartupMessage bytes = Envelope (StartupMessagePayload bytes) 'StartupMessageType

pattern StartupMessage :: ProtocolVersion -> [StartupParameter bytes] -> StartupMessage bytes
pattern StartupMessage {startupMessageProtocolVersion, startupMessageParameters} = Envelope (StartupMessagePayload startupMessageProtocolVersion startupMessageParameters)
{-# COMPLETE StartupMessage #-}

instance HasConstructor 'StartupMessageType (StartupMessage bytes) where
  type ConstructorOf 'StartupMessageType (StartupMessage bytes) r = ProtocolVersion -> [StartupParameter bytes] -> r
  constructorOf = StartupMessage
  destructorOf (StartupMessage version parameters) k = k version parameters

data SSLRequestPayload = SSLRequestPayload

deriving instance Eq SSLRequestPayload
deriving instance Show SSLRequestPayload

type SSLRequest = Envelope SSLRequestPayload 'SSLRequestType

pattern SSLRequest :: SSLRequest
pattern SSLRequest = Envelope SSLRequestPayload
{-# COMPLETE SSLRequest #-}

instance HasConstructor 'SSLRequestType SSLRequest where
  type ConstructorOf 'SSLRequestType SSLRequest r = r
  constructorOf = SSLRequest
  destructorOf SSLRequest k = k

data GSSENCRequestPayload = GSSENCRequestPayload

deriving instance Eq GSSENCRequestPayload
deriving instance Show GSSENCRequestPayload

type GSSENCRequest = Envelope GSSENCRequestPayload 'GSSENCRequestType

pattern GSSENCRequest :: GSSENCRequest
pattern GSSENCRequest = Envelope GSSENCRequestPayload
{-# COMPLETE GSSENCRequest #-}

instance HasConstructor 'GSSENCRequestType GSSENCRequest where
  type ConstructorOf 'GSSENCRequestType GSSENCRequest r = r
  constructorOf = GSSENCRequest
  destructorOf GSSENCRequest k = k

newtype CancelRequestPayload bytes = CancelRequestPayload (BackendKey bytes)

deriving instance Eq bytes => Eq (CancelRequestPayload bytes)
deriving instance Show bytes => Show (CancelRequestPayload bytes)

type CancelRequest bytes = Envelope (CancelRequestPayload bytes) 'CancelRequestType

pattern CancelRequest :: BackendKey bytes -> CancelRequest bytes
pattern CancelRequest key = Envelope (CancelRequestPayload key)
{-# COMPLETE CancelRequest #-}

instance HasConstructor 'CancelRequestType (CancelRequest bytes) where
  type ConstructorOf 'CancelRequestType (CancelRequest bytes) r = BackendKey bytes -> r
  constructorOf = CancelRequest
  destructorOf (CancelRequest key) k = k key

parseStartupMessage :: ByteString -> Either String (StartupMessage ByteString)
parseStartupMessage = parseMessage

parseCancelRequest :: ByteString -> Either String (CancelRequest ByteString)
parseCancelRequest = parseMessage
