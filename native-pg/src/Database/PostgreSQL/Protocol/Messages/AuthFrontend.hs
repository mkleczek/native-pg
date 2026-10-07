{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Protocol.Messages.AuthFrontend
  ( PasswordMessageType (PasswordMessageType),
    GSSResponseType (GSSResponseType),
    SASLInitialResponseType (SASLInitialResponseType),
    SASLResponseType (SASLResponseType),
    PasswordMessageEnvelope,
    GSSResponseEnvelope,
    SASLInitialResponseEnvelope,
    SASLResponseEnvelope,
    PasswordMessage (PasswordMessage),
    GSSResponse (GSSResponse),
    SASLInitialResponse (SASLInitialResponse),
    SASLResponse (SASLResponse),
  )
where

import Database.PostgreSQL.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Payload (..),
  )
import Database.PostgreSQL.Protocol.Messages.Types (Envelope)

type AuthFrontendTag = 'PayloadConst 1 'p'

type AuthFrontendMessage payload =
  'PayloadStruct
    '[ AuthFrontendTag,
       'PayloadBounded payload
     ]

data PasswordMessageType (payload :: Payload) where
  PasswordMessageType ::
    PasswordMessageType
      (AuthFrontendMessage 'CStringAtom)

data GSSResponseType (payload :: Payload) where
  GSSResponseType ::
    GSSResponseType
      (AuthFrontendMessage 'RemainingBytesAtom)

data SASLInitialResponseType (payload :: Payload) where
  SASLInitialResponseType ::
    SASLInitialResponseType
      (AuthFrontendMessage ('PayloadStruct '[ 'CStringAtom, 'MaybeBytes32Atom]))

data SASLResponseType (payload :: Payload) where
  SASLResponseType ::
    SASLResponseType
      (AuthFrontendMessage 'RemainingBytesAtom)

newtype PasswordMessage bytes = PasswordMessage bytes

deriving instance Eq bytes => Eq (PasswordMessage bytes)
deriving instance Show bytes => Show (PasswordMessage bytes)

type PasswordMessageEnvelope bytes =
  Envelope
    (PasswordMessage bytes)
    'PasswordMessageType

instance HasConstructor 'PasswordMessageType (PasswordMessage bytes) where
  type ConstructorOf 'PasswordMessageType (PasswordMessage bytes) r = bytes -> r
  constructorOf = PasswordMessage
  destructorOf (PasswordMessage password) k = k password

newtype GSSResponse bytes = GSSResponse bytes

deriving instance Eq bytes => Eq (GSSResponse bytes)
deriving instance Show bytes => Show (GSSResponse bytes)

type GSSResponseEnvelope bytes =
  Envelope
    (GSSResponse bytes)
    'GSSResponseType

instance HasConstructor 'GSSResponseType (GSSResponse bytes) where
  type ConstructorOf 'GSSResponseType (GSSResponse bytes) r = bytes -> r
  constructorOf = GSSResponse
  destructorOf (GSSResponse token) k = k token

data SASLInitialResponse bytes =
  SASLInitialResponse bytes (Maybe bytes)

deriving instance Eq bytes => Eq (SASLInitialResponse bytes)
deriving instance Show bytes => Show (SASLInitialResponse bytes)

type SASLInitialResponseEnvelope bytes =
  Envelope
    (SASLInitialResponse bytes)
    'SASLInitialResponseType

instance HasConstructor 'SASLInitialResponseType (SASLInitialResponse bytes) where
  type ConstructorOf 'SASLInitialResponseType (SASLInitialResponse bytes) r = bytes -> Maybe bytes -> r
  constructorOf = SASLInitialResponse
  destructorOf (SASLInitialResponse mechanism response) k = k mechanism response

newtype SASLResponse bytes = SASLResponse bytes

deriving instance Eq bytes => Eq (SASLResponse bytes)
deriving instance Show bytes => Show (SASLResponse bytes)

type SASLResponseEnvelope bytes =
  Envelope
    (SASLResponse bytes)
    'SASLResponseType

instance HasConstructor 'SASLResponseType (SASLResponse bytes) where
  type ConstructorOf 'SASLResponseType (SASLResponse bytes) r = bytes -> r
  constructorOf = SASLResponse
  destructorOf (SASLResponse response) k = k response
