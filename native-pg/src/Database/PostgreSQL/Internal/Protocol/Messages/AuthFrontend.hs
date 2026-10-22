{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.AuthFrontend
  ( PasswordMessageType (PasswordMessageType),
    GSSResponseType (GSSResponseType),
    SASLInitialResponseType (SASLInitialResponseType),
    SASLResponseType (SASLResponseType),
    PasswordMessage,
    pattern PasswordMessage,
    GSSResponse,
    pattern GSSResponse,
    SASLInitialResponse,
    pattern SASLInitialResponse,
    SASLResponse,
    pattern SASLResponse,
  )
where

import Database.PostgreSQL.Internal.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
  ( Payload (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types (Envelope (..))

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

newtype PasswordMessagePayload bytes = PasswordMessagePayload bytes

deriving instance Eq bytes => Eq (PasswordMessagePayload bytes)
deriving instance Show bytes => Show (PasswordMessagePayload bytes)

type PasswordMessage bytes = Envelope (PasswordMessagePayload bytes) 'PasswordMessageType

pattern PasswordMessage :: bytes -> PasswordMessage bytes
pattern PasswordMessage password = Envelope (PasswordMessagePayload password)
{-# COMPLETE PasswordMessage #-}

instance HasConstructor 'PasswordMessageType (PasswordMessage bytes) where
  type ConstructorOf 'PasswordMessageType (PasswordMessage bytes) r = bytes -> r
  constructorOf = PasswordMessage
  destructorOf (PasswordMessage password) k = k password

newtype GSSResponsePayload bytes = GSSResponsePayload bytes

deriving instance Eq bytes => Eq (GSSResponsePayload bytes)
deriving instance Show bytes => Show (GSSResponsePayload bytes)

type GSSResponse bytes = Envelope (GSSResponsePayload bytes) 'GSSResponseType

pattern GSSResponse :: bytes -> GSSResponse bytes
pattern GSSResponse token = Envelope (GSSResponsePayload token)
{-# COMPLETE GSSResponse #-}

instance HasConstructor 'GSSResponseType (GSSResponse bytes) where
  type ConstructorOf 'GSSResponseType (GSSResponse bytes) r = bytes -> r
  constructorOf = GSSResponse
  destructorOf (GSSResponse token) k = k token

data SASLInitialResponsePayload bytes = SASLInitialResponsePayload bytes (Maybe bytes)

deriving instance Eq bytes => Eq (SASLInitialResponsePayload bytes)
deriving instance Show bytes => Show (SASLInitialResponsePayload bytes)

type SASLInitialResponse bytes = Envelope (SASLInitialResponsePayload bytes) 'SASLInitialResponseType

pattern SASLInitialResponse :: bytes -> Maybe bytes -> SASLInitialResponse bytes
pattern SASLInitialResponse mechanism response = Envelope (SASLInitialResponsePayload mechanism response)
{-# COMPLETE SASLInitialResponse #-}

instance HasConstructor 'SASLInitialResponseType (SASLInitialResponse bytes) where
  type ConstructorOf 'SASLInitialResponseType (SASLInitialResponse bytes) r = bytes -> Maybe bytes -> r
  constructorOf = SASLInitialResponse
  destructorOf (SASLInitialResponse mechanism response) k = k mechanism response

newtype SASLResponsePayload bytes = SASLResponsePayload bytes

deriving instance Eq bytes => Eq (SASLResponsePayload bytes)
deriving instance Show bytes => Show (SASLResponsePayload bytes)

type SASLResponse bytes = Envelope (SASLResponsePayload bytes) 'SASLResponseType

pattern SASLResponse :: bytes -> SASLResponse bytes
pattern SASLResponse response = Envelope (SASLResponsePayload response)
{-# COMPLETE SASLResponse #-}

instance HasConstructor 'SASLResponseType (SASLResponse bytes) where
  type ConstructorOf 'SASLResponseType (SASLResponse bytes) r = bytes -> r
  constructorOf = SASLResponse
  destructorOf (SASLResponse response) k = k response
