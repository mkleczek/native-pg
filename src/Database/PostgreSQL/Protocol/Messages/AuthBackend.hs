{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase #-}

module Database.PostgreSQL.Protocol.Messages.AuthBackend where

import Database.PostgreSQL.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Payload (..),
  )
import Database.PostgreSQL.Protocol.Messages.Types
  ( Envelope (..),
  )

type AuthenticationSubcode code = 'PayloadConst 4 code

type AuthenticationMessage payload =
  'PayloadStruct
    '[ 'PayloadConst 1 'R',
       'PayloadBounded payload
     ]

data AuthenticationOkType (payload :: Payload) where
  AuthenticationOkType ::
    AuthenticationOkType
      (AuthenticationMessage (AuthenticationSubcode 0))

data AuthenticationKerberosV5Type (payload :: Payload) where
  AuthenticationKerberosV5Type ::
    AuthenticationKerberosV5Type
      (AuthenticationMessage (AuthenticationSubcode 2))

data AuthenticationCleartextPasswordType (payload :: Payload) where
  AuthenticationCleartextPasswordType ::
    AuthenticationCleartextPasswordType
      (AuthenticationMessage (AuthenticationSubcode 3))

data AuthenticationMD5PasswordType (payload :: Payload) where
  AuthenticationMD5PasswordType ::
    AuthenticationMD5PasswordType
      (AuthenticationMessage ('PayloadStruct '[ AuthenticationSubcode 5, 'FixedBytesAtom 4]))

data AuthenticationGSSType (payload :: Payload) where
  AuthenticationGSSType ::
    AuthenticationGSSType
      (AuthenticationMessage (AuthenticationSubcode 7))

data AuthenticationGSSContinueType (payload :: Payload) where
  AuthenticationGSSContinueType ::
    AuthenticationGSSContinueType
      (AuthenticationMessage ('PayloadStruct '[ AuthenticationSubcode 8, 'RemainingBytesAtom]))

data AuthenticationSSPIType (payload :: Payload) where
  AuthenticationSSPIType ::
    AuthenticationSSPIType
      (AuthenticationMessage (AuthenticationSubcode 9))

data AuthenticationSASLType (payload :: Payload) where
  AuthenticationSASLType ::
    AuthenticationSASLType
      (AuthenticationMessage ('PayloadStruct '[ AuthenticationSubcode 10, PayloadZeroTerminated 'CStringAtom]))

data AuthenticationSASLContinueType (payload :: Payload) where
  AuthenticationSASLContinueType ::
    AuthenticationSASLContinueType
      (AuthenticationMessage ('PayloadStruct '[ AuthenticationSubcode 11, 'RemainingBytesAtom]))

data AuthenticationSASLFinalType (payload :: Payload) where
  AuthenticationSASLFinalType ::
    AuthenticationSASLFinalType
      (AuthenticationMessage ('PayloadStruct '[ AuthenticationSubcode 12, 'RemainingBytesAtom]))

data AuthenticationOk = AuthenticationOk

deriving instance Eq AuthenticationOk
deriving instance Show AuthenticationOk

type AuthenticationOkEnvelope =
  Envelope
    AuthenticationOk
    'AuthenticationOkType

instance HasConstructor 'AuthenticationOkType AuthenticationOk where
  type ConstructorOf 'AuthenticationOkType AuthenticationOk = AuthenticationOk
  constructorOf = AuthenticationOk

data AuthenticationKerberosV5 = AuthenticationKerberosV5

deriving instance Eq AuthenticationKerberosV5
deriving instance Show AuthenticationKerberosV5

type AuthenticationKerberosV5Envelope =
  Envelope
    AuthenticationKerberosV5
    'AuthenticationKerberosV5Type

instance HasConstructor 'AuthenticationKerberosV5Type AuthenticationKerberosV5 where
  type ConstructorOf 'AuthenticationKerberosV5Type AuthenticationKerberosV5 = AuthenticationKerberosV5
  constructorOf = AuthenticationKerberosV5

data AuthenticationCleartextPassword = AuthenticationCleartextPassword

deriving instance Eq AuthenticationCleartextPassword
deriving instance Show AuthenticationCleartextPassword

type AuthenticationCleartextPasswordEnvelope =
  Envelope
    AuthenticationCleartextPassword
    'AuthenticationCleartextPasswordType

instance HasConstructor 'AuthenticationCleartextPasswordType AuthenticationCleartextPassword where
  type ConstructorOf 'AuthenticationCleartextPasswordType AuthenticationCleartextPassword = AuthenticationCleartextPassword
  constructorOf = AuthenticationCleartextPassword

newtype AuthenticationMD5Password bytes = AuthenticationMD5Password bytes

deriving instance Eq bytes => Eq (AuthenticationMD5Password bytes)
deriving instance Show bytes => Show (AuthenticationMD5Password bytes)

type AuthenticationMD5PasswordEnvelope bytes =
  Envelope
    (AuthenticationMD5Password bytes)
    'AuthenticationMD5PasswordType

instance HasConstructor 'AuthenticationMD5PasswordType (AuthenticationMD5Password bytes) where
  type ConstructorOf 'AuthenticationMD5PasswordType (AuthenticationMD5Password bytes) = bytes -> AuthenticationMD5Password bytes
  constructorOf = AuthenticationMD5Password

data AuthenticationGSS = AuthenticationGSS

deriving instance Eq AuthenticationGSS
deriving instance Show AuthenticationGSS

type AuthenticationGSSEnvelope =
  Envelope
    AuthenticationGSS
    'AuthenticationGSSType

instance HasConstructor 'AuthenticationGSSType AuthenticationGSS where
  type ConstructorOf 'AuthenticationGSSType AuthenticationGSS = AuthenticationGSS
  constructorOf = AuthenticationGSS

newtype AuthenticationGSSContinue bytes = AuthenticationGSSContinue bytes

deriving instance Eq bytes => Eq (AuthenticationGSSContinue bytes)
deriving instance Show bytes => Show (AuthenticationGSSContinue bytes)

type AuthenticationGSSContinueEnvelope bytes =
  Envelope
    (AuthenticationGSSContinue bytes)
    'AuthenticationGSSContinueType

instance HasConstructor 'AuthenticationGSSContinueType (AuthenticationGSSContinue bytes) where
  type ConstructorOf 'AuthenticationGSSContinueType (AuthenticationGSSContinue bytes) = bytes -> AuthenticationGSSContinue bytes
  constructorOf = AuthenticationGSSContinue

data AuthenticationSSPI = AuthenticationSSPI

deriving instance Eq AuthenticationSSPI
deriving instance Show AuthenticationSSPI

type AuthenticationSSPIEnvelope =
  Envelope
    AuthenticationSSPI
    'AuthenticationSSPIType

instance HasConstructor 'AuthenticationSSPIType AuthenticationSSPI where
  type ConstructorOf 'AuthenticationSSPIType AuthenticationSSPI = AuthenticationSSPI
  constructorOf = AuthenticationSSPI

newtype AuthenticationSASL bytes = AuthenticationSASL [bytes]

deriving instance Eq bytes => Eq (AuthenticationSASL bytes)
deriving instance Show bytes => Show (AuthenticationSASL bytes)

type AuthenticationSASLEnvelope bytes =
  Envelope
    (AuthenticationSASL bytes)
    'AuthenticationSASLType

instance HasConstructor 'AuthenticationSASLType (AuthenticationSASL bytes) where
  type ConstructorOf 'AuthenticationSASLType (AuthenticationSASL bytes) = [bytes] -> AuthenticationSASL bytes
  constructorOf = AuthenticationSASL

newtype AuthenticationSASLContinue bytes = AuthenticationSASLContinue bytes

deriving instance Eq bytes => Eq (AuthenticationSASLContinue bytes)
deriving instance Show bytes => Show (AuthenticationSASLContinue bytes)

type AuthenticationSASLContinueEnvelope bytes =
  Envelope
    (AuthenticationSASLContinue bytes)
    'AuthenticationSASLContinueType

instance HasConstructor 'AuthenticationSASLContinueType (AuthenticationSASLContinue bytes) where
  type ConstructorOf 'AuthenticationSASLContinueType (AuthenticationSASLContinue bytes) = bytes -> AuthenticationSASLContinue bytes
  constructorOf = AuthenticationSASLContinue

newtype AuthenticationSASLFinal bytes = AuthenticationSASLFinal bytes

deriving instance Eq bytes => Eq (AuthenticationSASLFinal bytes)
deriving instance Show bytes => Show (AuthenticationSASLFinal bytes)

type AuthenticationSASLFinalEnvelope bytes =
  Envelope
    (AuthenticationSASLFinal bytes)
    'AuthenticationSASLFinalType

instance HasConstructor 'AuthenticationSASLFinalType (AuthenticationSASLFinal bytes) where
  type ConstructorOf 'AuthenticationSASLFinalType (AuthenticationSASLFinal bytes) = bytes -> AuthenticationSASLFinal bytes
  constructorOf = AuthenticationSASLFinal
