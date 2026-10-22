{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.AuthBackend where

import Database.PostgreSQL.Internal.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
  ( Payload (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types
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
  type ConstructorOf 'AuthenticationOkType AuthenticationOk r = r
  constructorOf = AuthenticationOk
  destructorOf AuthenticationOk k = k

data AuthenticationKerberosV5 = AuthenticationKerberosV5

deriving instance Eq AuthenticationKerberosV5
deriving instance Show AuthenticationKerberosV5

type AuthenticationKerberosV5Envelope =
  Envelope
    AuthenticationKerberosV5
    'AuthenticationKerberosV5Type

instance HasConstructor 'AuthenticationKerberosV5Type AuthenticationKerberosV5 where
  type ConstructorOf 'AuthenticationKerberosV5Type AuthenticationKerberosV5 r = r
  constructorOf = AuthenticationKerberosV5
  destructorOf AuthenticationKerberosV5 k = k

data AuthenticationCleartextPassword = AuthenticationCleartextPassword

deriving instance Eq AuthenticationCleartextPassword
deriving instance Show AuthenticationCleartextPassword

type AuthenticationCleartextPasswordEnvelope =
  Envelope
    AuthenticationCleartextPassword
    'AuthenticationCleartextPasswordType

instance HasConstructor 'AuthenticationCleartextPasswordType AuthenticationCleartextPassword where
  type ConstructorOf 'AuthenticationCleartextPasswordType AuthenticationCleartextPassword r = r
  constructorOf = AuthenticationCleartextPassword
  destructorOf AuthenticationCleartextPassword k = k

newtype AuthenticationMD5Password bytes = AuthenticationMD5Password bytes

deriving instance Eq bytes => Eq (AuthenticationMD5Password bytes)
deriving instance Show bytes => Show (AuthenticationMD5Password bytes)

type AuthenticationMD5PasswordEnvelope bytes =
  Envelope
    (AuthenticationMD5Password bytes)
    'AuthenticationMD5PasswordType

instance HasConstructor 'AuthenticationMD5PasswordType (AuthenticationMD5Password bytes) where
  type ConstructorOf 'AuthenticationMD5PasswordType (AuthenticationMD5Password bytes) r = bytes -> r
  constructorOf = AuthenticationMD5Password
  destructorOf (AuthenticationMD5Password salt) k = k salt

data AuthenticationGSS = AuthenticationGSS

deriving instance Eq AuthenticationGSS
deriving instance Show AuthenticationGSS

type AuthenticationGSSEnvelope =
  Envelope
    AuthenticationGSS
    'AuthenticationGSSType

instance HasConstructor 'AuthenticationGSSType AuthenticationGSS where
  type ConstructorOf 'AuthenticationGSSType AuthenticationGSS r = r
  constructorOf = AuthenticationGSS
  destructorOf AuthenticationGSS k = k

newtype AuthenticationGSSContinue bytes = AuthenticationGSSContinue bytes

deriving instance Eq bytes => Eq (AuthenticationGSSContinue bytes)
deriving instance Show bytes => Show (AuthenticationGSSContinue bytes)

type AuthenticationGSSContinueEnvelope bytes =
  Envelope
    (AuthenticationGSSContinue bytes)
    'AuthenticationGSSContinueType

instance HasConstructor 'AuthenticationGSSContinueType (AuthenticationGSSContinue bytes) where
  type ConstructorOf 'AuthenticationGSSContinueType (AuthenticationGSSContinue bytes) r = bytes -> r
  constructorOf = AuthenticationGSSContinue
  destructorOf (AuthenticationGSSContinue token) k = k token

data AuthenticationSSPI = AuthenticationSSPI

deriving instance Eq AuthenticationSSPI
deriving instance Show AuthenticationSSPI

type AuthenticationSSPIEnvelope =
  Envelope
    AuthenticationSSPI
    'AuthenticationSSPIType

instance HasConstructor 'AuthenticationSSPIType AuthenticationSSPI where
  type ConstructorOf 'AuthenticationSSPIType AuthenticationSSPI r = r
  constructorOf = AuthenticationSSPI
  destructorOf AuthenticationSSPI k = k

newtype AuthenticationSASL bytes = AuthenticationSASL [bytes]

deriving instance Eq bytes => Eq (AuthenticationSASL bytes)
deriving instance Show bytes => Show (AuthenticationSASL bytes)

type AuthenticationSASLEnvelope bytes =
  Envelope
    (AuthenticationSASL bytes)
    'AuthenticationSASLType

instance HasConstructor 'AuthenticationSASLType (AuthenticationSASL bytes) where
  type ConstructorOf 'AuthenticationSASLType (AuthenticationSASL bytes) r = [bytes] -> r
  constructorOf = AuthenticationSASL
  destructorOf (AuthenticationSASL mechanisms) k = k mechanisms

newtype AuthenticationSASLContinue bytes = AuthenticationSASLContinue bytes

deriving instance Eq bytes => Eq (AuthenticationSASLContinue bytes)
deriving instance Show bytes => Show (AuthenticationSASLContinue bytes)

type AuthenticationSASLContinueEnvelope bytes =
  Envelope
    (AuthenticationSASLContinue bytes)
    'AuthenticationSASLContinueType

instance HasConstructor 'AuthenticationSASLContinueType (AuthenticationSASLContinue bytes) where
  type ConstructorOf 'AuthenticationSASLContinueType (AuthenticationSASLContinue bytes) r = bytes -> r
  constructorOf = AuthenticationSASLContinue
  destructorOf (AuthenticationSASLContinue challenge) k = k challenge

newtype AuthenticationSASLFinal bytes = AuthenticationSASLFinal bytes

deriving instance Eq bytes => Eq (AuthenticationSASLFinal bytes)
deriving instance Show bytes => Show (AuthenticationSASLFinal bytes)

type AuthenticationSASLFinalEnvelope bytes =
  Envelope
    (AuthenticationSASLFinal bytes)
    'AuthenticationSASLFinalType

instance HasConstructor 'AuthenticationSASLFinalType (AuthenticationSASLFinal bytes) where
  type ConstructorOf 'AuthenticationSASLFinalType (AuthenticationSASLFinal bytes) r = bytes -> r
  constructorOf = AuthenticationSASLFinal
  destructorOf (AuthenticationSASLFinal outcome) k = k outcome
