{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.AuthBackend
  ( AuthenticationSubcode,
    AuthenticationMessage,
    AuthenticationOkType (AuthenticationOkType),
    AuthenticationKerberosV5Type (AuthenticationKerberosV5Type),
    AuthenticationCleartextPasswordType (AuthenticationCleartextPasswordType),
    AuthenticationMD5PasswordType (AuthenticationMD5PasswordType),
    AuthenticationGSSType (AuthenticationGSSType),
    AuthenticationGSSContinueType (AuthenticationGSSContinueType),
    AuthenticationSSPIType (AuthenticationSSPIType),
    AuthenticationSASLType (AuthenticationSASLType),
    AuthenticationSASLContinueType (AuthenticationSASLContinueType),
    AuthenticationSASLFinalType (AuthenticationSASLFinalType),
    AuthenticationOk,
    pattern AuthenticationOk,
    AuthenticationKerberosV5,
    pattern AuthenticationKerberosV5,
    AuthenticationCleartextPassword,
    pattern AuthenticationCleartextPassword,
    AuthenticationMD5Password,
    pattern AuthenticationMD5Password,
    AuthenticationGSS,
    pattern AuthenticationGSS,
    AuthenticationGSSContinue,
    pattern AuthenticationGSSContinue,
    AuthenticationSSPI,
    pattern AuthenticationSSPI,
    AuthenticationSASL,
    pattern AuthenticationSASL,
    AuthenticationSASLContinue,
    pattern AuthenticationSASLContinue,
    AuthenticationSASLFinal,
    pattern AuthenticationSASLFinal,
  )
where

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

data AuthenticationOkPayload = AuthenticationOkPayload

deriving instance Eq AuthenticationOkPayload
deriving instance Show AuthenticationOkPayload

type AuthenticationOk = Envelope AuthenticationOkPayload 'AuthenticationOkType

pattern AuthenticationOk :: AuthenticationOk
pattern AuthenticationOk = Envelope AuthenticationOkPayload
{-# COMPLETE AuthenticationOk #-}

instance HasConstructor 'AuthenticationOkType AuthenticationOk where
  type ConstructorOf 'AuthenticationOkType AuthenticationOk r = r
  constructorOf = AuthenticationOk
  destructorOf AuthenticationOk k = k

data AuthenticationKerberosV5Payload = AuthenticationKerberosV5Payload

deriving instance Eq AuthenticationKerberosV5Payload
deriving instance Show AuthenticationKerberosV5Payload

type AuthenticationKerberosV5 = Envelope AuthenticationKerberosV5Payload 'AuthenticationKerberosV5Type

pattern AuthenticationKerberosV5 :: AuthenticationKerberosV5
pattern AuthenticationKerberosV5 = Envelope AuthenticationKerberosV5Payload
{-# COMPLETE AuthenticationKerberosV5 #-}

instance HasConstructor 'AuthenticationKerberosV5Type AuthenticationKerberosV5 where
  type ConstructorOf 'AuthenticationKerberosV5Type AuthenticationKerberosV5 r = r
  constructorOf = AuthenticationKerberosV5
  destructorOf AuthenticationKerberosV5 k = k

data AuthenticationCleartextPasswordPayload = AuthenticationCleartextPasswordPayload

deriving instance Eq AuthenticationCleartextPasswordPayload
deriving instance Show AuthenticationCleartextPasswordPayload

type AuthenticationCleartextPassword = Envelope AuthenticationCleartextPasswordPayload 'AuthenticationCleartextPasswordType

pattern AuthenticationCleartextPassword :: AuthenticationCleartextPassword
pattern AuthenticationCleartextPassword = Envelope AuthenticationCleartextPasswordPayload
{-# COMPLETE AuthenticationCleartextPassword #-}

instance HasConstructor 'AuthenticationCleartextPasswordType AuthenticationCleartextPassword where
  type ConstructorOf 'AuthenticationCleartextPasswordType AuthenticationCleartextPassword r = r
  constructorOf = AuthenticationCleartextPassword
  destructorOf AuthenticationCleartextPassword k = k

newtype AuthenticationMD5PasswordPayload bytes = AuthenticationMD5PasswordPayload bytes

deriving instance Eq bytes => Eq (AuthenticationMD5PasswordPayload bytes)
deriving instance Show bytes => Show (AuthenticationMD5PasswordPayload bytes)

type AuthenticationMD5Password bytes = Envelope (AuthenticationMD5PasswordPayload bytes) 'AuthenticationMD5PasswordType

pattern AuthenticationMD5Password :: bytes -> AuthenticationMD5Password bytes
pattern AuthenticationMD5Password salt = Envelope (AuthenticationMD5PasswordPayload salt)
{-# COMPLETE AuthenticationMD5Password #-}

instance HasConstructor 'AuthenticationMD5PasswordType (AuthenticationMD5Password bytes) where
  type ConstructorOf 'AuthenticationMD5PasswordType (AuthenticationMD5Password bytes) r = bytes -> r
  constructorOf = AuthenticationMD5Password
  destructorOf (AuthenticationMD5Password salt) k = k salt

data AuthenticationGSSPayload = AuthenticationGSSPayload

deriving instance Eq AuthenticationGSSPayload
deriving instance Show AuthenticationGSSPayload

type AuthenticationGSS = Envelope AuthenticationGSSPayload 'AuthenticationGSSType

pattern AuthenticationGSS :: AuthenticationGSS
pattern AuthenticationGSS = Envelope AuthenticationGSSPayload
{-# COMPLETE AuthenticationGSS #-}

instance HasConstructor 'AuthenticationGSSType AuthenticationGSS where
  type ConstructorOf 'AuthenticationGSSType AuthenticationGSS r = r
  constructorOf = AuthenticationGSS
  destructorOf AuthenticationGSS k = k

newtype AuthenticationGSSContinuePayload bytes = AuthenticationGSSContinuePayload bytes

deriving instance Eq bytes => Eq (AuthenticationGSSContinuePayload bytes)
deriving instance Show bytes => Show (AuthenticationGSSContinuePayload bytes)

type AuthenticationGSSContinue bytes = Envelope (AuthenticationGSSContinuePayload bytes) 'AuthenticationGSSContinueType

pattern AuthenticationGSSContinue :: bytes -> AuthenticationGSSContinue bytes
pattern AuthenticationGSSContinue token = Envelope (AuthenticationGSSContinuePayload token)
{-# COMPLETE AuthenticationGSSContinue #-}

instance HasConstructor 'AuthenticationGSSContinueType (AuthenticationGSSContinue bytes) where
  type ConstructorOf 'AuthenticationGSSContinueType (AuthenticationGSSContinue bytes) r = bytes -> r
  constructorOf = AuthenticationGSSContinue
  destructorOf (AuthenticationGSSContinue token) k = k token

data AuthenticationSSPIPayload = AuthenticationSSPIPayload

deriving instance Eq AuthenticationSSPIPayload
deriving instance Show AuthenticationSSPIPayload

type AuthenticationSSPI = Envelope AuthenticationSSPIPayload 'AuthenticationSSPIType

pattern AuthenticationSSPI :: AuthenticationSSPI
pattern AuthenticationSSPI = Envelope AuthenticationSSPIPayload
{-# COMPLETE AuthenticationSSPI #-}

instance HasConstructor 'AuthenticationSSPIType AuthenticationSSPI where
  type ConstructorOf 'AuthenticationSSPIType AuthenticationSSPI r = r
  constructorOf = AuthenticationSSPI
  destructorOf AuthenticationSSPI k = k

newtype AuthenticationSASLPayload bytes = AuthenticationSASLPayload [bytes]

deriving instance Eq bytes => Eq (AuthenticationSASLPayload bytes)
deriving instance Show bytes => Show (AuthenticationSASLPayload bytes)

type AuthenticationSASL bytes = Envelope (AuthenticationSASLPayload bytes) 'AuthenticationSASLType

pattern AuthenticationSASL :: [bytes] -> AuthenticationSASL bytes
pattern AuthenticationSASL mechanisms = Envelope (AuthenticationSASLPayload mechanisms)
{-# COMPLETE AuthenticationSASL #-}

instance HasConstructor 'AuthenticationSASLType (AuthenticationSASL bytes) where
  type ConstructorOf 'AuthenticationSASLType (AuthenticationSASL bytes) r = [bytes] -> r
  constructorOf = AuthenticationSASL
  destructorOf (AuthenticationSASL mechanisms) k = k mechanisms

newtype AuthenticationSASLContinuePayload bytes = AuthenticationSASLContinuePayload bytes

deriving instance Eq bytes => Eq (AuthenticationSASLContinuePayload bytes)
deriving instance Show bytes => Show (AuthenticationSASLContinuePayload bytes)

type AuthenticationSASLContinue bytes = Envelope (AuthenticationSASLContinuePayload bytes) 'AuthenticationSASLContinueType

pattern AuthenticationSASLContinue :: bytes -> AuthenticationSASLContinue bytes
pattern AuthenticationSASLContinue challenge = Envelope (AuthenticationSASLContinuePayload challenge)
{-# COMPLETE AuthenticationSASLContinue #-}

instance HasConstructor 'AuthenticationSASLContinueType (AuthenticationSASLContinue bytes) where
  type ConstructorOf 'AuthenticationSASLContinueType (AuthenticationSASLContinue bytes) r = bytes -> r
  constructorOf = AuthenticationSASLContinue
  destructorOf (AuthenticationSASLContinue challenge) k = k challenge

newtype AuthenticationSASLFinalPayload bytes = AuthenticationSASLFinalPayload bytes

deriving instance Eq bytes => Eq (AuthenticationSASLFinalPayload bytes)
deriving instance Show bytes => Show (AuthenticationSASLFinalPayload bytes)

type AuthenticationSASLFinal bytes = Envelope (AuthenticationSASLFinalPayload bytes) 'AuthenticationSASLFinalType

pattern AuthenticationSASLFinal :: bytes -> AuthenticationSASLFinal bytes
pattern AuthenticationSASLFinal outcome = Envelope (AuthenticationSASLFinalPayload outcome)
{-# COMPLETE AuthenticationSASLFinal #-}

instance HasConstructor 'AuthenticationSASLFinalType (AuthenticationSASLFinal bytes) where
  type ConstructorOf 'AuthenticationSASLFinalType (AuthenticationSASLFinal bytes) r = bytes -> r
  constructorOf = AuthenticationSASLFinal
  destructorOf (AuthenticationSASLFinal outcome) k = k outcome
