{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.Copy
  ( CopyDataType (CopyDataType),
    CopyDoneType (CopyDoneType),
    CopyFailType (CopyFailType),
    CopyDataEnvelope,
    CopyDoneEnvelope,
    CopyFailEnvelope,
    CopyData (CopyData),
    CopyDone (CopyDone),
    CopyFail (CopyFail),
  )
where

import Database.PostgreSQL.Internal.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types (Envelope)

type CopyMessage tag payload =
  'PayloadStruct
    '[ 'PayloadConst 1 tag,
       'PayloadBounded payload
     ]

data CopyDataType (payload :: Payload) where
  CopyDataType ::
    CopyDataType
      (CopyMessage 'd' 'RemainingBytesAtom)

data CopyDoneType (payload :: Payload) where
  CopyDoneType ::
    CopyDoneType
      (CopyMessage 'c' Empty)

data CopyFailType (payload :: Payload) where
  CopyFailType ::
    CopyFailType
      (CopyMessage 'f' 'CStringAtom)

newtype CopyData bytes = CopyData bytes

deriving instance Eq bytes => Eq (CopyData bytes)
deriving instance Show bytes => Show (CopyData bytes)

type CopyDataEnvelope bytes =
  Envelope
    (CopyData bytes)
    'CopyDataType

instance HasConstructor 'CopyDataType (CopyData bytes) where
  type ConstructorOf 'CopyDataType (CopyData bytes) r = bytes -> r
  constructorOf = CopyData
  destructorOf (CopyData chunk) k = k chunk

data CopyDone = CopyDone

deriving instance Eq CopyDone
deriving instance Show CopyDone

type CopyDoneEnvelope =
  Envelope
    CopyDone
    'CopyDoneType

instance HasConstructor 'CopyDoneType CopyDone where
  type ConstructorOf 'CopyDoneType CopyDone r = r
  constructorOf = CopyDone
  destructorOf CopyDone k = k

newtype CopyFail bytes = CopyFail bytes

deriving instance Eq bytes => Eq (CopyFail bytes)
deriving instance Show bytes => Show (CopyFail bytes)

type CopyFailEnvelope bytes =
  Envelope
    (CopyFail bytes)
    'CopyFailType

instance HasConstructor 'CopyFailType (CopyFail bytes) where
  type ConstructorOf 'CopyFailType (CopyFail bytes) r = bytes -> r
  constructorOf = CopyFail
  destructorOf (CopyFail reason) k = k reason
