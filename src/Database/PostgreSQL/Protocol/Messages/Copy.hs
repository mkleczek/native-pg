{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}

module Database.PostgreSQL.Protocol.Messages.Copy
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

import Database.PostgreSQL.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
import Database.PostgreSQL.Protocol.Messages.Types (Envelope)

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

data CopyDone = CopyDone

deriving instance Eq CopyDone
deriving instance Show CopyDone

type CopyDoneEnvelope =
  Envelope
    CopyDone
    'CopyDoneType

newtype CopyFail bytes = CopyFail bytes

deriving instance Eq bytes => Eq (CopyFail bytes)
deriving instance Show bytes => Show (CopyFail bytes)

type CopyFailEnvelope bytes =
  Envelope
    (CopyFail bytes)
    'CopyFailType
