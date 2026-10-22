{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.Copy
  ( CopyDataType (CopyDataType),
    CopyDoneType (CopyDoneType),
    CopyFailType (CopyFailType),
    CopyData,
    pattern CopyData,
    CopyDone,
    pattern CopyDone,
    CopyFail,
    pattern CopyFail,
  )
where

import Database.PostgreSQL.Internal.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types (Envelope (..))

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

newtype CopyDataPayload bytes = CopyDataPayload bytes

deriving instance Eq bytes => Eq (CopyDataPayload bytes)
deriving instance Show bytes => Show (CopyDataPayload bytes)

type CopyData bytes = Envelope (CopyDataPayload bytes) 'CopyDataType

pattern CopyData :: bytes -> CopyData bytes
pattern CopyData chunk = Envelope (CopyDataPayload chunk)

{-# COMPLETE CopyData #-}

instance HasConstructor 'CopyDataType (CopyData bytes) where
  type ConstructorOf 'CopyDataType (CopyData bytes) r = bytes -> r
  constructorOf = CopyData
  destructorOf (CopyData chunk) k = k chunk

data CopyDonePayload = CopyDonePayload

deriving instance Eq CopyDonePayload
deriving instance Show CopyDonePayload

type CopyDone = Envelope CopyDonePayload 'CopyDoneType

pattern CopyDone :: CopyDone
pattern CopyDone = Envelope CopyDonePayload

{-# COMPLETE CopyDone #-}

instance HasConstructor 'CopyDoneType CopyDone where
  type ConstructorOf 'CopyDoneType CopyDone r = r
  constructorOf = CopyDone
  destructorOf CopyDone k = k

newtype CopyFailPayload bytes = CopyFailPayload bytes

deriving instance Eq bytes => Eq (CopyFailPayload bytes)
deriving instance Show bytes => Show (CopyFailPayload bytes)

type CopyFail bytes = Envelope (CopyFailPayload bytes) 'CopyFailType

pattern CopyFail :: bytes -> CopyFail bytes
pattern CopyFail reason = Envelope (CopyFailPayload reason)

{-# COMPLETE CopyFail #-}

instance HasConstructor 'CopyFailType (CopyFail bytes) where
  type ConstructorOf 'CopyFailType (CopyFail bytes) r = bytes -> r
  constructorOf = CopyFail
  destructorOf (CopyFail reason) k = k reason
