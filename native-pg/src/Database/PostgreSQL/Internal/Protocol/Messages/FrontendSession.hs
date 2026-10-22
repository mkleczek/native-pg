{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

module Database.PostgreSQL.Internal.Protocol.Messages.FrontendSession
  ( BindType (BindType),
    CloseType (CloseType),
    DescribeType (DescribeType),
    ExecuteType (ExecuteType),
    FlushType (FlushType),
    FunctionCallType (FunctionCallType),
    ParseType (ParseType),
    QueryType (QueryType),
    SyncType (SyncType),
    TerminateType (TerminateType),
    BindEnvelope,
    CloseEnvelope,
    DescribeEnvelope,
    ExecuteEnvelope,
    FlushEnvelope,
    FunctionCallEnvelope,
    ParseEnvelope,
    QueryEnvelope,
    SyncEnvelope,
    TerminateEnvelope,
    Bind (Bind),
    Close (Close),
    Describe (Describe),
    Execute (Execute),
    Flush (Flush),
    FunctionCall (FunctionCall),
    Parse (Parse),
    Query (Query),
    Sync (Sync),
    Terminate (Terminate),
  )
where

import Data.Int (Int32)
import Database.PostgreSQL.Internal.Protocol.Codec
  ( HasConstructor (..),
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types
  ( BindBody,
    BindBodyType (BindBodyType),
    Envelope,
    FormatCode,
    FormatCodeType (FormatCodeType),
    NamedObject,
    NamedObjectType (NamedObjectType),
    Oid,
    OidType (OidType),
    OptionalOid,
    OptionalOidType (OptionalOidType),
  )

type FrontendSessionMessage tag payload =
  'PayloadStruct
    '[ 'PayloadConst 1 tag,
       'PayloadBounded payload
     ]

data BindType (payload :: Payload) where
  BindType ::
    BindType
      (FrontendSessionMessage 'B' ('PayloadField 'BindBodyType))

data CloseType (payload :: Payload) where
  CloseType ::
    CloseType
      (FrontendSessionMessage 'C' ('PayloadStruct '[PayloadField 'NamedObjectType, 'CStringAtom]))

data DescribeType (payload :: Payload) where
  DescribeType ::
    DescribeType
      (FrontendSessionMessage 'D' ('PayloadStruct '[PayloadField 'NamedObjectType, 'CStringAtom]))

data ExecuteType (payload :: Payload) where
  ExecuteType ::
    ExecuteType
      (FrontendSessionMessage 'E' ('PayloadStruct '[ 'CStringAtom, 'FixedBytesAtom 4]))

data FlushType (payload :: Payload) where
  FlushType ::
    FlushType
      (FrontendSessionMessage 'H' Empty)

data FunctionCallType (payload :: Payload) where
  FunctionCallType ::
    FunctionCallType
      ( FrontendSessionMessage
          'F'
          ( PayloadStruct
              '[ PayloadField 'OidType,
                 PayloadCounted16 ('PayloadField 'FormatCodeType),
                 PayloadCounted16 'MaybeBytes32Atom,
                 PayloadField 'FormatCodeType
               ]
          )
      )

data ParseType (payload :: Payload) where
  ParseType ::
    ParseType
      ( FrontendSessionMessage
          'P'
          ( PayloadStruct
              '[ 'CStringAtom,
                 'CStringAtom,
                 PayloadCounted16 ('PayloadField 'OptionalOidType)
               ]
          )
      )

data QueryType (payload :: Payload) where
  QueryType ::
    QueryType
      (FrontendSessionMessage 'Q' 'CStringAtom)

data SyncType (payload :: Payload) where
  SyncType ::
    SyncType
      (FrontendSessionMessage 'S' Empty)

data TerminateType (payload :: Payload) where
  TerminateType ::
    TerminateType
      (FrontendSessionMessage 'X' Empty)

newtype Bind bytes = Bind (BindBody bytes)

deriving instance Eq bytes => Eq (Bind bytes)
deriving instance Show bytes => Show (Bind bytes)

type BindEnvelope bytes =
  Envelope
    (Bind bytes)
    'BindType

instance HasConstructor 'BindType (Bind bytes) where
  type ConstructorOf 'BindType (Bind bytes) r = BindBody bytes -> r
  constructorOf = Bind
  destructorOf (Bind body) k = k body

data Close bytes = Close
  NamedObject
  bytes

deriving instance Eq bytes => Eq (Close bytes)
deriving instance Show bytes => Show (Close bytes)

type CloseEnvelope bytes =
  Envelope
    (Close bytes)
    'CloseType

instance HasConstructor 'CloseType (Close bytes) where
  type ConstructorOf 'CloseType (Close bytes) r = NamedObject -> bytes -> r
  constructorOf = Close
  destructorOf (Close object name) k = k object name

data Describe bytes = Describe
  NamedObject
  bytes

deriving instance Eq bytes => Eq (Describe bytes)
deriving instance Show bytes => Show (Describe bytes)

type DescribeEnvelope bytes =
  Envelope
    (Describe bytes)
    'DescribeType

instance HasConstructor 'DescribeType (Describe bytes) where
  type ConstructorOf 'DescribeType (Describe bytes) r = NamedObject -> bytes -> r
  constructorOf = Describe
  destructorOf (Describe object name) k = k object name

data Execute bytes = Execute bytes Int32

deriving instance Eq bytes => Eq (Execute bytes)
deriving instance Show bytes => Show (Execute bytes)

type ExecuteEnvelope bytes =
  Envelope
    (Execute bytes)
    'ExecuteType

instance HasConstructor 'ExecuteType (Execute bytes) where
  type ConstructorOf 'ExecuteType (Execute bytes) r = bytes -> Int32 -> r
  constructorOf = Execute
  destructorOf (Execute portalName maxRows) k = k portalName maxRows

data Flush = Flush

deriving instance Eq Flush
deriving instance Show Flush

type FlushEnvelope =
  Envelope
    Flush
    'FlushType

instance HasConstructor 'FlushType Flush where
  type ConstructorOf 'FlushType Flush r = r
  constructorOf = Flush
  destructorOf Flush k = k

data FunctionCall bytes = FunctionCall
  Oid
  [FormatCode]
  [Maybe bytes]
  FormatCode

deriving instance Eq bytes => Eq (FunctionCall bytes)
deriving instance Show bytes => Show (FunctionCall bytes)

type FunctionCallEnvelope bytes =
  Envelope
    (FunctionCall bytes)
    'FunctionCallType

instance HasConstructor 'FunctionCallType (FunctionCall bytes) where
  type ConstructorOf 'FunctionCallType (FunctionCall bytes) r = Oid -> [FormatCode] -> [Maybe bytes] -> FormatCode -> r
  constructorOf = FunctionCall
  destructorOf (FunctionCall function argumentFormats arguments resultFormat) k =
    k function argumentFormats arguments resultFormat

data Parse bytes = Parse bytes bytes [OptionalOid]

deriving instance Eq bytes => Eq (Parse bytes)
deriving instance Show bytes => Show (Parse bytes)

type ParseEnvelope bytes =
  Envelope
    (Parse bytes)
    'ParseType

instance HasConstructor 'ParseType (Parse bytes) where
  type ConstructorOf 'ParseType (Parse bytes) r = bytes -> bytes -> [OptionalOid] -> r
  constructorOf = Parse
  destructorOf (Parse statementName query parameterTypes) k = k statementName query parameterTypes

newtype Query bytes = Query bytes

deriving instance Eq bytes => Eq (Query bytes)
deriving instance Show bytes => Show (Query bytes)

type QueryEnvelope bytes =
  Envelope
    (Query bytes)
    'QueryType

instance HasConstructor 'QueryType (Query bytes) where
  type ConstructorOf 'QueryType (Query bytes) r = bytes -> r
  constructorOf = Query
  destructorOf (Query query) k = k query

data Sync = Sync

deriving instance Eq Sync
deriving instance Show Sync

type SyncEnvelope =
  Envelope
    Sync
    'SyncType

instance HasConstructor 'SyncType Sync where
  type ConstructorOf 'SyncType Sync r = r
  constructorOf = Sync
  destructorOf Sync k = k

data Terminate = Terminate

deriving instance Eq Terminate
deriving instance Show Terminate

type TerminateEnvelope =
  Envelope
    Terminate
    'TerminateType

instance HasConstructor 'TerminateType Terminate where
  type ConstructorOf 'TerminateType Terminate r = r
  constructorOf = Terminate
  destructorOf Terminate k = k
