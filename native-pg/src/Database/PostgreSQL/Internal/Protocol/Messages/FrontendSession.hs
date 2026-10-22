{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PatternSynonyms #-}
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
    Bind,
    pattern Bind,
    Close,
    pattern Close,
    Describe,
    pattern Describe,
    Execute,
    pattern Execute,
    Flush,
    pattern Flush,
    FunctionCall,
    pattern FunctionCall,
    Parse,
    pattern Parse,
    Query,
    pattern Query,
    Sync,
    pattern Sync,
    Terminate,
    pattern Terminate,
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
    Envelope (..),
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

newtype BindPayload bytes = BindPayload (BindBody bytes)

deriving instance Eq bytes => Eq (BindPayload bytes)
deriving instance Show bytes => Show (BindPayload bytes)

type Bind bytes = Envelope (BindPayload bytes) 'BindType

pattern Bind :: BindBody bytes -> Bind bytes
pattern Bind body = Envelope (BindPayload body)

{-# COMPLETE Bind #-}

instance HasConstructor 'BindType (Bind bytes) where
  type ConstructorOf 'BindType (Bind bytes) r = BindBody bytes -> r
  constructorOf = Bind
  destructorOf (Bind body) k = k body

data ClosePayload bytes = ClosePayload
  NamedObject
  bytes

deriving instance Eq bytes => Eq (ClosePayload bytes)
deriving instance Show bytes => Show (ClosePayload bytes)

type Close bytes = Envelope (ClosePayload bytes) 'CloseType

pattern Close :: NamedObject -> bytes -> Close bytes
pattern Close object name = Envelope (ClosePayload object name)

{-# COMPLETE Close #-}

instance HasConstructor 'CloseType (Close bytes) where
  type ConstructorOf 'CloseType (Close bytes) r = NamedObject -> bytes -> r
  constructorOf = Close
  destructorOf (Close object name) k = k object name

data DescribePayload bytes = DescribePayload
  NamedObject
  bytes

deriving instance Eq bytes => Eq (DescribePayload bytes)
deriving instance Show bytes => Show (DescribePayload bytes)

type Describe bytes = Envelope (DescribePayload bytes) 'DescribeType

pattern Describe :: NamedObject -> bytes -> Describe bytes
pattern Describe object name = Envelope (DescribePayload object name)

{-# COMPLETE Describe #-}

instance HasConstructor 'DescribeType (Describe bytes) where
  type ConstructorOf 'DescribeType (Describe bytes) r = NamedObject -> bytes -> r
  constructorOf = Describe
  destructorOf (Describe object name) k = k object name

data ExecutePayload bytes = ExecutePayload bytes Int32

deriving instance Eq bytes => Eq (ExecutePayload bytes)
deriving instance Show bytes => Show (ExecutePayload bytes)

type Execute bytes = Envelope (ExecutePayload bytes) 'ExecuteType

pattern Execute :: bytes -> Int32 -> Execute bytes
pattern Execute portalName maxRows = Envelope (ExecutePayload portalName maxRows)

{-# COMPLETE Execute #-}

instance HasConstructor 'ExecuteType (Execute bytes) where
  type ConstructorOf 'ExecuteType (Execute bytes) r = bytes -> Int32 -> r
  constructorOf = Execute
  destructorOf (Execute portalName maxRows) k = k portalName maxRows

data FlushPayload = FlushPayload

deriving instance Eq FlushPayload
deriving instance Show FlushPayload

type Flush = Envelope FlushPayload 'FlushType

pattern Flush :: Flush
pattern Flush = Envelope FlushPayload

{-# COMPLETE Flush #-}

instance HasConstructor 'FlushType Flush where
  type ConstructorOf 'FlushType Flush r = r
  constructorOf = Flush
  destructorOf Flush k = k

data FunctionCallPayload bytes = FunctionCallPayload
  Oid
  [FormatCode]
  [Maybe bytes]
  FormatCode

deriving instance Eq bytes => Eq (FunctionCallPayload bytes)
deriving instance Show bytes => Show (FunctionCallPayload bytes)

type FunctionCall bytes = Envelope (FunctionCallPayload bytes) 'FunctionCallType

pattern FunctionCall :: Oid -> [FormatCode] -> [Maybe bytes] -> FormatCode -> FunctionCall bytes
pattern FunctionCall function argumentFormats arguments resultFormat =
  Envelope (FunctionCallPayload function argumentFormats arguments resultFormat)

{-# COMPLETE FunctionCall #-}

instance HasConstructor 'FunctionCallType (FunctionCall bytes) where
  type ConstructorOf 'FunctionCallType (FunctionCall bytes) r = Oid -> [FormatCode] -> [Maybe bytes] -> FormatCode -> r
  constructorOf = FunctionCall
  destructorOf (FunctionCall function argumentFormats arguments resultFormat) k =
    k function argumentFormats arguments resultFormat

data ParsePayload bytes = ParsePayload bytes bytes [OptionalOid]

deriving instance Eq bytes => Eq (ParsePayload bytes)
deriving instance Show bytes => Show (ParsePayload bytes)

type Parse bytes = Envelope (ParsePayload bytes) 'ParseType

pattern Parse :: bytes -> bytes -> [OptionalOid] -> Parse bytes
pattern Parse statementName query parameterTypes = Envelope (ParsePayload statementName query parameterTypes)

{-# COMPLETE Parse #-}

instance HasConstructor 'ParseType (Parse bytes) where
  type ConstructorOf 'ParseType (Parse bytes) r = bytes -> bytes -> [OptionalOid] -> r
  constructorOf = Parse
  destructorOf (Parse statementName query parameterTypes) k = k statementName query parameterTypes

newtype QueryPayload bytes = QueryPayload bytes

deriving instance Eq bytes => Eq (QueryPayload bytes)
deriving instance Show bytes => Show (QueryPayload bytes)

type Query bytes = Envelope (QueryPayload bytes) 'QueryType

pattern Query :: bytes -> Query bytes
pattern Query query = Envelope (QueryPayload query)

{-# COMPLETE Query #-}

instance HasConstructor 'QueryType (Query bytes) where
  type ConstructorOf 'QueryType (Query bytes) r = bytes -> r
  constructorOf = Query
  destructorOf (Query query) k = k query

data SyncPayload = SyncPayload

deriving instance Eq SyncPayload
deriving instance Show SyncPayload

type Sync = Envelope SyncPayload 'SyncType

pattern Sync :: Sync
pattern Sync = Envelope SyncPayload

{-# COMPLETE Sync #-}

instance HasConstructor 'SyncType Sync where
  type ConstructorOf 'SyncType Sync r = r
  constructorOf = Sync
  destructorOf Sync k = k

data TerminatePayload = TerminatePayload

deriving instance Eq TerminatePayload
deriving instance Show TerminatePayload

type Terminate = Envelope TerminatePayload 'TerminateType

pattern Terminate :: Terminate
pattern Terminate = Envelope TerminatePayload

{-# COMPLETE Terminate #-}

instance HasConstructor 'TerminateType Terminate where
  type ConstructorOf 'TerminateType Terminate r = r
  constructorOf = Terminate
  destructorOf Terminate k = k
