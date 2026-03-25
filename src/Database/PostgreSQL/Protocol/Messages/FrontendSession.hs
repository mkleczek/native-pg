{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}

module Database.PostgreSQL.Protocol.Messages.FrontendSession
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
import Database.PostgreSQL.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
import Database.PostgreSQL.Protocol.Messages.Types
  ( BindBody,
    BindBodyType (BindBodyType),
    Envelope,
    FormatCode,
    FormatCodeType (FormatCodeType),
    NamedObject,
    NamedObjectType (NamedObjectType),
    Oid,
    OidType (OidType),
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
                 PayloadCounted16 ('FixedBytesAtom 4)
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

data Close bytes = Close
  NamedObject
  bytes

deriving instance Eq bytes => Eq (Close bytes)
deriving instance Show bytes => Show (Close bytes)

type CloseEnvelope bytes =
  Envelope
    (Close bytes)
    'CloseType

data Describe bytes = Describe
  NamedObject
  bytes

deriving instance Eq bytes => Eq (Describe bytes)
deriving instance Show bytes => Show (Describe bytes)

type DescribeEnvelope bytes =
  Envelope
    (Describe bytes)
    'DescribeType

data Execute bytes = Execute bytes Int32

deriving instance Eq bytes => Eq (Execute bytes)
deriving instance Show bytes => Show (Execute bytes)

type ExecuteEnvelope bytes =
  Envelope
    (Execute bytes)
    'ExecuteType

data Flush = Flush

deriving instance Eq Flush
deriving instance Show Flush

type FlushEnvelope =
  Envelope
    Flush
    'FlushType

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

data Parse bytes = Parse bytes bytes [Maybe Oid]

deriving instance Eq bytes => Eq (Parse bytes)
deriving instance Show bytes => Show (Parse bytes)

type ParseEnvelope bytes =
  Envelope
    (Parse bytes)
    'ParseType

newtype Query bytes = Query bytes

deriving instance Eq bytes => Eq (Query bytes)
deriving instance Show bytes => Show (Query bytes)

type QueryEnvelope bytes =
  Envelope
    (Query bytes)
    'QueryType

data Sync = Sync

deriving instance Eq Sync
deriving instance Show Sync

type SyncEnvelope =
  Envelope
    Sync
    'SyncType

data Terminate = Terminate

deriving instance Eq Terminate
deriving instance Show Terminate

type TerminateEnvelope =
  Envelope
    Terminate
    'TerminateType
