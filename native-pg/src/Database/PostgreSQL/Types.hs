-- | Public protocol values used in settings, parameters and query results.
module Database.PostgreSQL.Types
  ( ProtocolVersion (..), protocolVersion3_0, protocolVersion3_2,
    FormatCode (..), TransactionStatus (..),
    Oid, Param (..), Parameter (..),
  ) where

import Data.ByteString (ByteString)
import Data.Word (Word16, Word32)

data ProtocolVersion = ProtocolVersion
  { protocolVersionMajor :: Word16,
    protocolVersionMinor :: Word16
  }

deriving instance Eq ProtocolVersion
deriving instance Ord ProtocolVersion
deriving instance Show ProtocolVersion

protocolVersion3_0 :: ProtocolVersion
protocolVersion3_0 = ProtocolVersion 3 0

protocolVersion3_2 :: ProtocolVersion
protocolVersion3_2 = ProtocolVersion 3 2

-- | The OID of a type.
type Oid = Word32

data FormatCode
  = TextFormat
  | BinaryFormat

deriving instance Eq FormatCode
deriving instance Ord FormatCode
deriving instance Show FormatCode

data TransactionStatus
  = TxIdle
  | TxInTransaction
  | TxFailedTransaction

deriving instance Eq TransactionStatus
deriving instance Ord TransactionStatus
deriving instance Show TransactionStatus

-- | One encoded parameter. A missing type asks the server to infer it;
-- a missing value represents SQL NULL.
data Param = Param
  { paramType :: Maybe Word32,
    paramFormat :: FormatCode,
    paramValue :: Maybe Parameter
  }

-- | A parameter value the client sends: bytes at hand, whole or in chunks.
data Parameter
  = ParameterBytes ByteString
  | -- | Sent one after another, each from where it is, as the serializer sends a
    -- long one, so that they aren't copied into one first.
    ParameterChunks [ByteString]
  | -- | This many bytes of the stream of the statement that takes the parameter: they are not there to serialize, and a
    -- transaction sends them from the stream, as 'Database.PostgreSQL.Transaction.FromStream' does. A statement from a snippet
    -- marks the parameter that it streams so.
    ParameterStream Int
