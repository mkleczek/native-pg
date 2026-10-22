-- | Public protocol values used in settings, parameters and query results.
module Database.PostgreSQL.Types
  ( ProtocolVersion (..), protocolVersion3_0, protocolVersion3_2,
    FormatCode (..), TransactionStatus (..),
  ) where

import Data.Word (Word16)

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
