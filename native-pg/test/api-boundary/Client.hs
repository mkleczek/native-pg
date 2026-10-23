{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module Client where

import Database.PostgreSQL.ConnectionString ()
import Database.PostgreSQL.ConnectionString.Types ()
import Database.PostgreSQL.Decoding ()
import Database.PostgreSQL.Diagnostics ()
import Database.PostgreSQL.Encoders (Parameter (..))
import Database.PostgreSQL.Session
  ( Session, Connection, connection, close, isUsable, closeStatement, cancel,
  )
import Database.PostgreSQL.Session.Settings ()
import Database.PostgreSQL.Snippet ()
import Database.PostgreSQL.Streaming (ByteStream (..), withBuffers, withOperation)
import Database.PostgreSQL.Streaming.Types ()
import Database.PostgreSQL.Transport (Channel, Link (..), newChannel, serverEndPoint)
import Database.PostgreSQL.Transport.Link ()
import Database.PostgreSQL.Types ()
import qualified Database.PostgreSQL.Transaction as Transaction
import Data.ByteString (ByteString)
import Data.Functor.Const (Const (..))
import Data.Int (Int64)
import Data.Word (Word32)
import Network.Streaming.Resources (BufferAccess)
import Network.Streaming.Buffer (Buffer)

prepare :: Transaction.Prepare -> ByteString -> [Maybe Word32] -> IO Transaction.Prepared
prepare = id

parameterType :: Transaction.Param -> Maybe Word32
parameterType = Transaction.paramType

closeSession :: Session -> IO ()
closeSession = close . connection

query :: ByteString -> Transaction.Pipeline tx (Transaction.Result tx (Const Int64))
query sql = Transaction.statement () $
  Transaction.Statement sql (const []) (Transaction.singleRow (Transaction.row @'[Int64] Const)) False

newtype MaybeField tx = MaybeField (Maybe (Transaction.Field tx))

streamed :: Transaction.Statement () MaybeField
streamed = Transaction.Statement "SELECT 'body'::text" (const [])
  (Transaction.singleRow (Transaction.streamingRow @'[] MaybeField)) False

owned :: Transaction.Result tx (Const Int64) -> Transaction.Transaction tx [Int64]
owned = Transaction.rowList

borrowOperation :: BufferAccess -> Connection -> (Session -> IO a) -> IO a
borrowOperation = withOperation

borrowBuffers :: Buffer -> Buffer -> Connection -> (Session -> IO a) -> IO a
borrowBuffers = withBuffers

closeConnection :: Connection -> IO ()
closeConnection = close

connectionUsable :: Connection -> IO Bool
connectionUsable = isUsable

data Response tx = Response Int64 (Maybe (Transaction.Field tx))

responseStatement :: Transaction.Statement () Response
responseStatement = Transaction.Statement "SELECT 1::bigint, 'body'::text" (const [])
  (Transaction.singleRow (Transaction.streamingRow @'[Int64] Response)) False

transaction :: Session -> IO ()
transaction session = Transaction.runTransaction Transaction.ReadCommitted Transaction.Read session $ do
  cursor <- Transaction.pipeline (Transaction.statement () responseStatement)
  _ <- Transaction.nextRow cursor
  Transaction.finishTransaction
