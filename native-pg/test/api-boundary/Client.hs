{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module Client where

import Database.PostgreSQL.ConnectionString ()
import Database.PostgreSQL.ConnectionString.Types ()
import Database.PostgreSQL.Decoding ()
import Database.PostgreSQL.Diagnostics (ConnectionError (..))
import Database.PostgreSQL.Encoders (Parameter (..))
import Database.PostgreSQL.Session
  ( Session, Connection, connection, close, isUsable, checkIdle, closeStatement, cancel,
  )
import Database.PostgreSQL.Session.Settings ()
import Database.PostgreSQL.Snippet ()
import Database.PostgreSQL.Pool (Pool, UsageError, checkoutConnection, checkoutIn, markBroken)
import Database.PostgreSQL.Streaming (ByteStream (..), operationIn, withBuffers, withOperation)
import Database.PostgreSQL.Streaming.Types ()
import Database.PostgreSQL.Transport (Channel, Link (..), newChannel, serverEndPoint)
import Database.PostgreSQL.Transport.Link ()
import Database.PostgreSQL.Types ()
import qualified Database.PostgreSQL.Transaction as Transaction
import qualified Control.Concurrent.Resource.Acquisition as Acquisition
import Control.Exception (bracket)
import Data.ByteString (ByteString)
import Data.Functor.Const (Const (..))
import Data.Int (Int64)
import Data.Word (Word32)
import Network.Streaming.BufferPool
  ( BufferPool, PoolSettings (..), newBufferPool
  , BufferAccess, withRequestBuffers, BufferLoan, borrowBuffer, loanBuffer, releaseBuffer
  , BufferGrant, withSetupBuffers
  )
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Scope (withScope)
import Network.Streaming.Source (WaitingSource (..))
import Network.Streaming.Watch (watchCancel)

prepareNamed :: ByteString -> params -> Transaction.Statement params row -> Transaction.Pipeline tx (Transaction.Preparation tx)
prepareNamed = Transaction.prepare

awaitNamed :: Transaction.Preparation tx -> Transaction.Transaction tx ()
awaitNamed = Transaction.awaitPreparation

-- Results are read in the order of their statements, or discarded.
discarded :: Transaction.Result tx row -> Transaction.Transaction tx ()
discarded = Transaction.discard

misordered :: ConnectionError -> Maybe Int
misordered failure = case failure of
  ResultOutOfOrder unread -> Just unread
  _ -> Nothing

-- A client records a name once the server accepted its Parse, before the rows
-- that can still fail. The result of a statement before them that it does not
-- want is discarded, not left unread.
preparedRows :: ByteString -> Transaction.Statement () (Const Int64) -> Transaction.Transaction tx [Int64]
preparedRows name known = do
  (setting, preparation, rows) <- Transaction.pipeline ((,,) <$> query "SELECT 1" <*> prepareNamed name () known <*> executeNamed name () known)
  discarded setting
  awaitNamed preparation
  Transaction.rowList rows

executeNamed :: ByteString -> params -> Transaction.Statement params row -> Transaction.Pipeline tx (Transaction.Result tx row)
executeNamed = Transaction.executePrepared

parameterType :: Transaction.Param -> Maybe Word32
parameterType = Transaction.paramType

closeSession :: Session -> IO ()
closeSession = close . connection

query :: ByteString -> Transaction.Pipeline tx (Transaction.Result tx (Const Int64))
query sql = Transaction.statement () $
  Transaction.Statement sql (const []) (Transaction.singleRow (Transaction.row @'[Int64] Const))

newtype MaybeField tx = MaybeField (Maybe (Transaction.Field tx))

streamed :: Transaction.Statement () MaybeField
streamed = Transaction.Statement "SELECT 'body'::text" (const [])
  (Transaction.singleRow (Transaction.streamingRow @'[] MaybeField))

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

connectionIdle :: Connection -> IO (Either String ())
connectionIdle = checkIdle

data Response tx = Response Int64 (Maybe (Transaction.Field tx))

responseStatement :: Transaction.Statement () Response
responseStatement = Transaction.Statement "SELECT 1::bigint, 'body'::text" (const [])
  (Transaction.singleRow (Transaction.streamingRow @'[Int64] Response))

transaction :: Session -> IO ()
transaction session = Transaction.runTransaction Transaction.ReadCommitted Transaction.Read session $ do
  cursor <- Transaction.pipeline (Transaction.statement () responseStatement)
  _ <- Transaction.nextRow cursor
  Transaction.finishTransaction

-- A client can manage short loans while the request owns admission.
requestBuffer :: BufferPool -> (Buffer -> IO a) -> IO a
requestBuffer resources use = withRequestBuffers resources $ \access ->
  bracket (borrowBuffer access) releaseBuffer (use . loanBuffer)

manualLoan :: BufferAccess -> IO BufferLoan
manualLoan = borrowBuffer

setupBuffers :: BufferGrant grant => BufferPool -> (grant -> IO a) -> IO a
setupBuffers = withSetupBuffers

resourceSettings :: PoolSettings
resourceSettings = PoolSettings
  { bufferSize = positiveInt @4096
  , ordinaryBufferCount = positiveInt @2
  , reserveBufferCount = positiveInt @1
  , setupBufferCount = positiveInt @2
  }

createResources :: Acquisition.Context -> IO BufferPool
createResources acquisition = newBufferPool acquisition resourceSettings

-- A request in a scope: a connection checked out, the request's buffers bound
-- to it, a transaction, a field read as a source, and the ending's reply.
inScope :: BufferAccess -> Pool Connection failure -> IO (Either (UsageError failure) (Maybe ByteString))
inScope access pool = withScope $ \scope -> do
  checked <- checkoutIn scope pool (const (pure (Right ())))
  case checked of
    Left failure -> pure (Left failure)
    Right checkout -> do
      session <- operationIn scope access (checkoutConnection checkout)
      tx <- Transaction.transactionIn scope Transaction.ReadCommitted Transaction.Read session
      current <- Transaction.runIn tx $ do
        cursor <- Transaction.finalPipeline (Transaction.statement () streamed)
        Transaction.nextRow cursor
      case current of
        Just (MaybeField (Just field)) -> do
          chunk <- pullWaiting (Transaction.fieldSource field) (>>= watchCancel)
          _ <- Transaction.endingWaiting tx (>>= watchCancel)
          pure (Right chunk)
        _ -> Right Nothing <$ markBroken checkout Nothing
