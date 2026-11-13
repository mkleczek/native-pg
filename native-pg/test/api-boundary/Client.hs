{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module Client where

import Control.Arrow (arr, returnA, (&&&), (>>>), (^>>))
import Data.Int (Int64)
import Database.PostgreSQL.Connection (Address (..), ConnectSettings, Connection, cancelKey, connectSettings, connectedHost, serverInfo)
import Database.PostgreSQL.ConnectionString (ConnectionString, fromConnectionString)
import Database.PostgreSQL.ConnectionString.Types ()
import Database.PostgreSQL.Decoding (InPlace)
import Database.PostgreSQL.Diagnostics ()
import Database.PostgreSQL.Errors (ConnectError (..), ConnectionError (..), PoolError (..), QueryError, UsageError (..))
import Database.PostgreSQL.Encoders (Parameter (..))
import Database.PostgreSQL.Notifications (Notification, listen, notification)
import Database.PostgreSQL.Pool (ConnectionPoolSettings, connectionPoolSettings, newConnectionPool, withConnection)
import Database.PostgreSQL.Session.Settings ()
import Database.PostgreSQL.Snippet (fromSnippet)
import Database.PostgreSQL.Transaction
  ( Cardinality (..), Ending (..), IsolationLevel (..), Mode (..), Rows (..), Statement (..), column, foldRows, one, pipeline, single, statement, transaction )
import Database.PostgreSQL.Transport (serverEndPoint)
import Database.PostgreSQL.Types (Param, paramType)
import qualified Control.Concurrent.Resource.Acquisition as Acquisition
import Control.Exception (bracket)
import Data.Word (Word32)
import Network.Streaming.BufferPool
  ( BufferPool, PoolSettings (..), newBufferPool
  , BufferAccess, withRequestBuffers, BufferLoan, borrowBuffer, loanBuffer, releaseBuffer
  , BufferGrant, withSetupBuffers
  )
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Classes (Runtime)
import Network.Streaming.Loans (Loans)
import Network.Streaming.Pool (Pool)
import Network.Streaming.PositiveInt (positiveInt)

misordered :: UsageError -> Maybe Int
misordered failure = case failure of
  ResultOutOfOrder unread -> Just unread
  _ -> Nothing

terminated :: ConnectionError -> Bool
terminated failure = case failure of
  ConnectionTerminated _ -> True
  ConnectionBroken _ -> False

rejected :: ConnectError -> Bool
rejected failure = case failure of
  ConnectionRejected _ -> True
  _ -> False

timedOut :: PoolError -> Bool
timedOut failure = failure == CheckoutTimedOut

-- | A request's program, from the public API alone: a transaction on a pooled
-- connection, whose pipeline carries the COMMIT.
countRequest :: Runtime arr => Pool Connection -> arr Loans (Either PoolError (Either QueryError Int64))
countRequest pool =
  (, ((), ()))
    ^>> withConnection pool (transaction ReadCommitted Read (pipeline Final (statement count) single))
  where
    count :: Runtime arr => Statement arr () Int64
    count = Statement "SELECT count(*) FROM t" (const []) (one column)

-- | The statement that a snippet makes, and a fold of its rows.
foldRequest :: Runtime arr => Pool Connection -> arr Loans (Either PoolError (Either QueryError Int))
foldRequest pool =
  (, ((), ()))
    ^>> withConnection pool (transaction ReadCommitted Read (pipeline Final (statement ones) (foldRows (\n (_ :: Int64) -> n + 1) (0 :: Int))))
  where
    ones :: (Runtime arr, InPlace a) => Statement arr () a
    ones = fromSnippet "SELECT 1" (Rows ExactlyOne column)

-- | What a client can ask of settings, a connection and a pool.
settingsOf :: Either String ConnectSettings
settingsOf = Right (connectSettings (UnixAddress "/tmp") "postgres")

hostOf :: Connection -> (Bool, Bool)
hostOf connection = (serverInfo connection `seq` True, connectedHost connection `seq` cancelKey connection `seq` True)

makePool :: ConnectionPoolSettings -> IO (Pool Connection)
makePool = newConnectionPool

poolOf :: ConnectSettings -> ConnectionPoolSettings
poolOf = connectionPoolSettings 4

allHosts :: ConnectionString -> Either String ConnectSettings
allHosts = fromConnectionString

-- | Listens, and waits for a notification, on a connection of its own.
notified :: Runtime arr => arr Connection Notification
notified = (listen ["events"] &&& returnA) >>> arr snd >>> notification

parameterType :: Param -> Maybe Word32
parameterType = paramType

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
