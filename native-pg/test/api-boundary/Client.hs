{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module Client where

import Control.Arrow ((***), (>>>), (^>>))
import Data.Int (Int64)
import Database.PostgreSQL.Connection (Address (..), ConnectSettings, Connection, cancelKey, connectSettings, connectedHost, serverInfo)
import Database.PostgreSQL.ConnectionString (ConnectionString, fromConnectionString)
import Database.PostgreSQL.ConnectionString.Types ()
import Database.PostgreSQL.Decoding (InPlace)
import Database.PostgreSQL.Diagnostics ()
import Database.PostgreSQL.Errors (ConnectError (..), ConnectionError (..), PoolError (..), QueryError, UsageError (..))
import Database.PostgreSQL.Encoders (Parameter (..))
import Database.PostgreSQL.Notifications (Notification, listen, notification)
import Database.PostgreSQL.Pool (ConnectionPool, ConnectionPoolSettings, connectionPoolSettings, withConnection, withConnectionPool)
import Database.PostgreSQL.Session.Settings ()
import Database.PostgreSQL.Snippet (fromSnippet)
import Database.PostgreSQL.Transaction
  ( Cardinality (..), Ending (..), IsolationLevel (..), Mode (..), Rows (..), Statement (..), column, foldRows, one, pipeline, single, statement, transaction )
import Database.PostgreSQL.Transport (serverEndPoint)
import Database.PostgreSQL.Types (Param, paramType)
import Data.Word (Word32)
import Network.Streaming.BufferPool (BufferGrant, BufferPool, BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Classes (Runtime, scope)
import Network.Streaming.Combinators (tap)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers, withSetupBuffers)
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
countRequest :: Runtime arr => ConnectionPool -> arr Loans (Either PoolError (Either QueryError Int64))
countRequest pool =
  (, ((), ()))
    ^>> withConnection pool (transaction ReadCommitted Read (pipeline Final (statement count) single))
  where
    count :: Runtime arr => Statement arr () Int64
    count = Statement "SELECT count(*) FROM t" (const []) (one column)

-- | The statement that a snippet makes, and a fold of its rows.
foldRequest :: Runtime arr => ConnectionPool -> arr Loans (Either PoolError (Either QueryError Int))
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

poolFor :: ConnectionPoolSettings -> (ConnectionPool -> IO a) -> IO a
poolFor = withConnectionPool

poolOf :: ConnectSettings -> ConnectionPoolSettings
poolOf = connectionPoolSettings 4

allHosts :: ConnectionString -> Either String ConnectSettings
allHosts = fromConnectionString

-- | Listens, and waits for a notification, on a connection of its own.
notified :: Runtime arr => arr Connection Notification
notified = tap (listen ["events"]) >>> notification

parameterType :: Param -> Maybe Word32
parameterType = paramType

-- A client can manage short loans while the request owns admission.
requestBuffer :: Runtime arr => BufferPool -> arr (Buffer, a) b -> arr a b
requestBuffer resources use = withRequestBuffers resources $
  scope (fst ^>> borrow) (fst ^>> release) ((loanBuffer *** snd) ^>> use)

manualLoan :: Runtime arr => arr Loans Loan
manualLoan = borrow

setupBuffers :: (Runtime arr, BufferGrant grant) => BufferPool -> arr (grant, a) b -> arr a b
setupBuffers = withSetupBuffers

resourceSettings :: BufferPoolSettings
resourceSettings = BufferPoolSettings
  { bufferSize = positiveInt @4096
  , ordinaryBufferCount = positiveInt @2
  , reserveBufferCount = positiveInt @1
  , setupBufferCount = positiveInt @2
  }

createResources :: IO BufferPool
createResources = newBufferPool resourceSettings
