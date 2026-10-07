{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Transactions, with the names and shape of hasql's @Transaction@: a monad
-- of statements whose results are read into values, run between @BEGIN@ and
-- @COMMIT@, or @ABORT@ when it's condemned or a statement fails.
--
-- @BEGIN@ waits to be sent with the first statements, in one round trip.
-- Statements whose results the others don't depend on can also go together,
-- in a 'pipeline'. Nothing is retried, since a streamed result or parameter
-- can't be replayed. The statements, and their results, are those of
-- "Database.PostgreSQL.Statement".
module Database.PostgreSQL.Transaction
  ( -- * Transactions
    Transaction,
    IsolationLevel (..),
    Mode (..),
    transactionNoRetry,
    Prepare,
    Prepared (..),
    transactionNoRetryWith,
    statement,
    sql,
    pipeline,
    condemn,

    -- * Statements
    Statement (..),
    Param (..),
    QueryError (..),
    StatementError (..),

    -- * Results
    Result,
    RowDecoder,
    row,
    noResult,
    singleRow,
    rowMaybe,
    rowList,

    -- * Streaming
    ScopedRow,
    ScopedStream,
    foldRows,
    decodeScoped,
    decodeScopedStreaming,
    streamLength,
    streamChunk,
  )
where

import Control.Exception (SomeException, onException, try)
import Control.Monad (ap, liftM, void, when)
import Control.Monad.IO.Class (MonadIO (..))
import Data.ByteString (ByteString)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (isNothing)
import Database.PostgreSQL.Pipeline (Pipeline, Prepare, Prepared (..), runPipeline)
import qualified Database.PostgreSQL.Pipeline as Pipeline
import Database.PostgreSQL.Session (Phase (Ready), Session)
import Database.PostgreSQL.Statement

-- | Statements run in one transaction, which can be condemned to end in a
-- rollback. A failing statement ends it.
newtype Transaction a = Transaction (Context -> IO (Either QueryError a))

data Context = Context
  { contextSession :: Session 'Ready,
    contextPrepare :: Maybe Prepare,
    -- | The @BEGIN@ to send with the first statements, until they're sent.
    contextBegin :: IORef (Maybe ByteString),
    -- | Whether the transaction commits.
    contextCommits :: IORef Bool
  }

instance Functor Transaction where
  fmap = liftM

instance Applicative Transaction where
  pure value = Transaction (\_ -> pure (Right value))
  (<*>) = ap

instance Monad Transaction where
  Transaction run >>= next = Transaction $ \context ->
    run context >>= \case
      Left failure -> pure (Left failure)
      Right value -> let Transaction continue = next value in continue context

instance MonadIO Transaction where
  liftIO action = Transaction (\_ -> Right <$> action)

data IsolationLevel
  = ReadCommitted
  | RepeatableRead
  | Serializable
  deriving (Eq, Ord, Show)

data Mode
  = Read
  | Write
  deriving (Eq, Ord, Show)

-- | Runs the transaction: @BEGIN@ with the isolation level and the mode,
-- then @COMMIT@, or @ABORT@ if it was condemned or a statement failed. If it
-- throws, the transaction is aborted when the session is still usable. A
-- transaction that runs no statement sends nothing.
transactionNoRetry :: IsolationLevel -> Mode -> Transaction a -> Session 'Ready -> IO (Either QueryError a)
transactionNoRetry = runTransaction Nothing

-- | Like 'transactionNoRetry', but the statements that may be prepared run as
-- prepared statements, as 'Prepare' gives.
transactionNoRetryWith :: Prepare -> IsolationLevel -> Mode -> Transaction a -> Session 'Ready -> IO (Either QueryError a)
transactionNoRetryWith = runTransaction . Just

runTransaction :: Maybe Prepare -> IsolationLevel -> Mode -> Transaction a -> Session 'Ready -> IO (Either QueryError a)
runTransaction prepare isolation mode (Transaction body) session = do
  begin <- newIORef (Just (beginning isolation mode))
  commits <- newIORef True
  let context = Context session prepare begin commits
      run (Transaction action) = action context
      begun = isNothing <$> readIORef begin
      -- An exception may have broken the session; then there's nothing to
      -- abort, and the pool closes it.
      abortQuietly = begun >>= \sent -> when sent (void (try @SomeException (run (sql "ABORT"))))
  result <- body context `onException` abortQuietly
  sent <- begun
  if not sent
    then pure result
    else case result of
      Left failure -> run (sql "ABORT") >> pure (Left failure)
      Right value -> do
        committing <- readIORef commits
        fmap (const value) <$> run (sql (if committing then "COMMIT" else "ABORT"))

-- | The same text hasql-transaction sends.
beginning :: IsolationLevel -> Mode -> ByteString
beginning isolation mode = "BEGIN ISOLATION LEVEL " <> level <> " " <> access
  where
    level = case isolation of
      ReadCommitted -> "READ COMMITTED"
      RepeatableRead -> "REPEATABLE READ"
      Serializable -> "SERIALIZABLE"
    access = case mode of
      Read -> "READ ONLY"
      Write -> "READ WRITE"

-- | Makes the transaction end in a rollback.
condemn :: Transaction ()
condemn = Transaction (\context -> Right <$> writeIORef (contextCommits context) False)

-- | Runs one statement, without parameters, ignoring its result.
sql :: ByteString -> Transaction ()
sql query = statement () (Statement query (const []) noResult False)

-- | Runs the statement with the parameters, and reads its result.
statement :: params -> Statement params result -> Transaction result
statement params = pipeline . Pipeline.statement params

-- | Sends the pipeline's statements together, and reads their results. The
-- first statements go with @BEGIN@.
pipeline :: Pipeline a -> Transaction a
pipeline statements = Transaction $ \context -> do
  pending <- atomicModifyIORef' (contextBegin context) (Nothing,)
  let withBegin = maybe statements (\begin -> Pipeline.statement () (Statement begin (const []) noResult False) *> statements) pending
  runPipeline (contextPrepare context) withBegin (contextSession context)
