{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Transactions, with the names and shape of hasql's @Transaction@: a monad
-- of statements whose results are read into values, run between @BEGIN@ and
-- @COMMIT@, or @ABORT@ when it's condemned or a statement fails.
--
-- @BEGIN@ waits to be sent with the first statements, in one round trip.
-- Statements whose results the others don't depend on can also go together,
-- in a 'pipeline', and the last ones can go with @COMMIT@, in a
-- 'finalPipeline'. Nothing is retried, since a streamed result or parameter
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
    finalPipeline,
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
    scopedRow,
    decodeScoped,
    decodeScopedStreaming,
    decodeScopedStreamingNullable,
    streamLength,
    streamChunk,
  )
where

import Control.Exception (ErrorCall (..), SomeException, onException, throwIO, try)
import Control.Monad (ap, liftM, void, when)
import Control.Monad.IO.Class (MonadIO (..))
import Data.ByteString (ByteString)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (isNothing)
import Database.PostgreSQL.Pipeline (Pipeline, Prepare, Prepared (..), runPipelineWithStatus)
import qualified Database.PostgreSQL.Pipeline as Pipeline
import Database.PostgreSQL.Protocol (TransactionStatus (..))
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
    contextCommits :: IORef Bool,
    -- | Whether the transaction ended with its final pipeline.
    contextEnded :: IORef Bool
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
  ended <- newIORef False
  let context = Context session prepare begin commits ended
      run (Transaction action) = action context
      -- Whether the transaction is open on the server: begun and not ended.
      open = (&&) . isNothing <$> readIORef begin <*> (not <$> readIORef ended)
      -- An exception may have broken the session; then there's nothing to
      -- abort, and the pool closes it.
      abortQuietly = open >>= \sent -> when sent (void (try @SomeException (run (sql "ABORT"))))
  result <- body context `onException` abortQuietly
  sent <- open
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
pipeline statements = Transaction (\context -> fst <$> send context statements)

-- | Like 'pipeline', but ends the transaction with the statements: @COMMIT@,
-- or @ABORT@ if it's condemned already. That saves the round trip it would
-- take after them, but their results can't condemn the transaction, and no
-- statement can follow them.
--
-- After a statement fails, the server skips the end, and the transaction is
-- aborted as usual. A failure at @COMMIT@ is reported as the statement's.
-- A result that doesn't decode is reported too, but the transaction has
-- ended as it would have.
finalPipeline :: Pipeline a -> Transaction a
finalPipeline statements = Transaction $ \context -> do
  committing <- readIORef (contextCommits context)
  let ending = Pipeline.statement () (Statement (if committing then "COMMIT" else "ABORT") (const []) noResult False)
  (result, status) <- send context (statements <* ending)
  -- Unless a statement failed before the end, the transaction is over.
  writeIORef (contextEnded context) (status == TxIdle)
  pure result

-- | Sends the statements, with @BEGIN@ if they're the first, and reads their
-- results, with the transaction status after them.
send :: Context -> Pipeline a -> IO (Either QueryError a, TransactionStatus)
send context statements = do
  ended <- readIORef (contextEnded context)
  when ended (throwIO (ErrorCall "a statement after the transaction's final pipeline"))
  pending <- atomicModifyIORef' (contextBegin context) (Nothing,)
  let withBegin = maybe statements (\begin -> Pipeline.statement () (Statement begin (const []) noResult False) *> statements) pending
  runPipelineWithStatus (contextPrepare context) withBegin (contextSession context)
