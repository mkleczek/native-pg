{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Transactions, with the names and shape of hasql's @Transaction@: a monad
-- of statements whose results are read into values, run between @BEGIN@ and
-- @COMMIT@, or @ABORT@ when it's condemned or a statement fails.
--
-- Nothing is retried, since a streamed result or parameter can't be
-- replayed. The statements, and their results, are those of
-- "Database.PostgreSQL.Statement".
module Database.PostgreSQL.Transaction
  ( -- * Transactions
    Transaction,
    IsolationLevel (..),
    Mode (..),
    transactionNoRetry,
    Prepare,
    transactionNoRetryWith,
    statement,
    sql,
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
import Control.Monad (ap, liftM, void)
import Control.Monad.IO.Class (MonadIO (..))
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Database.PostgreSQL.Protocol (FormatCode (..), OptionalOid)
import Database.PostgreSQL.Session
  ( Execution (..),
    Phase (Ready),
    ServerError,
    Session,
    execute,
  )
import qualified Database.PostgreSQL.Session as Session
import Database.PostgreSQL.Statement

-- | Statements run in one transaction, which can be condemned to end in a
-- rollback. A failing statement ends it.
newtype Transaction a = Transaction (Context -> IO (Either QueryError a))

data Context = Context
  { contextSession :: Session 'Ready,
    contextPrepare :: Maybe Prepare,
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
-- throws, the transaction is aborted when the session is still usable.
transactionNoRetry :: IsolationLevel -> Mode -> Transaction a -> Session 'Ready -> IO (Either QueryError a)
transactionNoRetry = runTransaction Nothing

-- | Gives the name a statement is prepared under on the session, from its
-- SQL and its parameters' types, preparing it first if needed, e.g. from a
-- statement cache. Or the server's error, if preparing failed.
type Prepare = ByteString -> [OptionalOid] -> IO (Either ServerError ByteString)

-- | Like 'transactionNoRetry', but the statements that may be prepared run as
-- prepared statements, under the names 'Prepare' gives.
transactionNoRetryWith :: Prepare -> IsolationLevel -> Mode -> Transaction a -> Session 'Ready -> IO (Either QueryError a)
transactionNoRetryWith = runTransaction . Just

runTransaction :: Maybe Prepare -> IsolationLevel -> Mode -> Transaction a -> Session 'Ready -> IO (Either QueryError a)
runTransaction prepare isolation mode (Transaction body) session = do
  commits <- newIORef True
  let context = Context session prepare commits
      run (Transaction action) = action context
      -- An exception may have broken the session; then there's nothing to
      -- abort, and the pool closes it.
      abortQuietly = void (try @SomeException (run (sql "ABORT")))
  begun <- run (sql (beginning isolation mode))
  case begun of
    Left failure -> pure (Left failure)
    Right () -> do
      result <- body context `onException` abortQuietly
      case result of
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
statement params (Statement query encode (Result initial step finish) preparable) = Transaction $ \context -> do
  let parameters = encode params
      types = paramType <$> parameters
      execution executed =
        Execution
          { executionStatement = executed,
            executionParameterFormats = paramFormat <$> parameters,
            executionParameters = paramValue <$> parameters,
            -- The decoders read the binary formats.
            executionResultFormats = [BinaryFormat]
          }
      folding (Left failure) _ = pure (Left failure)
      folding (Right accumulated) current = step accumulated current
  prepared <- case contextPrepare context of
    Just prepare | preparable -> fmap Session.Prepared <$> prepare query types
    _ -> pure (Right (Session.Unprepared query types))
  case prepared of
    Left failure -> pure (Left (QueryError query (ServerFailed failure)))
    Right executed -> do
      (outcome, _) <- execute (contextSession context) (execution executed) folding (Right initial)
      pure . first (QueryError query) $ case outcome of
        Left failure -> Left (ServerFailed failure)
        Right (Left failure, _) -> Left (RowFailed failure)
        Right (Right accumulated, _) -> finish accumulated
