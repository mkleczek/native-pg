{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Transactions, with the names and shape of hasql's @Transaction@: a monad
-- of statements whose results are read into values, run between @BEGIN@ and
-- @COMMIT@, or @ABORT@ when it's condemned or a statement fails.
--
-- A result can also stream. 'foldRows' gives each row to a step whose type
-- has a scope variable, as 'runST' does, so the row and the streams taken
-- from it can't leave the step. Nothing is retried, since a streamed result
-- or parameter can't be replayed.
module Database.PostgreSQL.Transaction
  ( -- * Transactions
    Transaction,
    IsolationLevel (..),
    Mode (..),
    transactionNoRetry,
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

import Control.Exception (Exception, SomeException, onException, try)
import Control.Monad (ap, liftM, void)
import Control.Monad.IO.Class (MonadIO (..))
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Database.PostgreSQL.Decoding (DecodingError)
import Database.PostgreSQL.Protocol (FormatCode (..), OptionalOid, Parameter)
import Database.PostgreSQL.Protocol.Codec (ByteStream (..))
import Database.PostgreSQL.Session
  ( DecodeRow (RowFunction, mapRowFunction),
    Execution (..),
    Phase (Ready),
    Row,
    ServerError,
    Session,
    decodeRow,
    decodeRowStreaming,
    execute,
  )
import qualified Database.PostgreSQL.Session as Session

-- | Statements run in one transaction, which can be condemned to end in a
-- rollback. A failing statement ends it.
newtype Transaction a = Transaction (Context -> IO (Either QueryError a))

data Context = Context
  { contextSession :: Session 'Ready,
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
transactionNoRetry isolation mode (Transaction body) session = do
  commits <- newIORef True
  let context = Context session commits
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
statement params (Statement query encode (Result initial step finish) _) = Transaction $ \context -> do
  let parameters = encode params
      execution =
        Execution
          { executionStatement = Session.Unprepared query (paramType <$> parameters),
            executionParameterFormats = paramFormat <$> parameters,
            executionParameters = paramValue <$> parameters,
            -- The decoders read the binary formats.
            executionResultFormats = [BinaryFormat]
          }
      folding (Left failure) _ = pure (Left failure)
      folding (Right accumulated) current = step accumulated current
  (outcome, _) <- execute (contextSession context) execution folding (Right initial)
  pure . first (QueryError query) $ case outcome of
    Left failure -> Left (ServerFailed failure)
    Right (Left failure, _) -> Left (RowFailed failure)
    Right (Right accumulated, _) -> finish accumulated

-- | A statement: its SQL, its parameters, and how its result is read.
data Statement params result = Statement
  { statementSql :: ByteString,
    statementParams :: params -> [Param],
    statementResult :: Result result,
    -- | Whether a statement cache may prepare it. Without one, statements
    -- run unnamed.
    statementPreparable :: Bool
  }

-- | A parameter.
data Param = Param
  { -- | 'Nothing' lets the server infer the type.
    paramType :: OptionalOid,
    paramFormat :: FormatCode,
    -- | 'Nothing' is NULL.
    paramValue :: Maybe Parameter
  }

-- | A statement failed: its SQL, and why.
data QueryError = QueryError ByteString StatementError
  deriving (Eq, Show)

instance Exception QueryError

data StatementError
  = -- | The server reported an error.
    ServerFailed ServerError
  | -- | A row couldn't be decoded.
    RowFailed DecodingError
  | -- | The result had this many rows, which its shape doesn't allow.
    UnexpectedAmountOfRows Int
  deriving (Eq, Show)

-- | How the rows of a result become its value: a fold over them, and a last
-- step that can reject the result.
data Result a = forall accumulated. Result accumulated (accumulated -> Row -> IO (Either DecodingError accumulated)) (accumulated -> Either StatementError a)

instance Functor Result where
  fmap f (Result initial step finish) = Result initial step (fmap f . finish)

-- | Decodes a row into a value.
newtype RowDecoder a = RowDecoder (Row -> IO (Either DecodingError a))

instance Functor RowDecoder where
  fmap f (RowDecoder decode) = RowDecoder (fmap (fmap f) . decode)

-- | Decodes a row with the columns listed, applying a function to their
-- values, e.g. @row \@'[Int64, Text] Item@.
row :: forall columns a. DecodeRow columns => RowFunction columns a -> RowDecoder a
row function = RowDecoder (\current -> decodeRow @columns current (mapRowFunction @columns @a @(IO a) pure function))
{-# INLINE row #-}

-- | Skips the rows, if there are any.
noResult :: Result ()
noResult = Result () (\_ _ -> pure (Right ())) Right

-- | Exactly one row.
singleRow :: RowDecoder a -> Result a
singleRow decoder = Result (0, Nothing) (firstRow decoder) $ \case
  (1, Just value) -> Right value
  (count, _) -> Left (UnexpectedAmountOfRows count)

-- | No row, or one.
rowMaybe :: RowDecoder a -> Result (Maybe a)
rowMaybe decoder = Result (0, Nothing) (firstRow decoder) $ \case
  (count, value) | count <= 1 -> Right value
  (count, _) -> Left (UnexpectedAmountOfRows count)

-- | Counts the rows, and decodes only the first.
firstRow :: RowDecoder a -> (Int, Maybe a) -> Row -> IO (Either DecodingError (Int, Maybe a))
firstRow (RowDecoder decode) (count, Nothing) current = fmap (\value -> (count + 1, Just value)) <$> decode current
firstRow _ (count, value) _ = pure (Right (count + 1, value))

-- | All the rows.
rowList :: RowDecoder a -> Result [a]
rowList (RowDecoder decode) = Result [] step (Right . reverse)
  where
    step values current = fmap (: values) <$> decode current

-- | A row that can be read only within the step it was given to.
newtype ScopedRow s = ScopedRow Row

-- | A column stream that can be read only within that step.
newtype ScopedStream s = ScopedStream ByteStream

-- | Folds the rows with the step. The rows, and the streams taken from them,
-- have the step's scope in their type, which the result can't mention, so
-- they can't outlive the step.
foldRows :: (forall s. accumulated -> ScopedRow s -> IO accumulated) -> accumulated -> Result accumulated
foldRows step initial = Result initial (\accumulated current -> Right <$> step accumulated (ScopedRow current)) Right

-- | Like 'row', within a step.
decodeScoped :: forall columns s r. DecodeRow columns => ScopedRow s -> RowFunction columns (IO r) -> IO (Either DecodingError r)
decodeScoped (ScopedRow current) = decodeRow @columns current
{-# INLINE decodeScoped #-}

-- | Decodes the columns listed, and streams one more, the last, to the
-- function.
decodeScopedStreaming :: forall columns s r. DecodeRow columns => ScopedRow s -> RowFunction columns (ScopedStream s -> IO r) -> IO (Either DecodingError r)
decodeScopedStreaming (ScopedRow current) function =
  decodeRowStreaming @columns current (mapRowFunction @columns @(ScopedStream s -> IO r) @(ByteStream -> IO r) (. ScopedStream) function)
{-# INLINE decodeScopedStreaming #-}

-- | The stream's length in bytes.
streamLength :: ScopedStream s -> Int
streamLength (ScopedStream stream) = byteStreamLength stream

-- | The next chunk of the stream, or an empty one at its end.
streamChunk :: ScopedStream s -> IO ByteString
streamChunk (ScopedStream stream) = byteStreamNext stream
