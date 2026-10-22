{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Synchronous cursors that keep a transaction open until explicitly finished.
module Database.PostgreSQL.Transaction
  ( Transaction, Pipeline, Result, Field, Statement (..), RowDecoder,
    runTransaction, runTransactionWith, statement, pipeline, finalPipeline,
    nextRow, fieldLength, nextChunk, finishTransaction, condemn, sql,
    row, streamingRow, singleRow, rowMaybe, noResult,
    foldRows, rowList, singleRowValue, rowMaybeValue,
    QueryError (..), StatementError (..), FormatCode (..),
    IsolationLevel (..), Mode (..), Param (..), Prepare, Prepared (..),
  ) where

import Control.Concurrent (ThreadId, myThreadId)
import Control.Exception (IOException, SomeAsyncException, SomeException, finally, fromException, mask, onException, throwIO, try)
import Control.Monad ((>=>), unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.IO.Unlift (MonadUnliftIO (..))
import Control.Monad.Reader (ReaderT (..), ask)
import Control.Monad.State.Strict (StateT, modify', runStateT)
import qualified Data.Bifunctor as Bifunctor
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Functor.Const (Const (..))
import Data.IORef
import Data.Kind (Type)
import Data.Maybe (isJust, isNothing)
import Data.Word (Word32)
import Database.PostgreSQL.Decoding (DecodingError)
import Database.PostgreSQL.Diagnostics
import Database.PostgreSQL.Internal.Context
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import Database.PostgreSQL.Internal.Execution (Execution (..))
import qualified Database.PostgreSQL.Internal.Execution as Execution
import Database.PostgreSQL.Internal.Protocol.Codec (ByteStream (..))
import Database.PostgreSQL.Internal.Row (DecodeRow (..))
import qualified Database.PostgreSQL.Internal.Row as Row
import Database.PostgreSQL.Session (Session)
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import Database.PostgreSQL.Types (FormatCode (..), TransactionStatus (..))

-- | Work run synchronously on the thread that entered the transaction scope.
-- 'withRunInIO' supports synchronous IO callbacks. Database operations reject
-- overlapping protocol access, other threads and use after the scope ends.
newtype Transaction (tx :: Type) a = Transaction (ReaderT Runtime IO a)
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadUnliftIO)

isInterrupted :: SomeException -> Bool
isInterrupted failure = isJust (fromException @SomeAsyncException failure)
  || isJust (fromException @IOException failure)

-- | Statements queued together. Planning and sending a pipeline does not read
-- its results; its applicative values can contain independent result cursors.
newtype Pipeline (tx :: Type) a = Pipeline (StateT [Pending] (ReaderT Runtime IO) a)
  deriving newtype (Functor, Applicative)

-- | A forward cursor belonging to one transaction. Pulling a later cursor
-- drains earlier results and checks their cardinality and server errors.
data Result tx (row :: Type -> Type) = Result Runtime Pending (RowDecoder row)
-- | A trailing field borrowed from the current row. Advancing its row,
-- sending another pipeline or finishing the transaction expires the field.
data Field (tx :: Type) = Field Runtime ByteStream

data Cardinality = AnyRows | ExactlyOne | AtMostOne

-- | An owned metadata schema, optionally ending in a borrowed field.
data RowDecoder (row :: Type -> Type) = RowDecoder Cardinality (forall tx. Runtime -> Row.Row -> IO (Either DecodingError (row tx)))
-- | SQL, parameter encoding, result schema and eligibility for preparation.
-- Statements do not capture the session or its input storage.
data Statement params row = Statement
  { statementSql :: ByteString,
    statementParams :: params -> [Param],
    statementResult :: RowDecoder row,
    statementPreparable :: Bool
  }

-- | One encoded parameter. A missing type asks the server to infer it;
-- a missing value represents SQL NULL.
data Param = Param
  { paramType :: Maybe Word32,
    paramFormat :: FormatCode,
    paramValue :: Maybe Parameter
  }

-- | Selects a prepared statement name using the connection's existing cache.
type Prepare = ByteString -> [Maybe Word32] -> IO Prepared

data Prepared
  = -- | Reuse the statement already parsed under this name.
    PreparedAs ByteString
  | -- | Parse under this name. The action runs once the server accepts
    -- the parse, even when execution fails, so the cache can remember it.
    PrepareAs ByteString (IO ())

data IsolationLevel = ReadCommitted | RepeatableRead | Serializable
  deriving (Eq, Ord, Show)

data Mode = Read | Write
  deriving (Eq, Ord, Show)

data Pending = Pending
  { pendingQuery :: ByteString,
    pendingExecution :: Execution,
    pendingCardinality :: Cardinality,
    pendingState :: IORef CursorState,
    pendingCount :: IORef Int,
    pendingRow :: IORef (Maybe Row.Row)
  }
data CursorState = Queued | Reading | Done

data Runtime = Runtime
  { runtimeSession :: Session,
    runtimePrepare :: Maybe Prepare,
    runtimeThread :: ThreadId,
    runtimeLive :: IORef Bool,
    runtimeRunning :: IORef Bool,
    runtimeBegin :: IORef (Maybe ByteString),
    runtimeCommits :: IORef Bool,
    runtimeFinal :: IORef Bool,
    runtimeEnded :: IORef Bool,
    runtimePending :: IORef [Pending],
    runtimeSync :: IORef Bool,
    runtimeStatus :: IORef TransactionStatus,
    runtimeFailure :: IORef (Maybe QueryError),
    runtimeBroken :: IORef Bool
  }

-- | Holds exclusive session ownership while the transaction action runs on
-- the calling thread. Use 'withRunInIO' to enter synchronous IO callbacks.
-- Call 'finishTransaction' inside the action to commit; an unfinished
-- scope drains and rolls back any still-open transaction. An ending already
-- queued by 'finalPipeline' cannot be changed. Broken input is discarded.
-- Query failures retain their original SQL and leave the scope after cleanup.
runTransaction :: IsolationLevel -> Mode -> Session -> (forall tx. Transaction tx a) -> IO a
runTransaction = scoped Nothing

-- | Like 'runTransaction', using the existing prepared-statement cache hook.
runTransactionWith :: Prepare -> IsolationLevel -> Mode -> Session -> (forall tx. Transaction tx a) -> IO a
runTransactionWith prepare = scoped (Just prepare)

scoped :: Maybe Prepare -> IsolationLevel -> Mode -> Session -> (forall tx. Transaction tx a) -> IO a
scoped prepare isolation mode session (Transaction action) = do
  outcome <- Exchange.operation session $ mask $ \resume -> do
    runtime <- Runtime session prepare <$> myThreadId <*> newIORef True <*> newIORef False
      <*> newIORef (Just (beginning isolation mode)) <*> newIORef True <*> newIORef False
      <*> newIORef False <*> newIORef [] <*> newIORef False <*> newIORef TxIdle <*> newIORef Nothing <*> newIORef False
    let cleanup = do
          complete <- readIORef (runtimeEnded runtime)
          broken <- readIORef (runtimeBroken runtime)
          unless (complete || broken) $ do
            recovered <- try @SomeException (runOperation runtime (drain runtime False *> rollback runtime))
            case recovered of
              Left failure -> do
                breakConnection (Exchange.sessionConnection session)
                when (isJust (fromException @SomeAsyncException failure)) (throwIO failure)
              Right () -> pure ()
    (do
      result <- try @SomeException (resume (runReaderT action runtime))
      case result of
        Left failure | isInterrupted failure -> abandon runtime
        _ -> pure ()
      pure result) `finally` (cleanup `finally` writeIORef (runtimeLive runtime) False)
  either throwIO pure outcome

-- The runtime is shared by synchronous unlifted callbacks. Only protocol
-- operations claim it: application callbacks may enter another callback, but
-- a parameter producer must not start another operation while one is active.
-- Catch failures before returning to application code, which may handle them
-- and otherwise hide that the protocol can no longer be trusted.
operation :: (Runtime -> IO a) -> Transaction tx a
operation action = Transaction (ReaderT (\runtime -> runOperation runtime (action runtime)))

runOperation :: Runtime -> IO a -> IO a
runOperation runtime action = mask $ \restore -> do
  checkOwner runtime
  occupied <- atomicModifyIORef' (runtimeRunning runtime) (True,)
  when occupied (throwIO (SessionUnusable "a transaction operation is already active"))
  (do
    result <- try @SomeException (restore action)
    case result of
      Left failure -> do
        unless (recoverable failure) (abandon runtime)
        throwIO failure
      Right value -> pure value)
    `finally` writeIORef (runtimeRunning runtime) False

recoverable :: SomeException -> Bool
recoverable failure
  | isJust (fromException @QueryError failure) = True
  | otherwise = case fromException @ConnectionError failure of
      Just (RowMisuse _) -> True
      Just (SessionUnusable _) -> True
      _ -> False

abandon :: Runtime -> IO ()
abandon runtime = do
  complete <- readIORef (runtimeEnded runtime)
  unless complete $ do
    writeIORef (runtimeBroken runtime) True
    breakConnection (Exchange.sessionConnection (runtimeSession runtime))

-- User callbacks can throw the same exception types as our safe rejections.
-- Once they fail, no exception classifier can establish protocol alignment.
untrusted :: Runtime -> IO a -> IO a
untrusted runtime action = action `onException` abandon runtime

checkOwner :: Runtime -> IO ()
checkOwner runtime = do
  live <- readIORef (runtimeLive runtime)
  unless live (throwIO (SessionUnusable "the transaction scope has ended"))
  broken <- readIORef (runtimeBroken runtime)
  when broken (throwIO (SessionUnusable "the transaction broke during an earlier operation"))
  thread <- myThreadId
  unless (thread == runtimeThread runtime) (throwIO (SessionUnusable "the transaction runner belongs to another thread"))
  ensureContextActive (contextOf runtime)

contextOf :: Runtime -> Context
contextOf runtime = case runtimeSession runtime of Exchange.Session context -> context

-- | Adds a statement and returns its cursor without consuming its rows.
statement :: params -> Statement params row -> Pipeline tx (Result tx row)
statement params (Statement query encode decoder@(RowDecoder cardinality _) preparable) = Pipeline $ do
  runtime <- ask
  let parameters = encode params
      types = paramType <$> parameters
  prepared <- liftIO $ case runtimePrepare runtime of
    Just prepare | preparable -> untrusted runtime (prepare query types) >>= \case
      PreparedAs name -> pure (Execution.Prepared name)
      PrepareAs name parsed -> pure (Execution.Preparing name query types (untrusted runtime parsed))
    _ -> pure (Execution.Unprepared query types)
  pending <- liftIO $ Pending query (Execution prepared (paramFormat <$> parameters) (fmap (protectParameter runtime) . paramValue <$> parameters) [BinaryFormat]) cardinality
    <$> newIORef Queued <*> newIORef 0 <*> newIORef Nothing
  modify' (pending :)
  pure (Result runtime pending decoder)

protectParameter :: Runtime -> Parameter -> Parameter
protectParameter runtime = \case
  ParameterStream stream -> ParameterStream stream {byteStreamNext = untrusted runtime (byteStreamNext stream)}
  ParameterBorrowedStream stream -> ParameterBorrowedStream stream {byteStreamNext = untrusted runtime (byteStreamNext stream)}
  parameter -> parameter

-- | Drains the preceding pipeline, then sends these statements and Sync.
-- The first pipeline also sends BEGIN. No results of this pipeline are read.
pipeline :: Pipeline tx a -> Transaction tx a
pipeline = submit False

-- | Queues COMMIT, or ROLLBACK if condemned, after the statements. The ending
-- can no longer depend on their results. 'finishTransaction' still must
-- observe completion and report failures, including errors at COMMIT.
finalPipeline :: Pipeline tx a -> Transaction tx a
finalPipeline = submit True

submit :: Bool -> Pipeline tx a -> Transaction tx a
submit final plan = operation $ \runtime -> submitOn runtime final plan

runPipeline :: Runtime -> Pipeline tx a -> IO (a, [Pending])
runPipeline runtime (Pipeline plan) = Bifunctor.second reverse <$> runReaderT (runStateT plan []) runtime

submitOn :: Runtime -> Bool -> Pipeline tx a -> IO a
submitOn runtime final plan = do
  throwFailure runtime
  ended <- (||) <$> readIORef (runtimeFinal runtime) <*> readIORef (runtimeEnded runtime)
  when ended (throwIO (SessionUnusable "a statement followed the final pipeline"))
  drain runtime True
  (value, pending) <- runPipeline runtime plan
  beginningQuery <- readIORef (runtimeBegin runtime)
  before <- maybe (pure []) (command runtime) beginningQuery
  after <- if final then ending runtime >>= command runtime else pure []
  let queued = before <> pending <> after
  unless (null queued) $ do
    untrusted runtime (queueContextOutput (contextOf runtime) (foldMap (Execution.executionMessages . pendingExecution) queued <> Exchange.sync))
    writeIORef (runtimeBegin runtime) Nothing
    writeIORef (runtimePending runtime) queued
    writeIORef (runtimeSync runtime) True
  when final (writeIORef (runtimeFinal runtime) True)
  pure value

command :: Runtime -> ByteString -> IO [Pending]
command runtime query = snd <$> runPipeline runtime (statement () (Statement query (const []) noResult False))

sql :: ByteString -> Transaction tx ()
sql query = void (pipeline (statement () (Statement query (const []) noResult False)))

-- | Chooses rollback for an ending that has not yet been queued.
condemn :: Transaction tx ()
condemn = operation $ \runtime -> do
  final <- (||) <$> readIORef (runtimeFinal runtime) <*> readIORef (runtimeEnded runtime)
  when final (throwIO (SessionUnusable "the transaction ending is already queued"))
  writeIORef (runtimeCommits runtime) False

ending :: Runtime -> IO ByteString
ending runtime = (\commits -> if commits then "COMMIT" else "ROLLBACK") <$> readIORef (runtimeCommits runtime)

-- | Drains unread fields and rows, checks cardinality and server errors,
-- then sends and observes COMMIT or ROLLBACK. A final pipeline already has
-- its ending queued. Field EOF alone never establishes transaction success.
-- An empty transaction sends nothing. This action is idempotent after
-- success; further statements are rejected.
finishTransaction :: Transaction tx ()
finishTransaction = operation $ \runtime -> do
  finished <- readIORef (runtimeEnded runtime)
  unless finished $ do
    drain runtime True
    throwFailure runtime
    final <- readIORef (runtimeFinal runtime)
    unstarted <- isJust <$> readIORef (runtimeBegin runtime)
    unless (final || unstarted) $ do
      query <- ending runtime
      void (submitOn runtime False (statement () (Statement query (const []) noResult False)))
      drain runtime True
    status <- readIORef (runtimeStatus runtime)
    unless (status == TxIdle) (throwIO (ProtocolError "the transaction ending did not leave an idle session"))
    writeIORef (runtimeEnded runtime) True

rollback :: Runtime -> IO ()
rollback runtime = do
  begun <- readIORef (runtimeBegin runtime)
  status <- readIORef (runtimeStatus runtime)
  when (isNothing begun && status /= TxIdle) $ do
    pending <- command runtime "ROLLBACK"
    queueContextOutput (contextOf runtime) (foldMap (Execution.executionMessages . pendingExecution) pending <> Exchange.sync)
    writeIORef (runtimePending runtime) pending
    writeIORef (runtimeSync runtime) True
    drain runtime False
  writeIORef (runtimeEnded runtime) True

-- | Pulls the next row and decodes its owned metadata. Unread bytes of the
-- previous row are skipped and its field expires. Nothing means command
-- completion, whose cardinality has been checked; call 'finishTransaction'
-- to establish transaction success.
nextRow :: Result tx row -> Transaction tx (Maybe (row tx))
nextRow (Result owner pending (RowDecoder _ decode)) = operation $ \runtime -> do
  sameScope owner runtime
  throwFailure runtime
  state <- readIORef (pendingState pending)
  case state of
    Done -> pure Nothing
    _ -> do
      seek runtime pending
      current <- pullRow runtime pending
      case current of
        Nothing -> pure Nothing
        Just value -> do
          decoded <- decode runtime value
          either (failQuery runtime pending . RowFailed) (pure . Just) decoded

sameScope :: Runtime -> Runtime -> IO ()
sameScope owner runtime = unless (runtimeLive owner == runtimeLive runtime)
  (throwIO (RowMisuse "a cursor was used in another transaction scope"))

seek :: Runtime -> Pending -> IO ()
seek runtime target = readIORef (runtimePending runtime) >>= \case
  [] -> throwIO (RowMisuse "a cursor is no longer pending")
  first : _ | pendingState first == pendingState target -> pure ()
  first : _ -> skipResult runtime first True *> seek runtime target

pullRow :: Runtime -> Pending -> IO (Maybe Row.Row)
pullRow runtime pending = do
  previous <- atomicModifyIORef' (pendingRow pending) (Nothing,)
  traverse_ Row.finishRow previous
  state <- readIORef (pendingState pending)
  started <- case state of
    Queued -> Execution.startExecution (runtimeSession runtime) (pendingExecution pending)
    _ -> pure (Right ())
  case started of
    Left failure -> serverFailure runtime pending failure
    Right () -> do
      writeIORef (pendingState pending) Reading
      reply <- Row.nextRow (Exchange.continueIn (runtimeSession runtime))
      case reply of
        Left failure -> serverFailure runtime pending failure
        Right Nothing -> do
          writeIORef (pendingState pending) Done
          modifyIORef' (runtimePending runtime) (drop 1)
          count <- readIORef (pendingCount pending)
          case pendingCardinality pending of
            ExactlyOne | count /= 1 -> failQuery runtime pending (UnexpectedAmountOfRows count)
            AtMostOne | count > 1 -> failQuery runtime pending (UnexpectedAmountOfRows count)
            _ -> pure Nothing
        Right (Just value) -> do
          modifyIORef' (pendingCount pending) (+ 1)
          writeIORef (pendingRow pending) (Just value)
          pure (Just value)

skipResult :: Runtime -> Pending -> Bool -> IO ()
skipResult runtime pending validating = do
  result <- try @QueryError (pullRow runtime pending)
  case result of
    Left failure | validating -> throwIO failure
    Left _ -> pure ()
    Right Nothing -> pure ()
    Right (Just _) -> skipResult runtime pending validating

drain :: Runtime -> Bool -> IO ()
drain runtime validating = do
  let loop = readIORef (runtimePending runtime) >>= \case
        [] -> pure ()
        first : _ -> skipResult runtime first validating *> loop
  loop
  syncing <- readIORef (runtimeSync runtime)
  when syncing $ do
    (result, status) <- Exchange.awaitReady (runtimeSession runtime) (QueryError "" . ServerFailed) (Right ())
    writeIORef (runtimeSync runtime) False
    writeIORef (runtimeStatus runtime) status
    case result of
      Left failure -> do
        writeIORef (runtimeFailure runtime) (Just failure)
        when validating (throwIO failure)
      Right () -> pure ()

serverFailure :: Runtime -> Pending -> ServerError -> IO a
serverFailure runtime pending failure = do
  waiting <- atomicModifyIORef' (runtimePending runtime) ([],)
  traverse_ (\cursor -> writeIORef (pendingState cursor) Done) waiting
  failQuery runtime pending (ServerFailed failure)

failQuery :: Runtime -> Pending -> StatementError -> IO a
failQuery runtime pending failure = do
  let exception = QueryError (pendingQuery pending) failure
  writeIORef (runtimeFailure runtime) (Just exception)
  throwIO exception

throwFailure :: Runtime -> IO ()
throwFailure runtime = readIORef (runtimeFailure runtime) >>= traverse_ throwIO

-- | Consumes a result on the transaction's calling thread. The step can
-- pull fields before requesting the next row, using the same cursor runtime.
foldRows :: (accumulated -> row tx -> Transaction tx accumulated) -> accumulated -> Result tx row -> Transaction tx accumulated
foldRows step initial result = loop initial
  where
    loop accumulated = nextRow result >>= maybe (pure accumulated) (step accumulated >=> loop)

-- | Collects owned rows. Its values can safely leave the transaction scope.
rowList :: Result tx (Const a) -> Transaction tx [a]
rowList result = reverse <$> foldRows (\values value -> pure (getConst value : values)) [] result

-- | Collects exactly one owned row, checking completion before returning.
singleRowValue :: Result tx (Const a) -> Transaction tx a
singleRowValue result = do
  (count, value) <- firstValue result
  case (count, value) of
    (1, Just found) -> pure found
    _ -> unexpectedRows result count

-- | Collects zero or one owned rows, checking completion before returning.
rowMaybeValue :: Result tx (Const a) -> Transaction tx (Maybe a)
rowMaybeValue result = do
  (count, value) <- firstValue result
  if count <= 1 then pure value else unexpectedRows result count

firstValue :: Result tx (Const a) -> Transaction tx (Int, Maybe a)
firstValue result@(Result owner pending _) = nextRow result >>= \case
  Nothing -> pure (0, Nothing)
  Just value -> operation $ \runtime -> do
    sameScope owner runtime
    -- Extra rows only affect cardinality; decoding them could hide that
    -- error behind an unrelated failure in an unused value.
    before <- readIORef (pendingCount pending)
    skipResult runtime pending True
    after <- readIORef (pendingCount pending)
    pure (after - before + 1, Just (getConst value))

unexpectedRows :: Result tx row -> Int -> Transaction tx a
unexpectedRows (Result owner pending _) count = operation $ \runtime -> do
  sameScope owner runtime
  failQuery runtime pending (UnexpectedAmountOfRows count)

-- | The field's declared byte length, independent of how much was consumed.
fieldLength :: Field tx -> Int
fieldLength (Field _ stream) = byteStreamLength stream

-- | Pulls one borrowed chunk, or an empty chunk at field EOF. Its bytes are
-- valid until the next input operation (including nextChunk, nextRow,
-- pipeline or finishTransaction) or the enclosing scope ends. Copy before
-- retaining them across such an operation. EOF does not imply success.
nextChunk :: Field tx -> Transaction tx ByteString
nextChunk (Field owner stream) = operation $ \runtime -> do
  sameScope owner runtime
  throwFailure runtime
  byteStreamNext stream

-- | Decodes exactly the listed metadata columns into owned values.
row :: forall columns row. DecodeRow columns => (forall tx. RowFunction columns (row tx)) -> RowDecoder row
row construct = RowDecoder AnyRows decode
  where
    decode :: forall tx. Runtime -> Row.Row -> IO (Either DecodingError (row tx))
    decode _ current = Row.decodeRow @columns current (mapRowFunction @columns @(row tx) @(IO (row tx)) pure (construct @tx))

-- | Decodes owned metadata followed by one nullable borrowed field.
streamingRow :: forall columns row. DecodeRow columns => (forall tx. RowFunction columns (Maybe (Field tx) -> row tx)) -> RowDecoder row
streamingRow construct = RowDecoder AnyRows decode
  where
    decode :: forall tx. Runtime -> Row.Row -> IO (Either DecodingError (row tx))
    decode runtime current = Row.decodeRowStreamingUsing @columns @(Field tx) @(row tx) (fmap (Field runtime) . Row.columnBorrowedStream) current
      (mapRowFunction @columns @(Maybe (Field tx) -> row tx) @(Maybe (Field tx) -> IO (row tx)) (fmap pure) (construct @tx))

-- | Requires exactly one row, checked when the result is drained.
singleRow :: RowDecoder row -> RowDecoder row
singleRow (RowDecoder _ decode) = RowDecoder ExactlyOne decode

-- | Allows at most one row, checked when the result is drained.
rowMaybe :: RowDecoder row -> RowDecoder row
rowMaybe (RowDecoder _ decode) = RowDecoder AtMostOne decode

-- | Ignores any rows while retaining server-error checking.
noResult :: RowDecoder (Const ())
noResult = RowDecoder AnyRows (\_ _ -> pure (Right (Const ())))

beginning :: IsolationLevel -> Mode -> ByteString
beginning isolation mode = "BEGIN ISOLATION LEVEL " <> level <> " " <> access
  where
    level = case isolation of ReadCommitted -> "READ COMMITTED"; RepeatableRead -> "REPEATABLE READ"; Serializable -> "SERIALIZABLE"
    access = case mode of Read -> "READ ONLY"; Write -> "READ WRITE"
