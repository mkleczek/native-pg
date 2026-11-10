{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Synchronous cursors that keep a transaction open until explicitly finished.
module Database.PostgreSQL.Transaction
  ( Transaction, Pipeline, Result, Preparation, Field, Statement (..), RowDecoder,
    Tx, transactionIn, runIn, FieldSource, fieldSource, endingWaiting,
    runTransaction, statement, prepare, awaitPreparation, executePrepared, pipeline, finalPipeline,
    nextRow, discard, fieldLength, nextChunk, finishTransaction, condemn, sql,
    row, streamingRow, singleRow, rowMaybe, noResult,
    foldRows, rowList, singleRowValue, rowMaybeValue,
    QueryError (..), StatementError (..), FormatCode (..),
    IsolationLevel (..), Mode (..), Param (..),
  ) where

import Control.Concurrent (ThreadId, myThreadId)
import Control.Exception (IOException, SomeAsyncException, SomeException, catch, finally, fromException, mask, onException, throwIO, toException, try)
import Control.Monad ((>=>), unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.IO.Unlift (MonadUnliftIO (..))
import Control.Monad.Reader (ReaderT (..), ask)
import Control.Monad.State.Strict (StateT, modify', runStateT)
import qualified Data.Bifunctor as Bifunctor
import Data.Bool (bool)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
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
import Database.PostgreSQL.Internal.Protocol.Codec (ByteStream (..), Poll (..), Stall (..))
import Database.PostgreSQL.Internal.Row (DecodeRow (..))
import qualified Database.PostgreSQL.Internal.Row as Row
import Database.PostgreSQL.Session (Session)
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import Database.PostgreSQL.Types (FormatCode (..), TransactionStatus (..))
import Network.Streaming.Scope (Ending (..), Scope, acquireIn, owned)
import Network.Streaming.Watch (Watch, readyNow)
import Network.Streaming.Source (Source (..), WaitingSource (..))

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

-- | A forward cursor belonging to one transaction. Results are consumed in the
-- order of their statements: a result is read to its end, or dropped with
-- 'discard', before any later result is read. Reading a later one first raises
-- 'ResultOutOfOrder' instead of dropping the rows of the earlier one. The
-- results that a transaction ends with are an exception: 'finishTransaction',
-- the next 'pipeline' and leaving the scope drain what is unread.
data Result tx (row :: Type -> Type) = Result Runtime Pending (RowDecoder row)
-- | The server's answer to a 'prepare', belonging to one transaction.
data Preparation (tx :: Type) = Preparation Runtime Pending
-- | A trailing field borrowed from the current row. Advancing its row,
-- sending another pipeline or finishing the transaction expires the field.
data Field (tx :: Type) = Field Runtime ByteStream ((IO Watch -> IO ()) -> IO (Maybe ByteString))

data Cardinality = AnyRows | ExactlyOne | AtMostOne

-- | An owned metadata schema, optionally ending in a borrowed field.
data RowDecoder (row :: Type -> Type) = RowDecoder Cardinality (forall tx. Runtime -> Row.Row -> IO (Either DecodingError (row tx)))
-- | SQL, parameter encoding and result schema. Statements do not capture the
-- session or its input storage. The SQL is evaluated only when it is sent, so
-- a statement run by 'executePrepared' may leave it undefined.
data Statement params row = Statement
  { statementSql :: ByteString,
    statementParams :: params -> [Param],
    statementResult :: RowDecoder row
  }

-- | One encoded parameter. A missing type asks the server to infer it;
-- a missing value represents SQL NULL.
data Param = Param
  { paramType :: Maybe Word32,
    paramFormat :: FormatCode,
    paramValue :: Maybe Parameter
  }

data IsolationLevel = ReadCommitted | RepeatableRead | Serializable
  deriving (Eq, Ord, Show)

data Mode = Read | Write
  deriving (Eq, Ord, Show)

data Pending = Pending
  { pendingQuery :: ByteString,
    pendingExecution :: Execution,
    pendingCardinality :: Cardinality,
    -- | Whether a later result can be read while this one is unread, which
    -- reads past it. That holds for the acknowledgement of a 'prepare', which
    -- stays recorded in its state, and for the transaction's own BEGIN and
    -- ending, which are no result of the client.
    pendingSkippable :: Bool,
    pendingState :: IORef CursorState,
    pendingCount :: IORef Int,
    pendingRow :: IORef (Maybe Row.Row)
  }
-- | 'Failed' is a cursor the server reported a failure on, or skipped after
-- an earlier one; 'Done' is one that completed.
data CursorState = Queued | Reading | Done | Failed

data Runtime = Runtime
  { runtimeSession :: Session,
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
    runtimeFailure :: IORef (Maybe SomeException),
    runtimeBroken :: IORef Bool,
    -- | What the last read that didn't wait was stalled on, which the next
    -- watch is for.
    runtimeStalled :: IORef Stall
  }

-- | Holds exclusive session ownership while the transaction action runs on
-- the calling thread. Use 'withRunInIO' to enter synchronous IO callbacks.
-- Call 'finishTransaction' inside the action to commit; an unfinished
-- scope drains and rolls back any still-open transaction. An ending already
-- queued by 'finalPipeline' cannot be changed. Broken input is discarded.
-- Query failures retain their original SQL and leave the scope after cleanup.
runTransaction :: IsolationLevel -> Mode -> Session -> (forall tx. Transaction tx a) -> IO a
runTransaction isolation mode session (Transaction action) = do
  outcome <- Exchange.operation session $ mask $ \resume -> do
    runtime <- newRuntime isolation mode session
    (do
      result <- try @SomeException (resume (runReaderT action runtime))
      case result of
        Left failure | isInterrupted failure -> abandon runtime
        _ -> pure ()
      pure result) `finally` (cleanupRuntime runtime `finally` endLife runtime)
  either throwIO pure outcome

-- | A transaction that a scope holds open, in the operation of a session. Its
-- tag is the scope's, so neither the transaction nor what is read from it, the
-- cursors and fields that carry the same tag, can be returned from the scope.
newtype Tx (s :: Type) = Tx Runtime

-- | Begins a transaction on the session, which belongs to it, on the calling
-- thread, until the scope closes. Nothing is sent until the first pipeline.
-- The scope ends the transaction as how it ended:
--
-- * When it completed, the transaction is finished as 'finishTransaction'
--   finishes it: what is unread is drained, the ending is observed, whether it
--   was queued by 'finalPipeline' or is sent now, and the session has to be
--   idle. If the transaction failed on the server, and the code handled the
--   failure, it is rolled back instead, and the scope completes.
-- * When it aborted, the transaction is abandoned, and the connection broken,
--   if an interruption aborted it, or a field was left unread in the middle of
--   its stream, since the state of the connection is unknown. Otherwise what
--   is unread is drained, the transaction is rolled back, and the connection
--   is as it was.
--
-- The release fails if the ending finds an error, such as a deferred constraint
-- failing at COMMIT, or a failure of the server after the rows that were read.
-- The transaction is rolled back first, as when the scope aborted, or abandoned
-- if an interruption ended it, and then the releases after it learn that the
-- scope aborted.
transactionIn :: Scope s -> IsolationLevel -> Mode -> Session -> IO (Tx s)
transactionIn scope isolation mode session@(Exchange.Session context) =
  Tx . owned <$> acquireIn scope begin release
  where
    begin = do
      beginContextOperation context
      newRuntime isolation mode session `onException` endContextOperation context True
    release ending runtime = do
      outcome <- try @SomeException (settleTransaction runtime ending)
      endLife runtime
      -- A failure of the server doesn't break the connection, and one of the
      -- rest does, as an operation that fails decides.
      endContextOperation context (either (not . recoverable) (const False) outcome)
      either throwIO pure outcome

-- | Runs transaction work in the transaction of a scope, on the thread that
-- began it, as the action of 'runTransaction' runs. An interruption of the
-- work abandons the transaction.
runIn :: Tx s -> Transaction s a -> IO a
runIn (Tx runtime) (Transaction action) = do
  result <- try @SomeException (runReaderT action runtime)
  case result of
    Left failure -> do
      when (isInterrupted failure) (abandon runtime)
      throwIO failure
    Right value -> pure value

settleTransaction :: Runtime -> Ending -> IO ()
settleTransaction runtime = \case
  Completed -> do
    failed <- isJust <$> readIORef (runtimeFailure runtime)
    if failed then cleanupRuntime runtime else finish
  Aborted failure
    | isInterrupted failure -> abandon runtime
    | otherwise -> bool (cleanupRuntime runtime) (abandon runtime) =<< fieldUnread runtime
  where
    -- The first drain of the ending throws a failure that nothing has read
    -- before it reads the ReadyForQuery that follows, so, as when the scope
    -- aborted, the transaction is rolled back, or abandoned if an interruption
    -- ended it.
    finish = do
      ended <- try @SomeException (runOperation runtime (finishRuntime runtime))
      case ended of
        Left failure -> do
          bool (cleanupRuntime runtime) (abandon runtime) (isInterrupted failure)
          throwIO failure
        Right () -> pure ()

-- | Whether a field is borrowed, and has bytes left that nothing has read.
fieldUnread :: Runtime -> IO Bool
fieldUnread runtime = readIORef (runtimePending runtime) >>= \case
  first : _ -> readIORef (pendingRow first) >>= maybe (pure False) Row.rowValueUnread
  [] -> pure False

-- | A field as a source that is waited for in place: each pull takes the next
-- chunk of the field, from what the input holds and the receives that follow,
-- or 'Nothing' at the field's end. A chunk is as large as the one 'nextChunk'
-- gives: a pull gives it once the input holds that many bytes, receiving
-- again for as long as bytes arrive, and when fewer have arrived and the link has
-- nothing, it calls the wait it is given, in the middle of the pull, with an
-- action that registers a watch for what it is stalled on. That is when the
-- server has sent something, or, if the pull found no buffer to receive into,
-- when one is granted, and, while output is unsent, when the link is writable
-- or the source of an upload is ready. The wait releases the watch. A chunk is
-- borrowed until the next pull, as 'nextChunk''s is, and a pull may move the
-- input's bytes.
--
-- The rules are those of 'nextChunk': the thread that began the transaction
-- pulls, while its scope is live, and a field expires when its row advances.
-- Reading something else of the transaction ends the field's turn, as it does
-- for 'nextChunk'. A failure of the wait, like any other failure of a pull
-- that is not the transaction's own, abandons the transaction.
newtype FieldSource (s :: Type) = FieldSource (Field s)

fieldSource :: Field s -> FieldSource s
fieldSource = FieldSource

instance WaitingSource (FieldSource s) where
  pullWaiting (FieldSource (Field runtime _ waiting)) wait =
    runPull runtime (throwFailure runtime *> waiting wait)

-- | The transaction is over, and nothing can pull from it.
endLife :: Runtime -> IO ()
endLife runtime = writeIORef (runtimeLive runtime) False

-- | A watch for what the last read that didn't wait was stalled on.
watchStalled :: Runtime -> IO Watch
watchStalled runtime = do
  checkOwner runtime
  stall <- readIORef (runtimeStalled runtime)
  contextWatchStall (contextOf runtime) stall

-- | Reads what follows the body, and the ending's reply: the rest of the
-- result, the replies of the statements that follow it, and the reply to
-- COMMIT, or to ROLLBACK if the transaction was condemned, up to the
-- ReadyForQuery that ends them. When they are not all in the input, and the
-- receives that follow bring no more, or there is no buffer to receive into, it
-- calls the wait with an action that registers a watch for what it is stalled
-- on, which the wait releases, and tries again, in the middle of the call, as
-- long as it takes. That is when the server has sent something, or, if there was
-- no buffer to receive into, when one is granted, and while output is unsent
-- also when the link is writable or the source of an upload is ready. Then the
-- transaction is over: it gives 'Right' if it was committed, and the failure of
-- the ending, e.g. a deferred constraint that fails at COMMIT, or of a
-- statement before it, as 'Left'. Asking again after it gave an outcome gives
-- the same.
--
-- The ending is the one that 'finalPipeline' queued with the statements. If
-- none was queued, it's queued now, without waiting for what was sent before
-- it, and its reply is waited for as a reply that hasn't arrived.
--
-- What it reads is in the input, which it may move: a chunk of the field
-- that was borrowed before is not valid any more, and the last one has to be
-- copied. The replies that follow the field have to fit what the input holds,
-- unread bytes of the field included, and it raises 'RowMisuse' if they don't:
-- read the field to its end, which is what 'pullWaiting' says first. A failure
-- of the wait, like any other failure of a pull that is not the transaction's
-- own, abandons the transaction.
endingWaiting :: Tx s -> (IO Watch -> IO ()) -> IO (Either QueryError ())
endingWaiting (Tx runtime) wait = runPull runtime loop
  where
    loop =
      settleEnding runtime >>= \case
        Just outcome -> pure outcome
        Nothing -> wait (watchStalled runtime) *> loop

settleEnding :: Runtime -> IO (Maybe (Either QueryError ()))
settleEnding runtime = do
  ended <- readIORef (runtimeEnded runtime)
  if ended
    then pure (Just (Right ()))
    else do
      outstanding <- readIORef (runtimeSync runtime)
      arrived <- if outstanding then repliesBuffered runtime else pure True
      if not arrived
        then pure Nothing
        else
          try @QueryError (finishBuffered runtime) >>= \case
            Left failure -> pure (Just (Left failure))
            Right True -> pure (Just (Right ()))
            -- The ending was queued, and its reply has to come
            Right False -> settleEnding runtime

-- | Finishes the transaction from replies that are all in the input, so that
-- reading them waits for nothing. False if the ending was queued, and True if
-- it was observed.
finishBuffered :: Runtime -> IO Bool
finishBuffered runtime = do
  drain runtime True
  throwFailure runtime
  final <- readIORef (runtimeFinal runtime)
  unstarted <- isJust <$> readIORef (runtimeBegin runtime)
  if final || unstarted
    then do
      status <- readIORef (runtimeStatus runtime)
      unless (status == TxIdle) (throwIO (ProtocolError "the transaction ending did not leave an idle session"))
      writeIORef (runtimeEnded runtime) True
      pure True
    else do
      void (submitOn runtime True (pure ()))
      pure False

-- | Whether the replies up to the ReadyForQuery that ends the statements that
-- were sent are all in the input, after receiving, without waiting, while
-- they are not.
repliesBuffered :: Runtime -> IO Bool
repliesBuffered runtime = do
  skip <- readIORef (runtimePending runtime) >>= \case
    first : _ -> readIORef (pendingRow first) >>= maybe (pure 0) Row.rowUnreadBytes
    [] -> pure 0
  buffered <- contextBuffered context
  case repliesEnd skip buffered of
    RepliesEnd -> pure True
    RepliesMalformed -> throwIO (ProtocolError "a message of the server has an invalid length")
    RepliesIncomplete ->
      contextReceiveMore context >>= \case
        Arrived -> repliesBuffered runtime
        NotArrived stall -> False <$ writeIORef (runtimeStalled runtime) stall
        NoRoom -> throwIO (RowMisuse "the replies that follow the field do not fit the input buffer: read the field to its end first")
  where
    context = contextOf runtime

data RepliesEnd = RepliesEnd | RepliesIncomplete | RepliesMalformed

-- | Whether the bytes hold the messages of the server up to a ReadyForQuery,
-- after the first bytes, which are those that the current row has left.
repliesEnd :: Int -> BS.ByteString -> RepliesEnd
repliesEnd skip bytes
  | BS.length bytes < skip = RepliesIncomplete
  | otherwise = messages (BS.drop skip bytes)
  where
    messages rest
      | BS.length rest < 5 = RepliesIncomplete
      | size < 4 = RepliesMalformed
      | BS.length rest < 1 + size = RepliesIncomplete
      | BS.head rest == 90 = RepliesEnd
      | otherwise = messages (BS.drop (1 + size) rest)
      where
        size = BS.foldl' (\total byte -> total * 256 + fromIntegral byte) 0 (BS.take 4 (BS.drop 1 rest))

newRuntime :: IsolationLevel -> Mode -> Session -> IO Runtime
newRuntime isolation mode session =
  Runtime session <$> myThreadId <*> newIORef True <*> newIORef False
    <*> newIORef (Just (beginning isolation mode)) <*> newIORef True <*> newIORef False
    <*> newIORef False <*> newIORef [] <*> newIORef False <*> newIORef TxIdle <*> newIORef Nothing <*> newIORef False
    <*> newIORef (StallOnLink (pure readyNow))

-- | Drains and rolls back a transaction that was not finished, unless it is
-- broken.
cleanupRuntime :: Runtime -> IO ()
cleanupRuntime runtime = do
  complete <- readIORef (runtimeEnded runtime)
  broken <- readIORef (runtimeBroken runtime)
  unless (complete || broken) $ do
    recovered <- try @SomeException (runOperation runtime (drain runtime False *> rollback runtime))
    case recovered of
      Left failure -> do
        breakConnection (Exchange.sessionConnection (runtimeSession runtime))
        when (isJust (fromException @SomeAsyncException failure)) (throwIO failure)
      Right () -> pure ()

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

-- | 'runOperation' for a pull or a read that never waits, which a loop repeats
-- while it is pending, so it takes fewer steps: the same checks and the same
-- failures, with the handler installed first, and no mask, no atomic update of
-- the flag, and no @try@ or @finally@. One thread owns the runtime, as
-- 'checkOwner' enforces, so plain reads and writes of the flag are enough. An
-- exception anywhere inside is caught by the handler, which runs masked, so the
-- flag cannot stay set. The one difference is that an asynchronous exception
-- that arrives after the action succeeded, and before the flag is cleared,
-- breaks the connection, as one during the action does: it can only come when
-- the request is being aborted.
runPull :: Runtime -> IO a -> IO a
runPull runtime action = do
  checkOwner runtime
  occupied <- readIORef (runtimeRunning runtime)
  when occupied (throwIO (SessionUnusable "a transaction operation is already active"))
  (do
    writeIORef (runtimeRunning runtime) True
    result <- action
    writeIORef (runtimeRunning runtime) False
    pure result)
    `catch` \(failure :: SomeException) -> do
      writeIORef (runtimeRunning runtime) False
      unless (recoverable failure) (abandon runtime)
      throwIO failure

recoverable :: SomeException -> Bool
recoverable failure
  | isJust (fromException @QueryError failure) = True
  | otherwise = case fromException @ConnectionError failure of
      Just (RowMisuse _) -> True
      Just (ResultOutOfOrder _) -> True
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

-- | Adds a statement, parsed as the unnamed statement, and returns its cursor
-- without consuming its rows. Read the cursors of a pipeline in the order of
-- its statements, or 'discard' the ones not wanted.
statement :: params -> Statement params row -> Pipeline tx (Result tx row)
statement = queue Execution.Unprepared

-- | Adds a Parse of the statement's SQL under the given name, with the
-- parameter types, and keeps it for 'executePrepared'. It sends nothing else,
-- so it can share a pipeline with an 'executePrepared' of the name, which
-- follows it on the wire. Only the types of the parameters are taken from
-- @params@; their values are never encoded or sent.
--
-- The server keeps the name on the connection once it accepted the Parse, even
-- when a later statement fails or the transaction rolls back, and rejects a
-- name that is already in use (SQLSTATE 42P05). It never parses a statement it
-- skips after an earlier error, and a Parse it rejects leaves nothing.
-- 'awaitPreparation' tells which of these happened. The acknowledgement is
-- not a row result: a later result can be read while it is unread.
prepare :: ByteString -> params -> Statement params row -> Pipeline tx (Preparation tx)
prepare name params (Statement query encode _) = Pipeline $ do
  runtime <- ask
  pending <- enqueue query (Execution (Execution.Parsing name query (paramType <$> encode params)) [] [] []) AnyRows True
  pure (Preparation runtime pending)

-- | Waits for the server's answer to a 'prepare'. It returns once the server
-- accepted the Parse: the name exists on the connection from then on, whatever
-- happens to the statements after it or to the transaction. It raises a
-- 'QueryError' if the server did not accept it, and then the name does not
-- exist and needs no closing. The error is the server's own if it rejected the
-- Parse, and that of the earlier statement after which it skipped the Parse.
--
-- Like reading a result, awaiting a preparation needs the results before it to
-- be finished or discarded; the preparations before it are answered by
-- awaiting it. While a result before it is unread, it raises 'ResultOutOfOrder'
-- and aborts the transaction, as 'nextRow' does, unless the server skipped the
-- Parse after the failure of one of those statements: that failure is raised
-- instead. 'ResultOutOfOrder' tells nothing about the name, which the server
-- may have created: close it. A preparation answers the same however often it
-- is awaited, and whether a later result drained it first, even after another
-- statement failed. Await a preparation before the rows that follow it, to
-- learn about the name before they can fail. Like every result, it can only be
-- awaited inside its transaction.
awaitPreparation :: Preparation tx -> Transaction tx ()
awaitPreparation (Preparation owner pending) = operation $ \runtime -> do
  sameScope owner runtime
  let await = readIORef (pendingState pending) >>= \case
        Done -> pure ()
        -- A failure of the server, which skipped this Parse, is on record.
        Failed -> throwFailure runtime
        _ -> readIORef (runtimePending runtime) >>= \case
          [] -> throwIO (RowMisuse "a cursor is no longer pending")
          first : _
            | pendingState first == pendingState pending -> void (pullRow runtime pending) *> await
            | pendingSkippable first -> skipResult runtime first False *> await
            | otherwise -> misordered runtime False pending
  await

-- | Like 'statement', but runs the statement that 'prepare' parsed under the
-- given name on this connection, which only the caller knows. It never
-- evaluates 'statementSql'. The server rejects an unknown name with
-- SQLSTATE 26000, as a 'QueryError' that carries 'statementSql' unevaluated.
executePrepared :: ByteString -> params -> Statement params row -> Pipeline tx (Result tx row)
executePrepared name = queue (\_ _ -> Execution.Prepared name)

queue :: (ByteString -> [Maybe Word32] -> Execution.Statement) -> params -> Statement params row -> Pipeline tx (Result tx row)
queue prepared params (Statement query encode decoder@(RowDecoder cardinality _)) = Pipeline $ do
  runtime <- ask
  let parameters = encode params
  pending <- enqueue query (Execution (prepared query (paramType <$> parameters)) (paramFormat <$> parameters) (fmap (protectParameter runtime) . paramValue <$> parameters) [BinaryFormat]) cardinality False
  pure (Result runtime pending decoder)

-- | Adds the statement to the pipeline, and returns its cursor.
enqueue :: ByteString -> Execution -> Cardinality -> Bool -> StateT [Pending] (ReaderT Runtime IO) Pending
enqueue query execution cardinality skippable = do
  pending <- liftIO $ Pending query execution cardinality skippable <$> newIORef Queued <*> newIORef 0 <*> newIORef Nothing
  modify' (pending :)
  pure pending

protectParameter :: Runtime -> Parameter -> Parameter
protectParameter runtime = \case
  ParameterStream stream -> ParameterStream stream {byteStreamNext = untrusted runtime (byteStreamNext stream)}
  ParameterBorrowedStream size source -> ParameterBorrowedStream size (Untrusted runtime source)
  parameter -> parameter

-- | A parameter's source, whose failures abandon the transaction, like those
-- of a stream's callback.
data Untrusted source = Untrusted Runtime source

instance Source source => Source (Untrusted source) where
  pullWith (Untrusted runtime source) ready stall = untrusted runtime (pullWith source ready stall)

-- | Drains the preceding pipeline, then sends these statements and Sync.
-- The first pipeline also sends BEGIN. No results of this pipeline are read.
-- Results of the preceding pipeline that are unread are dropped, as they are
-- when the transaction is finished: reading one afterwards finds no rows.
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

-- | Whether the transaction's ending has been queued, or the transaction has ended.
finalQueued :: Runtime -> IO Bool
finalQueued runtime = (||) <$> readIORef (runtimeFinal runtime) <*> readIORef (runtimeEnded runtime)

submitOn :: Runtime -> Bool -> Pipeline tx a -> IO a
submitOn runtime final plan = do
  throwFailure runtime
  ended <- finalQueued runtime
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
command runtime query = fmap (\pending -> pending {pendingSkippable = True}) . snd <$> runPipeline runtime (statement () (Statement query (const []) noResult))

sql :: ByteString -> Transaction tx ()
sql query = void (pipeline (statement () (Statement query (const []) noResult)))

-- | Chooses rollback for an ending that has not yet been queued.
condemn :: Transaction tx ()
condemn = operation $ \runtime -> do
  final <- finalQueued runtime
  when final (throwIO (SessionUnusable "the transaction ending is already queued"))
  writeIORef (runtimeCommits runtime) False

ending :: Runtime -> IO ByteString
ending runtime = bool "ROLLBACK" "COMMIT" <$> readIORef (runtimeCommits runtime)

-- | Drains unread results, including their fields and rows, checks cardinality
-- and server errors, then sends and observes COMMIT or ROLLBACK. A final
-- pipeline already has its ending queued. Field EOF alone never establishes
-- transaction success. An empty transaction sends nothing. This action is
-- idempotent after success; further statements are rejected.
finishTransaction :: Transaction tx ()
finishTransaction = operation finishRuntime

finishRuntime :: Runtime -> IO ()
finishRuntime runtime = do
  finished <- readIORef (runtimeEnded runtime)
  unless finished $ do
    drain runtime True
    throwFailure runtime
    final <- readIORef (runtimeFinal runtime)
    unstarted <- isJust <$> readIORef (runtimeBegin runtime)
    unless (final || unstarted) $ do
      query <- ending runtime
      void (submitOn runtime False (statement () (Statement query (const []) noResult)))
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
--
-- The first row of a result starts it, and needs the results of the earlier
-- statements to be finished or discarded. Otherwise it raises
-- 'ResultOutOfOrder', without dropping their rows. That aborts the
-- transaction: reading, discarding, sending a pipeline and finishing raise the
-- same error from then on, and leaving the scope rolls it back, leaving the
-- connection usable. A failure of an earlier statement comes first: the
-- earlier results are read, as 'discard' reads them, to find it, and the first
-- failure found is raised instead.
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

-- | Reads the rest of a result, and drops its rows: it is finished, as when
-- its rows were read. It still reports what reading them would: a server
-- error, or a number of rows its statement does not allow. Discarding a result
-- that is finished, or half read, is fine. Like reading it, discarding it needs
-- the results before it to be finished or discarded, and raises
-- 'ResultOutOfOrder' otherwise.
discard :: Result tx row -> Transaction tx ()
discard (Result owner pending _) = operation $ \runtime -> do
  sameScope owner runtime
  throwFailure runtime
  state <- readIORef (pendingState pending)
  case state of
    Done -> pure ()
    _ -> seek runtime pending *> skipResult runtime pending True

sameScope :: Runtime -> Runtime -> IO ()
sameScope owner runtime = unless (runtimeLive owner == runtimeLive runtime)
  (throwIO (RowMisuse "a cursor was used in another transaction scope"))

-- Reads past what the client does not hold, to the target at the head of the
-- pending results. Every row of a result comes here, but only its first one can
-- find a result before it: once started, the target stays at the head until it
-- is finished. So the check of a result runs once, as it starts.
seek :: Runtime -> Pending -> IO ()
seek runtime target = readIORef (runtimePending runtime) >>= \case
  [] -> throwIO (RowMisuse "a cursor is no longer pending")
  first : _ | pendingState first == pendingState target -> pure ()
  first : _
    | pendingSkippable first -> skipResult runtime first True *> seek runtime target
    | otherwise -> misordered runtime True target

-- The target was reached while a result before it is unread. That is a usage
-- error, but the failures of the statements before the target are raised in
-- its place. Reading those results finds them: the server reports a failure at
-- its statement and skips the rest, so one unread statement can hide it. A
-- validating read raises the first failure it finds. One that does not, a wait
-- for a preparation, raises only the failure that made the server skip the
-- Parse, which is the answer it gives; the usage error tells nothing about the
-- name. The usage error aborts the transaction, which is left to the scope to
-- roll back, unless a failure of the transaction is recorded already.
misordered :: Runtime -> Bool -> Pending -> IO a
misordered runtime validating target = do
  let before = takeWhile ((/= pendingState target) . pendingState)
      unread = length . filter (not . pendingSkippable) . before
      settle = readIORef (runtimePending runtime) >>= \case
        first : _ | pendingState first /= pendingState target -> skipResult runtime first validating *> settle
        _ -> pure ()
  count <- unread <$> readIORef (runtimePending runtime)
  settle
  readIORef (pendingState target) >>= \case
    Failed -> throwFailure runtime
    _ -> pure ()
  let exception = ResultOutOfOrder count
  recorded <- readIORef (runtimeFailure runtime)
  when (isNothing recorded) (writeIORef (runtimeFailure runtime) (Just (toException exception)))
  throwIO exception

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
      reply <- if Execution.parsesOnly (pendingExecution pending) then pure (Right Nothing) else Row.nextRow (Exchange.continueIn (runtimeSession runtime))
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
skipResult runtime pending validating =
  if validating then go
  else void $ try @QueryError go
  where
    go = pullRow runtime pending >>= maybe (pure ()) (const go)

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
        writeIORef (runtimeFailure runtime) (Just (toException failure))
        when validating (throwIO failure)
      Right () -> pure ()

serverFailure :: Runtime -> Pending -> ServerError -> IO a
serverFailure runtime pending failure = do
  waiting <- atomicModifyIORef' (runtimePending runtime) ([],)
  traverse_ ((`writeIORef` Failed) . pendingState) waiting
  failQuery runtime pending (ServerFailed failure)

failQuery :: Runtime -> Pending -> StatementError -> IO a
failQuery runtime pending failure = do
  let exception = QueryError (pendingQuery pending) failure
  writeIORef (runtimeFailure runtime) (Just (toException exception))
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
fieldLength (Field _ stream _) = byteStreamLength stream

-- | Pulls one borrowed chunk, or an empty chunk at field EOF. Its bytes are
-- valid until the next input operation (including nextChunk, nextRow,
-- pipeline or finishTransaction) or the enclosing scope ends. Copy before
-- retaining them across such an operation. EOF does not imply success.
nextChunk :: Field tx -> Transaction tx ByteString
nextChunk (Field owner stream _) = operation $ \runtime -> do
  sameScope owner runtime
  throwFailure runtime
  byteStreamNext stream

-- | Decodes exactly the listed metadata columns into owned values.
row :: forall columns row. DecodeRow columns => (forall tx. RowFunction columns (row tx)) -> RowDecoder row
row construct = RowDecoder AnyRows decode
  where
    decode :: forall tx. Runtime -> Row.Row -> IO (Either DecodingError (row tx))
    decode _ current = Row.decodeRow @columns current (mapRowFunction @columns @(row tx) @(IO (row tx)) pure (construct @tx))

{-# INLINE row #-}

-- | Decodes owned metadata followed by one nullable borrowed field.
streamingRow :: forall columns row. DecodeRow columns => (forall tx. RowFunction columns (Maybe (Field tx) -> row tx)) -> RowDecoder row
streamingRow construct = RowDecoder AnyRows decode
  where
    decode :: forall tx. Runtime -> Row.Row -> IO (Either DecodingError (row tx))
    decode runtime current = Row.decodeRowStreamingUsing @columns @(Field tx) @(row tx) borrowed current
      (mapRowFunction @columns @(Maybe (Field tx) -> row tx) @(Maybe (Field tx) -> IO (row tx)) (fmap pure) (construct @tx))
      where
        borrowed column = field <$> Row.columnBorrowedStream column
        field (stream, waiting) = Field runtime stream waiting

{-# INLINE streamingRow #-}

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
