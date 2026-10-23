{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Private raw statement execution, batching and asynchronous notifications.
module Database.PostgreSQL.Internal.Execution
  ( Statement (..), Execution (..), Batch,
    prepare, execute, batched, runBatch, readExecution, startExecution, executionMessages, awaitNotification,
    prepareWithContext, closeStatementWithContext, executeWithContext,
    runBatchWithContext, awaitNotificationWithContext,
    backendProcessId, notificationProcessId, notificationChannel,
    notificationPayload,
  ) where

import Control.Exception (throwIO)
import Data.Bifunctor (first)
import Data.Foldable (for_)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef')
import Data.Int (Int32)
import Data.Word (Word32)
import Database.PostgreSQL.Diagnostics (ServerError)
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Internal.Exchange
import Database.PostgreSQL.Internal.Row (Row, readRows)
import Database.PostgreSQL.Session (closeStatement)
import Database.PostgreSQL.Internal.Protocol.Codec (Alternatives (..), Output)
import Database.PostgreSQL.Internal.Protocol.Messages hiding (notificationProcessId)
import qualified Database.PostgreSQL.Internal.Protocol.Messages.Types as Protocol

-- | A statement to run.
data Statement
  = -- | SQL parsed for this execution only, as the unnamed statement, with its
    -- parameters' types. 'Nothing' lets the server infer a type.
    Unprepared ByteString [Maybe Word32]
  | -- | A statement prepared earlier under this name.
    Prepared ByteString
  | -- | SQL parsed and prepared under the name, with its parameters' types,
    -- and run. The action runs once the server has parsed it, e.g. to
    -- remember that the statement is prepared, even if running it fails.
    Preparing ByteString ByteString [Maybe Word32] (IO ())

data Execution = Execution
  { executionStatement :: Statement,
    -- | No formats means text for all parameters, one means that format for
    -- all of them, and more give each parameter its own.
    executionParameterFormats :: [FormatCode],
    executionParameters :: [Maybe Parameter],
    -- | Formats for the result columns, in the same way.
    executionResultFormats :: [FormatCode]
  }

-- | Prepares a statement under a name, with its parameters' types. 'Nothing'
-- lets the server infer a type.
prepare :: Session 'Ready -> ByteString -> ByteString -> [Maybe Word32] -> IO (Either ServerError (), TransactionStatus)
prepare session name query parameterTypes = operation session $ do
  sendOutputs session (serialize (Parse name query (map (fmap Scalar) parameterTypes)) <> sync)
  parsed <- pullEither (continueIn session :: Session 'ParseResult) ((\ParseComplete -> Reply ()) :|: sessionTail)
  awaitReady session id parsed

-- | Runs a statement and folds its rows; the step pulls each row's columns.
-- Returns the fold's result and the command tag, e.g. @SELECT 3@, or the
-- server's error, with the transaction status after the statement.
--
-- If the step throws, the session is broken.
execute :: Session 'Ready -> Execution -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString), TransactionStatus)
execute session execution step initial = runBatch session id (batched id execution step initial)

-- | Statements sent together, before one Sync, whose results are read in
-- order. After a statement fails on the server, the server skips the
-- statements after it, so their results aren't read. A server error is
-- given as an @e@.
data Batch e a = Batch Output (Session 'Ready -> IO (Either e a))

instance Functor (Batch e) where
  fmap f (Batch output reading) = Batch output (fmap (fmap f) . reading)

instance Applicative (Batch e) where
  pure value = Batch mempty (\_ -> pure (Right value))
  Batch left readLeft <*> Batch right readRight =
    Batch (left <> right) $ \session ->
      readLeft session >>= either (pure . Left) (\f -> fmap f <$> readRight session)

-- | A statement of a batch, whose rows are folded as 'execute' folds them,
-- and whose server error is given by the function.
batched :: (ServerError -> e) -> Execution -> (acc -> Row -> IO acc) -> acc -> Batch e (acc, ByteString)
batched failed execution step initial =
  Batch (executionMessages execution) (\session -> first failed <$> readExecution session execution step initial)

-- | Sends the batch's statements and a Sync, and reads their results, with
-- the transaction status after them. An error the server reports after the
-- statements replaces the result, given by the function.
--
-- If a step throws, the session is broken.
runBatch :: Session 'Ready -> (ServerError -> e) -> Batch e a -> IO (Either e a, TransactionStatus)
runBatch session@(Session context) failed (Batch output reading) = operation session $ do
  -- The server may answer the first statements before it has read the last,
  -- and stop reading until its answers are read. So what the socket doesn't
  -- take at once is sent as the answers are waited for.
  queueContextOutput context (output <> sync)
  result <- reading session
  awaitReady session failed result

-- | Reads the results of a statement that has been sent.
readExecution :: Session 'Ready -> Execution -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString))
readExecution session execution step initial = do
  started <- startExecution session execution
  case started of
    Left failure -> pure (Left failure)
    Right () -> readRows (continueIn session) step initial

startExecution :: Session 'Ready -> Execution -> IO (Either ServerError ())
startExecution session execution = do
  parsed <- case executionStatement execution of
    Unprepared _ _ -> parseComplete
    Prepared _
      | streamsParameter execution -> described
      | otherwise -> pure (Right ())
    Preparing _ _ _ onParsed -> do
      parsed <- parseComplete
      for_ parsed (const onParsed)
      pure parsed
  case parsed of
    Left failure -> pure (Left failure)
    Right () -> do
      bound <- pullEither (continueIn session :: Session 'BindResult) ((\BindComplete -> Reply ()) :|: sessionTail)
      case bound of
        Left failure -> pure (Left failure)
        Right () -> pure (Right ())
  where
    parseComplete = pullEither (continueIn session :: Session 'ParseResult) ((\ParseComplete -> Reply ()) :|: sessionTail)
    described = do
      parameters <- pullEither (continueIn session :: Session 'DescribeResult) ((\(ParameterDescription _) -> Reply ()) :|: sessionTail)
      case parameters of
        Left failure -> pure (Left failure)
        Right () -> pullEither (continueIn session :: Session 'RowDescriptionResult) ((\(_ :: RowDescription ByteString) -> Reply ()) :|: (\NoData -> Reply ()) :|: sessionTail)

-- | Waits for a notification on a channel the session listens on (with
-- @LISTEN@). Notifications that arrived during earlier operations come
-- first, oldest first. If the server ends the session meanwhile, e.g.
-- because an administrator terminated it, this throws
-- 'ConnectionTerminated'.
awaitNotification :: Session 'Ready -> IO (Notification ByteString)
awaitNotification session = operation session $ do
  let connection = sessionConnection session
  kept <- atomicModifyIORef' (connectionNotifications connection) takeOldest
  maybe waitForOne pure kept
  where
    takeOldest [] = ([], Nothing)
    takeOldest kept = (init kept, Just (last kept))
    waitForOne = do
      reply <-
        pullEither session $
          onError
            :|: onNotice
            :|: onParameterStatus
            :|: (\(NotificationResponse notification) -> Reply (copyNotification notification))
            :|: NoAlternatives
      either (\failure -> throwIO (ProtocolError ("an error while idle: " <> show failure))) pure reply

-- | Parse (unless the statement is prepared already), Bind and Execute. A
-- prepared statement is described instead if a parameter streams: the server
-- starts @statement_timeout@ at a Parse or a Describe, and reads a Bind whole
-- before it runs it, so a slow stream counts either way.
executionMessages :: Execution -> Output
executionMessages execution =
  preamble
    <> serialize
      ( Bind
          BindBody
            { bindPortalName = "",
              bindStatementName = statementName,
              bindParameterFormats = executionParameterFormats execution,
              bindParameterValues = executionParameters execution,
              bindResultFormats = executionResultFormats execution
            }
      )
    <> serialize (Execute "" 0 :: Execute ByteString)
  where
    (preamble, statementName) = case executionStatement execution of
      Unprepared query parameterTypes -> (serialize (Parse "" query (map (fmap Scalar) parameterTypes)), "")
      Prepared name
        | streamsParameter execution -> (serialize (Describe PreparedStatement name), name)
        | otherwise -> (mempty, name)
      Preparing name query parameterTypes _ -> (serialize (Parse name query (map (fmap Scalar) parameterTypes)), name)

-- | Whether a parameter of the execution streams.
streamsParameter :: Execution -> Bool
streamsParameter = any streamed . executionParameters
  where
    streamed (Just (ParameterStream _)) = True
    streamed (Just (ParameterBorrowedStream _ _)) = True
    streamed _ = False

-- | Explicit-context variants for bounded integrations.
prepareWithContext :: Context -> ByteString -> ByteString -> [Maybe Word32] -> IO (Either ServerError (), TransactionStatus)
prepareWithContext = prepare . Session

closeStatementWithContext :: Context -> ByteString -> IO (Either ServerError (), TransactionStatus)
closeStatementWithContext = closeStatement . Session

executeWithContext :: Context -> Execution -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString), TransactionStatus)
executeWithContext = execute . Session

runBatchWithContext :: Context -> (ServerError -> e) -> Batch e a -> IO (Either e a, TransactionStatus)
runBatchWithContext = runBatch . Session

awaitNotificationWithContext :: Context -> IO (Notification ByteString)
awaitNotificationWithContext = awaitNotification . Session

-- | The backend process identifier, for server activity queries.
backendProcessId :: BackendKey bytes -> Int32
backendProcessId (Protocol.Scalar (Protocol.Scalar processId, _)) = processId

-- | The process that sent the notification.
notificationProcessId :: Notification bytes -> Int32
notificationProcessId notification = case Protocol.notificationProcessId notification of
  Protocol.Scalar processId -> processId
