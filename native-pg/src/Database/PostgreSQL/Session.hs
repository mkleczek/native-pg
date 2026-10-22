{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Public session API.
module Database.PostgreSQL.Session
  ( Phase (Ready),
    Session,
    Context,
    Connection,
    sessionFromContext,
    closeConnection,
    isConnectionUsable,
    prepareWithContext,
    closeStatementWithContext,
    executeWithContext,
    runBatchWithContext,
    awaitNotificationWithContext,
    ConnectSettings (..),
    ConnectionError (..),
    SslMode (..),
    ChannelBindingMode (..),
    ServerError (..),
    ServerInfo (..),
    close,
    connect,
    connectSettings,
    isUsable,
    Execution (..),
    Statement (..),
    closeStatement,
    execute,
    prepare,
    Batch,
    batched,
    runBatch,
    awaitNotification,
    cancel,
    Column (..),
    DecodeRow (RowFunction, mapRowFunction),
    Row,
    columnBytes,
    columnStream,
    columnStreamWithChunks,
    columnChunks,
    withColumnStream,
    decodeRow,
    decodeRowStreaming,
    decodeRowStreamingNullable,
    decodeRowStreamingUsing,
    nextColumn,
    rowColumnCount,
    FormatCode (..),
    TransactionStatus (..),
    ProtocolVersion (..),
    protocolVersion3_0,
    protocolVersion3_2,
    BackendKey,
    backendProcessId,
    Notification,
    notificationProcessId,
    notificationChannel,
    notificationPayload
  ) where

import Control.Concurrent (ThreadId, myThreadId, killThread)
import Control.Concurrent.MVar (newMVar, takeMVar, putMVar, readMVar)
import Control.Exception (IOException, catch, finally, mask, onException, throwIO, uninterruptibleMask_)
import Control.Monad.Except (throwError)
import Control.Monad (join, unless, when)
import Data.Bifunctor (first)
import Data.Foldable (for_)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int32)
import Data.Word (Word32)
import Data.Kind (Type)
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Internal.Exchange
import Database.PostgreSQL.Internal.Startup (startSession, withChannel)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Decoding (DecodeField (..), DecodingError (..))
import Database.PostgreSQL.Diagnostics (ServerError (..))
import Database.PostgreSQL.Internal.Protocol.Codec
  ( Alternatives (..), ByteStream (..), Input, InputSource (..), Output,
    Parsable (..), Serializable (..), inputPosition, readInputSome,
    readInputBorrowedSome, withInputSome, skipInput,
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Internal.Protocol.Messages hiding (notificationProcessId)
import qualified Database.PostgreSQL.Internal.Protocol.Messages.Types as Protocol
import Database.PostgreSQL.Internal.Channel (Channel (..), unsent)
import Foreign.Marshal.Alloc (allocaBytes)

-- | A compatibility handle backed directly by an explicit operation context.
-- No buffers are installed on the connection. The handle expires with it.
sessionFromContext :: Context -> Session 'Ready
sessionFromContext = Session

-- | Connects, authenticates and waits until the server is ready for a query.
-- Throws 'Database.PostgreSQL.Transport.ConnectError' when the socket doesn't
-- connect, and 'ConnectionError' when the server rejects the connection.
connect :: ConnectSettings -> IO (Session 'Ready, ServerInfo)
connect settings = withChannel settings start
  where
    -- Given how to make the channel, once there's somewhere for its unsent
    -- output.
    start channel binding = do
      connection <- newConnection channel (connectReadSize settings)
      context <- newOwnedContext connection
      startSession settings binding context

-- | Ends an idle or broken session and closes its socket. The session is
-- unusable from then on, even if closing throws. Further calls do nothing.
-- Throws 'SessionUnusable' if an operation is still running; interrupt it
-- and wait for it to finish before closing.
close :: Session phase -> IO ()
close = closeConnection . sessionConnection

closeConnection :: Connection -> IO ()
closeConnection connection = mask $ \restore -> do
  state <- atomicModifyIORef' (connectionState connection) $ \case
    Busy -> (Busy, Busy)
    previous -> (Closed, previous)
  case state of
    Closed -> pure ()
    Busy -> throwIO (SessionUnusable "the session is busy with another operation")
    _ ->
      -- A broken session may have stopped in the middle of a message, and
      -- the server may not be reading, so it isn't told to terminate.
      restore
        ( case state of
            Idle -> channelSend (connectionChannel connection) (serialize @Terminate @'TerminateType Terminate) `catch` \(_ :: IOException) -> pure ()
            _ -> pure ()
        )
        `finally` channelClose (connectionChannel connection)

-- | Whether the session can run an operation: it hasn't been closed, none
-- is running, and none broke it. An operation that an exception interrupts
-- leaves the session broken, e.g. when its thread is killed.
isUsable :: Session phase -> IO Bool
isUsable = isConnectionUsable . sessionConnection

isConnectionUsable :: Connection -> IO Bool
isConnectionUsable connection = do
  state <- readIORef (connectionState connection)
  pure $ case state of
    Idle -> True
    _ -> False

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
  sendOutputs session (serialize @(Parse ByteString) @'ParseType (Parse name query (map (fmap Scalar) parameterTypes)) <> sync)
  parsed <- pullEither (continueIn session :: Session 'ParseResult) ((\ParseComplete -> Reply ()) :|: sessionTail)
  awaitReady session id parsed

-- | Closes a named statement. Closing one that doesn't exist isn't an
-- error.
closeStatement :: Session 'Ready -> ByteString -> IO (Either ServerError (), TransactionStatus)
closeStatement session name = operation session $ do
  sendOutputs session (serialize @(Close ByteString) @'CloseType (Close PreparedStatement name) <> sync)
  closed <- pullEither (continueIn session :: Session 'CloseResult) ((\CloseComplete -> Reply ()) :|: sessionTail)
  awaitReady session id closed

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
        Right () -> readRows (continueIn session) step initial
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

-- | Asks the server to cancel what the session with this backend key is
-- running, over a new connection made as the settings ask, TLS included, as
-- libpq's does since PostgreSQL 17. The server doesn't answer: a cancelled
-- statement fails in its own session with SQLSTATE 57014, and a request that
-- comes too late does nothing.
cancel :: ConnectSettings -> BackendKey ByteString -> IO ()
cancel settings key =
  withChannel settings $ \opened _ -> do
    pending <- newIORef =<< unsent Nothing mempty
    let channel = opened
    ( do
        channelSend channel (serialize @(CancelRequest ByteString) @'CancelRequestType (CancelRequest key))
        -- The server closes the connection once it has handled the request.
        let waitForClose = do
              received <- allocaBytes 64 $ \buffer -> readInputInto (channelInput channel pending 64) buffer 64
              unless (received == 0) waitForClose
        waitForClose `catch` \(_ :: IOException) -> pure ()
      )
      `finally` channelClose channel

-- | Parse (unless the statement is prepared already), Bind and Execute. A
-- prepared statement is described instead if a parameter streams: the server
-- starts @statement_timeout@ at a Parse or a Describe, and reads a Bind whole
-- before it runs it, so a slow stream counts either way.
executionMessages :: Execution -> Output
executionMessages execution =
  preamble
    <> serialize @(Bind Parameter) @'BindType
      ( Bind
          BindBody
            { bindPortalName = "",
              bindStatementName = statementName,
              bindParameterFormats = executionParameterFormats execution,
              bindParameterValues = executionParameters execution,
              bindResultFormats = executionResultFormats execution
            }
      )
    <> serialize @(Execute ByteString) @'ExecuteType (Execute "" 0)
  where
    (preamble, statementName) = case executionStatement execution of
      Unprepared query parameterTypes -> (serialize @(Parse ByteString) @'ParseType (Parse "" query (map (fmap Scalar) parameterTypes)), "")
      Prepared name
        | streamsParameter execution -> (serialize @(Describe ByteString) @'DescribeType (Describe PreparedStatement name), name)
        | otherwise -> (mempty, name)
      Preparing name query parameterTypes _ -> (serialize @(Parse ByteString) @'ParseType (Parse name query (map (fmap Scalar) parameterTypes)), name)

-- | Whether a parameter of the execution streams.
streamsParameter :: Execution -> Bool
streamsParameter = any streamed . executionParameters
  where
    streamed (Just (ParameterStream _)) = True
    streamed (Just (ParameterBorrowedStream _)) = True
    streamed _ = False

sync :: Output
sync = serialize @Sync @'SyncType Sync

data RowsMessage
  = RowStarted Int32 Int
  | Completed ByteString

readRows :: Session 'Rows -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString))
readRows session@(Session context) step = go
  where
    go acc = do
      message <-
        pullEither session $
          (\(DataRowHeader rowLength columnCount) -> Reply (RowStarted rowLength (fromIntegral columnCount)))
            :|: (\(CommandComplete tag) -> Reply (Completed (BS.copy tag)))
            :|: (\EmptyQueryResponse -> Reply (Completed ""))
            :|: (\PortalSuspended -> Reply (Completed ""))
            :|: sessionTail
      case message of
        Left failure -> pure (Left failure)
        Right (Completed tag) -> pure (Right (acc, tag))
        Right (RowStarted rowLength columnCount) -> do
          row <- startRow context rowLength columnCount
          acc' <- step acc row `onException` invalidateRow row
          finishRow row
          go acc'

-- | Pulls the ReadyForQuery that ends a Sync. An error before it, e.g. a
-- deferred constraint failing at the implicit commit, replaces a successful
-- result, given by the function.
awaitReady :: Session phase -> (ServerError -> e) -> Either e a -> IO (Either e a, TransactionStatus)
awaitReady session failed result = do
  let syncing = continueIn session :: Session 'SyncResult
  reply <-
    receive syncing $
      (\(ReadyForQuery status) -> Reply status) :|: sessionTail
  case reply of
    Reply status -> pure (result, status)
    Skip -> awaitReady session failed result
    Notified notification -> do
      keepNotification session notification
      awaitReady session failed result
    Rejected fields -> do
      failure <- sessionError fields
      awaitReady session failed (result >>= const (Left (failed failure)))

-- | A row, whose columns are pulled in order. It's valid only during the step
-- it's given to.
data Row = Row
  { rowContext :: Context,
    rowColumnCount :: Int,
    -- | The position where the row ends.
    rowEnd :: Int,
    rowCursor :: IORef RowCursor
  }

data RowCursor = RowCursor
  { cursorActive :: !Bool,
    -- | How many columns were pulled.
    cursorColumn :: !Int,
    -- | Unread bytes of the current column's value.
    cursorValueLeft :: !Int,
    -- | Whether the current column's value is being read already.
    cursorValueTaken :: !Bool,
    -- | A synchronous borrowed consumer owns the column.
    cursorBorrowed :: !Bool
  }

data Column
  = NullColumn
  | -- | A value of this many bytes, to take with 'columnBytes' or
    -- 'columnStream'.
    Column Int
  deriving (Eq, Show)

startRow :: Context -> Int32 -> Int -> IO Row
startRow context rowLength columnCount = do
  unless (rowLength >= 6 && columnCount >= 0) $
    throwIO (ProtocolError ("invalid DataRow length " <> show rowLength <> " or column count " <> show columnCount))
  position <- inputPosition <$> readIORef (contextInput context)
  cursor <- newIORef (RowCursor True 0 0 False False)
  pure (Row context columnCount (position + fromIntegral rowLength - 6) cursor)

-- | Skips what the step left unread, so the next message can be read.
finishRow :: Row -> IO ()
finishRow row = do
  _ <- activeCursor row
  writeIORef (rowCursor row) (RowCursor False 0 0 True False)
  position <- inputPosition <$> readIORef (contextInput (rowContext row))
  inRow row (skipInput (rowEnd row - position))

-- | Pulls the next column, skipping what's left unread of the current one.
-- 'Nothing' after the last column.
nextColumn :: Row -> IO (Maybe Column)
nextColumn row = do
  cursor <- activeCursor row
  when (cursorValueLeft cursor > 0) $ inRow row (skipInput (cursorValueLeft cursor))
  if cursorColumn cursor == rowColumnCount row
    then do
      writeIORef (rowCursor row) cursor {cursorValueLeft = 0, cursorValueTaken = True}
      pure Nothing
    else do
      size <- inRow row (parser @Int32 @('FixedBytesAtom 4))
      let column = cursorColumn cursor + 1
      case compare size (-1) of
        EQ -> do
          writeIORef (rowCursor row) (RowCursor True column 0 True False)
          pure (Just NullColumn)
        GT -> do
          writeIORef (rowCursor row) (RowCursor True column (fromIntegral size) False False)
          pure (Just (Column (fromIntegral size)))
        LT -> throwIO (ProtocolError ("invalid column length " <> show size))

-- | Materializes the current column into an owned value that remains valid
-- across later input reads. May allocate memory proportional to the field;
-- use 'columnChunks' or 'withColumnStream' for borrowed body transfer.
columnBytes :: Row -> IO ByteString
columnBytes row = do
  cursor <- untakenCursor row
  value <- inRow row (readOwnedBytes (cursorValueLeft cursor))
  writeIORef (rowCursor row) cursor {cursorValueLeft = 0, cursorValueTaken = True}
  pure value

-- | The current column's value as owned chunks, each no larger than the
-- available input storage. The stream is valid until the next column is
-- pulled; returned chunks remain valid across reads.
columnStream :: Row -> IO ByteStream
columnStream = fmap fst . columnStreamWithChunks

-- | Owned pulls and borrowed draining for the same column cursor. Both
-- expire together when the row or column advances; neither may overlap.
columnStreamWithChunks :: Row -> IO (ByteStream, (ByteString -> IO ()) -> IO ())
columnStreamWithChunks row = do
  cursor <- untakenCursor row
  writeIORef (rowCursor row) cursor {cursorValueTaken = True}
  let currentCursor = do
        current <- activeCursor row
        unless (cursorColumn current == cursorColumn cursor) $
          throwIO (RowMisuse "a column stream was read after the next column was pulled")
        pure current
      next = do
        current <- currentCursor
        if cursorValueLeft current == 0
          then pure BS.empty
          else do
            chunk <- inRow row (readInputSome (cursorValueLeft current))
            writeIORef (rowCursor row) current {cursorValueLeft = cursorValueLeft current - BS.length chunk}
            pure chunk
      chunks use = currentCursor >>= \current -> borrowColumnChunks row current use
  pure (ByteStream (cursorValueLeft cursor) next, chunks)

-- | Calls the consumer with successive borrowed chunks of this column.
-- Chunks are valid only until their callback returns; copy retained bytes.
-- A callback exception invalidates the row even if the step catches it,
-- because input may have been refilled before the callback was invoked.
columnChunks :: Row -> (ByteString -> IO ()) -> IO ()
columnChunks row use = do
  (_, chunks) <- columnStreamWithChunks row
  chunks use

borrowColumnChunks :: Row -> RowCursor -> (ByteString -> IO ()) -> IO ()
borrowColumnChunks row cursor use = do
  writeIORef (rowCursor row) cursor {cursorValueTaken = True, cursorBorrowed = True}
  let loop remaining
        | remaining == 0 = pure ()
        | otherwise = do
            size <- inRow row $ withInputSome remaining $ \chunk -> do
              use chunk
              pure (BS.length chunk)
            let left = remaining - size
            writeIORef (rowCursor row) cursor {cursorValueLeft = left, cursorValueTaken = True, cursorBorrowed = True}
            loop left
  loop (cursorValueLeft cursor) `onException`
    writeIORef (rowCursor row) cursor {cursorActive = False, cursorValueTaken = True}
  writeIORef (rowCursor row) cursor {cursorValueLeft = 0, cursorValueTaken = True}

-- | A field-only borrowed Source, with its declared length. Chunks expire
-- on the next pull or callback return. Field EOF does not imply that the
-- statement or transaction succeeded; the enclosing operation still reads
-- trailing completion messages and can report a late server error.
withColumnStream :: Row -> (ByteStream -> IO a) -> IO a
withColumnStream row use = mask $ \restore -> do
  cursor <- untakenCursor row
  writeIORef (rowCursor row) cursor {cursorValueTaken = True, cursorBorrowed = True}
  state <- newIORef PullAvailable
  finished <- newMVar ()
  let next = mask $ \resume -> do
        thread <- myThreadId
        failure <- atomicModifyIORef' state $ \case
          PullAvailable -> (PullReading thread, Nothing)
          current@(PullReading _) -> (current, Just "a column Source was pulled concurrently")
          PullExpired -> (PullExpired, Just "a column Source was used after its scope ended")
        maybe (pure ()) (throwIO . RowMisuse) failure
        takeMVar finished
        let done = do
              putMVar finished ()
              atomicModifyIORef' state (\current -> (case current of PullReading _ -> PullAvailable; other -> other, ()))
            readChunk = do
              ensureContextActive (rowContext row)
              current <- readIORef (rowCursor row)
              unless (cursorActive current && cursorBorrowed current && cursorColumn current == cursorColumn cursor) $
                throwIO (RowMisuse "a column Source no longer owns its row")
              if cursorValueLeft current == 0
                then pure BS.empty
                else do
                  chunk <- inRow row (readInputBorrowedSome (cursorValueLeft current))
                  writeIORef (rowCursor row) current {cursorValueLeft = cursorValueLeft current - BS.length chunk}
                  pure chunk
        resume readChunk `onException` invalidateRow row `finally` done
      closeSource = do
        owner <- atomicModifyIORef' state $ \case
          PullReading thread -> (PullExpired, Just thread)
          _ -> (PullExpired, Nothing)
        for_ owner $ \thread -> do
          invalidateRow row
          -- The callback cannot return storage while a cancelled pull is
          -- still unwinding inside the transport or parser.
          uninterruptibleMask_ (killThread thread *> readMVar finished)
        atomicModifyIORef' (rowCursor row) (\current -> (current {cursorBorrowed = False}, ()))
  restore (use (ByteStream (cursorValueLeft cursor) next))
    `onException` invalidateRow row
    `finally` closeSource

data PullState = PullAvailable | PullReading ThreadId | PullExpired

-- Explicit materializers own their results separately from the reusable
-- body workspace. Each copied chunk is stable before the next refill.
readOwnedBytes :: Int -> Input ByteString
readOwnedBytes count = gather count []
  where
    gather remaining chunks
      | remaining == 0 = pure (BS.concat (reverse chunks))
      | otherwise = do
          chunk <- readInputSome remaining
          gather (remaining - BS.length chunk) (chunk : chunks)

readOwnedNullable :: Input (Maybe ByteString)
readOwnedNullable = do
  size <- parser @Int32 @('FixedBytesAtom 4)
  case compare size (-1) of
    EQ -> pure Nothing
    GT -> Just <$> readOwnedBytes (fromIntegral size)
    LT -> throwError ("negative length " <> show size <> " is invalid for MaybeBytes32Atom")

-- | A row's columns, described by the Haskell types they decode to.
class DecodeRow (columns :: [Type]) where
  -- | A function from the columns' values, in order, to @final@.
  type RowFunction columns (final :: Type) :: Type

  -- | How many columns there are.
  rowArity :: Int

  -- | Decodes the columns, numbered from the given one, and applies the
  -- function to them.
  applyColumns :: Int -> RowFunction columns final -> Input (Either DecodingError final)

  -- | Applies a function to what the row function finally gives, e.g. to
  -- make a pure constructor of the columns' values give an action.
  mapRowFunction :: (final -> final') -> RowFunction columns final -> RowFunction columns final'

instance DecodeRow '[] where
  type RowFunction '[] final = final
  rowArity = 0
  {-# INLINE rowArity #-}
  applyColumns _ final = pure (Right final)
  {-# INLINE applyColumns #-}
  mapRowFunction f = f
  {-# INLINE mapRowFunction #-}

instance (DecodeField a, DecodeRow columns) => DecodeRow (a ': columns) where
  type RowFunction (a ': columns) final = a -> RowFunction columns final
  rowArity = 1 + rowArity @columns
  {-# INLINE rowArity #-}
  mapRowFunction f function = mapRowFunction @columns f . function
  {-# INLINE mapRowFunction #-}
  applyColumns column function = do
    value <- readOwnedNullable
    case decodeField value of
      Left message -> pure (Left (ColumnDecodingFailed column message))
      Right decoded -> applyColumns @columns (column + 1) (function decoded)
  {-# INLINE applyColumns #-}

-- | Decodes the row's columns, as listed by their types, e.g.
-- @decodeRow \@'[Int32, Text] row (\\count name -> ...)@, and runs the
-- action the function returns. The row has to have exactly these columns,
-- and none of them may have been pulled. The columns are decoded in one pass
-- over the input. Materialized fields are owned and may allocate their full
-- size; use a streaming decoder for a borrowed body column.
decodeRow :: forall columns r. DecodeRow columns => Row -> RowFunction columns (IO r) -> IO (Either DecodingError r)
decodeRow row function = do
  started <- startDecoding row (rowArity @columns)
  case started of
    Left failure -> pure (Left failure)
    Right () -> do
      decoded <- inRow row (applyColumns @columns 1 function)
      writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True False)
      either (pure . Left) (fmap Right) decoded
-- Inlined so that each call site unrolls the decoding of its own columns.
{-# INLINE decodeRow #-}

-- | Like 'decodeRow', but the row has one more column, which is streamed:
-- the function gets it last, as a 'ByteStream' that is valid during the
-- action.
decodeRowStreaming :: forall columns r. DecodeRow columns => Row -> RowFunction columns (ByteStream -> IO r) -> IO (Either DecodingError r)
decodeRowStreaming row function =
  join <$> decodeRowStreamingNullable @columns row (mapRowFunction @columns @(ByteStream -> IO r) @(Maybe ByteStream -> IO (Either DecodingError r)) streamOrFail function)
  where
    streamOrFail continue = maybe (pure (Left (ColumnDecodingFailed (rowArity @columns + 1) "unexpected NULL in a streamed column"))) (fmap Right . continue)
{-# INLINE decodeRowStreaming #-}

-- | Like 'decodeRowStreaming', but the streamed column may be NULL, which the
-- function gets as 'Nothing'.
decodeRowStreamingNullable :: forall columns r. DecodeRow columns => Row -> RowFunction columns (Maybe ByteStream -> IO r) -> IO (Either DecodingError r)
decodeRowStreamingNullable = decodeRowStreamingUsing @columns columnStream
{-# INLINE decodeRowStreamingNullable #-}

-- | Chooses how the final column is consumed while its cursor is unclaimed.
decodeRowStreamingUsing :: forall columns stream r. DecodeRow columns => (Row -> IO stream) -> Row -> RowFunction columns (Maybe stream -> IO r) -> IO (Either DecodingError r)
decodeRowStreamingUsing makeStream row function = do
  let streamed = rowArity @columns + 1
  started <- startDecoding row streamed
  case started of
    Left failure -> pure (Left failure)
    Right () -> do
      decoded <- inRow row $ do
        applied <- applyColumns @columns 1 function
        case applied of
          Left failure -> pure (Left failure)
          Right continue -> do
            size <- parser @Int32 @('FixedBytesAtom 4)
            pure (Right (fromIntegral size, continue))
      case decoded of
        Left failure -> do
          writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True False)
          pure (Left failure)
        Right (size, continue)
          | size < 0 -> do
              writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True False)
              Right <$> continue Nothing
          | otherwise -> do
              writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) size False False)
              stream <- makeStream row
              Right <$> continue (Just stream)
{-# INLINE decodeRowStreamingUsing #-}

-- | Checks that no column of the row has been pulled yet, and that it has
-- the expected number of columns.
startDecoding :: Row -> Int -> IO (Either DecodingError ())
startDecoding row expected = do
  cursor <- activeCursor row
  unless (cursorColumn cursor == 0) $
    throwIO (RowMisuse "a row was decoded after some of its columns were pulled")
  pure $
    if expected == rowColumnCount row
      then Right ()
      else Left (ColumnCountMismatch expected (rowColumnCount row))

activeCursor :: Row -> IO RowCursor
activeCursor row = do
  ensureContextActive (rowContext row)
  cursor <- readIORef (rowCursor row)
  unless (cursorActive cursor) $ throwIO (RowMisuse "a row was used after its step returned or a read failed")
  when (cursorBorrowed cursor) $ throwIO (RowMisuse "a row was used during its column callback")
  pure cursor

untakenCursor :: Row -> IO RowCursor
untakenCursor row = do
  cursor <- activeCursor row
  when (cursorValueTaken cursor) $
    throwIO (RowMisuse "a column value was read twice, or before its column was pulled")
  pure cursor

inRow :: Row -> Input a -> IO a
inRow row parse = runOn (rowContext row) (rowEnd row) parse `onException` invalidateRow row

-- A parser may have compacted supplied input before it throws. Its previous
-- cursor cannot safely be resumed even if the row callback catches the error.
invalidateRow :: Row -> IO ()
invalidateRow row = atomicModifyIORef' (rowCursor row) (\cursor -> (cursor {cursorActive = False}, ()))

-- | Explicit-context variants for bounded integrations.
prepareWithContext :: Context -> ByteString -> ByteString -> [Maybe Word32] -> IO (Either ServerError (), TransactionStatus)
prepareWithContext = prepare . sessionFromContext

closeStatementWithContext :: Context -> ByteString -> IO (Either ServerError (), TransactionStatus)
closeStatementWithContext = closeStatement . sessionFromContext

executeWithContext :: Context -> Execution -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString), TransactionStatus)
executeWithContext = execute . sessionFromContext

runBatchWithContext :: Context -> (ServerError -> e) -> Batch e a -> IO (Either e a, TransactionStatus)
runBatchWithContext = runBatch . sessionFromContext

awaitNotificationWithContext :: Context -> IO (Notification ByteString)
awaitNotificationWithContext = awaitNotification . sessionFromContext

-- | The backend process identifier, for server activity queries.
backendProcessId :: BackendKey bytes -> Int32
backendProcessId (Protocol.Scalar (Protocol.Scalar processId, _)) = processId

-- | The process that sent the notification.
notificationProcessId :: Notification bytes -> Int32
notificationProcessId notification = case Protocol.notificationProcessId notification of
  Protocol.Scalar processId -> processId
