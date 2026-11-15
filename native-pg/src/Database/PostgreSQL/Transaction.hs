{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Statements, pipelines and transactions, and the cursors that read their rows.
--
-- A program is built once: its pipelines are values that name the positions of
-- their results, and the reading program that takes them is a function of the
-- positions, evaluated when the program is built. What it runs on, a connection
-- and the request's loans, are inputs of the arrow.
--
-- A transaction works in one buffer of the request's loans, its primary: what the
-- client says is rendered into it and sent, and the replies are read into it by
-- cursors. The two directions take turns in the one part: a pipeline is sent once
-- what the one before it said is answered. The queue of the answers that are
-- expected is the statements of the pipeline that was sent, which the server
-- answers in order, and the reading program takes the results from it in the
-- order of their statements. Prepared statements, streamed parameters and the
-- notifications are to come: their steps fail with
-- 'Network.Streaming.NotImplemented.NotImplemented'.
module Database.PostgreSQL.Transaction
  ( -- * Statements
    Statement (..),
    Param (..),
    Value (..),

    -- * Pipelines
    Pipeline,
    Result,
    Preparation,
    statement,
    prepare,
    executePrepared,
    closeStatement,
    command,
    from,

    -- * Transactions
    Tx,
    IsolationLevel (..),
    Mode (..),
    Ending (..),
    transaction,
    pipeline,
    condemn,

    -- * Reading
    nextRow,
    discard,
    prepared,
    commandTag,
    ending,
    single,
    optional,
    foldRows,

    -- * Rows, read by cursors
    Columns,
    Cardinality (..),
    Rows (..),
    Field,
    column,
    nullable,
    field,
    nullableField,
    fieldLength,
    skip,
    one,
    atMostOne,
    any,
    none,
  )
where

import Control.Arrow (Arrow, arr, first, returnA, second, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, SomeAsyncException, SomeException, displayException, evaluate, fromException, throwIO, toException, try)
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Foldable (traverse_)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.Kind (Type)
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Database.PostgreSQL.Connection.Internal
import Database.PostgreSQL.Decoding (DecodeField (..), DecodingError (..), InPlace)
import Database.PostgreSQL.Errors (ConnectionError (..), QueryError (..), ServerError (..), StatementError (..), UsageError (..), isConnectionFatal)
import Database.PostgreSQL.Internal.Diagnostics (bodyDiagnostics)
import Database.PostgreSQL.Internal.Frontend (sendOutput, sync)
import Database.PostgreSQL.Internal.Protocol.Codec.Cursor
  ( At (..),
    CursorError (..),
    Cases (..),
    FramedMessage,
    advance,
    cases,
    child,
    count,
    cstring,
    fixed,
    message,
    messages,
    piece,
    remaining,
    skipValue,
    valueLength,
    valueView,
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output, outputLength, outputWriter, runWriter)
import qualified Database.PostgreSQL.Internal.Protocol.Codec.Output as Output (WriteResult (..), bytes)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Internal.Carrier (Carrier)
import qualified Database.PostgreSQL.Internal.Protocol.Messages as Wire
import Database.PostgreSQL.Types (FormatCode (..), Oid, Parameter (..))
import Network.Streaming.Classes (Effect (..), Exit (..), Runtime, Scope (..), Stream (..))
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, boolA, consume, done, iterateA, iterateWith, keeping, tap)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release)
import Network.Streaming.NotImplemented (notImplemented, notImplementedIO)
import Network.Streaming.Reader (Reader, markFrame, markLimit, markStart, mark, newReader, readerCapacity)
import Network.Streaming.With (With (..), ask, asks, lift, with, withOnly)
import Prelude hiding (any)

-- | A statement: its SQL, the parameters that it takes from the input of the
-- program, and how its rows are read. The SQL is evaluated only when it is sent,
-- and a prepared statement the connection already has sends only its
-- parameters.
data Statement arr p row = Statement
  { sql :: ByteString,
    params :: p -> [Param arr],
    rows :: Rows arr row
  }

-- | A parameter: its type, which the server infers if there is none, its format,
-- and its value.
data Param arr = Param
  { paramType :: Maybe Oid,
    paramFormat :: FormatCode,
    paramValue :: Value arr
  }

data Value arr
  = Null
  | Bytes ByteString
  | -- | Sent one after another, without a copy.
    Pieces [ByteString]
  | -- | The declared length, and the stream that is pulled while the value is
    -- sent.
    forall s. (Stream arr s) => Streamed Int s

-- | What is sent, in order. A pipeline's value holds the positions of its
-- results in the queue of answers that are expected, relative to the start of the
-- pipeline, so putting pipelines together only shifts them.
data Pipeline arr p a = Pipeline Int [Sent arr p] (Int -> a)

-- | What a pipeline sends.
data Sent arr p
  = forall row. Execute (Statement arr p row)
  | forall row. Prepare ByteString (Statement arr p row)
  | forall row. ExecutePrepared ByteString (Statement arr p row)
  | Close ByteString
  | Command ByteString

instance Functor (Pipeline arr p) where
  fmap function (Pipeline count' sent value) = Pipeline count' sent (function . value)

instance Applicative (Pipeline arr p) where
  pure result = Pipeline 0 [] (const result)
  Pipeline count' sent value <*> Pipeline count'' sent' value' =
    Pipeline (count' + count'') (sent <> sent') (\base -> value base (value' (base + count')))

-- | How the rows of a result are read, which only the statement knows: how many
-- there may be, and the step of the cursor for each.
data Reading arr row = Reading Cardinality (arr Columns row) | Silent

-- | The position of the answers to one statement, and how its rows are read, as the
-- arrow of its pipeline reads them: a result is read only under that arrow, and
-- under @Delimited s@ only in the run that made it.
data Result arr (row :: Type) = Result !Int !(Reading arr row)

-- | The position of a preparation's acknowledgement.
newtype Preparation = Preparation Int

-- | The reading of the rows of a statement, as the result keeps it.
readingOf :: Rows arr row -> Reading arr row
readingOf (Rows cardinality cursor) = Reading cardinality cursor

-- | A statement with its parameters, which the server parses, binds and executes.
statement :: Statement arr p row -> Pipeline arr p (Result arr row)
statement given = Pipeline 1 [Execute given] (\base -> Result base (readingOf (rows given)))

-- | Parses the statement under the name. Parse sends no parameters, so what the
-- pipeline takes as its input doesn't matter.
prepare :: ByteString -> Statement arr p row -> Pipeline arr q Preparation
prepare name given = Pipeline 1 [Prepare name (given {params = const []})] Preparation

-- | Binds and executes the statement that the connection has under the name.
executePrepared :: ByteString -> Statement arr p row -> Pipeline arr p (Result arr row)
executePrepared name given = Pipeline 1 [ExecutePrepared name given] (\base -> Result base (readingOf (rows given)))

-- | Closes the statement that the connection has under the name.
closeStatement :: ByteString -> Pipeline arr p (Result arr ())
closeStatement name = Pipeline 1 [Close name] (`Result` Silent)

-- | A command with no parameters and no rows, as an unnamed statement: its SQL is
-- one statement.
command :: ByteString -> Pipeline arr p (Result arr ())
command given = Pipeline 1 [Command given] (`Result` Silent)

-- | The pipeline, with its parameters taken from another input: where a
-- pipeline's parameters come from.
from :: (q -> p) -> Pipeline arr p a -> Pipeline arr q a
from function (Pipeline count' sent value) = Pipeline count' (map sentFrom sent) value
  where
    sentFrom item = case item of
      Execute given -> Execute (given {params = params given . function})
      Prepare name given -> Prepare name (given {params = params given . function})
      ExecutePrepared name given -> ExecutePrepared name (given {params = params given . function})
      Close name -> Close name
      Command given -> Command given

data IsolationLevel = ReadCommitted | RepeatableRead | Serializable
  deriving (Eq, Ord, Show)

data Mode = Read | Write
  deriving (Eq, Ord, Show)

-- | Whether a pipeline is the last one: nothing can be sent after a 'Final' one,
-- which carries COMMIT, or ROLLBACK once the transaction is condemned.
data Ending = Open | Final
  deriving (Eq, Ord, Show)

-- * The transaction at run time

-- | A statement that the server answers: its SQL, for the errors that name it, and whether it belongs to the
-- transaction and not to the program, as BEGIN and the ending do, which no result stands for.
data Entry = Entry {entrySql :: ByteString, entrySkippable :: !Bool}

-- | Where the transaction is, which only the part that runs it changes.
data State = State
  { -- | Whether BEGIN was sent, and the transaction may be open on the server.
    stBegun :: !Bool,
    -- | Whether the pipeline that carries the ending was sent, so that nothing follows it.
    stFinal :: !Bool,
    -- | Whether it ends with ROLLBACK, as a condemned transaction does.
    stCondemned :: !Bool,
    -- | Whether it is over: its ending was read, or it was rolled back.
    stEnded :: !Bool,
    -- | The statements of the pipeline that the server has not answered, in order; the first is the one that is
    -- being read.
    stPending :: ![Entry],
    -- | The place of the first of them in the pipeline, and the place of the first statement of the program.
    stIndex :: !Int,
    stBase :: !Int,
    -- | The rows of the statement that is being read, so far.
    stRows :: !Int,
    -- | The command tags of the statements that were answered, by their place.
    stTags :: ![(Int, ByteString)],
    -- | The first failure that the server reported, which every read that follows raises.
    stFailure :: !(Maybe SomeException),
    -- | Whether a ReadyForQuery is owed: the pipeline was sent, and not all of it was read.
    stOwed :: !Bool,
    -- | What the last ReadyForQuery said: the byte of @I@, @T@ or @E@.
    stStatus :: !Int,
    -- | Whether a send is under way, which leaves something half sent if it is cancelled.
    stSending :: !Bool,
    -- | Whether a field was handed out of the row that is being read.
    stField :: !Bool
  }

-- | The transaction at run time: its connection, the loan that its buffer is, the reader of the replies in it, and
-- where it is.
data Tx = Tx
  { txConnection :: !Connection,
    txBegin :: !ByteString,
    -- | BEGIN as the messages that carry it, rendered once for the isolation level and the mode.
    txBeginOutput :: !Output,
    txReader :: !(Reader Carrier),
    txCursor :: !(At ('PayloadChoice Replies) Carrier),
    txLoan :: !Loan,
    txState :: !(IORef State),
    -- | The number that a field is valid for: it changes as the row moves on, and when the transaction ends.
    txEpoch :: !(IORef Int)
  }

-- | The messages of the server that a transaction reads, which are told apart by their tags. The body of those that
-- are not looked at is the empty one, which the next message skips, however large it is.
type Replies =
  '[ FramedMessage 'D' ('PayloadCounted16 'MaybeBytes32Atom),
     FramedMessage 'C' 'CStringAtom,
     FramedMessage 'Z' ('FixedBytesAtom 1),
     FramedMessage '1' ('PayloadStruct '[]),
     FramedMessage '2' ('PayloadStruct '[]),
     FramedMessage 'E' 'RemainingBytesAtom,
     FramedMessage 'I' ('PayloadStruct '[]),
     FramedMessage 'N' ('PayloadStruct '[]),
     FramedMessage 'S' ('PayloadStruct '[]),
     FramedMessage 'A' ('PayloadStruct '[]),
     FramedMessage '3' ('PayloadStruct '[]),
     FramedMessage 'n' ('PayloadStruct '[]),
     FramedMessage 't' ('PayloadStruct '[]),
     FramedMessage 'T' ('PayloadStruct '[])
   ]

-- | What a message of the server is, as far as the reading cares.
data Event
  = -- | A DataRow: the cursor of its columns.
    Row !(At ('PayloadCounted16 'MaybeBytes32Atom) Carrier)
  | -- | A CommandComplete: its tag, which is a view of the buffer.
    Complete !ByteString
  | EmptyQuery
  | -- | An ErrorResponse: its body, copied.
    Rejected !ByteString
  | -- | A ReadyForQuery: the byte of its status.
    Ready !Int
  | Other

-- | The next message of the server, told apart by its tag.
events :: (Runtime arr) => arr (At ('PayloadChoice Replies) Carrier) Event
events =
  withOnly $
    asks message
      >>> With
        ( cases
            ( arr Row
                :> (cstring >>^ Complete)
                :> (fixed >>^ Ready)
                :> other
                :> other
                :> (remaining >>^ Rejected)
                :> arr (const EmptyQuery)
                :> other
                :> other
                :> other
                :> other
                :> other
                :> other
                :> other
                :> NoCases
            )
        )
  where
    other :: (Arrow a) => a b Event
    other = arr (const Other)

-- | What the transaction makes of a message: what the reading goes on with.
data Step
  = -- | A message that no reading is waiting for.
    Stepped
  | -- | A row of the statement that is being read.
    Rowed !(At ('PayloadCounted16 'MaybeBytes32Atom) Carrier)
  | -- | The statement that was being read is answered, with this many rows.
    Completed !Int
  | -- | The server failed the statement that was being read, and so skipped the statements after it.
    Refused !SomeException
  | -- | Everything that was sent is answered.
    Synced

-- | The step that is given the transaction of its environment, besides its input.
withTx :: (Arrow arr, Effect arr) => (Tx -> a -> IO b) -> With Tx arr a b
withTx step = With (now (uncurry step))

-- | The step that is given the transaction of its environment, and ignores its input.
inTx :: (Arrow arr, Effect arr) => (Tx -> IO b) -> With Tx arr a b
inTx step = withTx (\tx _ -> step tx)
{-# INLINE inTx #-}

-- | The step that gives what the function makes of the state of the transaction.
inState :: (Arrow arr, Effect arr) => (State -> b) -> With Tx arr a b
inState view = inTx (fmap view . inspect)
{-# INLINE inState #-}

-- | The step that changes the state of the transaction.
modifying :: (Arrow arr, Effect arr) => (State -> State) -> With Tx arr a ()
modifying change = inTx (`modify` change)
{-# INLINE modifying #-}

modify :: Tx -> (State -> State) -> IO ()
modify tx = modifyIORef' (txState tx)

-- | The transaction as it is.
inspect :: Tx -> IO State
inspect = readIORef . txState

-- | The program's value, or the 'QueryError' that failed it, as a result; any other failure is raised again.
queryOutcome :: Either SomeException a -> IO (Either QueryError a)
queryOutcome = either failed (pure . Right)
  where
    failed failure = maybe (throwIO failure) (pure . Left) (fromException failure)

-- | What the server said that it should not have: it ends the connection.
unexpected :: String -> ConnectionError
unexpected reason = ConnectionBroken ("the server broke the protocol: " <> reason)

-- | What a message of the server does to the transaction: the statement that is being read is answered, or failed,
-- or has a row, or the pipeline is answered whole.
settleEvent :: Tx -> Event -> IO Step
settleEvent tx = \case
  Other -> pure Stepped
  Row cursor -> Rowed cursor <$ modify tx (\s -> s {stRows = stRows s + 1})
  Complete tag -> answered tx (Just tag)
  EmptyQuery -> answered tx Nothing
  Rejected body -> rejected tx body
  Ready status -> Synced <$ modify tx (\s -> s {stOwed = False, stStatus = status, stPending = []})

-- | The statement that was being read is answered: it is done, and its tag is kept if the program may ask for it.
answered :: Tx -> Maybe ByteString -> IO Step
answered tx tag = do
  s <- inspect tx
  case stPending s of
    [] -> throwIO (unexpected "an answer to a statement that wasn't sent")
    entry : others -> do
      -- The tag is a view of the buffer: it is copied now, and not when it is asked for.
      let !copied = maybe BS.empty BS.copy tag
          kept = if entrySkippable entry then stTags s else (stIndex s, copied) : stTags s
      modify tx (const s {stPending = others, stIndex = stIndex s + 1, stRows = 0, stTags = kept})
      pure (Completed (stRows s))

-- | The server failed the statement that was being read, and skipped the ones after it. The first failure is the
-- transaction's, which every read that follows raises, and the connection is lost if the server ended the session.
rejected :: Tx -> ByteString -> IO Step
rejected tx body = do
  s <- inspect tx
  failure <- either (throwIO . unexpected) (pure . ServerError) (bodyDiagnostics body)
  when (isConnectionFatal failure) (throwIO (ConnectionTerminated failure))
  let query = case stPending s of entry : _ -> entrySql entry; [] -> BS.empty
      exception = toException (QueryError query (ServerFailed failure))
  modify tx (const s {stPending = [], stFailure = Just (fromMaybe exception (stFailure s))})
  pure (Refused exception)

-- | The next message of the server, taken into the transaction.
pull :: (Runtime arr) => With Tx arr () Step
pull = (ask &&& (asks (arr txCursor) >>> lift events)) >>> now (uncurry settleEvent)

-- | The step that is given the epoch that the rows move on with: a read of the transaction, other than that of a field,
-- ends the turn of the field that a row handed out, and with it the bytes that its pieces were views of.
moved :: (Arrow arr, Effect arr) => With Tx arr a a
moved = withTx (\tx input -> input <$ (modifyIORef' (txEpoch tx) (+ 1) *> modify tx (\s -> s {stField = False})))

-- | Takes the messages of the server one after the other, until what the function makes of one is 'Finish'.
pullUntil :: (Runtime arr) => (Step -> IO (Either () ())) -> With Tx arr () ()
pullUntil interpret = iterateA (pull >>> now interpret)

-- | Reads until the statement that is being read is answered. Its rows are skipped, the bytes of each with the next
-- message. The failure of the server is raised if the read validates, and else it is only noted, and the statements
-- that the server skipped are answered with it.
finishEntry :: (Runtime arr) => Bool -> With Tx arr () ()
finishEntry validating = pullUntil interpret
  where
    interpret = \case
      Completed _ -> pure (Finish ())
      Refused failure | validating -> throwIO failure
      Refused _ -> pure (Finish ())
      Synced -> throwIO (unexpected "the pipeline ended before the statement was answered")
      _ -> pure (Again ())

-- | Reads everything that is owed, up to the ReadyForQuery that ends the pipeline, as 'finishEntry' reads a statement.
drain :: (Runtime arr) => Bool -> With Tx arr () ()
drain validating = inState stOwed >>> boolA returnA (pullUntil interpret)
  where
    interpret = \case
      Synced -> pure (Finish ())
      Refused failure | validating -> throwIO failure
      _ -> pure (Again ())

-- | Where a result is, among the statements that the server has yet to answer.
data Located
  = -- | It was answered already.
    Finished
  | -- | It is the one that is being read.
    Current
  | -- | A statement before it is not a result of the program, and may be read past.
    Skippable
  | -- | A result of the program is before it, with this many that were neither read nor discarded.
    Misordered !Int

-- | The way to the result at the position, which says whether it is the one that is being read, as opposed to answered
-- already. What the transaction owes before it is read past, if it isn't the program's: what is the program's, and
-- unread, is a failure to read out of order, after the failure of a statement before it, if the server reported one.
seek :: (Runtime arr) => Int -> With Tx arr () Bool
seek target = loop
  where
    loop = inTx locate >>> byLocation ^>> (returnA ||| ((finishEntry True >>> loop) ||| misordered))
    byLocation Finished = Left False
    byLocation Current = Left True
    byLocation Skippable = Right (Left ())
    byLocation (Misordered unread) = Right (Right unread)
    locate tx = do
      s <- inspect tx
      traverse_ throwIO (stFailure s)
      let wanted = stBase s + target
      pure $
        if
          | wanted < stIndex s -> Finished
          | wanted == stIndex s -> Current
          | otherwise -> case stPending s of
              entry : _ | entrySkippable entry -> Skippable
              pending -> Misordered (length (filter (not . entrySkippable) (take (wanted - stIndex s) pending)))
    -- The statements before it are read to their ends, which finds the failure of one of them if there is one.
    misordered = keeping settling >>> withTx outOfOrder
    settling = iterateWith (inState unsettled) (finishEntry True)
    unsettled s = if stIndex s < stBase s + target && not (null (stPending s)) then Again () else Finish ()
    outOfOrder tx ((), unread) = do
      modify tx (\s -> s {stFailure = Just (fromMaybe (toException (ResultOutOfOrder unread)) (stFailure s))})
      throwIO (ResultOutOfOrder unread)

-- | The next row of the result, or 'Nothing' after the last. Results are read in
-- the order of their statements: reading a later one while an earlier one is
-- unread fails with 'ResultOutOfOrder'. A result whose statement failed raises
-- its 'QueryError' here.
nextRow :: (Runtime arr) => Result arr row -> With Tx arr () (Maybe row)
nextRow (Result position reading) =
  moved >>> seek position >>> boolA noRow loop
  where
    noRow = arr (const Nothing)
    loop = iterateA (pull >>> withTx arrived) >>> (decoding ||| noRow)
    decoding = case reading of
      Reading _ cursor -> decodeRow cursor >>^ Just
      Silent -> now (const (throwIO (UnexpectedAmountOfRows 1)))
    cardinality = case reading of Reading how _ -> how; Silent -> NoRows
    -- A row is checked against what the statement allows as it comes: the first one too many fails.
    arrived tx = \case
      Stepped -> pure (Again ())
      Rowed cursor -> do
        s <- inspect tx
        case cardinality of
          ExactlyOne | stRows s > 1 -> throwIO (UnexpectedAmountOfRows (stRows s))
          AtMostOne | stRows s > 1 -> throwIO (UnexpectedAmountOfRows (stRows s))
          NoRows -> throwIO (UnexpectedAmountOfRows (stRows s))
          _ -> pure (Finish (Left cursor))
      Completed rowsRead -> do
        when (cardinality == ExactlyOne && rowsRead /= 1) (throwIO (UnexpectedAmountOfRows rowsRead))
        pure (Finish (Right ()))
      Refused failure -> throwIO failure
      Synced -> throwIO (unexpected "the pipeline ended before the statement was answered")

-- | The row of the cursor, read by the program's step, which has to take its columns, and no more.
decodeRow :: (Runtime arr) => arr Columns row -> With Tx arr (At ('PayloadCounted16 'MaybeBytes32Atom) Carrier) row
decodeRow rowArrow =
  alongside (lift count)
    >>> withTx columnsAt
    >>> alongside (lift rowArrow)
    >>> second (now leftover)
    >>^ fst
  where
    columnsAt tx (total, cursor) = do
      stamp <- readIORef (txEpoch tx)
      pure
        Columns
          { colsCursor = cursor,
            colsTotal = total,
            colsState = txState tx,
            colsEpoch = txEpoch tx,
            colsStamp = stamp
          }

-- | A row has more columns than the program read: it is one that the program doesn't describe, which isn't decoded
-- wrong, but isn't what was asked for.
leftover :: Columns -> IO ()
leftover columns@Columns {colsCursor = At reader depth, colsTotal} = do
  unread <- mark reader (markFrame depth)
  when (unread > 0) (failedColumns columns (RowFailed (ColumnCountMismatch (colsTotal - unread) colsTotal)))

-- | A column that can't be taken: the row's statement fails with the reason.
failedColumns :: Columns -> StatementError -> IO a
failedColumns Columns {colsState} reason = do
  s <- readIORef colsState
  throwIO (QueryError (case stPending s of entry : _ -> entrySql entry; [] -> BS.empty) reason)

-- | Drops the rest of the result.
discard :: (Runtime arr) => Result arr row -> With Tx arr () ()
discard (Result position _) = moved >>> seek position >>> boolA returnA (finishEntry True)

-- | Awaits the server's acknowledgement of a preparation.
prepared :: (Runtime arr) => Preparation -> With Tx arr () ()
prepared _ = notImplemented "prepared"

-- | The command tag of the result, once its rows are read: of the one that is being read, what is left of it is dropped.
commandTag :: (Runtime arr) => Result arr row -> With Tx arr () ByteString
commandTag result@(Result position _) = discard result >>> inState tagOf
  where
    tagOf s = fromMaybe BS.empty (lookup (stBase s + position) (stTags s))

-- | The transaction is over, as far as the program is concerned: COMMIT's own reply as a value. Anything that the server
-- said failed, and that wasn't read, is that reply, as the failure of the statement is.
ending :: (Runtime arr) => With Tx arr () (Either QueryError ())
ending = tryA finishing >>> now queryOutcome

-- | The one row of the result, which 'UnexpectedAmountOfRows' is raised without. The program is done with the result
-- then: what follows the row is read past when a later result is read, and the field of the row stays valid until then.
single :: (Runtime arr) => Result arr row -> With Tx arr () row
single result@(Result position _) = nextRow result >>> now (maybe (throwIO (UnexpectedAmountOfRows 0)) pure) >>> concluded position

-- | The row of the result, if it has one, as 'single' gives it.
optional :: (Runtime arr) => Result arr row -> With Tx arr () (Maybe row)
optional result@(Result position _) = nextRow result >>> concluded position

-- | The program is done with the result: it is no longer one that has to be read to its end before a later one is read,
-- and the rest of it is read past, with the row that is being read, when that is. If it is answered, nothing is left.
concluded :: (Arrow arr, Effect arr) => Int -> With Tx arr a a
concluded position = withTx (\tx input -> input <$ modify tx (\s -> if stBase s + position == stIndex s then s {stPending = skippable (stPending s)} else s))
  where
    skippable = \case entry : others -> entry {entrySkippable = True} : others; [] -> []

-- | Folds the rows of the result, one at a time, from the first to the last.
foldRows :: (Runtime arr) => (s -> row -> s) -> s -> Result arr row -> With Tx arr () s
foldRows step start result = const start ^>> iterateA (keeping (nextRow result) >>^ folded)
  where
    folded (next', accumulated) = maybe (Finish accumulated) (Again . step accumulated) next'

-- * Sending

-- | Sends the pipeline, with its parameters from the input, and gives its
-- positions to the reading program that follows. A program sends any number of
-- pipelines in order: what the one before was answered with, and not read, is
-- dropped, as is the field of a row.
pipeline :: (Runtime arr) => Ending -> Pipeline arr p a -> (a -> With Tx arr x y) -> With Tx arr (p, x) y
pipeline finality (Pipeline _ sent value) reading = consume (sending finality sent) >>> reading (value 0)

-- | What the server is sent: the statements, and a Sync, in as many sends as the buffer needs, once what the pipeline
-- before it was answered with is read. BEGIN comes with the first, and the ending with the last.
sending :: (Runtime arr) => Ending -> [Sent arr p] -> With Tx arr p ()
sending finality sent =
  tap (done >>> moved >>> readyToSend)
    >>> withTx (layout finality sent)
    >>> (returnA ||| transmit)

-- | The pipeline before this was answered: what is owed is read, and a failure of the server found is raised.
readyToSend :: (Runtime arr) => With Tx arr () ()
readyToSend = inTx check >>> drain True
  where
    check tx = do
      s <- inspect tx
      traverse_ throwIO (stFailure s)
      when (stFinal s || stEnded s) (throwIO AfterFinal)

-- | The output of a pipeline of the program, which is nothing if there is nothing to send. What the server is to answer
-- is put in the queue of the transaction, once all of it is rendered.
layout :: Ending -> [Sent arr p] -> Tx -> p -> IO (Either () Output)
layout finality sent tx input = do
  s <- inspect tx
  user <- traverse (rendered input) sent
  let starting = [(Entry (txBegin tx) True, txBeginOutput tx) | not (stBegun s), not (null sent) || finality == Final]
      closing = [(Entry text True, output) | finality == Final, let (text, output) = endingStatement (stCondemned s)]
  case starting <> user <> closing of
    [] -> pure (Left ())
    queued -> do
      output <- evaluate (foldMap snd queued <> sync)
      modify tx (const s {stBegun = True, stFinal = stFinal s || finality == Final, stPending = map fst queued, stIndex = 0, stBase = length starting, stRows = 0, stTags = [], stOwed = True, stSending = True})
      pure (Right output)
  where
    plain text = statementOutput text [] [] []
    rendered parameters = \case
      Execute given -> do
        (types, formats, values) <- parametersOf (params given parameters)
        pure (Entry (sql given) False, statementOutput (sql given) types formats values)
      Command text -> pure (Entry text False, plain text)
      Prepare {} -> notImplementedIO "prepare"
      ExecutePrepared {} -> notImplementedIO "executePrepared"
      Close _ -> notImplementedIO "closeStatement"

-- | The statement that ends a transaction: ROLLBACK if it is condemned, else COMMIT, as its SQL, which the errors name, and
-- the messages that carry it.
endingStatement :: Bool -> (ByteString, Output)
endingStatement condemned = if condemned then ("ROLLBACK", rollbackOutput) else ("COMMIT", commitOutput)

-- | COMMIT and ROLLBACK as messages, rendered once, since they are the same for every transaction.
commitOutput, rollbackOutput :: Output
commitOutput = prerendered (statementOutput "COMMIT" [] [] [])
rollbackOutput = prerendered (statementOutput "ROLLBACK" [] [] [])
{-# NOINLINE commitOutput #-}
{-# NOINLINE rollbackOutput #-}

-- | The output rendered into bytes, which are copied where they are sent: for what is the same every time, so that it is
-- not rendered each time. All of it is copied, and it is given room for all of it, so the writer always finishes.
prerendered :: Output -> Output
prerendered output = Output.bytes (BSI.unsafeCreate size render)
  where
    size = outputLength output
    render pointer =
      runWriter (outputWriter output) pointer size 0 >>= \case
        Output.Finished _ -> pure ()
        _ -> error "prerendered: an output that is not all copied"

-- | The messages for an unnamed statement: Parse, Bind with the parameters, which says that the rows come in binary,
-- and Execute.
statementOutput :: ByteString -> [Maybe Oid] -> [FormatCode] -> [Maybe Parameter] -> Output
statementOutput text types formats values =
  Wire.serialize (Wire.Parse "" text (map (fmap Wire.Scalar) types))
    <> Wire.serialize (Wire.Bind (Wire.BindBody "" "" formats values [BinaryFormat]))
    <> Wire.serialize (Wire.Execute "" 0 :: Wire.Execute ByteString)

-- | The types, the formats and the values of the parameters as the protocol has them.
parametersOf :: [Param arr] -> IO ([Maybe Oid], [FormatCode], [Maybe Parameter])
parametersOf given = (map paramType given,map paramFormat given,) <$> traverse (valueOf . paramValue) given
  where
    valueOf = \case
      Null -> pure Nothing
      Bytes bytes -> pure (Just (ParameterBytes bytes))
      Pieces pieces -> pure (Just (ParameterChunks pieces))
      Streamed _ _ -> notImplementedIO "streamed parameters"

-- | Sends what was laid out, in the loan of the transaction, whose buffer the replies are read into when it is sent.
transmit :: (Runtime arr) => With Tx arr Output ()
transmit =
  withTx (\tx output -> pure (sessionCarrier (connectionSession (txConnection tx)), loanBuffer (txLoan tx), output))
    >>> lift sendOutput
    >>> modifying (\s -> s {stSending = False})

-- | A statement of the transaction's own, as a pipeline of its own: ROLLBACK or COMMIT, read as the pipeline's
-- statements are.
alone :: (Runtime arr) => Bool -> With Tx arr (ByteString, Output) ()
alone validating = moved >>> withTx queued >>> transmit >>> drain validating
  where
    queued tx (text, carrier) = do
      s <- inspect tx
      let !output = carrier <> sync
      modify tx (const s {stPending = [Entry text True], stIndex = 0, stBase = 0, stRows = 0, stTags = [], stOwed = True, stSending = True})
      pure output

-- | Ends the transaction as the program didn't: what is owed is read, and what the server may have left open is rolled
-- back. The failures that are read are the program's to have seen.
cleanup :: (Runtime arr) => With Tx arr () ()
cleanup =
  moved
    >>> drain False
    >>> inState ((/= idle) . stStatus)
    >>> boolA returnA (const (endingStatement True) ^>> alone False)
    >>> modifying (\s -> s {stEnded = True})

-- | The byte of @I@, which the server's status is when it is idle.
idle :: Int
idle = 73

-- | Ends the transaction as the program ended it. If a failure of the server was reported, and the program read it
-- and went on, the transaction is rolled back, and the program's value stands. Else what is unread is read, which
-- raises the failure of a statement that nobody read, the ending is sent unless the pipeline carried it, and it is
-- read, which raises the failure of COMMIT.
finishing :: (Runtime arr) => With Tx arr () ()
finishing =
  inState finished >>> (returnA ||| boolA completing cleanup)
  where
    -- Whether the transaction is over, and else whether a failure of the server was reported.
    finished s = if stEnded s then Left () else Right (isJust (stFailure s))
    completing =
      drain True
        >>> inState unsent
        >>> (returnA ||| alone True)
        >>> inTx ended
    -- The ending, unless the pipeline carried it, or nothing began.
    unsent s = if stBegun s && not (stFinal s) then Right (endingStatement (stCondemned s)) else Left ()
    ended tx = do
      s <- inspect tx
      when (stBegun s && stStatus s /= idle) (throwIO (unexpected "the transaction's ending did not leave the session idle"))
      modify tx (\t -> t {stEnded = True})
{-# INLINE finishing #-}

-- | The transaction, with the connection and the loans of its input: it is the work of the part that runs it, for as
-- long as it takes, and it ends as the program ended: with the failure of the program, of a statement or of the ending,
-- the ending is rolled back, and the result is the program's, or the 'QueryError' of a statement or of the ending that
-- the program didn't catch. The connection stays usable, unless the cancellation rule breaks it.
transaction :: (Runtime arr) => IsolationLevel -> Mode -> With Tx arr x y -> arr (Connection, (Loans, x)) (Either QueryError y)
transaction level mode program =
  tryA (scope (begin level mode) leaving body)
    >>> now queryOutcome
  where
    body = programInput ^>> with (program >>> tap (done >>> finishing))
    -- A pattern takes the fields out; second (snd . snd) would leave a thunk in the pair.
    programInput (tx, (_, (_, input))) = (tx, input)

-- | Takes the connection for the transaction, and the loan that it works in, which is the primary if the request has not
-- lent it. A connection that is used by another part, or is broken, or one that the body has given up, is a failure,
-- and the loan goes back.
begin :: (Runtime arr) => IsolationLevel -> Mode -> arr (Connection, (Loans, x)) Tx
begin level mode =
  loansFirst ^>> first borrow
    >>> now (\(loan, connection) -> either (\failure -> Left (loan, failure)) Right <$> try (open loan connection))
    >>> ((first release >>> now (\((), failure) -> throwIO (failure :: SomeException))) ||| returnA)
  where
    -- A pattern takes the fields out; fst . snd &&& fst would leave a thunk for each.
    loansFirst (connection, (loans, _)) = (loans, connection)
    -- BEGIN is rendered once for the program, when it is built, and not for each transaction.
    !beginOutput = prerendered (statementOutput (beginning level mode) [] [] [])
    open loan connection = do
      reader <- newReader (sessionCarrier (connectionSession connection)) (loanBuffer loan)
      state <-
        newIORef
          State
            { stBegun = False,
              stFinal = False,
              stCondemned = False,
              stEnded = False,
              stPending = [],
              stIndex = 0,
              stBase = 0,
              stRows = 0,
              stTags = [],
              stFailure = Nothing,
              stOwed = False,
              stStatus = idle,
              stSending = False,
              stField = False
            }
      epoch <- newIORef 0
      -- The last, since it can't be undone but by the failure of this.
      claim connection
      pure
        Tx
          { txConnection = connection,
            txBegin = beginning level mode,
            txBeginOutput = beginOutput,
            txReader = reader,
            txCursor = messages reader,
            txLoan = loan,
            txState = state,
            txEpoch = epoch
          }

-- | BEGIN, with the isolation level and the mode.
beginning :: IsolationLevel -> Mode -> ByteString
beginning level mode = "BEGIN ISOLATION LEVEL " <> levelText <> " " <> modeText
  where
    levelText = case level of ReadCommitted -> "READ COMMITTED"; RepeatableRead -> "REPEATABLE READ"; Serializable -> "SERIALIZABLE"
    modeText = case mode of Read -> "READ ONLY"; Write -> "READ WRITE"

-- | What happens to the connection as the transaction ends, however it ends: nothing is owed on it, and it is as it
-- was; or it is, and the connection is broken, and closed, since it is never drained for a client that has gone
-- away; or the failure was the program's, which doesn't harm the connection, and what the server owes is read and what
-- it left open is rolled back, before the connection is given back. The loan goes back last.
leaving :: (Runtime arr) => arr (Tx, Exit) ()
leaving =
  (,) <$> arr fst <*> now (uncurry plan)
    >>> planned ^>> (now settled ||| recovering)
    >>> tap (now (\tx -> modifyIORef' (txEpoch tx) (+ 1)))
    >>> txLoan ^>> release
  where
    planned (tx, Recover) = Right tx
    planned (tx, Keep) = Left (tx, Nothing)
    planned (tx, Break why) = Left (tx, Just why)
    settled (tx, why) = tx <$ settle (connectionSession (txConnection tx)) why
    recovering = alongside (tryA ((, ()) ^>> with cleanup)) >>> now (uncurry recovered)
    recovered rolledBack tx = settled (tx, either (Just . ("the transaction could not be rolled back: " <>) . displayException) (const Nothing) rolledBack)

-- | What is to be done with the connection: nothing, or reading and rolling back, or breaking it for the reason.
data Plan = Keep | Recover | Break String

-- | The way that the transaction ended says what the connection is owed.
plan :: Tx -> Exit -> IO Plan
plan tx exit = do
  s <- inspect tx
  unread <- fieldUnread tx s
  let owed = stOwed s || stSending s
      open = stBegun s && not (stEnded s)
  pure $ case exit of
    Done
      | owed -> Break "the transaction ended with replies unread"
      | open -> Recover
      | otherwise -> Keep
    Failed failure
      | not (recoverable failure) -> Break (displayException failure)
      | stSending s -> Break "the transaction failed while it sent"
      | unread -> Break "a field was left unread in the middle of its stream"
      | otherwise -> Recover
    Cancelled'
      | owed -> Break "the transaction was cancelled while its replies were awaited"
      | open -> Recover
      | otherwise -> Keep

-- | Whether a field was handed out of the row that is being read, and has bytes of it unread, which would have to be
-- read to go on, for a client that is gone: it is not.
fieldUnread :: Tx -> State -> IO Bool
fieldUnread tx s
  | stField s = (>) <$> mark (txReader tx) markLimit <*> mark (txReader tx) markStart
  | otherwise = pure False

-- | Whether a failure leaves the connection where the protocol is: the failures of statements and of the use of the
-- API, and of the program, do; those of the connection or of the replies, and of the sending of what the
-- program gave, don't.
recoverable :: SomeException -> Bool
recoverable failure
  | isJust (fromException failure :: Maybe QueryError) = True
  | isJust (fromException failure :: Maybe UsageError) = True
  | isJust (fromException failure :: Maybe ConnectionError) = False
  | isJust (fromException failure :: Maybe IOException) = False
  | isJust (fromException failure :: Maybe CursorError) = False
  | isJust (fromException failure :: Maybe SomeAsyncException) = False
  | otherwise = True

-- | Rolls back at the end, and a later 'Final' pipeline carries ROLLBACK.
condemn :: (Runtime arr) => With Tx arr () ()
condemn = inTx condemned
  where
    condemned tx = do
      s <- inspect tx
      when (stFinal s || stEnded s) (throwIO AfterFinal)
      modify tx (\t -> t {stCondemned = True})

-- * Rows, read by cursors

-- | A cursor standing at a DataRow: the cursor of its columns, how many there are, and what a failure of a column and
-- a field need to know.
data Columns = Columns
  { colsCursor :: !(At ('PayloadCounted16 'MaybeBytes32Atom) Carrier),
    colsTotal :: !Int,
    colsState :: !(IORef State),
    colsEpoch :: !(IORef Int),
    colsStamp :: !Int
  }

-- | A column that was taken from its row: the cursor of its value, which has its length read, the length, which is
-- negative for NULL, and the columns of the row.
data Column = Column
  { columnValue :: At 'MaybeBytes32Atom Carrier,
    columnSize :: !Int,
    columnRow :: !Columns
  }

-- | How many rows a result may have, which the read that finds the count wrong
-- checks.
data Cardinality
  = -- | One, no more and no fewer.
    ExactlyOne
  | -- | None or one.
    AtMostOne
  | -- | Any number.
    AnyNumber
  | -- | None: the statement gives no rows.
    NoRows
  deriving (Eq, Show)

-- | How the rows of a result are read: how many there may be, and a step of
-- the cursor, for each row.
data Rows arr row = Rows Cardinality (arr Columns row)

-- | A column's value as a stream, of the declared length: the last column of a
-- row, and the way to read a value that is larger than the buffer. It is valid
-- until its row moves on, and then fails with 'FieldExpired'.
data Field = Field !Int !(At 'MaybeBytes32Atom Carrier) !(IORef Int) !Int

instance (Runtime arr) => Stream arr Field where
  next = now (\(Field _ cursor epoch stamp) -> readIORef epoch >>= \current -> if current == stamp then pure cursor else throwIO FieldExpired) >>> piece

-- | The next column, with its length read.
nextValue :: (Runtime arr) => arr Columns Column
nextValue =
  (\columns@Columns {colsCursor} -> (colsCursor, columns))
    ^>> first advance
    >>> now (\(found, columns@Columns {colsCursor, colsTotal}) -> if found then pure (child colsCursor, columns) else failedColumns columns (RowFailed (ColumnCountMismatch (colsTotal + 1) colsTotal)))
    >>> (\(value, columns) -> (value, (value, columns)))
    ^>> first valueLength
    >>^ taken
  where
    taken (size, (value, columns)) = Column {columnValue = value, columnSize = size, columnRow = columns}

-- | The column decoded by the type's decoder, from its bytes in the buffer, or from none if it is NULL. A column that
-- is larger than the buffer is a failure of its use, and one that doesn't decode is a failure of its statement.
decoded :: (Runtime arr) => (Maybe ByteString -> Either Text r) -> arr Columns r
decoded decode =
  nextValue
    >>> nullOrValue
    ^>> (now (\columns -> outcome columns (decode Nothing)) ||| (now fits >>> first valueView >>> now (\(bytes, columns) -> outcome columns (decode (Just bytes)))))
  where
    nullOrValue col
      | columnSize col < 0 = Left (columnRow col)
      | otherwise = Right col
    fits Column {columnValue, columnSize, columnRow = columns@Columns {colsCursor = At reader _}} =
      (columnValue, columns) <$ when (columnSize > readerCapacity reader) (throwIO (ValueTooLarge columnSize))
    outcome columns@Columns {colsCursor = At reader depth, colsTotal} = \case
      Right decodedValue -> pure $! decodedValue
      Left reason -> do
        unread <- mark reader (markFrame depth)
        failedColumns columns (RowFailed (ColumnDecodingFailed (colsTotal - unread) reason))

-- | The next column, decoded from the buffer. A column larger than the buffer
-- fails with 'ValueTooLarge'.
column :: (Runtime arr, InPlace a) => arr Columns a
column = decoded decodeField

-- | The next column, which may be NULL.
nullable :: (Runtime arr, InPlace a) => arr Columns (Maybe a)
nullable = decoded (maybe (Right Nothing) (fmap Just . decodeField . Just))

-- | The last column, as a stream.
field :: (Runtime arr) => arr Columns Field
field = nextValue >>> now (\col@Column {columnSize, columnRow} -> if columnSize < 0 then failedColumns columnRow (nullField columnRow) else opened col)
  where
    nullField Columns {colsTotal} = RowFailed (ColumnDecodingFailed colsTotal "unexpected NULL")

-- | The last column, as a stream, if it isn't NULL.
nullableField :: (Runtime arr) => arr Columns (Maybe Field)
nullableField = nextValue >>> now (\col -> if columnSize col < 0 then pure Nothing else Just <$> opened col)

-- | The field of a value, whose turn lasts until the row moves on.
opened :: Column -> IO Field
opened Column {columnValue, columnSize, columnRow = Columns {colsState, colsEpoch, colsStamp}} =
  Field columnSize columnValue colsEpoch colsStamp <$ modifyIORef' colsState (\s -> s {stField = True})

-- | How many bytes the field has, as it declares.
fieldLength :: Field -> Int
fieldLength (Field declared _ _ _) = declared

-- | Passes the next column.
skip :: (Runtime arr) => arr Columns ()
skip = nextValue >>> columnValue ^>> skipValue

-- | The rows that a step of the cursor reads, for a result of this many rows,
-- which the read that finds the count wrong checks: it fails with
-- 'UnexpectedAmountOfRows'.
one, atMostOne, any :: arr Columns row -> Rows arr row
one = Rows ExactlyOne
atMostOne = Rows AtMostOne
any = Rows AnyNumber

-- | A result that has no rows to read.
none :: (Runtime arr) => Rows arr ()
none = Rows NoRows done
