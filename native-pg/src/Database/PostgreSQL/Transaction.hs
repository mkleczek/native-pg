{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
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

import Control.Arrow (Arrow, arr, first, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, SomeAsyncException, SomeException, displayException, evaluate, fromException, throwIO, toException, try)
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (traverse_)
import Data.Functor (($>))
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
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import qualified Database.PostgreSQL.Internal.Protocol.Messages as Wire
import Database.PostgreSQL.Types (FormatCode (..), Oid, Parameter (..))
import GHC.Exts (Any)
import Network.Socket (Socket)
import Network.Streaming.Classes (Effect (..), Exit (..), Runtime, Scope (..), Stream (..))
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release)
import Network.Streaming.NotImplemented (notImplemented, notImplementedIO)
import Network.Streaming.Reader (Reader, markFrame, markLimit, markStart, mark, newReader, readerCapacity)
import Network.Streaming.With (With (..), ask, asks, lift, with, withOnly)
import Prelude hiding (any)
import Unsafe.Coerce (unsafeCoerce)

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
-- there may be, and the step of the cursor for each, with its arrow forgotten. The
-- arrow is the one that the pipeline is for, and so the one that reads it.
data Reading = Reading Cardinality Any | Silent

-- | The position of the answers to one statement, and how its rows are read.
data Result (row :: Type) = Result !Int !Reading

-- | The position of a preparation's acknowledgement.
newtype Preparation = Preparation Int

-- | The reading of the rows of a statement, as the result keeps it.
readingOf :: Rows arr row -> Reading
readingOf (Rows cardinality cursor) = Reading cardinality (unsafeCoerce cursor)

-- | A statement with its parameters, which the server parses, binds and executes.
statement :: Statement arr p row -> Pipeline arr p (Result row)
statement given = Pipeline 1 [Execute given] (\base -> Result base (readingOf (rows given)))

-- | Parses the statement under the name. Parse sends no parameters, so what the
-- pipeline takes as its input doesn't matter.
prepare :: ByteString -> Statement arr p row -> Pipeline arr q Preparation
prepare name given = Pipeline 1 [Prepare name (given {params = const []})] Preparation

-- | Binds and executes the statement that the connection has under the name.
executePrepared :: ByteString -> Statement arr p row -> Pipeline arr p (Result row)
executePrepared name given = Pipeline 1 [ExecutePrepared name given] (\base -> Result base (readingOf (rows given)))

-- | Closes the statement that the connection has under the name.
closeStatement :: ByteString -> Pipeline arr p (Result ())
closeStatement name = Pipeline 1 [Close name] (`Result` Silent)

-- | A command with no parameters and no rows, as an unnamed statement: its SQL is
-- one statement.
command :: ByteString -> Pipeline arr p (Result ())
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
    txReader :: !(Reader Socket),
    txCursor :: !(At ('PayloadChoice Replies) Socket),
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
    Row !(At ('PayloadCounted16 'MaybeBytes32Atom) Socket)
  | -- | A CommandComplete: its tag, which is a view of the buffer.
    Complete !ByteString
  | EmptyQuery
  | -- | An ErrorResponse: its body, copied.
    Rejected !ByteString
  | -- | A ReadyForQuery: the byte of its status.
    Ready !Int
  | Other

-- | The next message of the server, told apart by its tag.
events :: (Runtime arr) => arr (At ('PayloadChoice Replies) Socket) Event
events =
  withOnly $
    asks message
      >>> With
        ( cases
            ( arr Row
                :> (cstring >>^ Complete)
                :> (fixed >>^ Ready)
                :> arr (const Other)
                :> arr (const Other)
                :> (remaining >>^ Rejected)
                :> arr (const EmptyQuery)
                :> arr (const Other)
                :> arr (const Other)
                :> arr (const Other)
                :> arr (const Other)
                :> arr (const Other)
                :> arr (const Other)
                :> arr (const Other)
                :> NoCases
            )
        )

-- | What the transaction makes of a message: what the reading goes on with.
data Step
  = -- | A message that no reading is waiting for.
    Stepped
  | -- | A row of the statement that is being read.
    Rowed !(At ('PayloadCounted16 'MaybeBytes32Atom) Socket)
  | -- | The statement that was being read is answered, with this many rows.
    Completed !Int
  | -- | The server failed the statement that was being read, and so skipped the statements after it.
    Refused !SomeException
  | -- | Everything that was sent is answered.
    Synced

-- | The step that is given the transaction of its environment, besides its input.
withTx :: (Arrow arr, Effect arr) => (Tx -> a -> IO b) -> With Tx arr a b
withTx step = (ask &&& returnA) >>> now (uncurry step)

modify :: Tx -> (State -> State) -> IO ()
modify tx = modifyIORef' (txState tx)

-- | The transaction as it is.
inspect :: Tx -> IO State
inspect = readIORef . txState

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

-- | Reads until the statement that is being read is answered. Its rows are skipped, the bytes of each with the next
-- message. The failure of the server is raised if the read validates, and else it is only noted, and the statements
-- that the server skipped are answered with it.
finishEntry :: (Runtime arr) => Bool -> With Tx arr () ()
finishEntry validating = loop
  where
    loop = pull >>> now interpret >>> (loop ||| returnA)
    interpret = \case
      Completed _ -> pure (Right ())
      Refused failure | validating -> throwIO failure
      Refused _ -> pure (Right ())
      Synced -> throwIO (unexpected "the pipeline ended before the statement was answered")
      _ -> pure (Left ())

-- | Reads everything that is owed, up to the ReadyForQuery that ends the pipeline, as 'finishEntry' reads a statement.
drain :: (Runtime arr) => Bool -> With Tx arr () ()
drain validating = withTx (\tx _ -> (\s -> if stOwed s then Right () else Left ()) <$> inspect tx) >>> (returnA ||| loop)
  where
    loop = pull >>> now interpret >>> (loop ||| returnA)
    interpret = \case
      Synced -> pure (Right ())
      Refused failure | validating -> throwIO failure
      _ -> pure (Left ())

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
    loop =
      withTx (\tx _ -> locate tx)
        >>> ( \case
                Finished -> Left False
                Current -> Left True
                Skippable -> Right (Left ())
                Misordered unread -> Right (Right unread)
            )
          ^>> (returnA ||| ((finishEntry True >>> loop) ||| misordered))
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
    misordered = (returnA &&& (arr (const ()) >>> settling)) >>> withTx (\tx (unread, ()) -> outOfOrder tx unread)
    settling =
      withTx (\tx _ -> (\s -> if stIndex s < stBase s + target && not (null (stPending s)) then Right () else Left ()) <$> inspect tx)
        >>> (returnA ||| (finishEntry True >>> settling))
    outOfOrder tx unread = do
      modify tx (\s -> s {stFailure = Just (fromMaybe (toException (ResultOutOfOrder unread)) (stFailure s))})
      throwIO (ResultOutOfOrder unread)

-- | The next row of the result, or 'Nothing' after the last. Results are read in
-- the order of their statements: reading a later one while an earlier one is
-- unread fails with 'ResultOutOfOrder'. A result whose statement failed raises
-- its 'QueryError' here.
nextRow :: (Runtime arr) => Result row -> With Tx arr () (Maybe row)
nextRow (Result position reading) =
  moved >>> seek position >>> ((\found -> if found then Right () else Left ()) ^>> (arr (const Nothing) ||| loop))
  where
    loop = pull >>> withTx arrived >>> (loop ||| (decoding ||| arr (const Nothing)))
    decoding = case reading of
      Reading _ cursor -> decodeRow (unsafeCoerce cursor) >>^ Just
      Silent -> now (\_ -> throwIO (UnexpectedAmountOfRows 1))
    cardinality = case reading of Reading how _ -> how; Silent -> NoRows
    -- A row is checked against what the statement allows as it comes: the first one too many fails.
    arrived tx = \case
      Stepped -> pure (Left ())
      Rowed cursor -> do
        s <- inspect tx
        case cardinality of
          ExactlyOne | stRows s > 1 -> throwIO (UnexpectedAmountOfRows (stRows s))
          AtMostOne | stRows s > 1 -> throwIO (UnexpectedAmountOfRows (stRows s))
          NoRows -> throwIO (UnexpectedAmountOfRows (stRows s))
          _ -> pure (Right (Left cursor))
      Completed rowsRead -> do
        when (cardinality == ExactlyOne && rowsRead /= 1) (throwIO (UnexpectedAmountOfRows rowsRead))
        pure (Right (Right ()))
      Refused failure -> throwIO failure
      Synced -> throwIO (unexpected "the pipeline ended before the statement was answered")

-- | The row of the cursor, read by the program's step, which has to take its columns, and no more.
decodeRow :: (Runtime arr) => arr Columns row -> With Tx arr (At ('PayloadCounted16 'MaybeBytes32Atom) Socket) row
decodeRow rowArrow =
  (\cursor -> (cursor, cursor))
    ^>> first (lift count)
    >>> withTx (\tx (total, cursor) -> Columns cursor total (txState tx) (txEpoch tx) <$> readIORef (txEpoch tx))
    >>> (returnA &&& lift rowArrow)
    >>> withTx (\_ (columns, row) -> row <$ leftover columns)

-- | A row has more columns than the program read: it is one that the program doesn't describe, which isn't decoded
-- wrong, but isn't what was asked for.
leftover :: Columns -> IO ()
leftover columns@(Columns (At reader depth) total _ _ _) = do
  left <- mark reader (markFrame depth)
  when (left > 0) (failedColumns columns (RowFailed (ColumnCountMismatch (total - left) total)))

-- | A column that can't be taken: the row's statement fails with the reason.
failedColumns :: Columns -> StatementError -> IO a
failedColumns (Columns _ _ state _ _) reason = do
  s <- readIORef state
  throwIO (QueryError (case stPending s of entry : _ -> entrySql entry; [] -> BS.empty) reason)

-- | Drops the rest of the result.
discard :: (Runtime arr) => Result row -> With Tx arr () ()
discard (Result position _) = moved >>> seek position >>> ((\found -> if found then Right () else Left ()) ^>> (returnA ||| finishEntry True))

-- | Awaits the server's acknowledgement of a preparation.
prepared :: (Runtime arr) => Preparation -> With Tx arr () ()
prepared _ = notImplemented "prepared"

-- | The command tag of the result, once its rows are read: of the one that is being read, what is left of it is dropped.
commandTag :: (Runtime arr) => Result row -> With Tx arr () ByteString
commandTag (Result position _) =
  moved
    >>> seek position
    >>> ((\found -> if found then Right () else Left ()) ^>> (returnA ||| finishEntry True))
    >>> withTx (\tx _ -> (\s -> fromMaybe BS.empty (lookup (stBase s + position) (stTags s))) <$> inspect tx)

-- | The transaction is over, as far as the program is concerned: COMMIT's own reply as a value. Anything that the server
-- said failed, and that wasn't read, is that reply, as the failure of the statement is.
ending :: (Runtime arr) => With Tx arr () (Either QueryError ())
ending = tryA finishing >>> now (either (\failure -> maybe (throwIO failure) (pure . Left) (fromException failure)) (pure . Right))

-- | The one row of the result, which 'UnexpectedAmountOfRows' is raised without. The program is done with the result
-- then: what follows the row is read past when a later result is read, and the field of the row stays valid until then.
single :: (Runtime arr) => Result row -> With Tx arr () row
single result@(Result position _) = nextRow result >>> now (maybe (throwIO (UnexpectedAmountOfRows 0)) pure) >>> concluded position

-- | The row of the result, if it has one, as 'single' gives it.
optional :: (Runtime arr) => Result row -> With Tx arr () (Maybe row)
optional result@(Result position _) = nextRow result >>> concluded position

-- | The program is done with the result: it is no longer one that has to be read to its end before a later one is read,
-- and the rest of it is read past, with the row that is being read, when that is. If it is answered, nothing is left.
concluded :: (Arrow arr, Effect arr) => Int -> With Tx arr a a
concluded position = withTx (\tx input -> input <$ modify tx (\s -> if stBase s + position == stIndex s then s {stPending = skippable (stPending s)} else s))
  where
    skippable = \case entry : others -> entry {entrySkippable = True} : others; [] -> []

-- | Folds the rows of the result, one at a time, from the first to the last.
foldRows :: (Runtime arr) => (s -> row -> s) -> s -> Result row -> With Tx arr () s
foldRows step start result = const start ^>> loop
  where
    loop =
      ((),)
        ^>> first (nextRow result)
        >>> (\(next', accumulated) -> maybe (Left accumulated) (Right . step accumulated) next')
        ^>> (returnA ||| loop)

-- * Sending

-- | Sends the pipeline, with its parameters from the input, and gives its
-- positions to the reading program that follows. A program sends any number of
-- pipelines in order: what the one before was answered with, and not read, is
-- dropped, as is the field of a row.
pipeline :: (Runtime arr) => Ending -> Pipeline arr p a -> (a -> With Tx arr x y) -> With Tx arr (p, x) y
pipeline finality (Pipeline _ sent value) reading = (first (sending finality sent) >>^ snd) >>> reading (value 0)

-- | What the server is sent: the statements, and a Sync, in as many sends as the buffer needs, once what the pipeline
-- before it was answered with is read. BEGIN comes with the first, and the ending with the last.
sending :: (Runtime arr) => Ending -> [Sent arr p] -> With Tx arr p ()
sending finality sent =
  ((returnA &&& (arr (const ()) >>> moved >>> readyToSend)) >>^ fst)
    >>> withTx (\tx input -> layout tx finality sent input)
    >>> (returnA ||| transmit)

-- | The pipeline before this was answered: what is owed is read, and a failure of the server found is raised.
readyToSend :: (Runtime arr) => With Tx arr () ()
readyToSend = withTx (\tx _ -> inspect tx >>= \s -> traverse_ throwIO (stFailure s) *> when (stFinal s || stEnded s) (throwIO AfterFinal)) >>> drain True

-- | The output of a pipeline of the program, which is nothing if there is nothing to send. What the server is to answer
-- is put in the queue of the transaction, once all of it is rendered.
layout :: Tx -> Ending -> [Sent arr p] -> p -> IO (Either () Output)
layout tx finality sent input = do
  s <- inspect tx
  user <- traverse (rendered input) sent
  let starting = [(Entry (txBegin tx) True, plain (txBegin tx)) | not (stBegun s), not (null sent) || finality == Final]
      closing = [(Entry text True, plain text) | finality == Final, let text = if stCondemned s then "ROLLBACK" else "COMMIT"]
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
  withTx (\tx output -> pure (sessionSocket (connectionSession (txConnection tx)), loanBuffer (txLoan tx), output))
    >>> lift sendOutput
    >>> withTx (\tx () -> modify tx (\s -> s {stSending = False}))

-- | A statement of the transaction's own, as a pipeline of its own: ROLLBACK or COMMIT, read as the pipeline's
-- statements are.
alone :: (Runtime arr) => Bool -> With Tx arr ByteString ()
alone validating =
  moved
    >>> withTx
      ( \tx text -> do
          s <- inspect tx
          output <- evaluate (statementOutput text [] [] [] <> sync)
          modify tx (const s {stPending = [Entry text True], stIndex = 0, stBase = 0, stRows = 0, stTags = [], stOwed = True, stSending = True})
          pure output
      )
    >>> transmit
    >>> drain validating

-- | Ends the transaction as the program didn't: what is owed is read, and what the server may have left open is rolled
-- back. The failures that are read are the program's to have seen.
cleanup :: (Runtime arr) => With Tx arr () ()
cleanup =
  moved
    >>> drain False
    >>> withTx (\tx _ -> (\s -> if stStatus s /= idle then Right () else Left ()) <$> inspect tx)
    >>> (returnA ||| (arr (const "ROLLBACK") >>> alone False))
    >>> withTx (\tx _ -> modify tx (\s -> s {stEnded = True}))

-- | The byte of @I@, which the server's status is when it is idle.
idle :: Int
idle = 73

-- | Ends the transaction as the program ended it. If a failure of the server was reported, and the program read it
-- and went on, the transaction is rolled back, and the program's value stands. Else what is unread is read, which
-- raises the failure of a statement that nobody read, the ending is sent unless the pipeline carried it, and it is
-- read, which raises the failure of COMMIT.
finishing :: (Runtime arr) => With Tx arr () ()
finishing =
  withTx (\tx _ -> (\s -> if stEnded s then Left () else Right (isJust (stFailure s))) <$> inspect tx)
    >>> (returnA ||| ((\handled -> if handled then Left () else Right ()) ^>> (cleanup ||| completing)))
  where
    completing =
      drain True
        >>> withTx (\tx _ -> (\s -> if stBegun s && not (stFinal s) then Right (if stCondemned s then "ROLLBACK" else "COMMIT") else Left ()) <$> inspect tx)
        >>> (returnA ||| alone True)
        >>> withTx
          ( \tx _ -> do
              s <- inspect tx
              when (stBegun s && stStatus s /= idle) (throwIO (unexpected "the transaction's ending did not leave the session idle"))
              modify tx (\t -> t {stEnded = True})
          )

-- | The transaction, with the connection and the loans of its input: it is the work of the part that runs it, for as
-- long as it takes, and it ends as the program ended: with the failure of the program, of a statement or of the ending,
-- the ending is rolled back, and the result is the program's, or the 'QueryError' of a statement or of the ending that
-- the program didn't catch. The connection stays usable, unless the cancellation rule breaks it.
transaction :: (Runtime arr) => IsolationLevel -> Mode -> With Tx arr x y -> arr (Connection, (Loans, x)) (Either QueryError y)
transaction level mode program =
  tryA (scope (begin level mode) leaving body)
    >>> now (either (\failure -> maybe (throwIO failure) (pure . Left) (fromException failure)) (pure . Right))
  where
    body = (\(tx, (_, (_, input))) -> (tx, input)) ^>> with (program >>> (returnA &&& (arr (const ()) >>> finishing)) >>^ fst)

-- | Takes the connection for the transaction, and the loan that it works in, which is the primary if the request has not
-- lent it. A connection that is used by another part, or is broken, or one that the body has given up, is a failure,
-- and the loan goes back.
begin :: (Runtime arr) => IsolationLevel -> Mode -> arr (Connection, (Loans, x)) Tx
begin level mode =
  (\(connection, (loans, _)) -> (loans, connection)) ^>> first borrow
    >>> now (\(loan, connection) -> either (\failure -> Left (loan, failure)) Right <$> try (open loan connection))
    >>> ((first release >>> now (\((), failure) -> throwIO (failure :: SomeException))) ||| returnA)
  where
    open loan connection = do
      reader <- newReader (sessionSocket (connectionSession connection)) (loanBuffer loan)
      state <- newIORef (State False False False False [] 0 0 0 [] Nothing False idle False False)
      epoch <- newIORef 0
      -- The last, since it can't be undone but by the failure of this.
      claim connection
      pure (Tx connection (beginning level mode) reader (messages reader) loan state epoch)

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
  now (\(tx, exit) -> (tx,) <$> plan tx exit)
    >>> ((\(tx, how) -> case how of Recover -> Right tx; Keep -> Left (tx, Nothing); Break why -> Left (tx, Just why)) ^>> (now (\(tx, why) -> tx <$ settle (connectionSession (txConnection tx)) why) ||| recovering))
    >>> now (\tx -> modifyIORef' (txEpoch tx) (+ 1) $> txLoan tx)
    >>> release
  where
    recovering = (\tx -> (tx, tx)) ^>> first (tryA ((, ()) ^>> with cleanup)) >>> now (\(done, tx) -> tx <$ settle (connectionSession (txConnection tx)) (either (Just . ("the transaction could not be rolled back: " <>) . displayException) (const Nothing) done))

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
condemn = withTx (\tx () -> inspect tx >>= \s -> when (stFinal s || stEnded s) (throwIO AfterFinal) *> modify tx (\t -> t {stCondemned = True}))

-- * Rows, read by cursors

-- | A cursor standing at a DataRow: the cursor of its columns, how many there are, and what a failure of a column and
-- a field need to know.
data Columns = Columns !(At ('PayloadCounted16 'MaybeBytes32Atom) Socket) !Int !(IORef State) !(IORef Int) !Int

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
data Field = Field !Int !(At 'MaybeBytes32Atom Socket) !(IORef Int) !Int

instance (Runtime arr) => Stream arr Field where
  next = now (\(Field _ cursor epoch stamp) -> readIORef epoch >>= \current -> if current == stamp then pure cursor else throwIO FieldExpired) >>> piece

-- | The next column: the cursor of its value, which has its length read, and the length, which is negative for NULL.
nextValue :: (Runtime arr) => arr Columns (At 'MaybeBytes32Atom Socket, Int, Columns)
nextValue =
  (\columns@(Columns cursor _ _ _ _) -> (cursor, columns))
    ^>> first advance
    >>> now (\(found, columns@(Columns cursor _ _ _ _)) -> if found then pure (child cursor, columns) else failedColumns columns (RowFailed (ColumnCountMismatch (columnsOf columns + 1) (columnsOf columns))))
    >>> (\(value, columns) -> (value, (value, columns)))
    ^>> first valueLength
    >>^ \(size, (value, columns)) -> (value, size, columns)
  where
    columnsOf (Columns _ total _ _ _) = total

-- | The column decoded by the type's decoder, from its bytes in the buffer, or from none if it is NULL. A column that
-- is larger than the buffer is a failure of its use, and one that doesn't decode is a failure of its statement.
decoded :: (Runtime arr) => (Maybe ByteString -> Either Text r) -> arr Columns r
decoded decode =
  nextValue
    >>> (\(value, size, columns) -> if size < 0 then Left columns else Right (value, size, columns))
    ^>> (now (\columns -> outcome columns (decode Nothing)) ||| (now fits >>> first valueView >>> now (\(bytes, columns) -> outcome columns (decode (Just bytes)))))
  where
    fits (value, size, columns@(Columns (At reader _) _ _ _ _)) = (value, columns) <$ when (size > readerCapacity reader) (throwIO (ValueTooLarge size))
    outcome columns@(Columns (At reader depth) total _ _ _) = \case
      Right decodedValue -> pure $! decodedValue
      Left reason -> do
        left <- mark reader (markFrame depth)
        failedColumns columns (RowFailed (ColumnDecodingFailed (total - left) reason))

-- | The next column, decoded from the buffer. A column larger than the buffer
-- fails with 'ValueTooLarge'.
column :: (Runtime arr, InPlace a) => arr Columns a
column = decoded decodeField

-- | The next column, which may be NULL.
nullable :: (Runtime arr, InPlace a) => arr Columns (Maybe a)
nullable = decoded (maybe (Right Nothing) (fmap Just . decodeField . Just))

-- | The last column, as a stream.
field :: (Runtime arr) => arr Columns Field
field = nextValue >>> now (\(value, size, columns) -> if size < 0 then failedColumns columns (nullField columns) else opened value size columns)
  where
    nullField (Columns _ total _ _ _) = RowFailed (ColumnDecodingFailed total "unexpected NULL")

-- | The last column, as a stream, if it isn't NULL.
nullableField :: (Runtime arr) => arr Columns (Maybe Field)
nullableField = nextValue >>> now (\(value, size, columns) -> if size < 0 then pure Nothing else Just <$> opened value size columns)

-- | The field of a value, whose turn lasts until the row moves on.
opened :: At 'MaybeBytes32Atom Socket -> Int -> Columns -> IO Field
opened value size (Columns _ _ state epoch stamp) = Field size value epoch stamp <$ modifyIORef' state (\s -> s {stField = True})

-- | How many bytes the field has, as it declares.
fieldLength :: Field -> Int
fieldLength (Field declared _ _ _) = declared

-- | Passes the next column.
skip :: (Runtime arr) => arr Columns ()
skip = nextValue >>> (\(value, _, _) -> value) ^>> skipValue

-- | The rows that a step of the cursor reads, for a result of this many rows,
-- which the read that finds the count wrong checks: it fails with
-- 'UnexpectedAmountOfRows'.
one, atMostOne, any :: arr Columns row -> Rows arr row
one = Rows ExactlyOne
atMostOne = Rows AtMostOne
any = Rows AnyNumber

-- | A result that has no rows to read.
none :: (Runtime arr) => Rows arr ()
none = Rows NoRows (arr (const ()))
