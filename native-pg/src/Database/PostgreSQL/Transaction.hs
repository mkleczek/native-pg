{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns #-}

-- | Statements, pipelines and transactions, and the cursors that read their rows.
--
-- A program is built once: its pipelines are values that name the positions of
-- their results, and the reading program that takes them is a function of the
-- positions, evaluated when the program is built. What it runs on, a connection
-- and the request's loans, are inputs of the arrow.
--
-- A transaction works in one buffer of the request's loans, its primary: what the
-- client says is rendered into it and sent, and the replies are read into it by
-- cursors. A pipeline is sent once what the one before it said is answered. A
-- pipeline that is no larger than one buffer, and streams nothing, is sent in turn
-- with the reading: all of it is sent, and then read. A larger one, or one that
-- streams, is sent while it is read, as two parts, since the server may stop reading
-- what it is sent while nobody reads its answers; the sending then renders into a
-- second buffer, which it borrows. The queue of the answers that are expected is the
-- statements of the pipeline that was sent, which the server answers in order, and it is
-- made before either part starts. The reading program takes the results from it in the
-- order of their statements. A statement declares the one stream that its
-- 'FromStream' parameters take their bytes from, when the program is built, with
-- 'streamFrom'; the stream's state is taken from the input of the program, and
-- the pipeline's sending pulls the stream while it sends the parameters: each
-- 'FromStream' takes the next bytes of the stream, as many as it declares.
--
-- A prepared statement is named by the program, which keeps the names of the
-- connection: 'prepare' sends the Parse of a name, 'executePrepared' sends only the
-- Bind and Execute of a name that the connection has, without its SQL, and
-- 'closeStatement' sends the Close. The library keeps no record of those names. A statement that the
-- program caches by a key, with 'cached', is named by the library, which keeps the connection's cache of them.
module Database.PostgreSQL.Transaction
  ( -- * Statements
    Statement (Statement, sql, params, stream, rows),
    statementFrom,
    StreamSource (..),
    streamFrom,
    Param (..),
    Value (..),

    -- * Pipelines
    Pipeline,
    Result,
    Preparation,
    statement,
    prepare,
    executePrepared,
    cached,
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
    foldRows',

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

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, second, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, SomeAsyncException, SomeException, displayException, evaluate, fromException, throwIO, toException)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Foldable (traverse_)
import Data.Hashable (Hashable)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int32)
import Data.Kind (Type)
import qualified Data.List as List
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Text (Text)
import Data.Tuple (Solo (MkSolo))
import Data.Typeable (Typeable)
import Data.Word (Word8)
import Database.PostgreSQL.Connection.Internal
import Database.PostgreSQL.Decoding (DecodeField (..), DecodingError (..), InPlace)
import Database.PostgreSQL.Errors (ConnectionError (..), QueryError (..), ServerError (..), StatementError (..), UsageError (..), diagnosticCode, isConnectionFatal)
import Database.PostgreSQL.Internal.Diagnostics (bodyDiagnostics)
import Database.PostgreSQL.Internal.Frontend (Room (..), sendOutput, sendStreamed, sync)
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
import Database.PostgreSQL.Internal.Protocol.Codec (Serializable (..))
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output, hole, int32, outputLength, outputWriter, runWriter)
import qualified Database.PostgreSQL.Internal.Protocol.Codec.Output as Output (WriteResult (..), bytes)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import qualified Database.PostgreSQL.Internal.StatementCache as Cache
import Database.PostgreSQL.Internal.Carrier (Carrier, share)
import qualified Database.PostgreSQL.Internal.Protocol.Messages as Wire
import Database.PostgreSQL.Types (FormatCode (..), Oid, Parameter (..))
import Foreign.Storable (peekByteOff)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Streaming.Buffer (Buffer (bufferCapacity))
import Network.Streaming.Classes (Effect (..), Exit (..), Runtime, Scope (..), Stream (..))
import Network.Streaming.Combinators (pattern Again, pattern Finish, _1, _2, alongside, boolA, concurrently, consume, done, iterateA, iterateWith, keeping, on, tap, through, undoing)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, loanIsPrimary, loansOf, release, withSetupBuffersOf)
import Network.Streaming.Reader (Reader, markFrame, markLimit, markStart, mark, newReader, readerCapacity, readerMemory)
import Network.Streaming.With (With (..), ask, asks, lift, with, withOnly)
import Prelude hiding (any)

-- | A statement: its SQL, the parameters that it takes from the input of the
-- program, the stream that its 'FromStream' parameters read, and how its rows are
-- read. The SQL is evaluated only when it is sent, and a prepared statement the
-- connection already has sends only its parameters.
data Statement arr p row
  = Statement
      { sql :: ByteString,
        params :: p -> [Param],
        stream :: Maybe (StreamSource arr p),
        rows :: Rows arr row
      }
  | -- | A statement whose SQL and parameters are made from the input when it is sent, as 'statementFrom' makes it. Its own
    -- @sql@ and @params@ are empty.
    Made
      { sql :: ByteString,
        params :: p -> [Param],
        stream :: Maybe (StreamSource arr p),
        rows :: Rows arr row,
        made :: p -> (ByteString, [Param])
      }

-- | A statement whose SQL and parameters are made from the input of the program when it is sent: the one place where the
-- text of a statement is a run-time value, for a program that is built once and sends a different statement each time.
-- The rows are read as the statement says, whatever its SQL is, and the statement takes 'streamFrom', 'prepare' and
-- 'executePrepared', and 'from' like any other. The SQL is evaluated when it is sent, and not by 'executePrepared'.
statementFrom :: (p -> (ByteString, [Param])) -> Rows arr row -> Statement arr p row
statementFrom make reading = Made {sql = BS.empty, params = const [], stream = Nothing, rows = reading, made = make}

-- | The stream of a statement, as its type is fixed when the statement is built, and so is its 'next': only the state
-- of the stream is taken from the input of the program, when it runs.
data StreamSource arr p = forall s. (Stream arr s) => StreamSource (p -> s)

-- | The statement, with the stream that its 'FromStream' parameters take their bytes from: each takes the next bytes
-- of the stream, as many as it declares.
streamFrom :: (Stream arr s) => (p -> s) -> Statement arr p row -> Statement arr p row
streamFrom source given = given {stream = Just (StreamSource source)}

-- | A parameter: its type, which the server infers if there is none, its format,
-- and its value.
data Param = Param
  { paramType :: Maybe Oid,
    paramFormat :: FormatCode,
    paramValue :: Value
  }

data Value
  = Null
  | Bytes ByteString
  | -- | Sent one after another, without a copy.
    Pieces [ByteString]
  | -- | This many bytes, which are the next of the statement's stream.
    FromStream Int

-- | What is sent, in order. A pipeline's value holds the positions of its
-- results in the queue of answers that are expected, relative to the start of the
-- pipeline, so putting pipelines together only shifts them.
data Pipeline arr p a = Pipeline Int [Sent arr p] (Int -> a)

-- | What a pipeline sends.
data Sent arr p
  = forall row. Execute (Statement arr p row)
  | forall row. Prepare ByteString (Statement arr p row)
  | forall row. ExecutePrepared ByteString (Statement arr p row)
  | forall k row. (Typeable k, Hashable k) => Cached (p -> k) (p -> ByteString) (Statement arr p row)
  | -- | What a cached statement is for a run, once 'layoutCached' has looked it up: its entry and its messages.
    Rendered Entry Output
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

-- | Parses the statement under the name, with the types of the parameters that the input of the pipeline gives it, which
-- the server infers for a parameter that has none.
prepare :: ByteString -> Statement arr p row -> Pipeline arr p Preparation
prepare name given = Pipeline 1 [Prepare name given] Preparation

-- | Binds and executes the statement that the connection has under the name.
executePrepared :: ByteString -> Statement arr p row -> Pipeline arr p (Result arr row)
executePrepared name given = Pipeline 1 [ExecutePrepared name given] (\base -> Result base (readingOf (rows given)))

-- | The statement, prepared on the connection under a name that the connection chooses and keeps for the key that the input gives,
-- so that a run whose key the connection has executes the prepared statement, and does not evaluate the SQL. The SQL is a function
-- of the input of its own, evaluated only when the connection has no statement for the key, which is prepared before it is
-- executed, in the same pipeline: the pipeline, and the position of its result, are the same whether the connection has it
-- or not. The parameters, and the stream, are the statement's, made for the input on every run; its own SQL is not used. The
-- connection keeps as many statements as 'connectStatementCache' says, and forgets the least recently used.
cached :: (Typeable k, Hashable k) => (p -> k) -> (p -> ByteString) -> Statement arr p row -> Pipeline arr p (Result arr row)
cached key makeSql given = Pipeline 1 [Cached key makeSql given] (\base -> Result base (readingOf (rows given)))

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
      Execute given -> Execute (taken given)
      Prepare name given -> Prepare name (taken given)
      ExecutePrepared name given -> ExecutePrepared name (taken given)
      Cached key makeSql given -> Cached (key . function) (makeSql . function) (taken given)
      Rendered entry output -> Rendered entry output
      Close name -> Close name
      Command given -> Command given
    taken = \case
      Statement {sql, params, stream, rows} -> Statement {sql, params = params . function, stream = (\(StreamSource source) -> StreamSource (source . function)) <$> stream, rows}
      Made {sql, params, stream, rows, made} -> Made {sql, params = params . function, stream = (\(StreamSource source) -> StreamSource (source . function)) <$> stream, rows, made = made . function}

data IsolationLevel = ReadCommitted | RepeatableRead | Serializable
  deriving (Eq, Ord, Show)

data Mode = Read | Write
  deriving (Eq, Ord, Show)

-- | Whether a pipeline is the last one: nothing can be sent after a 'Final' one,
-- which carries COMMIT, or ROLLBACK once the transaction is condemned.
data Ending = Open | Final
  deriving (Eq, Ord, Show)

-- * The transaction at run time

-- | A statement that the server answers: its SQL, for the errors that name it, and what it is.
data Entry = Entry {entrySql :: ByteString, entryRole :: !Role}

-- | What an entry of the queue is: whether a result of the program stands for it, and what the server answers it with.
data Role
  = -- | A statement of the program that gives rows, which a CommandComplete or an EmptyQueryResponse answers: a result
    -- stands for it, which has to be read or discarded before a later one is read.
    Wanted
  | -- | A statement that nobody has to read: one of the transaction's own, as BEGIN and the ending are, or one whose
    -- result the program is done with. A later read passes it.
    Passed
  | -- | A Parse of a name, which a ParseComplete answers: a preparation, which a later read passes.
    Parsing
  | -- | A Close of a name, which a CloseComplete answers, and which a later read passes.
    Closing
  | -- | A statement of the program that gives rows, like 'Wanted', which the connection keeps prepared for a key: the messages
    -- that precede its own, a CloseComplete of a name that goes and a ParseComplete of the statement, are told to the cache,
    -- and so is its failing before the ParseComplete.
    Remembered !Cache.Memory

-- | Whether a later read may pass the entry, as it isn't a result that the program has yet to read.
entrySkippable :: Entry -> Bool
{-# NOINLINE entrySkippable #-}
entrySkippable entry = case entryRole entry of
  Wanted -> False
  Remembered _ -> False
  _ -> True

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
    txEpoch :: !(IORef Int),
    -- | Whether a send is under way, which leaves something half sent if it is cancelled. The part that sends changes it and the
    -- part that reads doesn't, since the two may run at once.
    txSending :: !(IORef Bool)
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
     FramedMessage 'A' 'RemainingBytesAtom,
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
  | -- | An ErrorResponse or a NotificationResponse, as its tag says: its body, copied.
    Rare !Word8 !ByteString
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
                :> rare
                :> arr (const EmptyQuery)
                :> other
                :> other
                :> rare
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
    -- Both bodies are copied whole, and the tag, which the copy takes the cursor past, is looked at first. The one arrow
    -- serves both messages, so that the reply cursor has no arm more than it had.
    rare = Rare <$> now (\(At reader _) -> lastTag reader) <*> remaining

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
  Other -> acknowledged tx
  Row cursor -> Rowed cursor <$ modify tx (\s -> s {stRows = stRows s + 1})
  Complete tag -> answered tx (Just tag)
  EmptyQuery -> answered tx Nothing
  Rare tag body -> if tag == notificationResponseTag then notified tx body else rejected tx body
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

-- | A message that no statement's answer is made of: it is the ParseComplete or the CloseComplete that the entry which is
-- being read waits for, as its tag tells, or else one that nothing waits for. A ParseComplete of an unnamed statement's
-- own Parse is one of these, as the entry being read is that statement.
acknowledged :: Tx -> IO Step
acknowledged tx = do
  s <- inspect tx
  case stPending s of
    entry : _ -> case entryRole entry of
      Parsing -> awaiting tx parseCompleteTag
      Closing -> awaiting tx closeCompleteTag
      Remembered memory -> remembering tx memory
      _ -> pure Stepped
    [] -> pure Stepped

-- | A message that no statement's answer is made of, while a statement that the connection keeps prepared is being read: a
-- ParseComplete or a CloseComplete, as its tag says, is one of the cache's, which it is told, and any other is ignored, as it is for
-- a statement that is not.
remembering :: Tx -> Cache.Memory -> IO Step
remembering tx memory = lastTag (txReader tx) >>= \tag -> Stepped <$ (if tag == parseCompleteTag then Cache.memoryParsed memory else when (tag == closeCompleteTag) (Cache.memoryClosed memory))

-- | The entry that is being read is answered if the message that was framed last has the tag that answers it.
awaiting :: Tx -> Word8 -> IO Step
awaiting tx tag = lastTag (txReader tx) >>= \found -> if found == tag then answered tx Nothing else pure Stepped

-- | The tags of ParseComplete, CloseComplete and NotificationResponse, as numbers.
parseCompleteTag, closeCompleteTag, notificationResponseTag :: Word8
parseCompleteTag = 0x31
closeCompleteTag = 0x33
notificationResponseTag = 0x41

-- | A notification arrived among the replies: its body is kept, for the next wait of the connection for a notification,
-- and nothing waits for it here.
notified :: Tx -> ByteString -> IO Step
notified tx body = Stepped <$ keepNotification (connectionSession (txConnection tx)) body

-- | The tag of the message that was framed last, which is five bytes before the bytes in hand when nothing of its body
-- is taken, as is so for the messages that no cursor reads.
lastTag :: Reader Carrier -> IO Word8
lastTag reader = do
  start <- mark reader markStart
  unsafeWithForeignPtr (readerMemory reader) (\memory -> peekByteOff memory (start - 5))

-- | What the failure of a statement means to the statements that the connection keeps prepared: one that the server doesn't have, or
-- whose result type it found changed, so that the connection forgets them all.
staleStatements :: ServerError -> Bool
staleStatements (ServerError diagnostics) = diagnosticCode diagnostics `elem` ["26000", "0A000"]

-- | What a statement that was not answered tells the cache that it keeps: that the pipeline failed.
undone :: Entry -> IO ()
undone entry = case entryRole entry of
  Remembered memory -> Cache.memoryFailed memory
  _ -> pure ()

-- | The server failed the statement that was being read, and skipped the ones after it. The first failure is the
-- transaction's, which every read that follows raises, and the connection is lost if the server ended the session.
rejected :: Tx -> ByteString -> IO Step
rejected tx body = do
  s <- inspect tx
  failure <- either (throwIO . unexpected) (pure . ServerError) (bodyDiagnostics body)
  when (isConnectionFatal failure) (throwIO (ConnectionTerminated failure))
  -- The statements that the server did not parse, since it failed one before them or this, are not the connection's.
  traverse_ undone (stPending s)
  when (staleStatements failure) (Cache.forget (sessionStatements (connectionSession (txConnection tx))))
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
-- An entry that was answered already is found without the failure of the server being raised if the first argument says
-- that it is as it was, as a preparation is.
seek :: (Runtime arr) => Bool -> Int -> With Tx arr () Bool
seek answeredIsFine target = loop
  where
    loop = inTx locate >>> byLocation ^>> (returnA ||| ((finishEntry True >>> loop) ||| misordered))
    byLocation Finished = Left False
    byLocation Current = Left True
    byLocation Skippable = Right (Left ())
    byLocation (Misordered unread) = Right (Right unread)
    locate tx = do
      s <- inspect tx
      let wanted = stBase s + target
      unless (answeredIsFine && wanted < stIndex s) (traverse_ throwIO (stFailure s))
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
  moved >>> seek False position >>> boolA noRow loop
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
discard (Result position _) = moved >>> seek False position >>> boolA returnA (finishEntry True)

-- | Awaits the server's acknowledgement of a preparation, as a result is read: it fails with the 'QueryError' of a Parse
-- that the server rejected, or skipped after a statement that failed, and with 'ResultOutOfOrder' while an earlier
-- result is neither read nor discarded. A preparation that was acknowledged stays so, however a statement after it
-- ended.
prepared :: (Runtime arr) => Preparation -> With Tx arr () ()
prepared (Preparation position) = moved >>> seek True position >>> boolA returnA (finishEntry True)

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
concluded position = withTx (\tx input -> input <$ modify tx (\s -> if stBase s + position == stIndex s then s {stPending = passed (stPending s)} else s))
  where
    passed = \case entry : others -> entry {entryRole = Passed} : others; [] -> []

-- | Folds the rows of the result, one at a time, from the first to the last.
foldRows :: (Runtime arr) => (s -> row -> s) -> s -> Result arr row -> With Tx arr () s
foldRows step start result = const start ^>> iterateA (keeping (nextRow result) >>^ folded)
  where
    folded (next', accumulated) = maybe (Finish accumulated) (Again . step accumulated) next'

-- | Folds the rows from first to last, forcing the new accumulator to weak
-- head normal form before reading another row.
foldRows' :: (Runtime arr) => (s -> row -> s) -> s -> Result arr row -> With Tx arr () s
foldRows' step start result = const start ^>> iterateA (keeping (nextRow result) >>^ folded)
  where
    folded (Nothing, accumulated) = Finish accumulated
    folded (Just row, accumulated) = Again $! step accumulated row
{-# INLINE foldRows' #-}

-- * Sending

-- | Sends the pipeline, with its parameters from the input, and gives its
-- positions to the reading program that follows. A program sends any number of
-- pipelines in order: what the one before was answered with, and not read, is
-- dropped, as is the field of a row.
--
-- What the server is sent is the statements, and a Sync, in as many sends as the buffer needs, once what the pipeline before it
-- was answered with is read. BEGIN comes with the first, and the ending with the last. A pipeline that fits one buffer and
-- streams nothing is sent in turn with the reading program: it is sent in the buffer of the transaction, and is read after it.
-- A larger one, or one whose statements declare a stream, is sent while it is read, in two parts of a group: the request side,
-- which renders into a buffer that it borrows and pulls the streams, and the reading program. The failure of either part
-- cancels the other where it waits. A request that has no buffer to lend, since a stream holds its primary, sends in turn from
-- the buffer of the transaction the pipeline whose one result is the statement that streams, and sends any other pipeline that
-- is sent while it is read from a setup buffer of the pool, which the request side takes for as long as it sends.
pipeline :: (Runtime arr) => Ending -> Pipeline arr p a -> (a -> With Tx arr x y) -> With Tx arr (p, x) y
pipeline finality (Pipeline _ sent value) reading =
  tap (done >>> moved >>> readyToSend)
    >>> withTx (layouter finality streams numbered)
    >>> ((program ||| apart) ||| inTurn)
  where
    streams = List.any (isJust . streamOf) sent
    -- A pipeline that has a cached statement looks the connection's cache up for it, as it is laid out.
    layouter = if List.any isCached sent then layoutCached else layout
    isCached = \case Cached {} -> True; _ -> False
    -- Whether the one statement of the pipeline that gives a result is the one that streams.
    streamsAlone = case filter gives sent of [item] -> isJust (streamOf item); _ -> False
    gives = \case Prepare _ _ -> False; Close _ -> False; _ -> True
    numbered = numberStreams sent
    program = reading (value 0)
    inTurn = consume transmit >>> program
    -- At once with the reading, from a buffer that the request lends, or else, when it has none to lend, from a setup buffer of the
    -- pool. Except that the pipeline whose one result is the statement that streams is sent in turn from the transaction's own
    -- buffer, if the request has none to lend, which costs no second buffer.
    apart = withTx (\tx sending -> pure (if loanIsPrimary (txLoan tx) || not streamsAlone then Left sending else Right sending)) >>> (atOnce ||| onBuffer)
    -- What the reader holds and what the connection kept are put together before the parts start, as a send in turn does it
    -- around its rendering, and nothing is rendered into the buffer of the reader after.
    atOnce =
      tap (inTx (\tx -> setAside tx >> bringBack tx))
        >>> sharing (regrouped ^>> concurrently (fst ^>> request) (snd ^>> program) >>^ snd)
    regrouped (output, input, rest) = ((input, output), rest)
    request =
      withTx (\tx (input, output) -> pure (roomFor tx input output))
        >>> (lift pulling ||| lift (withSetupBuffersOf lent))
        >>> inTx (\tx -> writeIORef (txSending tx) False)
    pulling = sendStreamed (selecting (concatMap puller sent))
    -- A request that can lend a buffer has the sender borrow it. One that can't, since a stream holds its primary and the transaction
    -- has the one loan that is left, has it take a setup buffer of the pool, which no request's loan comes from: it is the last
    -- that the request waits for, and whoever has one waits for nothing that a request holds, so the wait is over when it ends.
    roomFor tx input output
      | loanIsPrimary (txLoan tx) = Left (carrierOf tx, Borrowed (loansOf (txLoan tx)), input, output)
      | otherwise = Right (loansOf (txLoan tx), (carrierOf tx, input, output))
    lent = (\(MkSolo buffer, (_, (carrier, input, output))) -> (carrier, Given buffer, input, output)) ^>> pulling
    -- Sent in turn with the reading, as a send in turn is, from the buffer of the transaction: the request had no other to lend.
    onBuffer =
      regrouped
        ^>> consume
          ( withTx (\tx (input, output) -> setAside tx >> pure (carrierOf tx, Held (txLoan tx), input, output))
              >>> lift (sendStreamed (selecting (concatMap puller sent)))
              >>> inTx (\tx -> bringBack tx >> writeIORef (txSending tx) False)
          )
        >>> program

-- | The connection's carrier of the transaction.
carrierOf :: Tx -> Carrier
carrierOf = sessionCarrier . connectionSession . txConnection

-- | The step, with the calls on the connection's TLS session taking their lock while it runs, as its two parts make them at once.
sharing :: (Runtime arr) => With Tx arr a b -> With Tx arr a b
sharing body = scope (inTx (\tx -> share (carrierOf tx) True)) (inTx (\tx -> share (carrierOf tx) False)) (snd ^>> body)

-- | The step that pulls the stream that the statement declares, if it declares one, with the input that its state is taken from.
puller :: (Arrow arr) => Sent arr p -> [arr p (Maybe ByteString)]
puller item = case streamOf item of
  Just (StreamSource source) -> [source ^>> next]
  Nothing -> []

-- | The stream that the statement declares.
streamOf :: Sent arr p -> Maybe (StreamSource arr p)
streamOf = \case
  Execute given -> stream given
  ExecutePrepared _ given -> stream given
  Cached _ _ given -> stream given
  _ -> Nothing

-- | The statements, each with the number of its stream, among the streams of the pipeline's statements in order, if it has one.
numberStreams :: [Sent arr p] -> [(Maybe Int, Sent arr p)]
numberStreams = go 0
  where
    go _ [] = []
    go number (item : others) = case streamOf item of
      Nothing -> (Nothing, item) : go number others
      Just _ -> (Just number, item) : go (number + 1) others

-- | The next piece of the stream with the number, whose state is taken from the input.
selecting :: (ArrowChoice arr) => [arr p (Maybe ByteString)] -> arr (Int, p) (Maybe ByteString)
selecting [] = arr (const Nothing)
selecting (this : others) = (\(number, input) -> if number == 0 then Left input else Right (number - 1, input)) ^>> (this ||| selecting others)

-- | The pipeline before this was answered: what is owed is read, and a failure of the server found is raised.
readyToSend :: (Runtime arr) => With Tx arr () ()
readyToSend = inTx check >>> drain True
  where
    check tx = do
      s <- inspect tx
      traverse_ throwIO (stFailure s)
      when (stFinal s || stEnded s) (throwIO AfterFinal)

-- | The output of a pipeline of the program, and how it is sent: not at all if there is nothing to send, in turn with the
-- reading if it is no larger than one buffer and no statement of it streams, and else at once with it, or apart. What the server is
-- to answer is put in the queue of the transaction, once all of it is rendered.
layout :: Ending -> Bool -> [(Maybe Int, Sent arr p)] -> Tx -> (p, x) -> IO (Either (Either x (Output, p, x)) (Output, x))
layout finality streams sent tx (input, rest) = do
  s <- inspect tx
  user <- traverse (rendered input) sent
  let starting = [(Entry (txBegin tx) Passed, txBeginOutput tx) | not (stBegun s), not (null sent) || finality == Final]
      closing = [(Entry text Passed, output) | finality == Final, let (text, output) = endingStatement (stCondemned s)]
  case starting <> user <> closing of
    [] -> pure (Left (Left rest))
    queued -> do
      output <- evaluate (foldMap snd queued <> sync)
      modify tx (const s {stBegun = True, stFinal = stFinal s || finality == Final, stPending = map fst queued, stIndex = 0, stBase = length starting, stRows = 0, stTags = [], stOwed = True})
      writeIORef (txSending tx) True
      pure $
        if streams || outputLength output > bufferCapacity (loanBuffer (txLoan tx))
          then Left (Right (output, input, rest))
          else Right (output, rest)
  where
    plain text = statementOutput text [] [] []
    rendered parameters (source, item) = case item of
      Execute (resolved parameters -> given) -> case source of
        Nothing -> do
          (types, formats, values) <- parametersOf (params given parameters)
          pure (Entry (sql given) Wanted, statementOutput (sql given) types formats values)
        Just number -> do
          (types, formats, values) <- argumentsOf number (params given parameters)
          pure (Entry (sql given) Wanted, streamedStatementOutput (sql given) types formats values)
      Command text -> pure (Entry text Wanted, plain text)
      Prepare name (resolved parameters -> given) -> pure (Entry (sql given) Parsing, parseOutput name (sql given) (map paramType (params given parameters)))
      ExecutePrepared name (resolved parameters -> given) -> case source of
        Nothing -> do
          (_, formats, values) <- parametersOf (params given parameters)
          pure (Entry (sql given) Wanted, boundOutput name formats values)
        Just number -> do
          (_, formats, values) <- argumentsOf number (params given parameters)
          -- The server starts statement_timeout at a Describe, and reads a Bind whole before it runs it, so it is described first
          -- for the time that the stream takes to count.
          pure (Entry (sql given) Wanted, Wire.serialize (Wire.Describe Wire.PreparedStatement name) <> boundStreamed name formats values)
      Rendered entry output -> pure (entry, output)
      Cached {} -> throwIO (userError "a cached statement is laid out by layoutCached")
      Close name -> pure (Entry ("DEALLOCATE " <> name) Closing, Wire.serialize (Wire.Close Wire.PreparedStatement name))

-- | The output of a pipeline that has a statement that the connection keeps prepared for a key, and how it is sent, as 'layout' says:
-- the connection's cache is looked up for each such statement first, which becomes what it sends in this run, and the pipeline is laid
-- out as one that has no cached statement. A pipeline that has none is laid out by 'layout' alone.
layoutCached :: Ending -> Bool -> [(Maybe Int, Sent arr p)] -> Tx -> (p, x) -> IO (Either (Either x (Output, p, x)) (Output, x))
layoutCached finality streams sent tx (input, rest) = do
  resolvedSent <- traverse (resolving tx input) sent
  layout finality streams resolvedSent tx (input, rest)

-- | What the statement is for a run: a cached statement is what the cache makes of it, and any other is as it is.
resolving :: Tx -> p -> (Maybe Int, Sent arr p) -> IO (Maybe Int, Sent arr p)
resolving tx parameters (source, item) = case item of
  Cached key makeSql given -> (source,) . uncurry Rendered <$> case source of
    Nothing -> do
      (types, formats, values) <- parametersOf (params given parameters)
      cachedStatement tx (key parameters) makeSql parameters types False (\name -> boundOutput name formats values) (\text -> statementOutput text types formats values)
    Just number -> do
      (types, formats, values) <- argumentsOf number (params given parameters)
      cachedStatement tx (key parameters) makeSql parameters types True (\name -> boundStreamed name formats values) (\text -> streamedStatementOutput text types formats values)
  _ -> pure (source, item)

-- | A statement that the connection keeps prepared for the key: the entry that stands for it, and its messages. A statement
-- that the cache has is executed under its name: a Close of a name that the connection is to close goes before it, and for a
-- statement that streams a Describe, which starts the server's statement timeout as a Parse does. One that it has not is
-- prepared first, with the SQL that the function makes, which only a miss evaluates, and a name that goes with it, if the cache was
-- full, is closed before. Where the cache keeps nothing the statement is an unnamed one. The positions are those of one statement
-- in every case, since the answers of the messages before its own are ignored, but for the cache's being told of them.
cachedStatement :: (Typeable k, Hashable k) => Tx -> k -> (p -> ByteString) -> p -> [Maybe Oid] -> Bool -> (ByteString -> Output) -> (ByteString -> Output) -> IO (Entry, Output)
cachedStatement tx key makeSql parameters types streams bound unnamed = do
  let statements = sessionStatements (connectionSession (txConnection tx))
      closing reserved = maybe mempty (Wire.serialize . Wire.Close Wire.PreparedStatement) (Cache.reservedClose reserved)
      remembered text reserved = Entry text (maybe Wanted Remembered (Cache.reservedMemory reserved))
  Cache.find statements key >>= \case
    Cache.Disabled -> (\text -> (Entry text Wanted, unnamed text)) <$> evaluate (makeSql parameters)
    Cache.Has reserved ->
      let name = Cache.reservedName reserved
          describing = if streams then Wire.serialize (Wire.Describe Wire.PreparedStatement name) else mempty
       in pure (remembered ("EXECUTE " <> name) reserved, closing reserved <> describing <> bound name)
    Cache.Lacks -> do
      text <- evaluate (makeSql parameters)
      reserved <- Cache.insert statements key
      let name = Cache.reservedName reserved
      pure (remembered text reserved, closing reserved <> parseOutput name text types <> bound name)

-- | The statement as it is for the input: one that has its SQL is as it is, and one that is made from the input is the
-- statement that it makes. The pair is made once, and its SQL is left as it is until a message needs it.
resolved :: p -> Statement arr p row -> Statement arr p row
resolved parameters = \case
  Made {stream, rows, made} -> case made parameters of (text, given) -> Statement {sql = text, params = const given, stream, rows}
  given -> given

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
    <> boundOutput "" formats values

-- | The messages for an unnamed statement that has a stream: Parse, Bind with arguments, and Execute.
streamedStatementOutput :: ByteString -> [Maybe Oid] -> [FormatCode] -> [Maybe Argument] -> Output
streamedStatementOutput text types formats values =
  Wire.serialize (Wire.Parse "" text (map (fmap Wire.Scalar) types))
    <> boundStreamed "" formats values

-- | Bind of the statement that has the name, with arguments of which some are holes, and Execute.
boundStreamed :: ByteString -> [FormatCode] -> [Maybe Argument] -> Output
boundStreamed name formats values =
  Wire.serialize (Wire.Bind (Wire.BindBody "" name formats values [BinaryFormat]))
    <> Wire.serialize (Wire.Execute "" 0 :: Wire.Execute ByteString)

-- | The Parse of the statement under the name, with the types of its parameters: the server infers the ones that have none.
parseOutput :: ByteString -> ByteString -> [Maybe Oid] -> Output
parseOutput name text types = Wire.serialize (Wire.Parse name text (map (fmap Wire.Scalar) types))

-- | Bind of the statement that has the name, with the parameters, which says that the rows come in binary, and Execute.
boundOutput :: ByteString -> [FormatCode] -> [Maybe Parameter] -> Output
boundOutput name formats values =
  Wire.serialize (Wire.Bind (Wire.BindBody "" name formats values [BinaryFormat]))
    <> Wire.serialize (Wire.Execute "" 0 :: Wire.Execute ByteString)

-- | The types, the formats and the values of the parameters as the protocol has them, for a statement that has no stream: a
-- parameter that takes from one fails with 'StreamLengthError', since it is given no bytes.
parametersOf :: [Param] -> IO ([Maybe Oid], [FormatCode], [Maybe Parameter])
parametersOf given = (map paramType given,map paramFormat given,) <$> traverse (valueOf . paramValue) given
  where
    valueOf = \case
      Null -> pure Nothing
      Bytes bytes -> pure (Just (ParameterBytes bytes))
      Pieces pieces -> pure (Just (ParameterChunks pieces))
      FromStream size -> throwIO (StreamLengthError size 0)

-- | A parameter's value, as the Bind that carries it of a statement that has a stream renders it.
data Argument
  = Whole Parameter
  | -- | This many bytes of the stream of the number, which are a hole of the output for the sender to fill, and whether
    -- the parameter is the last of its statement that takes from the stream, which the sender then checks to be at its end,
    -- before it renders what follows. The hole's tag is the number twice, and one more if it is the last.
    Pulled !Int !Int !Bool

instance Serializable (Maybe Argument) 'MaybeBytes32Atom where
  serializeField = \case
    Nothing -> serializeField @(Maybe Parameter) @'MaybeBytes32Atom Nothing
    Just (Whole value) -> serializeField @(Maybe Parameter) @'MaybeBytes32Atom (Just value)
    Just (Pulled number size final) -> int32 (fromIntegral size) <> hole (2 * number + fromEnum final) size
  {-# INLINE serializeField #-}

-- | The types, the formats and the values of the parameters of a statement whose stream has the number. A length that no
-- Bind can carry fails with 'StreamLengthError', as the length of a stream that has none does.
argumentsOf :: Int -> [Param] -> IO ([Maybe Oid], [FormatCode], [Maybe Argument])
argumentsOf number given = (map paramType given,map paramFormat given,) <$> sequence (zipWith valueOf (map paramValue given) (drop 1 (scanr ((||) . pulls) False (map paramValue given))))
  where
    pulls = \case FromStream _ -> True; _ -> False
    valueOf value more = case value of
      Null -> pure Nothing
      Bytes bytes -> pure (Just (Whole (ParameterBytes bytes)))
      Pieces pieces -> pure (Just (Whole (ParameterChunks pieces)))
      FromStream size
        | size < 0 || size > fromIntegral (maxBound :: Int32) -> throwIO (StreamLengthError size 0)
        | otherwise -> pure (Just (Pulled number size (not more)))

-- | Sends what was laid out, in the loan of the transaction, whose buffer the replies are read into when it is sent.
transmit :: (Runtime arr) => With Tx arr Output ()
transmit =
  withTx (\tx output -> setAside tx >> pure (sessionCarrier (connectionSession (txConnection tx)), loanBuffer (txLoan tx), output))
    >>> lift sendOutput
    >>> inTx (\tx -> bringBack tx >> writeIORef (txSending tx) False)

-- | The buffer is rendered into, so what the reader holds of what the server sent after its last message goes to the
-- connection first: the notification that came after the last reply of a pipeline is not lost to the next pipeline.
setAside :: Tx -> IO ()
setAside tx = do
  held <- pastLastMessage (txReader tx)
  unless (BS.null held) (leaveCarry (connectionSession (txConnection tx)) held)

-- | The reader starts with what the connection kept: what was set aside, and what the use before left, which the first
-- send of a transaction finds, since the reader of a new transaction has nothing in hand.
bringBack :: Tx -> IO ()
bringBack tx = do
  kept <- takeCarry (connectionSession (txConnection tx))
  unless (BS.null kept) (seedReader (txReader tx) kept)

-- | A statement of the transaction's own, as a pipeline of its own: ROLLBACK or COMMIT, read as the pipeline's
-- statements are.
alone :: (Runtime arr) => Bool -> With Tx arr (ByteString, Output) ()
alone validating = moved >>> withTx queued >>> transmit >>> drain validating
  where
    queued tx (text, carrier) = do
      s <- inspect tx
      let !output = carrier <> sync
      modify tx (const s {stPending = [Entry text Passed], stIndex = 0, stBase = 0, stRows = 0, stTags = [], stOwed = True})
      writeIORef (txSending tx) True
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
  through (_2 . _1) borrow
    >>> undoing (now opening) (on (_2 . _1) release)
  where
    -- BEGIN is rendered once for the program, when it is built, and not for each transaction.
    !beginOutput = prerendered (statementOutput (beginning level mode) [] [] [])
    opening (connection, (loan, _)) = open loan connection
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
              stField = False
            }
      epoch <- newIORef 0
      sending <- newIORef False
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
            txEpoch = epoch,
            txSending = sending
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
    settled (tx, why) = do
      when (isNothing why) (keepWhatCame tx)
      settle (connectionSession (txConnection tx)) why
      pure tx
    -- A connection that goes on keeps what the server sent after the last message that the transaction read.
    keepWhatCame tx = do
      came <- pastLastMessage (txReader tx)
      unless (BS.null came) (leaveCarry (connectionSession (txConnection tx)) came)
    recovering = alongside (tryA ((, ()) ^>> with cleanup)) >>> now (uncurry recovered)
    recovered rolledBack tx = settled (tx, either (Just . ("the transaction could not be rolled back: " <>) . displayException) (const Nothing) rolledBack)

-- | What is to be done with the connection: nothing, or reading and rolling back, or breaking it for the reason.
data Plan = Keep | Recover | Break String

-- | The way that the transaction ended says what the connection is owed.
plan :: Tx -> Exit -> IO Plan
plan tx exit = do
  s <- inspect tx
  sending <- readIORef (txSending tx)
  unread <- fieldUnread tx s
  let owed = stOwed s || sending
      open = stBegun s && not (stEnded s)
  pure $ case exit of
    Done
      | owed -> Break "the transaction ended with replies unread"
      | open -> Recover
      | otherwise -> Keep
    Failed failure
      | not (recoverable failure) -> Break (displayException failure)
      | sending -> Break "the transaction failed while it sent"
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
