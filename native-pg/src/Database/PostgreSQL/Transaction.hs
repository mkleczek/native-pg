{-# LANGUAGE ExistentialQuantification #-}

-- | Statements, pipelines and transactions, and the cursors that read their rows.
--
-- A program is built once: its pipelines are values that name the positions of
-- their results, and the reading program that takes them is a function of the
-- positions, evaluated when the program is built. What it runs on, a connection
-- and the request's loans, are inputs of the arrow. The steps that run a
-- transaction and read are stubs, which fail with
-- 'Network.Streaming.NotImplemented.NotImplemented' until the code is there.
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

import Control.Arrow (first, returnA, (>>>), (^>>), (|||))
import Control.Exception (throwIO)
import Data.ByteString (ByteString)
import Data.Kind (Type)
import Database.PostgreSQL.Connection (Connection)
import Database.PostgreSQL.Decoding (InPlace)
import Database.PostgreSQL.Errors (QueryError, UsageError (..))
import Database.PostgreSQL.Types (FormatCode, Oid)
import Network.Streaming.Classes (Effect (..), Runtime, Stream (..))
import Network.Streaming.Loans (Loans)
import Network.Streaming.NotImplemented (notImplemented)
import Network.Streaming.With (With)
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
  fmap function (Pipeline count sent value) = Pipeline count sent (function . value)

instance Applicative (Pipeline arr p) where
  pure result = Pipeline 0 [] (const result)
  Pipeline count sent value <*> Pipeline count' sent' value' =
    Pipeline (count + count') (sent <> sent') (\base -> value base (value' (base + count)))

-- | The position of the answers to one statement.
newtype Result (row :: Type) = Result Int

-- | The position of a preparation's acknowledgement.
newtype Preparation = Preparation Int

-- | A statement with its parameters, which the server parses, binds and executes.
statement :: Statement arr p row -> Pipeline arr p (Result row)
statement given = Pipeline 1 [Execute given] Result

-- | Parses the statement under the name. Parse sends no parameters, so what the
-- pipeline takes as its input doesn't matter.
prepare :: ByteString -> Statement arr p row -> Pipeline arr q Preparation
prepare name given = Pipeline 1 [Prepare name (given {params = const []})] Preparation

-- | Binds and executes the statement that the connection has under the name.
executePrepared :: ByteString -> Statement arr p row -> Pipeline arr p (Result row)
executePrepared name given = Pipeline 1 [ExecutePrepared name given] Result

-- | Closes the statement that the connection has under the name.
closeStatement :: ByteString -> Pipeline arr p (Result ())
closeStatement name = Pipeline 1 [Close name] Result

-- | A command with no parameters and no rows, in the simple protocol's way.
command :: ByteString -> Pipeline arr p (Result ())
command given = Pipeline 1 [Command given] Result

-- | The pipeline, with its parameters taken from another input: where a
-- pipeline's parameters come from.
from :: (q -> p) -> Pipeline arr p a -> Pipeline arr q a
from function (Pipeline count sent value) = Pipeline count (map sentFrom sent) value
  where
    sentFrom sending = case sending of
      Execute given -> Execute (given {params = params given . function})
      Prepare name given -> Prepare name (given {params = params given . function})
      ExecutePrepared name given -> ExecutePrepared name (given {params = params given . function})
      Close name -> Close name
      Command given -> Command given

-- | The transaction at run time: its connection, its loans and its queue of
-- answers that are expected.
data Tx

data IsolationLevel = ReadCommitted | RepeatableRead | Serializable
  deriving (Eq, Ord, Show)

data Mode = Read | Write
  deriving (Eq, Ord, Show)

-- | Whether a pipeline is the last one: nothing can be sent after a 'Final' one,
-- which carries COMMIT, or ROLLBACK once the transaction is condemned.
data Ending = Open | Final
  deriving (Eq, Ord, Show)

-- | Runs the reading program in a transaction on the connection, which begins
-- when the first pipeline is sent. A server error that nobody caught rolls it
-- back and is the 'Left'; a failure rolls it back and is rethrown; otherwise it
-- ends with COMMIT or ROLLBACK, unless a 'Final' pipeline did, after draining
-- what wasn't read.
transaction :: (Runtime arr) => IsolationLevel -> Mode -> With Tx arr x y -> arr (Connection, (Loans, x)) (Either QueryError y)
transaction _ _ _ = notImplemented "transaction"

-- | Sends the pipeline, with its parameters from the input, and gives its
-- positions to the reading program that follows. A program sends any number of
-- pipelines in order.
pipeline :: (Runtime arr) => Ending -> Pipeline arr p a -> (a -> With Tx arr x y) -> With Tx arr (p, x) y
pipeline _ _ _ = notImplemented "pipeline"

-- | Rolls back at the end, and a later 'Final' pipeline carries ROLLBACK.
condemn :: (Runtime arr) => With Tx arr () ()
condemn = notImplemented "condemn"

-- | The next row of the result, or 'Nothing' after the last. Results are read in
-- the order of their statements: reading a later one while an earlier one is
-- unread fails with 'ResultOutOfOrder'. A result whose statement failed raises
-- its 'QueryError' here.
nextRow :: (Runtime arr) => Result row -> With Tx arr () (Maybe row)
nextRow _ = notImplemented "nextRow"

-- | Drops the rest of the result.
discard :: (Runtime arr) => Result row -> With Tx arr () ()
discard _ = notImplemented "discard"

-- | Awaits the server's acknowledgement of a preparation.
prepared :: (Runtime arr) => Preparation -> With Tx arr () ()
prepared _ = notImplemented "prepared"

-- | The command tag of the result, once its rows are read.
commandTag :: (Runtime arr) => Result row -> With Tx arr () ByteString
commandTag _ = notImplemented "commandTag"

-- | COMMIT's own reply, as a value.
ending :: (Runtime arr) => With Tx arr () (Either QueryError ())
ending = notImplemented "ending"

-- | The one row of the result, which 'UnexpectedAmountOfRows' is raised without.
single :: (Runtime arr) => Result row -> With Tx arr () row
single result = nextRow result >>> now (maybe (throwIO (UnexpectedAmountOfRows 0)) pure)

-- | The row of the result, if it has one.
optional :: (Runtime arr) => Result row -> With Tx arr () (Maybe row)
optional = nextRow

-- | Folds the rows of the result, one at a time, from the first to the last.
foldRows :: (Runtime arr) => (s -> row -> s) -> s -> Result row -> With Tx arr () s
foldRows step start result = const start ^>> loop
  where
    loop =
      ((),)
        ^>> first (nextRow result)
        >>> (\(next', accumulated) -> maybe (Left accumulated) (Right . step accumulated) next')
        ^>> (returnA ||| loop)

-- | A cursor standing at a DataRow.
data Columns

-- | How the rows of a result are read: a step of the cursor, for each row.
newtype Rows arr row = Rows (arr Columns row)

-- | A column's value as a stream, of the declared length: the last column of a
-- row, and the way to read a value that is larger than the buffer.
newtype Field = Field Int

instance (Effect arr) => Stream arr Field where
  next = notImplemented "next"

-- | The next column, decoded from the buffer. A column larger than the buffer
-- fails with 'ValueTooLarge'.
column :: (Runtime arr, InPlace a) => arr Columns a
column = notImplemented "column"

-- | The next column, which may be NULL.
nullable :: (Runtime arr, InPlace a) => arr Columns (Maybe a)
nullable = notImplemented "nullable"

-- | The last column, as a stream.
field :: (Runtime arr) => arr Columns Field
field = notImplemented "field"

-- | The last column, as a stream, if it isn't NULL.
nullableField :: (Runtime arr) => arr Columns (Maybe Field)
nullableField = notImplemented "nullableField"

-- | How many bytes the field has, as it declares.
fieldLength :: Field -> Int
fieldLength (Field declared) = declared

-- | Passes the next column.
skip :: (Runtime arr) => arr Columns ()
skip = notImplemented "skip"

-- | How many rows the result may have, checked by the read that finds the count
-- wrong, which fails with 'UnexpectedAmountOfRows'. These stubs check nothing: the
-- rows don't say how many they expect yet.
one, atMostOne, any :: Rows arr row -> Rows arr row
one = id
atMostOne = id
any = id

-- | A result that has no rows to read.
none :: (Runtime arr) => Rows arr ()
none = Rows (notImplemented "none")
