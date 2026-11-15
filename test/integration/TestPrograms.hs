{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | What the ported specs share: the programs' type and the runners that run them, the connection of a program,
-- the transaction's plumbing, readers for the results, and sources for the tests of streamed parameters.
--
-- Everything here is written once for any arrow that has the capabilities of 'Runtime', as in
-- @directions/test/ScopesSpec.hs@, and in the combinator style: no proc notation.
--
-- The v2 vocabulary of streaming-io is re-exported, so that a spec imports the module names of native-pg only.
module TestPrograms
  ( -- * Programs and runners
    Program,
    Runner (..),
    runners,
    runOn,
    newPool,
    ordinary,
    withBuffers,
    notYet,
    needs,
    smallBuffers,
    -- * Settings of the cluster's connections
    unixSettings,
    tlsSettings,
    -- * A connection in a program
    OnConn,
    across,
    viaPool,
    session,
    connectedTo,
    alsoOn,
    conn,
    onFirst,
    onSecond,
    steps,
    yields,
    satisfies,
    failsWith,
    failsAs,
    withinFor,
    after,
    timed,
    -- * Transactions
    transact,
    transactOn,
    committed,
    committedAt,
    ddl,
    closed,
    query,
    -- * Reading in a transaction
    readAll,
    countRows,
    attempt,
    serverCode,
    diagnosticsOf,
    -- * Rows
    single,
    pair,
    triple,
    plain,
    valued,
    textual,
    streamedText,
    piecesText,
    -- * Streams
    streaming,
    foldPieces,
    measure,
    copyOut,
    Chunks,
    newChunks,
    SlowChunks (..),
    newSlowChunks,
    FailingChunks (..),
    newFailingChunks,
    -- * The vocabulary of streaming-io, re-exported
    module Network.Streaming.Classes,
    module Network.Streaming.Combinators,
    module Network.Streaming.With,
    BufferPool,
    Loans,
  )
where

import Control.Arrow (Arrow, ArrowChoice, first, returnA, right, second, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (Exception, evaluate, fromException, throwIO)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Word (Word64)
import Database.PostgreSQL.Connection (Address (..), ConnectSettings (..), Connection, SslMode (..), connect, connectSettings)
import qualified Database.PostgreSQL.Connection as PG
import Database.PostgreSQL.Decoding (InPlace)
import Database.PostgreSQL.Errors (ConnectFailure, Diagnostics, QueryError (..), ServerError (..), StatementError (..), diagnosticCode)
import Database.PostgreSQL.Pool (ConnectionPool, PoolError, withConnection)
import Database.PostgreSQL.Tls (tlsClient)
import Database.PostgreSQL.Transaction (Rows (..))
import qualified Database.PostgreSQL.Transaction as Tx
import Database.PostgreSQL.Types (FormatCode (..))
import Network.Socket (PortNumber)
import Network.Streaming.BufferPool (BufferPool, BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes
import Network.Streaming.Combinators hiding (step)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.Loans (Loans, withRequestBuffers)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Threads (runThreads)
import Network.Streaming.With
import Skeletest
import System.Timeout (timeout)

-- * Programs and runners

-- | A program for any arrow that has what 'Runtime' bundles. Its input is the loans of the request it runs in, which
-- the runner gives it.
type Program a = forall arr. (Runtime arr) => arr Loans a

-- | A way to run a program: an arrow's runner, over a pool of buffers.
data Runner = Runner String (forall a. BufferPool -> Program a -> IO a)

-- | Every runner the suites run under. A case is written once, as a 'Program'.
runners :: [Runner]
runners =
  [ Runner "threads" onThreads,
    Runner "delimited" onDelimited,
    Runner "compiled" onCompiled
  ]

onThreads :: BufferPool -> Program a -> IO a
onThreads buffers program = runThreads (withRequestBuffers (fst ^>> program)) (buffers, ())

onDelimited :: BufferPool -> Program a -> IO a
onDelimited buffers program = fst <$> runDelimited (withRequestBuffers (fst ^>> program)) (buffers, ())

onCompiled :: BufferPool -> Program a -> IO a
onCompiled buffers program = fst <$> runCompiled (withRequestBuffers (fst ^>> program)) (buffers, ())

-- | The program, as one request, on the pool. A program that never ends fails the case after a minute.
runOn :: Runner -> BufferPool -> Program a -> IO a
runOn (Runner _ run) buffers program =
  timeout 60000000 (run buffers program) >>= maybe (failTest "the program did not end in a minute") pure

-- | A pool of buffers of the settings.
newPool :: BufferPoolSettings -> IO BufferPool
newPool = newBufferPool

-- | The program on a pool of buffers of the usual size: 1 KiB, so that a few kilobytes of a value already take
-- several pieces, and a bit more than a request needs of them.
ordinary :: Runner -> Program a -> IO a
ordinary runner program = newPool settings >>= \buffers -> runOn runner buffers program
  where
    settings = BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2)

-- | The program on a pool of the settings.
withBuffers :: BufferPoolSettings -> Runner -> Program a -> IO a
withBuffers settings runner program = newPool settings >>= \buffers -> runOn runner buffers program

-- | Tiny buffers, for the cases about bounded memory: 16 bytes, two for the requests in flight, one reserved, two
-- for setting up connections.
smallBuffers :: BufferPoolSettings
smallBuffers = BufferPoolSettings (positiveInt @16) (positiveInt @2) (positiveInt @1) (positiveInt @2)

-- | Marks every case of the spec as an expected failure, for what it needs and the v2 code doesn't have yet. The mark
-- comes off as the part that it waits for lands, and a case that then passes unexpectedly fails the suite.
needs :: String -> Spec -> Spec
needs what = xfail ("v2: " <> what <> " is not there yet")

-- | The same, for a spec that waits for the whole of v2.
notYet :: Spec -> Spec
notYet = xfail "v2: not implemented"

-- * Settings of the cluster's connections

-- | A connection to the cluster's Unix-domain socket, to the @postgres@ database.
unixSettings :: FilePath -> ByteString -> Maybe ByteString -> ConnectSettings
unixSettings socketPath user password =
  (connectSettings (UnixAddress socketPath) user)
    { connectPassword = password,
      connectDatabase = Just "postgres"
    }

-- | A connection to the cluster's TCP port, which requires TLS.
tlsSettings :: PortNumber -> ByteString -> Maybe ByteString -> ConnectSettings
tlsSettings port user password =
  (connectSettings (TcpAddress "127.0.0.1" port) user)
    { connectPassword = password,
      connectDatabase = Just "postgres",
      connectSslMode = SslRequire,
      connectTlsClient = Just tlsClient
    }

-- * A connection in a program

-- | A step that runs on a connection, with the loans of its request: what a transaction takes as its input.
type OnConn arr b = arr (Connection, Loans) b

-- | The body on a connection made to the settings, closed when the body ends, however it ends. A connection that
-- can't be made is the program's failure.
session :: (Runtime arr) => ConnectSettings -> OnConn arr b -> arr Loans b
session settings body = connectedTo settings body >>> orFail

-- | The body on a connection of the pool: 'withConnection' for a pool that the program has, as a test has.
viaPool :: (Runtime arr) => ConnectionPool -> arr (Connection, a) b -> arr a (Either PoolError b)
viaPool pool body = (pool,) ^>> withConnection body

-- | The step on each of the inputs in turn, whichever their number: one arrow, built once, which takes what differs from run
-- to run, the settings of a connection or a pool, from its input.
across :: (Runtime arr) => arr (x, Loans) b -> [x] -> arr Loans [b]
across step inputs = (\loans -> (inputs, loans, [])) ^>> iterateA following
  where
    following = untried ^>> (returnA ||| tried)
    untried (remaining, loans, results) = case remaining of
      [] -> Left (Finish (reverse results))
      input : others -> Right ((input, loans), (others, loans, results))
    tried = first step >>^ \(result, (others, loans, results)) -> Again (others, loans, result : results)

-- | The same, with the failure to connect as a value, and the body not run.
connectedTo :: (Runtime arr) => ConnectSettings -> OnConn arr b -> arr Loans (Either ConnectFailure b)
connectedTo settings body = scope ((settings,) ^>> connect) closeConnected (withLoans ^>> right body)
  where
    withLoans (connected, loans) = fmap (,loans) connected

-- | The body on a second connection too, which is closed when the body ends. The body takes both connections, the
-- one of the enclosing 'session' first. A connection that can't be made is the program's failure.
alsoOn :: (Runtime arr) => ConnectSettings -> arr ((Connection, Connection), Loans) b -> OnConn arr b
alsoOn settings body = scope (((settings,) . snd) ^>> connect) closeConnected (bothWithLoans ^>> right body) >>> orFail
  where
    bothWithLoans (connected, (outer, loans)) = fmap (\inner -> ((outer, inner), loans)) connected

-- | Closes the connection if it was made.
closeConnected :: (Runtime arr) => arr (Either ConnectFailure Connection, Exit) ()
closeConnected = fst ^>> (done ||| PG.close)

-- | The result of what needed a connection, or the program's failure if there was none.
orFail :: (Effect arr) => arr (Either ConnectFailure b) b
orFail = now (either (\failure -> fail ("no connection: " <> show failure)) pure)

-- | A step that takes the connection only, as a step of the connection.
conn :: (Arrow arr) => arr Connection b -> OnConn arr b
conn step = fst ^>> step

-- | A step of the first of two connections.
onFirst :: (Arrow arr) => OnConn arr b -> arr ((Connection, Connection), Loans) b
onFirst step = first fst ^>> step

-- | A step of the second of two connections.
onSecond :: (Arrow arr) => OnConn arr b -> arr ((Connection, Connection), Loans) b
onSecond step = first snd ^>> step

-- | The steps one after the other, each on the same input.
steps :: (Arrow arr) => [arr x ()] -> arr x ()
steps = foldr ((>>>) . tap) done

-- | A step whose result must be the expected one.
yields :: (Runtime arr, Eq b, Show b) => arr x b -> b -> arr x ()
yields step expected = step >>> now (`shouldBe` expected)

-- | A step whose result must satisfy the predicate.
satisfies :: (Runtime arr, Show b) => arr x b -> Predicate IO b -> arr x ()
satisfies step predicate = step >>> now (`shouldSatisfy` predicate)

-- | A step that must fail, with this failure. A cancellation isn't one: it passes through.
failsWith :: (Runtime arr, Exception e, Eq e, Show e) => arr x b -> e -> arr x ()
failsWith step expected = failsAs step (`shouldBe` expected)

-- | A step that must fail with a failure of this type, which the check takes.
failsAs :: (Runtime arr, Exception e) => arr x b -> (e -> IO ()) -> arr x ()
failsAs step check =
  tryA step
    >>> now
      ( \case
          Left failure | Just expected <- fromException failure -> check expected
          Left failure -> failTest ("failed otherwise: " <> show failure)
          Right _ -> failTest "did not fail"
      )

-- | The step, given this long. 'Nothing' if it didn't end: it was cancelled where it waited, and has released what it held.
withinFor :: (Runtime arr) => Duration -> arr x b -> arr x (Maybe b)
withinFor = within

-- | The step, after waiting this long: a wait, so that a cancellation reaches it.
after :: (Runtime arr) => Duration -> arr x b -> arr x b
after delay step = keeping (sleep delay) >>> snd ^>> step

-- | The step, and how many nanoseconds it took on the arrow's clock.
timed :: (Runtime arr) => arr x b -> arr x (b, Word64)
timed step = elapsed <$> instant <*> step <*> instant
  where
    instant = done >>> clock
    elapsed (Instant start) result (Instant end) = (result, end - start)

-- * Transactions

-- | A transaction on the connection with one pipeline, for a pipeline whose parameters come from the input, and the reading
-- program that takes the positions of its results.
transactOn ::
  (Runtime arr) =>
  Tx.IsolationLevel ->
  Tx.Mode ->
  Tx.Ending ->
  Tx.Pipeline arr p a ->
  (a -> With Tx.Tx arr () y) ->
  arr (Connection, (Loans, p)) (Either QueryError y)
transactOn level mode ending pipeline reading =
  through (_2 . _2) (,()) ^>> Tx.transaction level mode (Tx.pipeline ending pipeline reading)

-- | A transaction on the connection, for a pipeline that takes no input.
transact ::
  (Runtime arr) =>
  Tx.IsolationLevel ->
  Tx.Mode ->
  Tx.Ending ->
  Tx.Pipeline arr () a ->
  (a -> With Tx.Tx arr () y) ->
  OnConn arr (Either QueryError y)
transact level mode ending pipeline reading = second (,()) ^>> transactOn level mode ending pipeline reading

-- | A transaction that stays open after its pipeline, as 'Tx.Open' says, and commits when the reading program ends.
committedAt :: (Runtime arr) => Tx.IsolationLevel -> Tx.Mode -> Tx.Pipeline arr () a -> (a -> With Tx.Tx arr () y) -> OnConn arr (Either QueryError y)
committedAt level mode = transact level mode Tx.Open

-- | The same at read committed, v1's usual.
committed :: (Runtime arr) => Tx.Mode -> Tx.Pipeline arr () a -> (a -> With Tx.Tx arr () y) -> OnConn arr (Either QueryError y)
committed = committedAt Tx.ReadCommitted

-- | A command with no rows, in a transaction of its own: v1's @Tx.sql@.
ddl :: (Runtime arr) => ByteString -> OnConn arr (Either QueryError ())
ddl sql = transact Tx.ReadCommitted Tx.Write Tx.Final (Tx.command sql) Tx.discard

-- | Closes the statement that the connection has prepared under the name, in a transaction of its own.
closed :: (Runtime arr) => ByteString -> OnConn arr (Either QueryError ())
closed name = committed Tx.Write (Tx.closeStatement name) Tx.discard

-- | One statement, in a transaction of its own that commits, and all its rows.
query :: (Runtime arr) => Tx.Statement arr () row -> OnConn arr (Either QueryError [row])
query statement = transact Tx.ReadCommitted Tx.Write Tx.Final (Tx.statement statement) readAll

-- * Reading in a transaction

-- | Every row of the result, in order.
readAll :: (Runtime arr) => Tx.Result arr row -> With Tx.Tx arr () [row]
readAll result = Tx.foldRows (flip (:)) [] result >>^ reverse

-- | How many rows the result has, taking them one at a time.
countRows :: (Runtime arr) => Tx.Result arr row -> With Tx.Tx arr () Int
countRows = Tx.foldRows (\count _ -> count + 1) 0

-- | The step, with the 'QueryError' it fails with as a value, so that the transaction goes on. Another failure
-- goes on as it is.
attempt :: (Runtime arr) => With Tx.Tx arr a b -> With Tx.Tx arr a (Either QueryError b)
attempt step =
  tryA step
    >>> now (either (\failure -> maybe (throwIO failure) (pure . Left) (fromException failure)) (pure . Right))

-- | The SQLSTATE of a statement that the server failed.
serverCode :: Either QueryError a -> Maybe ByteString
serverCode = \case
  Left (QueryError _ (ServerFailed (ServerError diagnostics))) -> Just (diagnosticCode diagnostics)
  _ -> Nothing

-- | The diagnostics of a statement that the server failed.
diagnosticsOf :: Either QueryError a -> Maybe Diagnostics
diagnosticsOf = \case
  Left (QueryError _ (ServerFailed (ServerError diagnostics))) -> Just diagnostics
  _ -> Nothing

-- * Rows

-- | One column.
single :: (Runtime arr, InPlace a) => Rows arr a
single = Tx.any Tx.column

-- | Two columns. The cursor takes them in order, as '&&&' runs its halves, left first.
pair :: (Runtime arr, InPlace a, InPlace b) => Rows arr (a, b)
pair = Tx.any (Tx.column &&& Tx.column)

-- | Three columns.
triple :: (Runtime arr, InPlace a, InPlace b, InPlace c) => Rows arr (a, b, c)
triple = Tx.any ((,,) <$> Tx.column <*> Tx.column <*> Tx.column)

-- | A statement without parameters.
plain :: ByteString -> Rows arr row -> Tx.Statement arr () row
plain sql = Tx.Statement sql (const []) Nothing

-- | A statement whose parameters are these values, as text, and NULL for 'Nothing'.
valued :: ByteString -> [Maybe ByteString] -> Rows arr row -> Tx.Statement arr () row
valued sql values = Tx.Statement sql (const (map textual values)) Nothing

-- | A parameter that is text of a type that the server infers, or NULL.
textual :: Maybe ByteString -> Tx.Param
textual value = Tx.Param Nothing TextFormat (maybe Tx.Null Tx.Bytes value)

-- | A parameter that is text of a type that the server infers, of this many bytes, which are the next of the stream of
-- its statement.
streamedText :: Int -> Tx.Param
streamedText declared = Tx.Param Nothing TextFormat (Tx.FromStream declared)

-- | A parameter that is text of a type that the server infers, which is sent as these pieces.
piecesText :: [ByteString] -> Tx.Param
piecesText pieces = Tx.Param Nothing TextFormat (Tx.Pieces pieces)

-- * Streams

-- | The step, with a new source of the pieces for its input.
streaming :: (Runtime arr) => IO s -> arr (Connection, (Loans, s)) b -> OnConn arr b
streaming source step = now (\(connection, loans) -> (\s -> (connection, (loans, s))) <$> source) >>> step

-- | The stream to its end, giving each piece to the step with what it has kept. A piece is a view, valid until the
-- next: the step copies what it keeps.
foldPieces :: (Runtime arr, Stream arr s) => (acc -> ByteString -> IO acc) -> arr (s, acc) acc
foldPieces step =
  uncurry Folding
    ^>> iterateWith
      (alongside (foldStream ^>> next) >>^ pulled)
      (now (\(piece, Folding source kept) -> Folding source <$> step kept piece))
  where
    pulled (piece, folding) = maybe (Finish (foldKept folding)) (Again . (,folding)) piece

-- | The state of 'foldPieces' between the pieces: the stream, and what the step has kept.
data Folding s acc = Folding {foldStream :: s, foldKept :: acc}

-- | How many bytes the stream has, in how many pieces.
measure :: (Runtime arr, Stream arr s) => arr s (Int, Int)
measure = (, (0, 0)) ^>> foldPieces (\(bytes, pieces) chunk -> pure (bytes + BS.length chunk, pieces + 1 :: Int))

-- | The bytes of the stream, copied out of the views.
copyOut :: (Runtime arr, Stream arr s) => arr s ByteString
copyOut = (, []) ^>> foldPieces (\copies chunk -> (: copies) <$> evaluate (BS.copy chunk)) >>^ (BS.concat . reverse)

-- | A source of pieces, then the end: a parameter that a test streams.
newtype Chunks = Chunks (IORef [ByteString])

newChunks :: [ByteString] -> IO Chunks
newChunks pieces = Chunks <$> newIORef pieces

instance (Effect arr) => Stream arr Chunks where
  next =
    now
      ( \(Chunks remaining) ->
          atomicModifyIORef' remaining (\case [] -> ([], Nothing); piece : others -> (others, Just piece))
      )

-- | A source that is slow: it waits this long before each piece, as a client of a request body does.
data SlowChunks = SlowChunks Duration Chunks

newSlowChunks :: Duration -> [ByteString] -> IO SlowChunks
newSlowChunks delay pieces = SlowChunks delay <$> newChunks pieces

instance (Runtime arr) => Stream arr SlowChunks where
  next =
    (\(SlowChunks delay chunks) -> (delay, chunks))
      ^>> consume sleepIn
      >>> next

-- | Waits as long as the duration.
sleepIn :: (Runtime arr) => arr Duration ()
sleepIn = keeping clock >>> Until . uncurry plus ^>> block

-- | A source that gives its pieces and then fails, as a client that went away does.
newtype FailingChunks = FailingChunks Chunks

newFailingChunks :: [ByteString] -> IO FailingChunks
newFailingChunks pieces = FailingChunks <$> newChunks pieces

instance (Effect arr, ArrowChoice arr) => Stream arr FailingChunks where
  next =
    (\(FailingChunks chunks) -> chunks)
      ^>> next
      >>> maybe (Left ()) (Right . Just)
      ^>> (now (const (throwIO (userError "the client went away"))) ||| returnA)
