{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | Transactions and pipelines against a real PostgreSQL, ported to the v2 API: the "transactions" group of
-- v1's @IntegrationSpec@. A pipeline is an Applicative whose value holds the positions of its results, and the
-- reading program is a function of them: statements are built once, and the order of reading is checked when the
-- program runs.
--
-- Parameters are baked into the statements (@'valued'@). Where the input of the program carries them, "IntegrationSpec" has
-- the cases.
--
-- A case that the v2 code can't pass yet is marked with what it needs ('needs'): the marks come off as the part that
-- they wait for lands.
module TransactionSpec (spec) where

import Control.Arrow (arr, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (ErrorCall (..), throwIO)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import Data.Bifunctor (bimap)
import Data.Either (isRight)
import Data.Int (Int32, Int64)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Database.PostgreSQL.Errors
import Database.PostgreSQL.Transaction (Rows (..))
import qualified Database.PostgreSQL.Transaction as Tx
import Skeletest
import TestPrograms hiding (named)

spec :: FilePath -> Spec
spec socketPath = describe "transactions" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let run :: Program a -> IO a
      run = ordinary runner
      withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
      withSession body = run (session (unixSettings socketPath "postgres" Nothing) body)

  describe "ending" $ do
    it "commits" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              committed Tx.Write (Tx.statement (insertItem "1")) Tx.discard `yields` Right (),
              items `yields` Right 1
            ]
        )
        `shouldReturn` ()
    -- A condemned transaction rolls back at its end, and a 'Tx.Final' pipeline that follows 'Tx.condemn' carries ROLLBACK
    -- in place of COMMIT.
    it "rolls back when condemned, also when a final pipeline follows" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              committed Tx.Write (Tx.statement (insertItem "1")) (\inserted -> Tx.discard inserted >>> Tx.condemn) `yields` Right (),
              committed
                Tx.Write
                (Tx.statement (insertItem "1"))
                (\inserted -> Tx.discard inserted >>> Tx.condemn >>> const ((), ()) ^>> Tx.pipeline Tx.Final (Tx.statement (insertItem "2")) Tx.discard)
                `yields` Right (),
              items `yields` Right 0
            ]
        )
        `shouldReturn` ()
    it "rolls back after a failing statement, and reports it with its SQL" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              (committed Tx.Write ((,) <$> Tx.statement (insertItem "1") <*> Tx.statement dividingByZero) discardBoth >>^ failedAt)
                `yields` Just ("SELECT 1 / 0", "22012"),
              items `yields` Right 0
            ]
        )
        `shouldReturn` ()
    it "begins with the isolation level and the mode" $
      withSession
        ( committedAt
            Tx.RepeatableRead
            Tx.Read
            (Tx.statement (plain "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')" (pair @_ @Text @Text)))
            Tx.single
            `yields` Right ("repeatable read", "on")
        )
        `shouldReturn` ()
    it "sends nothing for a transaction without statements" $
      withSession (committed Tx.Write (pure ()) (\() -> arr (const (5 :: Int))) `yields` Right 5) `shouldReturn` ()
    it "reports a failure at commit" $
      withSession
        ( steps
            [ ddl "CREATE TEMP TABLE tx_unique (n int UNIQUE DEFERRABLE INITIALLY DEFERRED)" `yields` Right (),
              ( committed
                  Tx.Write
                  ((,) <$> Tx.command "INSERT INTO tx_unique VALUES (1)" <*> Tx.command "INSERT INTO tx_unique VALUES (1)")
                  discardBoth
                  >>^ failedAt
              )
                `yields` Just ("COMMIT", "23505")
            ]
        )
        `shouldReturn` ()
    it "aborts when the reading program fails between statements, and leaves the connection usable" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              committed Tx.Write (Tx.statement (insertItem "1")) (\inserted -> Tx.discard inserted >>> now (const (throwIO (ErrorCall "boom"))))
                `failsWith` ErrorCall "boom",
              items `yields` Right 0
            ]
        )
        `shouldReturn` ()

  -- The pipeline of 'Tx.Final' carries the COMMIT, so that the transaction is one flight to the server.
  describe "the final pipeline" $ do
    it "commits with the final pipeline" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              transact Tx.ReadCommitted Tx.Write Tx.Final (Tx.statement (insertItem "1")) Tx.discard `yields` Right (),
              items `yields` Right 1
            ]
        )
        `shouldReturn` ()
    it "rolls back after a statement in the final pipeline fails" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              ( transact Tx.ReadCommitted Tx.Write Tx.Final ((,) <$> Tx.statement (insertItem "1") <*> Tx.statement dividingByZero) discardBoth
                  >>^ failedAt
              )
                `yields` Just ("SELECT 1 / 0", "22012"),
              items `yields` Right 0,
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "reports a failure at commit in the final pipeline, after which the transaction is over" $
      withSession
        ( steps
            [ ddl "CREATE TEMP TABLE tx_unique (n int UNIQUE DEFERRABLE INITIALLY DEFERRED)" `yields` Right (),
              ( transact
                  Tx.ReadCommitted
                  Tx.Write
                  Tx.Final
                  ((,) <$> Tx.command "INSERT INTO tx_unique VALUES (1)" <*> Tx.command "INSERT INTO tx_unique VALUES (1)")
                  discardBoth
                  >>^ failedAt
              )
                `yields` Just ("COMMIT", "23505"),
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "rejects a pipeline after the final one, and rolls back" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              transact Tx.ReadCommitted Tx.Write Tx.Final (Tx.statement (insertItem "1")) (\inserted -> Tx.discard inserted >>> const ((), ()) ^>> Tx.pipeline Tx.Open (Tx.command "SELECT 1") Tx.discard)
                `failsWith` AfterFinal,
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()

  describe "pipelines" $ do
    it "runs a pipeline's statements in order, and gives their results" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              committed Tx.Write ((,) <$> Tx.statement (insertItem "1") <*> Tx.statement countItems) (\(inserted, counted) -> Tx.discard inserted &&& Tx.single counted)
                `yields` Right ((), 1)
            ]
        )
        `shouldReturn` ()
    it "refuses to read a result before the earlier ones, aborts the transaction and leaves the connection usable" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              committed Tx.Write ((,) <$> Tx.statement (insertItem "1") <*> Tx.statement countItems) (\(_, counted) -> Tx.single counted)
                `failsWith` ResultOutOfOrder 1,
              -- The insert before it was rolled back, and the connection is idle.
              items `yields` Right 0,
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "reads the result after one that is discarded, keeping the effect of the discarded statement" $
      withSession
        ( committed
            Tx.Read
            ((,) <$> Tx.statement setting <*> Tx.statement current)
            (\(configured, value) -> Tx.discard configured >>> Tx.single value)
            `yields` Right "kept"
        )
        `shouldReturn` ()
    it "discards a streamed result that is unread or half read, and reads the one after it" $
      withSession
        ( committed
            Tx.Read
            ((,,) <$> Tx.statement streamed <*> Tx.statement streamed <*> Tx.statement (answerFor "1"))
            ( \(unread, halfRead, later) ->
                Tx.discard unread
                  >>> (Tx.nextRow halfRead >>> now (maybe (failTest "expected a streamed row") pure) >>> snd ^>> next >>> done)
                  >>> Tx.discard halfRead
                  >>> Tx.single later
            )
            `yields` Right 2
        )
        `shouldReturn` ()
    it "reports a failure of an earlier statement, not the usage error, when a later result is read first" $
      withSession
        ( steps
            [ (committed Tx.Read ((,) <$> Tx.statement dividingByZero <*> Tx.statement (answerFor "1")) (\(_, later) -> Tx.single later) >>^ failedAt)
                `yields` Just ("SELECT 1 / 0", "22012"),
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "skips a pipeline's statements after one that fails on the server, and reports that one" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              -- Nothing is read: the failure comes when the transaction drains what is left.
              (committed Tx.Write ((,,) <$> Tx.statement (insertItem "1") <*> Tx.statement dividingByZero <*> Tx.statement (insertItem "2")) (const done) >>^ failedAt)
                `yields` Just ("SELECT 1 / 0", "22012"),
              items `yields` Right 0
            ]
        )
        `shouldReturn` ()
    it "reads a pipeline's results after one that doesn't decode, and stays usable" $
      withSession
        ( steps
            [ ( committed
                  Tx.Read
                  ((,) <$> Tx.statement (plain "SELECT 'abc'::text" (single @_ @Int32)) <*> Tx.statement (plain "SELECT 2::int4" (single @_ @Int32)))
                  (\(bad, good) -> Tx.single bad &&& Tx.single good)
                  >>^ rowFailure
              )
                `yields` Just "SELECT 'abc'::text",
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    -- The read that finds the count wrong fails with 'UnexpectedAmountOfRows', a usage failure, not an error of the query.
    it "checks how many rows a result has" $
      withSession
        ( steps
            [ shaped (Tx.one Tx.column) 0 `failsAs` wrongAmount,
              shaped (Tx.one Tx.column) 2 `failsAs` wrongAmount,
              shaped (Tx.atMostOne Tx.column) 0 `yields` Right [],
              shaped (Tx.atMostOne Tx.column) 1 `yields` Right [1],
              shaped (Tx.any Tx.column) 3 `yields` Right [1, 2, 3],
              committed Tx.Read (Tx.statement (numbers Tx.none 3)) Tx.discard `yields` Right ()
            ]
        )
        `shouldReturn` ()
    it "reads a result that has at most one row" $
      withSession
        ( committed
            Tx.Read
            ((,) <$> Tx.statement (numbers (Tx.atMostOne (Tx.column @_ @Int32)) 0) <*> Tx.statement (numbers (Tx.atMostOne (Tx.column @_ @Int32)) 1))
            (\(nothing, just) -> Tx.optional nothing &&& Tx.optional just)
            `yields` Right (Nothing, Just 1)
        )
        `shouldReturn` ()
    it "gives the command tag of each result once its rows are read" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              committed
                Tx.Write
                ((,) <$> Tx.statement (insertItem "1") <*> Tx.statement (numbers (single @_ @Int32) 3))
                (\(inserted, selected) -> tagged inserted &&& tagged selected)
                `yields` Right ("INSERT 0 1", "SELECT 3")
            ]
        )
        `shouldReturn` ()
    it "reports a row that doesn't decode" $
      withSession
        ( (query (plain "SELECT 'abc'::text" (single @_ @Int32)) >>^ rowFailure) `yields` Just "SELECT 'abc'::text"
        )
        `shouldReturn` ()
    it "streams a column larger than the read buffer within the transaction" $
      withSession
        ( committed Tx.Read (Tx.statement (plain "SELECT 7::int4, repeat('x', 1000000)" statusAndBody)) lengths
            `yields` Right (7, 1000000, 1000000)
        )
        `shouldReturn` ()
    it "streams a column that may be NULL within a single row's scope" $
      withSession
        ( committed
            Tx.Read
            ((,) <$> Tx.statement (plain "SELECT 7::int4, repeat('x', 1000000)" maybeBody) <*> Tx.statement (plain "SELECT 8::int4, NULL::text" maybeBody))
            (\(large, none') -> maybeLengths large &&& maybeLengths none')
            `yields` Right ((7, Just (1000000, 1000000)), (8, Nothing))
        )
        `shouldReturn` ()

  describe "prepared statements" $ do
    it "runs a statement prepared earlier on the connection by its name, without evaluating its SQL" $
      withSession
        ( steps
            [ preparedResult "answer" (answerFor "41") `yields` Right 42,
              committed Tx.Read (Tx.executePrepared "answer" (unrendered (answerFor "1"))) Tx.single `yields` Right 2,
              committed
                Tx.Read
                ((,) <$> Tx.executePrepared "answer" (unrendered (answerFor "2")) <*> Tx.executePrepared "answer" (unrendered (answerFor "3")))
                (\(left, right) -> Tx.single left &&& Tx.single right)
                `yields` Right (3, 4),
              preparedNames `yields` Right ["answer"],
              -- A name the connection doesn't have is the server's failure: a predicate on the error would render the SQL.
              (committed Tx.Read (Tx.executePrepared "unknown" (unrendered (answerFor "1"))) Tx.single >>^ serverCode) `yields` Just "26000",
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "keeps a name whose preparation was awaited when its execution fails and the transaction rolls back" $ do
      acknowledged <- newIORef False
      withSession
        ( steps
            [ (committed Tx.Read ((,) <$> Tx.prepare "divided" (dividedBy "0") <*> Tx.executePrepared "divided" (dividedBy "0")) (\(preparation, result) -> Tx.prepared preparation >>> now (const (writeIORef acknowledged True)) >>> readAll result) >>^ serverCode)
                `yields` Just "22012",
              preparedNames `yields` Right ["divided"],
              committed Tx.Read (Tx.executePrepared "divided" (unrendered (dividedBy "1"))) Tx.discard `yields` Right (),
              -- It is in use, so preparing it again is rejected until the client closes it.
              (committed Tx.Read (Tx.prepare "divided" (dividedBy "1")) Tx.prepared >>^ serverCode) `yields` Just "42P05",
              closed "divided" `yields` Right (),
              preparedNames `yields` Right [],
              committed Tx.Read (Tx.prepare "divided" (dividedBy "1")) Tx.prepared `yields` Right (),
              preparedNames `yields` Right ["divided"]
            ]
        )
        `shouldReturn` ()
      -- The client learned of the name before the rows failed, and it outlives the rollback.
      readIORef acknowledged `shouldReturn` True
    -- A failure that the reading program catches with 'tryA' is the program's to handle: the transaction still rolls back,
    -- because the server aborted it, and its value is the program's own.
    it "keeps a preparation acknowledged when the rows that follow it were read first and failed" $
      withSession
        ( steps
            [ ( committed
                  Tx.Read
                  ((,) <$> Tx.prepare "divided" (dividedBy "0") <*> Tx.executePrepared "divided" (dividedBy "0"))
                  (\(preparation, result) -> attempt (readAll result) &&& attempt (Tx.prepared preparation))
                  >>^ fmap (bimap serverCode isRight)
              )
                `yields` Right (Just "22012", True),
              preparedNames `yields` Right ["divided"]
            ]
        )
        `shouldReturn` ()
    it "reports a Parse the server rejects when its preparation is awaited, and prepares nothing" $
      withSession
        ( steps
            [ -- Alone.
              ( committed Tx.Read (Tx.prepare "broken" broken) (attempt . Tx.prepared)
                  >>^ fmap failedAt
              )
                `yields` Right (Just ("SELEC 1", "42601")),
              -- The error comes first, whichever result of the pipeline is read.
              ( committed Tx.Read (Tx.prepare "broken" broken *> Tx.executePrepared "broken" broken) (attempt . readAll)
                  >>^ fmap failedAt
              )
                `yields` Right (Just ("SELEC 1", "42601")),
              preparedNames `yields` Right [],
              -- A rejected Parse does not undo the one before it, whichever is awaited first.
              ( committed
                  Tx.Read
                  ((,) <$> Tx.prepare "accepted" (answerFor "1") <*> Tx.prepare "broken" broken)
                  (\(accepted, rejecting) -> attempt (Tx.prepared rejecting) &&& attempt (Tx.prepared accepted))
                  >>^ fmap (bimap failedAt isRight)
              )
                `yields` Right (Just ("SELEC 1", "42601"), True),
              preparedNames `yields` Right ["accepted"]
            ]
        )
        `shouldReturn` ()
    it "skips the Parse after a statement that fails on the server, so that its preparation is not acknowledged" $ do
      let skipping = Just ("SELECT 1 / 0", "22012")
      withSession
        ( steps
            [ ( committed Tx.Read skippingPipeline (\(earlier, preparation, _) -> attempt (Tx.discard earlier) &&& attempt (Tx.prepared preparation))
                  >>^ fmap (bimap failedAt failedAt)
              )
                `yields` Right (skipping, skipping),
              -- Awaiting it first reaches the same failure.
              (committed Tx.Read skippingPipeline (\(_, preparation, _) -> attempt (Tx.prepared preparation)) >>^ fmap failedAt)
                `yields` Right skipping,
              preparedNames `yields` Right []
            ]
        )
        `shouldReturn` ()
    it "answers an unnamed statement, a preparation and its execution in wire order, reading the rows or the preparation first" $
      withSession
        ( steps
            [ committed Tx.Read (wire "in_order") (\(unnamed, preparation, named) -> Tx.single unnamed &&& (Tx.prepared preparation >>> Tx.single named))
                `yields` Right (2, 4),
              -- The result before them is dropped on purpose. Reading the rows reads the preparation before them.
              committed Tx.Read (wire "rows_first") (\(unnamed, preparation, named) -> Tx.discard unnamed >>> Tx.single named >>> thenDo (Tx.prepared preparation))
                `yields` Right 4,
              committed Tx.Read (wire "preparation_first") (\(unnamed, preparation, named) -> Tx.discard unnamed >>> Tx.prepared preparation >>> Tx.single named)
                `yields` Right 4,
              preparedNames `yields` Right ["in_order", "preparation_first", "rows_first"]
            ]
        )
        `shouldReturn` ()
    it "prepares and runs named statements in one pipeline with writes, including a name prepared earlier in it" $
      withSession
        ( steps
            [ createItems `yields` Right (),
              committed
                Tx.Write
                ( (,,,,)
                    <$> Tx.statement (insertItem "1")
                    <*> (Tx.prepare "count_items" countItems *> Tx.executePrepared "count_items" countItems)
                    <*> (Tx.prepare "answer" (answerFor "41") *> Tx.executePrepared "answer" (answerFor "41"))
                    <*> Tx.executePrepared "answer" (unrendered (answerFor "1"))
                    <*> ((,) <$> Tx.statement (insertItem "2") <*> Tx.executePrepared "count_items" (unrendered countItems))
                )
                ( \(inserted, counted, incremented, again, (insertedAgain, recounted)) ->
                    Tx.discard inserted
                      >>> ( (,,,)
                              <$> Tx.single counted
                              <*> Tx.single incremented
                              <*> Tx.single again
                              <* Tx.discard insertedAgain
                              <*> Tx.single recounted
                          )
                )
                `yields` Right (1, 42, 2, 2),
              committed
                Tx.Write
                ((,,) <$> Tx.statement (insertItem "3") <*> Tx.executePrepared "count_items" (unrendered countItems) <*> Tx.executePrepared "answer" (unrendered (answerFor "9")))
                (\(inserted, counted, incremented) -> Tx.discard inserted >>> (Tx.single counted &&& Tx.single incremented))
                `yields` Right (3, 10),
              preparedNames `yields` Right ["answer", "count_items"],
              items `yields` Right 3
            ]
        )
        `shouldReturn` ()
    it "refuses to await a preparation while an earlier result is unread, and the connection closes the name it tried" $
      withSession
        ( steps
            [ committed Tx.Read ((,) <$> Tx.statement (answerFor "1") <*> Tx.prepare "refused" (answerFor "1")) (\(_, preparation) -> Tx.prepared preparation)
                `failsWith` ResultOutOfOrder 1,
              -- The server parsed it, though the client did not see that: it is closed, as after any attempt it did not see succeed.
              preparedNames `yields` Right ["refused"],
              closed "refused" `yields` Right ()
            ]
        )
        `shouldReturn` ()
    -- The server answers a Close of a name it doesn't have as it does one that it has.
    it "closes a name that was never prepared without an error, and a name once only" $
      withSession
        ( steps
            [ closed "never_prepared" `yields` Right (),
              committed Tx.Read (Tx.prepare "once" (answerFor "1")) Tx.prepared `yields` Right (),
              closed "once" `yields` Right (),
              closed "once" `yields` Right (),
              preparedNames `yields` Right []
            ]
        )
        `shouldReturn` ()

-- * Statements of the cases

createItems :: (Runtime arr) => OnConn arr (Either QueryError ())
createItems = ddl "CREATE TEMP TABLE tx_items (n int)"

insertItem :: (Runtime arr) => ByteString -> Tx.Statement arr () ()
insertItem value = valued "INSERT INTO tx_items VALUES ($1)" [Just value] Tx.none

countItems :: (Runtime arr) => Tx.Statement arr () Int64
countItems = plain "SELECT count(*) FROM tx_items" single

-- | How many items there are, in a transaction of its own.
items :: (Runtime arr) => OnConn arr (Either QueryError Int64)
items = committed Tx.Read (Tx.statement countItems) Tx.single

selectOne :: (Runtime arr) => OnConn arr (Either QueryError [Int32])
selectOne = query (plain "SELECT 1" single)

-- | Adds one to its parameter, which is the text.
answerFor :: (Runtime arr) => ByteString -> Tx.Statement arr () Int32
answerFor value = valued "SELECT $1::int4 + 1" [Just value] single

-- | Divides one by its parameter.
dividedBy :: (Runtime arr) => ByteString -> Tx.Statement arr () Int32
dividedBy value = valued "SELECT 1 / $1::int4" [Just value] single

dividingByZero :: (Runtime arr) => Tx.Statement arr () ()
dividingByZero = plain "SELECT 1 / 0" Tx.none

broken :: (Runtime arr) => Tx.Statement arr () ()
broken = plain "SELEC 1" Tx.none

-- | A statement whose SQL nobody has: the connection knows the name, and the SQL is never evaluated.
unrendered :: Tx.Statement arr p row -> Tx.Statement arr p row
unrendered known = known {Tx.sql = error "SQL forced"}

setting :: (Runtime arr) => Tx.Statement arr () Text
setting = plain "SELECT set_config('native_pg.discarded', 'kept', true)" single

current :: (Runtime arr) => Tx.Statement arr () Text
current = plain "SELECT current_setting('native_pg.discarded')" single

-- | A result of three rows, each with a large field.
streamed :: (Runtime arr) => Tx.Statement arr () (Int32, Tx.Field)
streamed = plain "SELECT 7::int4, repeat('x', 1000000) FROM generate_series(1, 3)" statusAndBody

statusAndBody :: (Runtime arr) => Rows arr (Int32, Tx.Field)
statusAndBody = Tx.any (Tx.column &&& Tx.field)

maybeBody :: (Runtime arr) => Rows arr (Int32, Maybe Tx.Field)
maybeBody = Tx.any (Tx.column &&& Tx.nullableField)

-- | An unnamed statement, a preparation and its execution, with the parameters that tell them apart.
wire :: (Runtime arr) => ByteString -> Tx.Pipeline arr () (Tx.Result arr Int32, Tx.Preparation, Tx.Result arr Int32)
wire name = (,,) <$> Tx.statement (answerFor "1") <*> Tx.prepare name (answerFor "2") <*> Tx.executePrepared name (answerFor "3")

-- | The names the server has prepared on the connection.
preparedNames :: (Runtime arr) => OnConn arr (Either QueryError [Text])
preparedNames = query (plain "SELECT name FROM pg_prepared_statements ORDER BY name" single)

-- | Prepares a name and runs it in a pipeline of its own, and takes its one row after the server acknowledged the preparation.
preparedResult :: (Runtime arr) => ByteString -> Tx.Statement arr () Int32 -> OnConn arr (Either QueryError Int32)
preparedResult name statement =
  committed Tx.Read ((,) <$> Tx.prepare name statement <*> Tx.executePrepared name statement) (\(preparation, result) -> Tx.prepared preparation >>> Tx.single result)

-- | The statement of a number of rows, of the cardinality that the rows say.
numbers :: (Runtime arr) => Rows arr row -> Int -> Tx.Statement arr () row
numbers rows count = plain ("SELECT n FROM generate_series(1, " <> Char8.pack (show count) <> ") n") rows

-- | Every row that the shape lets through, and the transaction's failure if the count is not the shape's.
shaped :: (Runtime arr) => Rows arr Int32 -> Int -> OnConn arr (Either QueryError [Int32])
shaped rows count = query (numbers rows count)

-- | A failing statement, and a preparation and an execution after it.
skippingPipeline :: (Runtime arr) => Tx.Pipeline arr () (Tx.Result arr (), Tx.Preparation, Tx.Result arr Int32)
skippingPipeline = (,,) <$> Tx.statement dividingByZero <*> Tx.prepare "skipped" (dividedBy "1") <*> Tx.executePrepared "skipped" (dividedBy "1")

-- | Discards both results, in order.
discardBoth :: (Runtime arr) => (Tx.Result arr a, Tx.Result arr b) -> With Tx.Tx arr () ()
discardBoth (one, other) = Tx.discard one >>> Tx.discard other

-- | What the failure of a result with the wrong number of rows is.
wrongAmount :: UsageError -> IO ()
wrongAmount = \case
  UnexpectedAmountOfRows _ -> pure ()
  other -> failTest ("expected UnexpectedAmountOfRows, got " <> show other)

-- | The command tag of a result, once its rows are read.
tagged :: (Runtime arr) => Tx.Result arr row -> With Tx.Tx arr () ByteString
tagged result = countRows result >>> done >>> Tx.commandTag result

-- | After the step, the value goes on.
thenDo :: (Runtime arr) => With Tx.Tx arr () () -> With Tx.Tx arr a a
thenDo step = tap (done >>> step)

-- | The length that a field declares, and the bytes that its pieces add up to.
lengths :: (Runtime arr) => Tx.Result arr (Int32, Tx.Field) -> With Tx.Tx arr () (Int32, Int, Int)
lengths result = Tx.single result >>> ((,,) <$> arr fst <*> arr (Tx.fieldLength . snd) <*> (snd ^>> measure >>^ fst))

maybeLengths :: (Runtime arr) => Tx.Result arr (Int32, Maybe Tx.Field) -> With Tx.Tx arr () (Int32, Maybe (Int, Int))
maybeLengths result = Tx.single result >>> (arr fst &&& (snd ^>> lengthOf))
  where
    lengthOf = maybe (Left ()) Right ^>> (arr (const Nothing) ||| ((arr Tx.fieldLength &&& (measure >>^ fst)) >>^ Just))

-- * What a failure was

-- | The SQL and the SQLSTATE of a statement that the server failed.
failedAt :: Either QueryError a -> Maybe (ByteString, ByteString)
failedAt = \case
  Left (QueryError sql (ServerFailed (ServerError diagnostics))) -> Just (sql, diagnosticCode diagnostics)
  _ -> Nothing

-- | The SQL of a statement whose row didn't decode.
rowFailure :: Either QueryError a -> Maybe ByteString
rowFailure = \case
  Left (QueryError sql (RowFailed _)) -> Just sql
  _ -> Nothing
