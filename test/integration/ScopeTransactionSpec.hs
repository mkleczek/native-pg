{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | Transactions in a request, against a real server and a pool of real connections, ported to the v2 API.
--
-- A request is one lexical scope: 'withConnection' checks a connection out of the pool, the transaction runs on it,
-- and everything is given back when the body ends, however it ends. What v1 observed in the pool's events
-- (@ready: succeeded@, @ready: failed@, @terminated@) is observed here from the outside: a connection that went back
-- to the pool is the one the next request gets (the same backend), and a connection that was broken is not.
--
-- Every group is marked as an expected failure ('notYet') until the v2 code is there.
module ScopeTransactionSpec (spec) where

import Control.Arrow (arr, (>>>), (>>^))
import Control.Exception (ErrorCall (..), throwIO)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.Text (Text)
import Database.PostgreSQL.Connection (ConnectSettings, Connection)
import Database.PostgreSQL.Errors
import Database.PostgreSQL.Pool (connectionPoolSettings, withConnection, withConnectionPool)
import qualified Database.PostgreSQL.Transaction as Tx
import GHC.Clock (getMonotonicTime)
import Network.Socket (PortNumber)
import Network.Streaming.BufferPool (PoolSettings (..))
import Network.Streaming.Pool (Pool)
import Network.Streaming.PositiveInt (positiveInt)
import Skeletest
import TestPrograms

spec :: FilePath -> PortNumber -> Spec
spec socketPath port = describe "operations and transactions in a request" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let withEnv' :: (Env -> IO a) -> IO a
      withEnv' = withEnv runner (tlsSettings port "native_pg_scram" (Just "scram secret")) 1
      administrator :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
      administrator body = ordinary runner (session (unixSettings socketPath "postgres" Nothing) body)
      -- What the server says a backend is doing, as pg_stat_activity does, asked on a connection of its own.
      stateOf :: Int64 -> IO (Either QueryError [Text])
      stateOf pid = administrator (query (plain ("SELECT state FROM pg_stat_activity WHERE pid = " <> BC.pack (show pid)) single))

  notYet $ describe "when the request completes" $ do
    it "commits the transaction, and gives the connection back" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (ddl "CREATE TEMP TABLE scope_commit (id int)")) `shouldReturn` Right ()
      onEnv env (request env (committed Tx.Write (Tx.statement (plain "INSERT INTO scope_commit VALUES (1)" Tx.none)) Tx.discard)) `shouldReturn` Right ()
      count env "scope_commit" `shouldReturn` Right 1
      pidOf env `shouldReturn` pid

    it "sends nothing for a transaction that did nothing" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (committed Tx.Read (pure ()) (\() -> arr (const ())))) `shouldReturn` Right ()
      onEnv env (request env selectOne) `shouldReturn` Right [1]
      pidOf env `shouldReturn` pid

    -- v1: a transaction that finished itself inside the scope. In v2 that is the pipeline of 'Tx.Final': its COMMIT is sent
    -- with it, and nothing is left for the end of the request.
    it "is done with a transaction that finished itself" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (ddl "CREATE TEMP TABLE scope_finished (id int)")) `shouldReturn` Right ()
      onEnv env (request env (transact Tx.ReadCommitted Tx.Write Tx.Final (Tx.statement (plain "INSERT INTO scope_finished VALUES (1)" Tx.none)) Tx.discard))
        `shouldReturn` Right ()
      count env "scope_finished" `shouldReturn` Right 1
      pidOf env `shouldReturn` pid

    it "drains a field that was not read, and keeps the connection" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (committed Tx.Read (Tx.statement (bodyOf 5000)) (\result -> Tx.single result >>> arr Tx.fieldLength)))
        `shouldReturn` Right 5000
      pidOf env `shouldReturn` pid

    -- A failure that the reading program caught with 'tryA' is the program's to handle: the transaction is rolled back all
    -- the same, because the server aborted it, and its value is the program's own, so the request goes on.
    it "rolls back a transaction whose failure the code handled, without failing the request" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (ddl "CREATE TEMP TABLE scope_handled (id int)")) `shouldReturn` Right ()
      onEnv
        env
        ( request env $
            committed Tx.Write (Tx.statement (plain "INSERT INTO scope_handled VALUES (1)" Tx.none)) $ \inserted ->
              Tx.discard inserted
                >>> arr (const ((), ()))
                >>> Tx.pipeline Tx.Open (Tx.statement divisionByZero) (attempt . Tx.single)
                >>^ either (const True) (const False)
        )
        `shouldReturn` Right True
      count env "scope_handled" `shouldReturn` Right 0
      pidOf env `shouldReturn` pid

    -- The failure comes after the row of the field, and nothing read it before the request ended, so the ending is
    -- what finds it, with the ReadyForQuery still unread.
    it "rolls back when the ending finds a failure after a field was read to its end, and keeps the connection" $ withEnv' $ \env -> do
      pid <- pidOf env
      (serverCode <$> onEnv env (request env (committed Tx.Read (Tx.statement failingAfterFirstRow) (\result -> Tx.single result >>> measure))))
        `shouldReturn` Just "22012"
      stateOf pid `shouldReturn` Right ["idle"]
      pidOf env `shouldReturn` pid

    -- The server's reply to COMMIT is the failure, and the status that follows it is idle: there is nothing to roll
    -- back, but the ReadyForQuery has to be read.
    it "keeps the connection when the ending's COMMIT fails on a deferred constraint, as the request ends" $ withEnv' $ \env -> do
      pid <- pidOf env
      ( serverCode
          <$> onEnv
            env
            ( request env $
                committed
                  Tx.Write
                  ((,,) <$> Tx.command "CREATE TEMP TABLE closing_parent (id int PRIMARY KEY)" <*> Tx.command "CREATE TEMP TABLE closing_child (id int REFERENCES closing_parent(id) DEFERRABLE INITIALLY DEFERRED)" <*> Tx.command "INSERT INTO closing_child VALUES (1)")
                  (\(parent, child, inserted) -> Tx.discard parent >>> Tx.discard child >>> Tx.discard inserted)
            )
        )
        `shouldReturn` Just "23503"
      pidOf env `shouldReturn` pid

    -- v1: the thread of a request that times out is interrupted in the release, where it drains. In v2 the cancellation
    -- arrives at the wait of the drain, and the connection is broken rather than waited for.
    it "breaks the connection when the request is cancelled while the ending drains, without waiting for the rest" $ withEnv' $ \env -> do
      pid <- pidOf env
      bodyDone <- newIORef False
      started <- getMonotonicTime
      outcome <-
        onEnv
          env
          ( withinFor (secs 2) $
              request env $
                committed Tx.Read (Tx.statement sleepingAtTheEnd) (\result -> Tx.single result >>> measure >>> now (\_ -> writeIORef bodyDone True))
          )
      -- the request was closing, and not the body running, when the cancellation came
      readIORef bodyDone `shouldReturn` True
      outcome `shouldBe` Nothing
      elapsed <- subtract started <$> getMonotonicTime
      (elapsed < 10) `shouldBe` True
      brokenAfter env pid

  notYet $ describe "when the request aborts" $ do
    it "rolls the transaction back, and gives the connection back" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (ddl "CREATE TEMP TABLE scope_rollback (id int)")) `shouldReturn` Right ()
      onEnv
        env
        ( request env (committed Tx.Write (Tx.statement (plain "INSERT INTO scope_rollback VALUES (1)" Tx.none)) (\inserted -> Tx.discard inserted >>> failing "the request failed"))
            `failsWith` ErrorCall "the request failed"
        )
        `shouldReturn` ()
      count env "scope_rollback" `shouldReturn` Right 0
      pidOf env `shouldReturn` pid

    it "rolls back after a query error, and keeps the connection" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (ddl "CREATE TEMP TABLE scope_error (id int)")) `shouldReturn` Right ()
      ( serverCode
          <$> onEnv
            env
            ( request env $
                committed Tx.Write ((,) <$> Tx.statement (plain "INSERT INTO scope_error VALUES (1)" Tx.none) <*> Tx.statement divisionByZero) (\(inserted, dividing) -> Tx.discard inserted >>> Tx.single dividing)
            )
        )
        `shouldReturn` Just "22012"
      count env "scope_error" `shouldReturn` Right 0
      pidOf env `shouldReturn` pid

    it "rolls back when the failure comes after a field was read to its end" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv
        env
        ( request env (committed Tx.Read (Tx.statement (bodyOf 5000)) (\result -> Tx.single result >>> measure >>> failing "the request failed"))
            `failsWith` ErrorCall "the request failed"
        )
        `shouldReturn` ()
      pidOf env `shouldReturn` pid

    -- The cancellation rule: a failure that finds the reader in the middle of a message, as a field half pulled is, breaks
    -- the connection rather than draining it: draining a field of any size for a client that went away is no use. v1
    -- did the same. It covers v1's second case about a half-pulled field, which only differed in the API that pulled.
    it "breaks the connection when a field was left in the middle of its stream" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv
        env
        ( request
            env
            ( committed Tx.Read (Tx.statement (bodyOf 50000)) $ \result ->
                Tx.single result
                  >>> next
                  >>> now (maybe (failTest "the field has no piece") (\piece -> (BS.length piece < 50000) `shouldBe` True))
                  >>> failing "the client went away"
            )
            `failsWith` ErrorCall "the client went away"
        )
        `shouldReturn` ()
      brokenAfter env pid

    it "breaks the connection when the request is cancelled while a result is read" $ withEnv' $ \env -> do
      pid <- pidOf env
      started <- getMonotonicTime
      onEnv env (withinFor (millis 300) (request env (committed Tx.Read (Tx.statement sleeping) Tx.single))) `shouldReturn` Nothing
      elapsed <- subtract started <$> getMonotonicTime
      (elapsed < 10) `shouldBe` True
      brokenAfter env pid

    it "breaks the connection when the request is cancelled between operations, and doesn't wait for the answer" $ withEnv' $ \env -> do
      pid <- pidOf env
      started <- getMonotonicTime
      -- The statement is sent, and not read.
      onEnv env (withinFor (millis 300) (request env (committed Tx.Read (Tx.statement sleeping) (\_ -> sleepFor' (secs 30))))) `shouldReturn` Nothing
      elapsed <- subtract started <$> getMonotonicTime
      (elapsed < 10) `shouldBe` True
      brokenAfter env pid

  -- v1 found out with a preflight check that the server had ended an idle connection, without waiting for it. The pool of v2 does
  -- the same when it hands the connection out: the request gets another connection, and fails no more than it waits.
  notYet $ describe "an idle connection" $
    forM_ [("without TLS", unixSettings socketPath "postgres" Nothing), ("with TLS", tlsSettings port "postgres" Nothing)] $ \(label, settings) ->
      it ("is replaced when the server ended it, " <> label) $ withEnv runner settings 1 $ \env -> do
        pid <- pidOf env
        -- with a timeout, it returns once the backend has exited
        administrator (query (valued "SELECT pg_terminate_backend($1::int, 10000)" [Just (BC.pack (show pid))] (single @_ @Bool))) `shouldReturn` Right [True]
        started <- getMonotonicTime
        pid' <- pidOf env
        elapsed <- subtract started <$> getMonotonicTime
        (pid' /= pid) `shouldBe` True
        (elapsed < 5) `shouldBe` True

  -- A field is a stream: the pieces are views, and the way to pull it to its end is the stream's.
  notYet $ describe "a field as a stream" $
    it "pulls a field larger than the buffers to its end, exactly" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (committed Tx.Read (Tx.statement (numbersOf 20000)) (\result -> Tx.single result >>> copyOut)))
        `shouldReturn` Right (numbers 20000)
      pidOf env `shouldReturn` pid

  notYet $ describe "the ending's reply" $ do
    it "gives COMMIT's success once the body was pulled, and the request has nothing left to do" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv env (request env (ddl "CREATE TEMP TABLE reply_commit (id int)")) `shouldReturn` Right ()
      onEnv
        env
        ( request env $
            transact
              Tx.ReadCommitted
              Tx.Write
              Tx.Final
              ((,) <$> Tx.command "INSERT INTO reply_commit VALUES (1)" <*> Tx.statement (numbersOf 3000))
              (\(inserted, body) -> Tx.discard inserted >>> Tx.single body >>> copyOut >>> now (\delivered -> delivered `shouldBe` numbers 3000) >>> Tx.ending)
        )
        `shouldReturn` Right (Right ())
      count env "reply_commit" `shouldReturn` Right 1
      pidOf env `shouldReturn` pid

    -- 'Tx.ending' is COMMIT's own reply as a value, so the transaction is 'Right' of it.
    it "gives the failure of a deferred constraint at COMMIT as a value, after the body was pulled" $ withEnv' $ \env -> do
      pid <- pidOf env
      onEnv
        env
        ( request env $
            transact
              Tx.ReadCommitted
              Tx.Write
              Tx.Final
              ( (,,,)
                  <$> Tx.command "CREATE TEMP TABLE reply_parent (id int PRIMARY KEY)"
                  <*> Tx.command "CREATE TEMP TABLE reply_child (id int REFERENCES reply_parent(id) DEFERRABLE INITIALLY DEFERRED)"
                  <*> Tx.command "INSERT INTO reply_child VALUES (1)"
                  <*> Tx.statement (numbersOf 3000)
              )
              (\(parent, child, inserted, body) -> Tx.discard parent >>> Tx.discard child >>> Tx.discard inserted >>> Tx.single body >>> copyOut >>> now (\delivered -> delivered `shouldBe` numbers 3000) >>> Tx.ending)
            >>> arr (either (const Nothing) (either failedAtCommit (const Nothing)))
        )
        `shouldReturn` Just "23503"
      -- The failure was seen, and the connection is fine
      pidOf env `shouldReturn` pid

-- * The pool of the cases

-- | The pool of real connections over the request buffers, and the way to run a program over them.
data Env = Env
  { envRunner :: Runner,
    envBuffers :: BufferPool,
    envPool :: Pool Connection
  }

-- | The program in one request of its own, on the runner of the environment.
onEnv :: Env -> Program a -> IO a
onEnv env = runOn (envRunner env) (envBuffers env)

-- | The environment for the use, with a pool that is closed when the use ends.
withEnv :: Runner -> ConnectSettings -> Int -> (Env -> IO a) -> IO a
withEnv runner settings size use = do
  buffers <- newPool (PoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
  withConnectionPool (connectionPoolSettings size settings) $ \pool -> use (Env runner buffers pool)

-- | The body in a request: a connection of the pool, which the body uses and gives back. A pool that has none is the
-- program's failure.
request :: (Runtime arr) => Env -> OnConn arr b -> arr Loans b
request env body =
  withConnection (envPool env) body
    >>> now (either (\failure -> failTest ("no connection: " <> show failure)) pure)

selectOne :: (Runtime arr) => OnConn arr (Either QueryError [Int64])
selectOne = query (plain "SELECT 1::int8" single)

-- | The backend of the connection that a request runs on.
pidOf :: Env -> IO Int64
pidOf env =
  onEnv env (request env (committed Tx.Read (Tx.statement (plain "SELECT pg_backend_pid()::bigint" single)) Tx.single))
    >>= either (\failure -> failTest ("no backend: " <> show failure)) pure

-- | How many rows a table has, in a request.
count :: Env -> ByteString -> IO (Either QueryError Int64)
count env table = onEnv env (request env (committed Tx.Read (Tx.statement (plain ("SELECT count(*) FROM " <> table) single)) Tx.single))

-- | The connection was closed, and the pool opens a new one for the next request: a connection with another backend.
brokenAfter :: Env -> Int64 -> IO ()
brokenAfter env pid = do
  now' <- pidOf env
  (now' /= pid) `shouldBe` True
  count env "pg_class" >>= \result -> either (const False) (> 0) result `shouldBe` True

-- | A statement with a field of this many bytes.
bodyOf :: (Runtime arr) => Int -> Tx.Statement arr () Tx.Field
bodyOf size = plain ("SELECT repeat('x', " <> BC.pack (show size) <> ")") (Tx.any Tx.field)

-- | The numbers from 1 to this one, separated by commas, as a field of that size.
numbersOf :: (Runtime arr) => Int -> Tx.Statement arr () Tx.Field
numbersOf n = plain ("SELECT string_agg(n::text, ',') FROM generate_series(1, " <> BC.pack (show n) <> ") n") (Tx.any Tx.field)

numbers :: Int -> ByteString
numbers n = BC.intercalate "," (map (BC.pack . show) [1 .. n])

divisionByZero :: (Runtime arr) => Tx.Statement arr () Int64
divisionByZero = plain "SELECT 1 / 0::int8" single

-- | A field of the first row, and then the server's failure, which comes with the row after it.
failingAfterFirstRow :: (Runtime arr) => Tx.Statement arr () Tx.Field
failingAfterFirstRow = plain "SELECT (1 / (2 - s))::text FROM generate_series(1, 2) s" (Tx.any Tx.field)

-- | Enough rows for the server to send most of them, and then a wait of 30 seconds before the last.
sleepingAtTheEnd :: (Runtime arr) => Tx.Statement arr () Tx.Field
sleepingAtTheEnd =
  plain
    "SELECT (CASE WHEN s < 3000 THEN 1 ELSE (SELECT 1 FROM pg_sleep(30)) END)::text FROM generate_series(1, 3000) s"
    (Tx.any Tx.field)

sleeping :: (Runtime arr) => Tx.Statement arr () Int64
sleeping = plain "SELECT 1::int8 FROM pg_sleep(30)" single

-- | Fails the reading program, as a request that goes wrong does.
failing :: (Runtime arr) => String -> With Tx.Tx arr a b
failing reason = now (\_ -> throwIO (ErrorCall reason))

sleepFor' :: (Runtime arr) => Duration -> With Tx.Tx arr a ()
sleepFor' duration = arr (const ()) >>> sleep duration

-- | The SQLSTATE of a COMMIT that failed.
failedAtCommit :: QueryError -> Maybe ByteString
failedAtCommit = \case
  QueryError "COMMIT" (ServerFailed (ServerError diagnostics)) -> Just (diagnosticCode diagnostics)
  _ -> Nothing
