{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Tests against a real PostgreSQL. A temporary cluster is created with the
-- @initdb@ and @pg_ctl@ found on the path, e.g. inside
-- @postgrest-with-pg-17@ to test against PostgreSQL 17.
module Main (main) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (ErrorCall (..), bracket, bracket_, displayException, throwIO, try)
import Control.Monad (unless, void)
import Control.Monad.IO.Class (liftIO)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Int (Int16, Int32, Int64)
import Data.List (intercalate, isPrefixOf)
import Data.Maybe (isJust)
import Data.Scientific (Scientific)
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import Data.Time (Day, DiffTime, UTCTime (..), fromGregorian)
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Database.PostgreSQL.ConnectionString
import Database.PostgreSQL.Decoding
import Database.PostgreSQL.Diagnostics
import qualified Database.PostgreSQL.Encoders as Encoders
import Database.PostgreSQL.Protocol
  ( FormatCode (..),
    Notification (..),
    Parameter (..),
    ProcessId,
    Scalar (..),
    TransactionStatus (..),
    protocolVersion3_0,
    protocolVersion3_2,
  )
import Database.PostgreSQL.Protocol.Codec (ByteStream (..))
import Database.PostgreSQL.Session
import qualified Database.PostgreSQL.Snippet as Snippet
import qualified Database.PostgreSQL.Transaction as Tx
import Database.PostgreSQL.Transport (Address (..), StreamLengthError)
import GHC.Clock (getMonotonicTime)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import qualified Network.Socket as Socket
import System.Directory (getTemporaryDirectory, removeDirectoryRecursive)
import System.Exit (ExitCode (..))
import System.FilePath (takeDirectory, (</>))
import System.Mem (performMajorGC)
import System.Posix.Temp (mkdtemp)
import System.Process (readProcessWithExitCode)
import Test.Hspec

main :: IO ()
main = withCluster $ \socketPath -> hspec $ do
  let settings user password =
        (connectSettings (UnixAddress socketPath) user)
          { connectPassword = password,
            connectDatabase = Just "postgres"
          }
      withSession = bracket (fst <$> connect (settings "postgres" Nothing)) close
      withSessionInfo action =
        bracket (connect (settings "postgres" Nothing)) (close . fst) (uncurry action)

  describe "connect" $ do
    it "connects a trusted user and reports the server's parameters" $ do
      (session, info) <- connect (settings "postgres" Nothing)
      lookup "server_version" (serverParameters info) `shouldSatisfy` isJust
      serverBackendKey info `shouldSatisfy` isJust
      serverProtocolVersion info `shouldBe` protocolVersion3_0
      close session
    it "authenticates with SCRAM-SHA-256" $
      connectAndClose (settings "native_pg_scram" (Just "scram secret"))
    it "authenticates with MD5" $
      connectAndClose (settings "native_pg_md5" (Just "md5 secret"))
    it "authenticates with a cleartext password" $
      connectAndClose (settings "native_pg_cleartext" (Just "cleartext secret"))
    it "reports a wrong password" $
      connect (settings "native_pg_scram" (Just "wrong")) `shouldThrow` rejectedWith "28P01"
    it "reports a missing password" $
      connect (settings "native_pg_scram" Nothing) `shouldThrow` authenticationError
    it "reports a missing database" $
      connect ((settings "postgres" Nothing) {connectDatabase = Just "missing"}) `shouldThrow` rejectedWith "3D000"
    it "uses protocol 3.2 where the server has it, and 3.0 otherwise" $ do
      (session, info) <- connect ((settings "postgres" Nothing) {connectProtocolVersion = protocolVersion3_2})
      close session
      let major = fmap fst . Char8.readInt =<< lookup "server_version" (serverParameters info)
      serverProtocolVersion info
        `shouldBe` (if maybe False (>= 18) major then protocolVersion3_2 else protocolVersion3_0)

  describe "connection strings" $ do
    let directory = takeDirectory socketPath
        -- Without the environment, so that the PG* variables of the shell
        -- running the tests don't change them.
        connectWith string =
          either fail (connectFirst id) (parseConnectionString (const Nothing) (Just "postgres") string)
        withConnection string = bracket (connectWith string) (\(session, _, _) -> close session)
        connectFailure string = do
          result <- try (connectWith string)
          case result of
            Left failure -> pure (displayException (failure :: ConnectFailure))
            Right (session, _, _) -> close session *> fail "connected"
        local extra = "host=" <> Char8.pack directory <> " dbname=postgres " <> extra
        value session sql = fst <$> query session sql []
    it "connects with a URI that names the socket directory" $
      withConnection ("postgresql:///postgres?host=" <> Char8.pack directory) $ \(session, _, host) -> do
        hostName host `shouldBe` directory
        value session "SELECT 1" `shouldReturn` Right ([[Just "1"]], "SELECT 1")
    it "tries the hosts in order" $
      withConnection ("host=/nonexistent," <> Char8.pack directory <> " dbname=postgres") $ \(_, _, host) ->
        hostName host `shouldBe` directory
    it "words each host's failure as libpq does" $
      connectFailure "host=/nonexistent-1,/nonexistent-2"
        `shouldReturn` intercalate
          "\n"
          [ "connection to server on socket \"" <> path <> "/.s.PGSQL.5432\" failed: No such file or directory\n\tIs the server running locally and accepting connections on that socket?"
          | path <- ["/nonexistent-1", "/nonexistent-2"]
          ]
    it "words a host name that doesn't resolve as libpq does" $ do
      failure <- connectFailure "host=no-such-host.invalid"
      failure `shouldSatisfy` isPrefixOf "could not translate host name \"no-such-host.invalid\" to address: "
    it "names the application, with the fallback name if none is given" $ do
      withConnection (local "fallback_application_name=fallback") $ \(session, _, _) ->
        value session "SELECT current_setting('application_name')" `shouldReturn` Right ([[Just "fallback"]], "SELECT 1")
      withConnection (local "fallback_application_name=fallback application_name=app") $ \(session, _, _) ->
        value session "SELECT current_setting('application_name')" `shouldReturn` Right ([[Just "app"]], "SELECT 1")
    it "connects only to a server with the session attributes asked for" $ do
      let readOnly = "options='-c default_transaction_read_only=on' "
      withConnection (local (readOnly <> "target_session_attrs=read-only")) (\_ -> pure ())
      connectFailure (local (readOnly <> "target_session_attrs=read-write"))
        `shouldReturn` ("connection to server on socket \"" <> socketPath <> "\" failed: session is read-only")
      withConnection (local "target_session_attrs=primary") (\_ -> pure ())
      connectFailure (local "target_session_attrs=standby")
        `shouldReturn` ("connection to server on socket \"" <> socketPath <> "\" failed: server is not in hot standby mode")
      withConnection (local "target_session_attrs=prefer-standby") (\_ -> pure ())
    it "gives up on a host after connect_timeout" $
      withSilentServer $ \silent -> do
        started <- getMonotonicTime
        connectFailure ("host=" <> Char8.pack silent <> " connect_timeout=2")
          `shouldReturn` ("connection to server on socket \"" <> silent <> "/.s.PGSQL.5432\" failed: timeout expired")
        elapsed <- subtract started <$> getMonotonicTime
        elapsed `shouldSatisfy` (\seconds -> seconds >= 2 && seconds < 5)

  describe "execute" $ around withSession $ do
    it "runs an unprepared statement with parameters" $ \session ->
      query session "SELECT $1::int + 1, $2::text" [Just "41", Just "x"]
        `shouldReturn` (Right ([[Just "42", Just "x"]], "SELECT 1"), TxIdle)
    it "passes NULL parameters and returns NULL columns" $ \session ->
      query session "SELECT $1::text, 1" [Nothing]
        `shouldReturn` (Right ([[Nothing, Just "1"]], "SELECT 1"), TxIdle)
    it "folds many rows" $ \session -> do
      (result, _) <- execute session (statement "SELECT generate_series(1, 10000)" []) (\count row -> (count + 1 :: Int) <$ columns row) 0
      result `shouldBe` Right (10000, "SELECT 10000")
    it "runs a prepared statement until it's closed" $ \session -> do
      prepare session "add_one" "SELECT $1::int + 1" [] `shouldReturn` (Right (), TxIdle)
      prepared session "add_one" [Just "1"] `shouldReturn` (Right ([[Just "2"]], "SELECT 1"), TxIdle)
      prepared session "add_one" [Just "41"] `shouldReturn` (Right ([[Just "42"]], "SELECT 1"), TxIdle)
      closeStatement session "add_one" `shouldReturn` (Right (), TxIdle)
      (result, _) <- prepared session "add_one" [Just "1"]
      either errorCode (const Nothing) result `shouldBe` Just "26000"
    it "returns an error and stays usable" $ \session -> do
      (result, status) <- query session "SELECT 1/0" []
      either errorCode (const Nothing) result `shouldBe` Just "22012"
      status `shouldBe` TxIdle
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "reports the transaction status" $ \session -> do
      fmap snd (query session "BEGIN" []) `shouldReturn` TxInTransaction
      fmap snd (query session "SELECT 1/0" []) `shouldReturn` TxFailedTransaction
      fmap snd (query session "ROLLBACK" []) `shouldReturn` TxIdle
    it "streams a parameter from its source" $ \session -> do
      let pattern = BS.concat (replicate 4096 "0123456789abcdef")
          chunks = 128
      source <- chunkSource (replicate chunks pattern)
      (result, _) <-
        execute
          session
          (Execution (Unprepared "SELECT $1::text = repeat('0123456789abcdef', 524288)" []) [] [Just (ParameterStream (ByteStream (chunks * BS.length pattern) source))] [])
          (\rows row -> (: rows) <$> columns row)
          []
      result `shouldBe` Right ([[Just "t"]], "SELECT 1")
    it "takes a value larger than the read buffer whole" $ \session -> do
      (result, _) <- query session "SELECT repeat('y', 100000)" []
      result `shouldBe` Right ([[Just (Char8.replicate 100000 'y')]], "SELECT 1")
    it "streams a column to a slow consumer without holding it in memory" $ \session -> do
      enabled <- getRTSStatsEnabled
      unless enabled $ pendingWith "needs +RTS -T"
      let size = 64 * 1024 * 1024
      (result, _) <-
        execute session (statement ("SELECT repeat('x', " <> Char8.pack (show size) <> ")") []) (\_ row -> Just <$> consumeSlowly row) Nothing
      fmap fst result `shouldBe` Right (Just (size, True, True))
    it "rejects a row used after its step" $ \session -> do
      saved <- newIORef Nothing
      _ <- execute session (statement "SELECT 1" []) (\() row -> writeIORef saved (Just row)) ()
      Just row <- readIORef saved
      nextColumn row `shouldThrow` rowMisuse
    it "is unusable while busy, and broken after a failed operation" $ \session -> do
      let nested () _ = void (execute session (statement "SELECT 2" []) (\() _ -> pure ()) ())
      execute session (statement "SELECT 1" []) nested () `shouldThrow` sessionUnusable
      query session "SELECT 1" [] `shouldThrow` sessionUnusable
  describe "decodeRow" $ around withSession $ do
    it "decodes integers, booleans, floats and numerics" $ \session ->
      decodeSingle @'[Int16, Int32, Int64, Bool, Float, Double, Scientific] session "SELECT 1::int2, -2::int4, 3::int8, true, 1.5::float4, 2.5::float8, 3.25::numeric" (\a b c d e f g -> pure (a, b, c, d, e, f, g))
        `shouldReturn` Right (1, -2, 3, True, 1.5, 2.5, 3.25)
    it "decodes text, bytea, char, uuid and json" $ \session ->
      decodeSingle @'[Text, ByteString, Char, UUID, Json, Jsonb] session "SELECT 'héllo'::text, '\\xdeadbeef'::bytea, 'x'::char, '00000000-0000-0000-0000-000000000001'::uuid, '{\"a\":1}'::json, '{\"a\":1}'::jsonb" (\a b c d e f -> pure (a, b, c, d, e, f))
        `shouldReturn` Right ("héllo", BS.pack [0xde, 0xad, 0xbe, 0xef], 'x', UUID.fromWords 0 0 0 1, Json "{\"a\":1}", Jsonb "{\"a\": 1}")
    it "decodes dates, timestamps and intervals" $ \session ->
      decodeSingle @'[Day, UTCTime, DiffTime] session "SELECT '2024-01-02'::date, '2024-01-02 03:04:05+00'::timestamptz, '2 hours'::interval" (\a b c -> pure (a, b, c))
        `shouldReturn` Right (fromGregorian 2024 1 2, UTCTime (fromGregorian 2024 1 2) 11045, 7200)
    it "decodes NULL into Maybe and rejects it otherwise" $ \session -> do
      decodeSingle @'[Maybe Int32, Maybe Int32] session "SELECT NULL::int4, 1::int4" (curry pure)
        `shouldReturn` Right (Nothing, Just 1)
      decodeSingle @'[Int32] session "SELECT NULL::int4" (\_ -> pure ())
        `shouldReturn` Left (ColumnDecodingFailed 1 "unexpected NULL")
    it "decodes arrays, with NULL elements into Maybe" $ \session ->
      decodeSingle @'[[Int32], [Maybe Text]] session "SELECT ARRAY[1, 2, 3]::int4[], ARRAY['a', NULL]::text[]" (curry pure)
        `shouldReturn` Right ([1, 2, 3], [Just "a", Nothing])
    it "decodes composites and arrays of them" $ \session ->
      decodeSingle @'[Pair, [Pair]] session "SELECT ROW(1, 'a'::text), ARRAY[ROW(2, 'b'::text), ROW(3, 'c'::text)]" (curry pure)
        `shouldReturn` Right (Pair 1 "a", [Pair 2 "b", Pair 3 "c"])
    it "reports a wrong column count, or a column of another type, and stays usable" $ \session -> do
      decodeSingle @'[Int32] session "SELECT 1::int4, 2::int4" (\_ -> pure ())
        `shouldReturn` Left (ColumnCountMismatch 1 2)
      decodeSingle @'[Int64] session "SELECT 1::int4" (\_ -> pure ())
        `shouldReturn` Left (ColumnDecodingFailed 1 "int8 needs 8 bytes, got 4")
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "streams a last column after decoding the others" $ \session ->
      decodeSingleStreaming @'[Int32] session "SELECT 200::int4, repeat('x', 1000000)" (\status body -> (status,) <$> streamedLength body)
        `shouldReturn` Right (200, 1000000)

  describe "errors" $ around withSession $ do
    it "reports a missing relation with its position in the query" $ \session -> do
      (result, _) <- query session "SELECT * FROM missing_table" []
      fmap (\d -> (diagnosticSeverity d, diagnosticCode d, diagnosticMessage d, diagnosticPosition d)) (failed result)
        `shouldBe` Just (SeverityError, "42P01", "relation \"missing_table\" does not exist", Just 15)
    it "reports a constraint violation's detail, table and constraint" $ \session -> do
      _ <- query session "CREATE TEMPORARY TABLE item (id int PRIMARY KEY)" []
      _ <- query session "INSERT INTO item VALUES (1)" []
      (result, _) <- query session "INSERT INTO item VALUES (1)" []
      fmap (\d -> (diagnosticCode d, diagnosticDetail d, diagnosticTable d, diagnosticConstraint d)) (failed result)
        `shouldBe` Just ("23505", Just "Key (id)=(1) already exists.", Just "item", Just "item_pkey")
    it "reports a raised error's own SQLSTATE, detail, hint and context" $ \session -> do
      (result, _) <- query session "DO $$ BEGIN RAISE EXCEPTION 'custom' USING ERRCODE = 'PGRST', DETAIL = 'the detail', HINT = 'the hint'; END $$" []
      fmap (\d -> (diagnosticCode d, diagnosticMessage d, diagnosticDetail d, diagnosticHint d, isJust (diagnosticContext d))) (failed result)
        `shouldBe` Just ("PGRST", "custom", Just "the detail", Just "the hint", True)
    it "throws when the server ends the session, which is then broken" $ \session -> do
      outcome <- try (query session "SELECT pg_terminate_backend(pg_backend_pid())" [])
      case outcome of
        Left (ConnectionTerminated (ServerError d)) -> (diagnosticSeverity d, diagnosticCode d) `shouldBe` (SeverityFatal, "57P01")
        Left other -> expectationFailure ("expected ConnectionTerminated, got " <> show other)
        Right _ -> expectationFailure "expected ConnectionTerminated"
      query session "SELECT 1" [] `shouldThrow` sessionUnusable

  describe "cancel" $
    it "cancels a running statement, and the session stays usable" $
      withSessionInfo $ \session info -> do
        Just key <- pure (serverBackendKey info)
        _ <- forkIO (threadDelay 300000 *> cancel (UnixAddress socketPath) key)
        started <- getMonotonicTime
        (result, status) <- query session "SELECT pg_sleep(30)" []
        finished <- getMonotonicTime
        (either errorCode (const Nothing) result, status) `shouldBe` (Just "57014", TxIdle)
        finished - started `shouldSatisfy` (< 10)
        query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)

  describe "notifications" $ do
    it "delivers a notification from another session" $
      withSessionInfo $ \listener _ ->
        withSessionInfo $ \notifier notifierInfo -> do
          _ <- query listener "LISTEN events" []
          _ <- query notifier "NOTIFY events, 'hello'" []
          Notification sender channel payload <- awaitNotification listener
          (Just sender, channel, payload) `shouldBe` (processIdOf notifierInfo, "events", "hello")
    it "keeps notifications that arrive during statements, in order" $
      withSessionInfo $ \session _ -> do
        _ <- query session "LISTEN events" []
        _ <- query session "NOTIFY events, 'first'" []
        _ <- query session "NOTIFY events, 'second'" []
        payloads <- traverse (const (notificationPayload <$> awaitNotification session)) [1 :: Int, 2]
        payloads `shouldBe` ["first", "second"]
    it "throws when a listening session is terminated" $
      withSessionInfo $ \listener listenerInfo ->
        withSessionInfo $ \administrator _ -> do
          _ <- query listener "LISTEN events" []
          Just (Scalar pid) <- pure (processIdOf listenerInfo)
          _ <- query administrator "SELECT pg_terminate_backend($1::int)" [Just (Char8.pack (show pid))]
          awaitNotification listener `shouldThrow` connectionTerminated

  describe "transactions" $ around withSession $ do
    let createItems session = void (query session "CREATE TEMP TABLE tx_items (n int)" [])
        countItems = Tx.Statement "SELECT count(*) FROM tx_items" (const []) (Tx.singleRow (Tx.row @'[Int64] id)) False
        insertItem = Tx.Statement "INSERT INTO tx_items VALUES ($1)" (\value -> [Tx.Param Nothing TextFormat (Just (ParameterBytes value))]) Tx.noResult False
        items = Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.statement () countItems)
        failsWith check = either (\(Tx.QueryError _ failure) -> check failure) (const False)
    it "commits" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem) session `shouldReturn` Right ()
      items session `shouldReturn` Right 1
    it "rolls back when condemned" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem *> Tx.condemn) session `shouldReturn` Right ()
      items session `shouldReturn` Right 0
    it "rolls back after a failing statement, and reports it with its SQL" $ \session -> do
      createItems session
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem *> Tx.sql "SELECT 1 / 0") session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "SELECT 1 / 0" && failure `servedCode` "22012") (const False)
      items session `shouldReturn` Right 0
    it "begins with the isolation level and the mode" $ \session ->
      Tx.transactionNoRetry
        Tx.RepeatableRead
        Tx.Read
        (Tx.statement () (Tx.Statement "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')" (const []) (Tx.singleRow (Tx.row @'[Text, Text] (,))) False))
        session
        `shouldReturn` Right ("repeatable read", "on")
    it "reports a failure at commit" $ \session -> do
      void (query session "CREATE TEMP TABLE tx_unique (n int UNIQUE DEFERRABLE INITIALLY DEFERRED)" [])
      let insertTwice = Tx.sql "INSERT INTO tx_unique VALUES (1)" *> Tx.sql "INSERT INTO tx_unique VALUES (1)"
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Write insertTwice session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "COMMIT" && failure `servedCode` "23505") (const False)
    it "checks how many rows a result has" $ \session -> do
      let numbers :: Tx.Result a -> Int -> Tx.Transaction a
          numbers shape count = Tx.statement () (Tx.Statement ("SELECT n FROM generate_series(1, " <> Char8.pack (show count) <> ") n") (const []) shape False)
          number = Tx.row @'[Int32] id
          run transaction = Tx.transactionNoRetry Tx.ReadCommitted Tx.Read transaction session
      run (numbers (Tx.singleRow number) 0) >>= (`shouldSatisfy` failsWith (== Tx.UnexpectedAmountOfRows 0))
      run (numbers (Tx.singleRow number) 2) >>= (`shouldSatisfy` failsWith (== Tx.UnexpectedAmountOfRows 2))
      run (numbers (Tx.rowMaybe number) 0) `shouldReturn` Right Nothing
      run (numbers (Tx.rowMaybe number) 1) `shouldReturn` Right (Just 1)
      run (numbers (Tx.rowList number) 3) `shouldReturn` Right [1, 2, 3]
      run (numbers Tx.noResult 3) `shouldReturn` Right ()
    it "reports a row that doesn't decode" $ \session -> do
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.statement () (Tx.Statement "SELECT 'abc'::text" (const []) (Tx.singleRow (Tx.row @'[Int32] id)) False)) session
      result `shouldSatisfy` failsWith (\case Tx.RowFailed _ -> True; _ -> False)
    it "streams a column larger than the read buffer within the transaction" $ \session -> do
      let streamed accumulated current =
            case accumulated of
              Nothing -> Just <$> Tx.decodeScopedStreaming @'[Int32] current (\number stream -> (number,Tx.streamLength stream,) <$> drain stream 0)
              Just _ -> pure accumulated
          drain stream received =
            Tx.streamChunk stream >>= \chunk ->
              if BS.null chunk then pure received else drain stream (received + BS.length chunk)
      Tx.transactionNoRetry
        Tx.ReadCommitted
        Tx.Read
        (Tx.statement () (Tx.Statement "SELECT 7::int4, repeat('x', 1000000)" (const []) (Tx.foldRows streamed Nothing) False))
        session
        `shouldReturn` Right (Just (Right (7, 1000000, 1000000)))
    it "aborts when the transaction throws between statements, and leaves the session usable" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem *> liftIO (throwIO (ErrorCall "boom"))) session
        `shouldThrow` errorCall "boom"
      items session `shouldReturn` Right 0

  describe "snippets" $ around withSession $ do
    it "sends parameters the server reads as their types" $ \session -> do
      let snippet =
            "SELECT "
              <> Snippet.encoderAndParam (Encoders.nonNullable Encoders.unknown) "41"
              <> "::int + 1, array_to_string("
              <> Snippet.encoderAndParam (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))) ["a", "zażółć"]
              <> ", ','), "
              <> Snippet.encoderAndParam (Encoders.nonNullable Encoders.jsonbLazyBytes) "{\"a\": 1}"
              <> " ->> 'a', "
              <> Snippet.encoderAndParam (Encoders.nonNullable Encoders.jsonLazyBytes) "[1, 2]"
              <> "::text, "
              <> Snippet.encoderAndParam (Encoders.nullable Encoders.bytea) Nothing
              <> " IS NULL"
          statement = Snippet.dynamicallyParameterized snippet (Tx.singleRow (Tx.row @'[Int32, Text, Text, Text, Bool] (,,,,))) False
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.statement () statement) session
        `shouldReturn` Right (42, "a,zażółć", "1", "[1, 2]", True)

  describe "execute (failures)" $ around withSession $ do
    it "breaks the session when a parameter stream is shorter than declared" $ \session -> do
      source <- chunkSource ["short"]
      let short = Execution (Unprepared "SELECT $1::text" []) [] [Just (ParameterStream (ByteStream 10 source))] []
      try (execute session short (\() _ -> pure ()) ()) >>= \case
        Left (_ :: StreamLengthError) -> pure ()
        Right _ -> expectationFailure "expected a StreamLengthError"
      query session "SELECT 1" [] `shouldThrow` sessionUnusable
  where
    -- Streams the single column slowly, checking its bytes and that no chunk
    -- is larger than the read buffer, while sampling the live heap. The
    -- accumulators are strict, so they don't keep the chunks alive.
    consumeSlowly row = do
      Just (Column size) <- nextColumn row
      source <- columnStream row
      let loop !count !received !small !valid !peak = do
            chunk <- byteStreamNext source
            if BS.null chunk
              then pure (received, small, valid && size == received, peak)
              else do
                peak' <-
                  if count `mod` 256 == 255
                    then threadDelay 2000 *> (max peak <$> liveBytes)
                    else pure peak
                loop
                  (count + 1 :: Int)
                  (received + BS.length chunk)
                  (small && BS.length chunk <= 16384)
                  (valid && Char8.all (== 'x') chunk)
                  peak'
      baseline <- liveBytes
      (received, small, valid, peak) <- loop 0 0 True True baseline
      peak - baseline `shouldSatisfy` (< 8 * 1024 * 1024)
      pure (received, small, valid)

data Pair = Pair Int32 Text
  deriving (Eq, Show)

instance DecodeValue Pair where
  valueDecoder = composite (Pair <$> field <*> field)

-- | Runs a query with binary results and decodes its single row.
decodeSingle :: forall columns r. DecodeRow columns => Session 'Ready -> Text -> RowFunction columns (IO r) -> IO (Either DecodingError r)
decodeSingle session sql function = single session sql (\row -> decodeRow @columns row function)

decodeSingleStreaming :: forall columns r. DecodeRow columns => Session 'Ready -> Text -> RowFunction columns (ByteStream -> IO r) -> IO (Either DecodingError r)
decodeSingleStreaming session sql function = single session sql (\row -> decodeRowStreaming @columns row function)

single :: Session 'Ready -> Text -> (Row -> IO (Either DecodingError r)) -> IO (Either DecodingError r)
single session sql decode = do
  (result, _) <-
    execute
      session
      (Execution (Unprepared (encodeUtf8 sql) []) [] [] [BinaryFormat])
      (\_ row -> Just <$> decode row)
      Nothing
  case result of
    Right (Just decoded, _) -> pure decoded
    Right (Nothing, _) -> fail "the query returned no row"
    Left failure -> fail (show failure)

streamedLength :: ByteStream -> IO Int
streamedLength source = go 0
  where
    go !count = do
      chunk <- byteStreamNext source
      if BS.null chunk then pure count else go (count + BS.length chunk)

statement :: ByteString -> [Maybe ByteString] -> Execution
statement sql parameters = Execution (Unprepared sql []) [] (map (fmap ParameterBytes) parameters) []

query :: Session 'Ready -> ByteString -> [Maybe ByteString] -> IO (Either ServerError ([[Maybe ByteString]], ByteString), TransactionStatus)
query session sql parameters = collectRows session (statement sql parameters)

prepared :: Session 'Ready -> ByteString -> [Maybe ByteString] -> IO (Either ServerError ([[Maybe ByteString]], ByteString), TransactionStatus)
prepared session name parameters =
  collectRows session (Execution (Prepared name) [] (map (fmap ParameterBytes) parameters) [])

collectRows :: Session 'Ready -> Execution -> IO (Either ServerError ([[Maybe ByteString]], ByteString), TransactionStatus)
collectRows session execution = do
  (result, status) <- execute session execution (\rows row -> (: rows) <$> columns row) []
  pure (fmap (first reverse) result, status)

-- | All of a row's columns, copied out of the read buffer.
columns :: Row -> IO [Maybe ByteString]
columns row = do
  column <- nextColumn row
  case column of
    Nothing -> pure []
    Just NullColumn -> (Nothing :) <$> columns row
    Just (Column _) -> do
      value <- BS.copy <$> columnBytes row
      (Just value :) <$> columns row

-- | A source that gives the chunks, then the end.
chunkSource :: [ByteString] -> IO (IO ByteString)
chunkSource chunks = do
  remaining <- newIORef chunks
  pure $ do
    queued <- readIORef remaining
    case queued of
      [] -> pure BS.empty
      chunk : others -> chunk <$ writeIORef remaining others

-- | Live heap bytes after a major collection.
liveBytes :: IO Int
liveBytes = do
  performMajorGC
  fromIntegral . gcdetails_live_bytes . gc <$> getRTSStats

connectAndClose :: ConnectSettings -> Expectation
connectAndClose settings = do
  (session, _) <- connect settings
  close session

errorCode :: ServerError -> Maybe ByteString
errorCode (ServerError fields) = Just (diagnosticCode fields)

-- | Whether the statement failed on the server with the SQLSTATE.
servedCode :: Tx.StatementError -> ByteString -> Bool
servedCode (Tx.ServerFailed failure) code = errorCode failure == Just code
servedCode _ _ = False

-- | The diagnostics of a failed statement.
failed :: Either ServerError a -> Maybe Diagnostics
failed = either (\(ServerError fields) -> Just fields) (const Nothing)

rejectedWith :: ByteString -> ConnectionError -> Bool
rejectedWith code (ConnectionRejected failure) = errorCode failure == Just code
rejectedWith _ _ = False

authenticationError :: ConnectionError -> Bool
authenticationError (AuthenticationError _) = True
authenticationError _ = False

processIdOf :: ServerInfo -> Maybe ProcessId
processIdOf info = fst . unScalar <$> serverBackendKey info

connectionTerminated :: ConnectionError -> Bool
connectionTerminated (ConnectionTerminated _) = True
connectionTerminated _ = False

sessionUnusable :: ConnectionError -> Bool
sessionUnusable (SessionUnusable _) = True
sessionUnusable _ = False

rowMisuse :: ConnectionError -> Bool
rowMisuse (RowMisuse _) = True
rowMisuse _ = False

-- | Runs the action with the directory of a Unix-domain socket that accepts
-- connections but never answers.
withSilentServer :: (FilePath -> IO a) -> IO a
withSilentServer action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-silent-")) removeDirectoryRecursive $ \directory ->
    bracket (Socket.socket Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
      Socket.bind sock (Socket.SockAddrUnix (directory </> ".s.PGSQL.5432"))
      Socket.listen sock 4
      action directory

-- | Runs the action with the socket path of a new cluster, which has a role
-- for each password method.
withCluster :: (FilePath -> IO a) -> IO a
withCluster action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-")) removeDirectoryRecursive $ \directory -> do
    let dataDirectory = directory </> "data"
    run "initdb" ["--pgdata", dataDirectory, "--username", "postgres", "--auth", "trust", "--no-sync", "--no-locale", "--encoding", "UTF8"]
    writeFile (dataDirectory </> "pg_hba.conf") hbaRules
    appendFile (dataDirectory </> "postgresql.conf") $
      unlines ["listen_addresses = ''", "unix_socket_directories = '" <> directory <> "'"]
    bracket_
      (run "pg_ctl" ["--pgdata", dataDirectory, "--log", directory </> "postgres.log", "--wait", "start"])
      (run "pg_ctl" ["--pgdata", dataDirectory, "--mode", "immediate", "stop"])
      $ do
        run "psql" ["--host", directory, "--username", "postgres", "--dbname", "postgres", "--quiet", "--set", "ON_ERROR_STOP=1", "--command", rolesSql]
        action (directory </> ".s.PGSQL.5432")
  where
    hbaRules =
      unlines
        [ "local all native_pg_scram scram-sha-256",
          "local all native_pg_md5 md5",
          "local all native_pg_cleartext password",
          "local all all trust"
        ]
    rolesSql =
      unlines
        [ "SET password_encryption = 'scram-sha-256';",
          "CREATE ROLE native_pg_scram LOGIN PASSWORD 'scram secret';",
          "CREATE ROLE native_pg_cleartext LOGIN PASSWORD 'cleartext secret';",
          "SET client_min_messages = error;",
          "SET password_encryption = 'md5';",
          "CREATE ROLE native_pg_md5 LOGIN PASSWORD 'md5 secret';"
        ]
    run command arguments = do
      (exitCode, out, err) <- readProcessWithExitCode command arguments ""
      case exitCode of
        ExitSuccess -> pure ()
        ExitFailure _ -> fail (unwords (command : arguments) <> " failed:\n" <> out <> err)
