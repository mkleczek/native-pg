{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | Bounded buffers against a real PostgreSQL, ported to the v2 API: a connection is set up from the pool's setup
-- buffers and returns them however setting it up ends, and a request in flight works from the one primary buffer it
-- has (and the loans it borrows), also when that is tiny.
--
-- This replaces v1's @StreamingSetupSpec@ and @OperationContextSpec@. What those checked about the layout of v1's
-- storage (setup pairs, workspace sizes, owned metadata) is observed here from the outside: a connection that can be
-- set up again and again with two setup buffers has returned them, and a request that works with the smallest buffers
-- needs no more.
--
-- A case that the v2 code can't pass yet is marked with what it needs ('needs'): the marks come off as the part that
-- they wait for lands.
module BufferSpec (spec) where

import Control.Arrow (arr, first, returnA, second, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkFinally, killThread)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar)
import Control.Exception (bracket)
import Control.Monad (forM_)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as Lazy
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int32, Int64)
import Data.Maybe (isNothing)
import Data.Word (Word8)
import Database.PostgreSQL.Connection (ChannelBindingMode (..), ConnectSettings (..), Connection)
import qualified Database.PostgreSQL.Connection as PG
import Database.PostgreSQL.ConnectionString (Defaults (..), Host (..), fromConnectionString, parseConnectionString)
import Database.PostgreSQL.Errors
import Database.PostgreSQL.Tls (tlsClient)
import Database.PostgreSQL.Transaction (Rows (..))
import qualified Database.PostgreSQL.Transaction as Tx
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.BufferPool (BufferPoolSettings (..))
import Network.Streaming.Loans (Loan, borrow, loanBuffer, release)
import Network.Streaming.PositiveInt (positiveInt)
import Skeletest
import System.FilePath (takeDirectory)
import System.Timeout (timeout)
import TestPrograms

spec :: FilePath -> Socket.PortNumber -> Spec
spec socketPath port = describe "bounded buffers" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let tiny :: Program a -> IO a
      tiny = withBuffers smallBuffers runner
      unix = unixSettings socketPath
      scram = unix "native_pg_scram" (Just "scram secret")
      -- A request with room for one primary buffer and one borrowed one, of 256 bytes.
      sharing :: Program a -> IO a
      sharing = withBuffers (BufferPoolSettings (positiveInt @256) (positiveInt @1) (positiveInt @1) (positiveInt @2)) runner
      connects settings = session settings selectOne
      -- The setup buffers are two: a connection that was not returned them would be the last that could be set up.
      -- Each of the connections here is set up while the request holds its primary buffer.
      thrice settings = withinFor (secs 10) (steps (replicate 3 (connects settings `yields` Right [1])))

  -- Setting up a connection borrows from the setup buffers of the pool, which are apart from the request buffers. A wait
  -- for them is a wait of the arrow, so a cancellation reaches it.
  describe "connection setup" $ do
    it "authenticates with tiny setup buffers while the request's own is busy, and returns them" $
      tiny (thrice scram) `shouldReturn` Just ()

    needs "TLS" $ it "authenticates SCRAM over TLS with required binding and tiny setup buffers" $
      tiny (thrice ((tlsSettings port "native_pg_scram" (Just "scram secret")) {connectChannelBinding = ChannelBindingRequire})) `shouldReturn` Just ()

    -- A connection keeps nothing of the setup storage: what it learned there is its own, however the storage is used after.
    it "streams repeated operations after setup storage has been overwritten and loaned again" $
      tiny
        ( session ((unix "postgres" Nothing) {connectParameters = [("application_name", "pooled-setup")]}) $
            alsoOn ((unix "postgres" Nothing) {connectParameters = [("application_name", "overwriting")]}) $
              steps
                [ arr (lookup "application_name" . PG.serverParameters . PG.serverInfo . fst . fst) `yields` Just "pooled-setup",
                  onFirst (field100000 `yields` Right 100000),
                  onFirst (field100000 `yields` Right 100000)
                ]
        )
        `shouldReturn` ()

    it "returns the setup buffers after authentication fails" $
      tiny
        ( withinFor (secs 10) $
            steps
              [ (connectedTo (unix "native_pg_scram" (Just "wrong")) selectOne >>^ rejection) `yields` Just "28P01",
                connects scram `yields` Right [1]
              ]
        )
        `shouldReturn` Just ()

    -- v1 closed a TLS host that was unsuitable because its session was read-only, and tried the next, with the same pair
    -- of setup buffers. v2's connection string has one set of options for all its hosts, so the first host is unsuitable
    -- by another reason: the server rejects its role over TLS (pg_hba.conf), and the Unix-domain socket takes it.
    needs "TLS and host selection" $ it "closes a host that rejects the connection and tries the next one with the same setup buffers" $ do
      settings <- settingsOf ("host=127.0.0.1," <> Char8.pack (takeDirectory socketPath) <> " port=" <> Char8.pack (show port) <> " dbname=postgres user=native_pg_plaintext sslmode=require")
      tiny
        ( withinFor (secs 10) $
            steps
              [ session settings (arr (hostName . PG.connectedHost . fst)) `yields` takeDirectory socketPath,
                connects scram `yields` Right [1]
              ]
        )
        `shouldReturn` Just ()

    it "retries primary hosts when prefer-standby finds no standby" $ do
      settings <- settingsOf ("host=" <> Char8.pack (takeDirectory socketPath) <> " port=" <> Char8.pack (show port) <> " dbname=postgres user=postgres target_session_attrs=prefer-standby")
      tiny (withinFor (secs 10) (steps [session settings (arr (hostName . PG.connectedHost . fst)) `yields` takeDirectory socketPath, connects scram `yields` Right [1]]))
        `shouldReturn` Just ()

    needs "host selection" $ it "reports failed hosts in order and returns the setup buffers" $ do
      let missing = ["/missing-native-pg-first", "/missing-native-pg-second"]
      settings <- settingsOf ("host=" <> Char8.intercalate "," (map Char8.pack missing) <> " dbname=postgres user=postgres")
      tiny
        ( withinFor (secs 10) $
            steps
              [ (connectedTo settings done >>^ failedHosts) `yields` Just missing,
                connects scram `yields` Right [1]
              ]
        )
        `shouldReturn` Just ()

    needs "target_session_attrs" $ it "asks a server that doesn't report its attributes at startup, with setup buffers" $
      withOlderServer $ \scriptedPort -> do
        settings <-
          settingsOf ("host=127.0.0.1 user=postgres dbname=postgres sslmode=disable target_session_attrs=read-write port=" <> Char8.pack (show scriptedPort))
        tiny (session settings (arr (PG.serverParameters . PG.serverInfo . fst) &&& selectOne))
          `shouldReturn` ([], Right [1])

    -- v1: a thread timed out in the startup. A deadline of the program does the same, and it reaches the wait of the setup.
    it "returns the setup buffers when a silent startup is cancelled" $
      bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \listener -> do
        Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
        Socket.listen listener 1
        silentPort <- Socket.socketPort listener
        tiny
          ( steps
              [ (withinFor (millis 100) (connectedTo (PG.connectSettings (PG.TcpAddress "127.0.0.1" silentPort) "postgres") done) >>^ isNothing) `yields` True,
                withinFor (secs 10) (connects scram `yields` Right [1]) `yields` Just ()
              ]
          )
          `shouldReturn` ()

  -- A request works from one primary buffer, and what it borrows besides. Whatever it sends and receives passes
  -- through them, so the smallest ones are enough.
  needs "TLS" $ describe "a request's buffers" $ do
    -- v1 had a workspace of four bytes and "owned metadata" for what did not fit it. A column that v2 decodes in place has
    -- to fit the buffer, and one that does not is read as a field, so the smallest buffer that takes a request is the largest
    -- column that it decodes in place.
    it "authenticates over TLS and streams a field after a column, with buffers of 16 bytes" $ do
      total <- newIORef (0 :: Int)
      tiny
        ( session (tlsSettings port "native_pg_scram" (Just "scram secret")) $
            committed Tx.Read (Tx.statement (plain "SELECT 42::bigint, repeat('x', 256)" columnAndBody)) $ \result ->
              Tx.single result >>> second (measure >>> now (\(bytes, _) -> atomicModifyIORef' total (\n -> (n + bytes, ())))) >>^ fst
        )
        `shouldReturn` Right 42
      readIORef total `shouldReturn` 256

    -- The upload borrows a buffer for each piece it sends, from the loans of the request that also reads the response.
    it "shares one primary across a borrowed upload and a streamed TLS response" $ do
      remaining <- newIORef 16385
      held <- newIORef Nothing
      sharing
        ( session (tlsSettings port "native_pg_scram" (Just "scram secret")) $
            streamingFrom (\loans -> pure (LoanSource loans remaining held)) echoed
        )
        `shouldReturn` Right (BS.replicate 16385 120)
      readIORef remaining `shouldReturn` 0
      (isNothing <$> readIORef held) `shouldReturn` True

    -- A source that is slow makes the request wait for it, and the response is read meanwhile: one wait of the arrow, not two.
    it "uploads through a source that has nothing ready for every pull, waiting for it beside the socket" $ do
      let pieces = replicate 64 (BS.replicate 256 120) <> [BS.replicate 1 120]
      sharing
        ( session (tlsSettings port "native_pg_scram" (Just "scram secret")) $
            streaming (newSlowChunks (millis 1) pieces) echoed
        )
        `shouldReturn` Right (BS.replicate 16385 120)

    -- The deferred COMMIT fails after the body was delivered: the transaction's value is the failure of COMMIT, whichever
    -- shape the row has.
    forM_ [Shape "a field alone" "SELECT repeat('x', 256)" Tx.field, Shape "a column and a field" "SELECT 42::bigint, repeat('x', 256)" (Tx.skip &&& Tx.field >>^ snd)] $
      \(Shape label sql shape) ->
        it ("reports a deferred COMMIT failure after a streamed body was delivered, with " <> label) $ do
          delivered <- newIORef False
          result <-
            tiny
              ( session (tlsSettings port "native_pg_scram" (Just "scram secret")) $
                  committed
                    Tx.Write
                    ( (,,,)
                        <$> Tx.command "CREATE TEMP TABLE context_parent (id int PRIMARY KEY)"
                        <*> Tx.command "CREATE TEMP TABLE context_child (id int REFERENCES context_parent(id) DEFERRABLE INITIALLY DEFERRED)"
                        <*> Tx.command "INSERT INTO context_child VALUES (1)"
                        <*> Tx.statement (plain sql (Tx.any shape))
                    )
                    ( \(parent, child, inserted, body) ->
                        Tx.discard parent >>> Tx.discard child >>> Tx.discard inserted >>> Tx.single body >>> measure >>> now (const (writeIORef delivered True))
                    )
                    >>^ failedCommitCode
              )
          readIORef delivered `shouldReturn` True
          result `shouldBe` Just "23503"

    it "pulls a column and a streamed body and commits with its pipeline, and the connection is usable after" $
      tiny
        ( session (tlsSettings port "native_pg_scram" (Just "scram secret")) $
            steps
              [ transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT 42::bigint, repeat('x', 256)" columnAndBody)) lengthsOf
                  `yields` Right (42, 256, 256),
                query (plain "SELECT 42::bigint" (single @_ @Int64)) `yields` Right [42]
              ]
        )
        `shouldReturn` ()

    it "rolls back a COMMIT failure after the pull, and reuses the connection" $
      tiny
        ( session (tlsSettings port "native_pg_scram" (Just "scram secret")) $
            steps
              [ ( committed
                    Tx.Write
                    ( (,,,)
                        <$> Tx.command "CREATE TEMP TABLE pull_parent (id int PRIMARY KEY)"
                        <*> Tx.command "CREATE TEMP TABLE pull_child (id int REFERENCES pull_parent(id) DEFERRABLE INITIALLY DEFERRED)"
                        <*> Tx.command "INSERT INTO pull_child VALUES (1)"
                        <*> Tx.statement (plain "SELECT 42::bigint, repeat('x', 256)" columnAndBody)
                    )
                    (\(parent, child, inserted, body) -> Tx.discard parent >>> Tx.discard child >>> Tx.discard inserted >>> lengthsOf body)
                    >>^ failedCommitCode
                )
                  `yields` Just "23503",
                query (plain "SELECT 42::bigint" (single @_ @Int64)) `yields` Right [42]
              ]
        )
        `shouldReturn` ()

-- * Programs and sources of the cases

selectOne :: (Runtime arr) => OnConn arr (Either QueryError [Int32])
selectOne = query (plain "SELECT 1" single)

-- | How many bytes a field of 100000 has, in a transaction of its own.
field100000 :: (Runtime arr) => OnConn arr (Either QueryError Int)
field100000 =
  transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT repeat('x', 100000)" (Tx.any Tx.field))) (\result -> Tx.single result >>> measure >>^ fst)

-- | The declared length of the field, and the bytes that it gave, after a column.
lengthsOf :: (Runtime arr) => Tx.Result arr (Int64, Tx.Field) -> With Tx.Tx arr () (Int64, Int, Int)
lengthsOf result = Tx.single result >>> ((,,) <$> arr fst <*> arr (Tx.fieldLength . snd) <*> (snd ^>> measure >>^ fst))

-- | A statement that gives a column and a field.
columnAndBody :: (Runtime arr) => Rows arr (Int64, Tx.Field)
columnAndBody = Tx.any (Tx.column &&& Tx.field)

-- | A text of 16385 bytes that the source streams as the parameter, and that the server gives back as a field, copied out.
echoed :: (Runtime arr, Stream arr s) => arr (Connection, (Loans, s)) (Either QueryError ByteString)
echoed =
  transactOn
    Tx.ReadCommitted
    Tx.Read
    Tx.Final
    (Tx.statement (Tx.Statement "SELECT $1::text" (\source -> [streamedText 16385 source]) (Tx.any Tx.field)))
    (\result -> Tx.single result >>> copyOut)

-- | The shape of a row that has a field in the end, as a value that any arrow takes.
data Shape = Shape String ByteString (forall arr. (Runtime arr) => arr Tx.Columns Tx.Field)

rejection :: Either ConnectFailure a -> Maybe ByteString
rejection = \case
  Left (ConnectFailure [(_, ConnectionRejected (ServerError diagnostics))]) -> Just (diagnosticCode diagnostics)
  _ -> Nothing

-- | The names of the hosts that failed, in order.
failedHosts :: Either ConnectFailure a -> Maybe [String]
failedHosts = \case
  Left (ConnectFailure failures) -> Just (map (hostName . fst) failures)
  _ -> Nothing

failedCommitCode :: Either QueryError a -> Maybe ByteString
failedCommitCode = \case
  Left (QueryError "COMMIT" (ServerFailed (ServerError diagnostics))) -> Just (diagnosticCode diagnostics)
  _ -> Nothing

-- | The settings of a connection string, with the TLS client of native-pg-tls.
settingsOf :: ByteString -> IO ConnectSettings
settingsOf string = case parseConnectionString (Defaults "/tmp" Nothing) (const Nothing) (Just "postgres") string of
  Left problem -> fail problem
  Right parsed -> either fail (\settings -> pure settings {connectTlsClient = Just tlsClient}) (fromConnectionString parsed)

-- | The step, with a new source made of the loans of the request for its input.
streamingFrom :: (Runtime arr) => (Loans -> IO s) -> arr (Connection, (Loans, s)) b -> OnConn arr b
streamingFrom source step = now (\(connection, loans) -> (\s -> (connection, (loans, s))) <$> source loans) >>> step

-- | A source whose pieces are views of buffers that it borrows from the request's loans, one at a time: a loan is kept
-- until the next pull, as a view is valid until then. It gives 256 bytes of 120 at most for a piece.
--
-- The loans are borrowed and released as steps, and the view is of the loan's buffer, whose capacity is the 256 here.
data LoanSource = LoanSource Loans (IORef Int) (IORef (Maybe Loan))

instance (Runtime arr) => Stream arr LoanSource where
  next = giveBack >>> remainingOrEnd >>> (arr (const Nothing) ||| piece)
    where
      -- The loan of the piece before is given back.
      giveBack =
        now (\source@(LoanSource _ _ held) -> (source,) <$> atomicModifyIORef' held (Nothing,))
          >>> heldLoan
          ^>> (returnA ||| (second release >>^ fst))
      heldLoan (source, loan) = maybe (Left source) (Right . (source,)) loan
      remainingOrEnd =
        now (\source@(LoanSource _ left _) -> (\n -> if n == 0 then Left () else Right (source, n)) <$> readIORef left)
      -- A buffer is borrowed, filled as the view that is the piece, and kept until the next pull. A piece is the
      -- pool's buffer size of 256 bytes at most.
      piece =
        (\(source@(LoanSource loans _ _), n) -> (loans, (source, n)))
          ^>> first borrow
          >>> now
            ( \(loan, (LoanSource _ left held, n)) -> do
                let count = min n 256
                writeIORef held (Just loan)
                writeIORef left (n - count)
                pure (loan, (120, count))
            )
          >>> filled
          >>^ Just

-- | The first bytes of the loan's buffer, filled with the byte, as a view of it: valid until the loan is released.
filled :: (Effect arr) => arr (Loan, (Word8, Int)) ByteString
filled =
  now
    ( \(loan, (byte, count)) -> do
        let buffer = loanBuffer loan
        withForeignPtr (bufferMemory buffer) (\pointer -> fillBytes pointer byte count)
        pure (view buffer count)
    )

-- | A scripted server of before PostgreSQL 14: its startup omits the attributes that target_session_attrs asks
-- about, and it answers the SHOW query that follows. It serves one connection, and the case's action runs with its port.
withOlderServer :: (Socket.PortNumber -> IO a) -> IO a
withOlderServer action =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \listener -> do
    Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen listener 1
    scriptedPort <- Socket.socketPort listener
    served <- newEmptyMVar
    bracket
      (forkFinally (serve listener) (putMVar served))
      (\thread -> killThread thread >> readMVar served >> pure ())
      $ \_ -> do
        result <- timeout 5000000 (action scriptedPort) >>= maybe (failTest "scripted connection timed out") pure
        outcome <- timeout 1000000 (readMVar served)
        case outcome of
          Just (Right ()) -> pure result
          Just (Left failure) -> failTest (show failure)
          Nothing -> failTest "scripted server did not finish"
  where
    serve listener = bracket (fst <$> Socket.accept listener) Socket.close $ \socket -> do
      size <- messageSize <$> receiveExactly socket 4
      _ <- receiveExactly socket (size - 4)
      SocketBytes.sendAll socket (frame 'R' (bytes (Builder.word32BE 0)) <> frame 'Z' "I")
      frontendUntil socket 'S'
      let row = bytes $ Builder.word16BE 1 <> Builder.word32BE 3 <> Builder.byteString "off"
      SocketBytes.sendAll socket $
        frame '1' "" <> frame '2' ""
          <> frame 'D' row <> frame 'C' "SHOW\0" <> frame 'Z' "I"
      frontendUntil socket 'X'
    frontendUntil socket wanted = do
      kind <- receiveExactly socket 1
      size <- messageSize <$> receiveExactly socket 4
      _ <- receiveExactly socket (size - 4)
      if kind == Char8.singleton wanted then pure () else frontendUntil socket wanted
    frame kind payload = Char8.singleton kind <> bytes (Builder.word32BE (fromIntegral (BS.length payload + 4))) <> payload
    bytes = Lazy.toStrict . Builder.toLazyByteString
    messageSize = BS.foldl' (\total byte -> total * 256 + fromIntegral byte) 0

receiveExactly :: Socket.Socket -> Int -> IO ByteString
receiveExactly socket = go []
  where
    go chunks 0 = pure (BS.concat (reverse chunks))
    go chunks remaining = do
      chunk <- SocketBytes.recv socket remaining
      if BS.null chunk
        then fail "scripted peer closed early"
        else go (chunk : chunks) (remaining - BS.length chunk)
