{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module TransactionSpec (spec) where
import Control.Monad.IO.Unlift (withRunInIO)
import Control.Exception (Exception, ErrorCall (..), IOException, evaluate, finally, fromException, throwIO, try)
import Control.Concurrent (forkFinally, killThread, myThreadId)
import Control.Concurrent.MVar
import Data.Maybe (isNothing)
import Data.Functor.Const (Const (..))
import System.Timeout (timeout)
import Control.Monad (forM_, join, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Int (Int32)
import Data.List (sort)
import GHC.Conc (getAllocationCounter)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Buffer (Buffer, bufferMemory, bufferCapacity, newBuffer)
import Database.PostgreSQL.Internal.Context
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import qualified Database.PostgreSQL.Diagnostics as PG
import qualified Database.PostgreSQL.Session as Session
import Database.PostgreSQL.Streaming.Types (ByteStream (..), Parameter (..))
import qualified Database.PostgreSQL.Transaction as Tx
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr, minusPtr)
import Data.Word (Word8)
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Skeletest hiding (context)

newtype Body tx = Body (Maybe (Tx.Field tx))

bodyStatement :: Tx.Statement () Body
bodyStatement = Tx.Statement "SELECT body" (const []) (Tx.singleRow (Tx.streamingRow @'[] Body)) False

spec :: Spec
spec = describe "canonical transactions" $ do
  it "supports nested synchronous unlifting between protocol operations" $
    withResponse ordinary $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
        cursor <- Tx.pipeline (Tx.statement () bodyStatement)
        withRunInIO $ \outer -> outer $ do
          withRunInIO $ \inner -> do
            result <- inner (Tx.nextRow cursor)
            case result of
              Just (Body (Just field)) -> drain (inner (Tx.nextChunk field)) (const (pure ()))
              _ -> failTest "nested unlift lost its row"
        Tx.finishTransaction

  it "preserves a caught lifted exception and permits subsequent protocol work" $
    withResponse ordinary $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
        marker <- liftIO (newIORef ())
        withRunInIO $ \run -> do
          caught <- try @MarkerFailure (run (liftIO (throwIO (MarkerFailure marker))))
          case caught of
            Left (MarkerFailure actual) -> actual `shouldBe` marker
            Right () -> failTest "lift/unlift swallowed the original exception"
        _ <- Tx.pipeline (Tx.statement () bodyStatement)
        Tx.finishTransaction

  it "preserves unlifted exception identity through rollback and session reuse" $ do
    marker <- newIORef ()
    let rolledBack = command "BEGIN" <> rows [[Just payload]] <> readyTx <> command "ROLLBACK" <> ready
    withResponse (rolledBack <> ordinary) $ \context _ -> do
      caught <- try @MarkerFailure $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
        _ <- Tx.pipeline (Tx.statement () bodyStatement)
        withRunInIO $ \run -> run (liftIO (throwIO (MarkerFailure marker)))
      case caught of
        Left (MarkerFailure actual) -> actual `shouldBe` marker
        Right () -> failTest "rollback replaced the application exception"
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
        _ <- Tx.pipeline (Tx.statement () bodyStatement)
        Tx.finishTransaction

  it "keeps the owning protocol guard after rejected parameter callback reentry" $ do
    connection <- withResponse (command "BEGIN" <> command "SELECT 1" <> readyTx <> command "COMMIT" <> ready) $ \context _ -> do
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        attempted <- newIORef (0 :: Int)
        remaining <- newIORef "x"
        let rejectReentry = do
              result <- try @ConnectionError (run (Tx.sql "SELECT reentered"))
              case result of
                Left (SessionUnusable _) -> modifyIORef' attempted (+ 1)
                _ -> failTest "parameter callback reentered the protocol"
            source = ParameterStream (ByteStream 1 $ do
              bytes <- atomicModifyIORef' remaining (BS.empty,)
              unless (BS.null bytes) (rejectReentry *> rejectReentry)
              pure bytes)
            query = Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just source)]) Tx.noResult False
        _ <- run (Tx.pipeline (Tx.statement () query))
        run Tx.finishTransaction
        readIORef attempted `shouldReturn` 2
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` True

  it "guards parameter callbacks resumed while cleaning up an unfinished scope" $ do
    attempted <- newIORef (0 :: Int)
    remaining <- newIORef "x"
    connection <- withDeferredResponse (command "BEGIN" <> command "SELECT 1" <> readyTx <> command "ROLLBACK" <> ready) $ \context _ -> do
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let rejectReentry = do
              result <- try @ConnectionError (run (Tx.sql "SELECT reentered"))
              case result of
                Left (SessionUnusable _) -> modifyIORef' attempted (+ 1)
                _ -> failTest "cleanup parameter callback reentered the protocol"
            source = ParameterStream (ByteStream 1 $ do
              bytes <- atomicModifyIORef' remaining (BS.empty,)
              unless (BS.null bytes) (rejectReentry *> rejectReentry)
              pure bytes)
            query = Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just source)]) Tx.noResult False
        void (run (Tx.pipeline (Tx.statement () query)))
        readIORef attempted `shouldReturn` 0
      readIORef attempted `shouldReturn` 2
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` True

  it "keeps a caught parameter callback failure from making the session reusable" $ do
    opened <- newIORef Nothing
    outcome <- try @ConnectionError $ withResponse ordinary $ \context _ -> do
      writeIORef opened (Just (contextConnection context))
      marker <- newIORef ()
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let source = ParameterStream (ByteStream 1 (throwIO (MarkerFailure marker)))
            query = Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just source)]) Tx.noResult False
        result <- try @MarkerFailure (run (Tx.pipeline (Tx.statement () query) *> Tx.finishTransaction))
        case result of
          Left (MarkerFailure actual) -> actual `shouldBe` marker
          Right _ -> failTest "parameter callback failure was lost"
      pure (contextConnection context)
    case outcome of
      Left (SessionUnusable _) -> pure ()
      Left _ -> failTest "broken context raised an unexpected error"
      Right _ -> pure ()
    connection <- readIORef opened
    case connection of
      Just value -> Session.isUsable value `shouldReturn` False
      Nothing -> failTest "transaction context did not open"

  it "poisons a producer failure even when it uses a recoverable public exception type" $ do
    opened <- newIORef Nothing
    outcome <- try @ConnectionError $ withDeferredResponse ordinary $ \context _ -> do
      writeIORef opened (Just (contextConnection context))
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let source = ParameterStream (ByteStream 1 (throwIO (SessionUnusable "producer marker")))
            query = Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just source)]) Tx.noResult False
        _ <- run (Tx.pipeline (Tx.statement () query))
        failure <- try @ConnectionError (run Tx.finishTransaction)
        case failure of
          Left (SessionUnusable detail) -> detail `shouldBe` "producer marker"
          _ -> failTest "producer exception identity was changed"
        reuse <- try @ConnectionError (run Tx.condemn)
        case reuse of
          Left (SessionUnusable _) -> pure ()
          _ -> failTest "caught producer failure allowed another operation"
      pure (contextConnection context)
    case outcome of
      Left (SessionUnusable _) -> pure ()
      Left _ -> failTest "broken context raised an unexpected error"
      Right _ -> pure ()
    connection <- readIORef opened
    case connection of
      Just value -> Session.isUsable value `shouldReturn` False
      Nothing -> failTest "transaction context did not open"

  it "poisons a parse observer failure even when it uses a recoverable public exception type" $ do
    let response = command "BEGIN" <> command "SELECT 1" <> readyTx <> command "ROLLBACK" <> ready
        prepare _ _ = pure (Tx.PrepareAs "named" (throwIO (SessionUnusable "observer marker")))
        query = Tx.Statement "SELECT 1" (const []) Tx.noResult True
    connection <- withResponse response $ \context _ -> do
      Tx.runTransactionWith prepare Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        failure <- try @ConnectionError (run (Tx.pipeline (Tx.statement () query) *> Tx.finishTransaction))
        case failure of
          Left (SessionUnusable detail) -> detail `shouldBe` "observer marker"
          _ -> failTest "parse observer exception identity was changed"
        reuse <- try @ConnectionError (run Tx.condemn)
        case reuse of
          Left (SessionUnusable _) -> pure ()
          _ -> failTest "caught parse observer failure allowed another operation"
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` False

  it "finishes an empty scope without reading or writing the transport" $ do
    readCalls <- newIORef (0 :: Int)
    writes <- newIORef (0 :: Int)
    withResponseCallbacks (const (modifyIORef' readCalls (+ 1))) (const (modifyIORef' writes (+ 1))) "" $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run ->
        run Tx.finishTransaction
    readIORef readCalls `shouldReturn` 0
    readIORef writes `shouldReturn` 0

  it "preserves preparation, wire and cursor order across applicative association" $ do
    prepared <- newIORef []
    parsed <- newIORef []
    sent <- newIORef BS.empty
    let queries = ["SELECT left", "SELECT middle-left", "SELECT middle-right", "SELECT right"]
        values = ["first", "second", "third", "fourth"]
        prepare sql _ = do
          modifyIORef' prepared (<> [sql])
          pure (Tx.PrepareAs sql (modifyIORef' parsed (<> [sql])))
        query sql = Tx.statement () (Tx.Statement sql (const []) (Tx.singleRow (Tx.row @'[BS.ByteString] Const)) True)
        response = command "BEGIN" <> foldMap (rows . pure . pure . Just) values <> command "COMMIT" <> ready
    withResponseCallbacks (const (pure ())) (\chunks -> modifyIORef' sent (<> BS.concat chunks)) response $ \context _ ->
      Tx.runTransactionWith prepare Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        (left, middle, right) <- run $ Tx.finalPipeline $
          (,,) <$> query "SELECT left"
            <*> ((,) <$> query "SELECT middle-left" <*> query "SELECT middle-right")
            <*> query "SELECT right"
        readIORef prepared `shouldReturn` queries
        first <- run (Tx.singleRowValue left)
        second <- run (Tx.singleRowValue (fst middle))
        third <- run (Tx.singleRowValue (snd middle))
        fourth <- run (Tx.singleRowValue right)
        [first, second, third, fourth] `shouldBe` values
        run Tx.finishTransaction
    readIORef parsed `shouldReturn` queries
    bytes <- readIORef sent
    let wireQueries = ["BEGIN", "SELECT left", "SELECT middle-left", "SELECT middle-right", "SELECT right", "COMMIT\0"]
        positions = map (BS.length . fst . (`BS.breakSubstring` bytes)) wireQueries
    map (`BS.isInfixOf` bytes) wireQueries `shouldBe` replicate 6 True
    positions `shouldBe` sort positions

  it "isolates pending statements between empty and repeated pipeline plans" $ do
    sent <- newIORef BS.empty
    let response = command "BEGIN" <> readyTx <> rows [[Just "first"]] <> readyTx
          <> rows [[Just "second"]] <> readyTx <> command "COMMIT" <> ready
        query = Tx.statement () (Tx.Statement "SELECT repeated" (const []) (Tx.singleRow (Tx.row @'[BS.ByteString] Const)) False)
    withResponseCallbacks (const (pure ())) (\chunks -> modifyIORef' sent (<> BS.concat chunks)) response $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        run (Tx.pipeline (pure ()))
        first <- run (Tx.pipeline query)
        run (Tx.singleRowValue first) `shouldReturn` "first"
        run (Tx.pipeline (pure ()))
        second <- run (Tx.pipeline query)
        run (Tx.singleRowValue second) `shouldReturn` "second"
        run (Tx.finalPipeline (pure ()))
        run Tx.finishTransaction
    bytes <- readIORef sent
    let (_, first) = BS.breakSubstring "SELECT repeated" bytes
        (_, second) = BS.breakSubstring "SELECT repeated" (BS.drop (BS.length "SELECT repeated") first)
    BS.null first `shouldBe` False
    BS.null second `shouldBe` False
    BS.isInfixOf "SELECT repeated" (BS.drop (BS.length "SELECT repeated") second) `shouldBe` False

  it "scales allocation linearly for left-associated pipeline composition" $ do
    small <- pipelineAllocation 1000
    large <- pipelineAllocation 4000
    -- A fourfold batch leaves room for fixed costs while rejecting quadratic growth.
    unless (large < small * 7) $
      failTest ("pipeline allocation grew from " <> show small <> " to " <> show large <> " bytes")

  it "keeps bounded output allocation below six megabytes for a thousand statements" $ do
    output <- newBuffer (positiveInt @256)
    allocated <- pipelineAllocationWith output 1000
    unless (allocated < 6000000) $
      failTest ("bounded pipeline allocated " <> show allocated <> " bytes")

  it "collects owned and streamed results in one ordered pipeline" $
    withResponse (command "BEGIN" <> rows [[Just payload]] <> rows [[Just payload]] <> readyTx <> command "COMMIT" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let owned = Tx.Statement "SELECT owned" (const []) (Tx.row @'[BS.ByteString] Const) False
        (ownedCursor, streamedCursor) <- run (Tx.pipeline ((,) <$> Tx.statement () owned <*> Tx.statement () bodyStatement))
        run (Tx.rowList ownedCursor) `shouldReturn` [payload]
        streamed <- run (Tx.nextRow streamedCursor)
        received <- newIORef BS.empty
        case streamed of
          Just (Body (Just field)) -> drain (run (Tx.nextChunk field)) (\chunk -> evaluate (BS.copy chunk) >>= \stable -> modifyIORef' received (<> stable))
          _ -> failTest "missing streamed row"
        readIORef received `shouldReturn` payload
        run Tx.finishTransaction

  it "rolls back an owned collector's scope unless completion is explicit" $ do
    sent <- newIORef BS.empty
    let response = command "BEGIN" <> rows [[Just payload]] <> readyTx <> command "ROLLBACK" <> ready
        record chunks = modifyIORef' sent (<> BS.concat chunks)
    withResponseCallbacks (const (pure ())) record response $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let owned = Tx.Statement "SELECT owned" (const []) (Tx.singleRow (Tx.row @'[BS.ByteString] Const)) False
        cursor <- run (Tx.pipeline (Tx.statement () owned))
        run (Tx.singleRowValue cursor) `shouldReturn` payload
    bytes <- readIORef sent
    BS.isInfixOf "ROLLBACK\0" bytes `shouldBe` True
    BS.isInfixOf "COMMIT\0" bytes `shouldBe` False

  it "collects owned rows in order" $
    withResponse (command "BEGIN" <> rows [[Just "first"], [Just "second"]] <> readyTx <> command "COMMIT" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let owned = Tx.Statement "SELECT owned" (const []) (Tx.row @'[BS.ByteString] Const) False
        cursor <- run (Tx.pipeline (Tx.statement () owned))
        run (Tx.rowList cursor) `shouldReturn` ["first", "second"]
        run Tx.finishTransaction

  it "checks owned cardinality before allowing transaction completion" $
    withResponse (command "BEGIN" <> rows [[Just "first"], [Just "second"]] <> readyTx <> command "ROLLBACK" <> ready) $ \context _ -> do
      result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let owned = Tx.Statement "SELECT owned" (const []) (Tx.singleRow (Tx.row @'[BS.ByteString] Const)) False
        cursor <- run (Tx.pipeline (Tx.statement () owned))
        _ <- run (Tx.singleRowValue cursor)
        run Tx.finishTransaction
      case result of
        Left (PG.QueryError "SELECT owned" (PG.UnexpectedAmountOfRows 2)) -> pure ()
        _ -> failTest "owned collector accepted extra rows"

  it "reports extra owned rows before decoding their malformed columns" $
    forM_ [False, True] $ \optional ->
      withResponse (command "BEGIN" <> rows [[Just (number 7)], [Just "x"]] <> readyTx <> command "ROLLBACK" <> ready) $ \context _ -> do
        result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
          let shape = if optional then Tx.rowMaybe else Tx.singleRow
              owned = Tx.Statement "SELECT owned" (const []) (shape (Tx.row @'[Int32] Const)) False
          cursor <- run (Tx.pipeline (Tx.statement () owned))
          if optional
            then void (run (Tx.rowMaybeValue cursor))
            else void (run (Tx.singleRowValue cursor))
          run Tx.finishTransaction
        case result of
          Left (PG.QueryError "SELECT owned" (PG.UnexpectedAmountOfRows 2)) -> pure ()
          _ -> failTest "extra-row decoding hid the owned cardinality error"

  it "reports the same late statement failure through an owned collector" $
    withResponse (command "BEGIN" <> parse <> bind <> dataRow [Just payload] <> lateError <> readyFailed <> command "ROLLBACK" <> ready) $ \context _ -> do
      result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let owned = Tx.Statement "SELECT owned" (const []) (Tx.row @'[BS.ByteString] Const) False
        cursor <- run (Tx.pipeline (Tx.statement () owned))
        _ <- run (Tx.rowList cursor)
        run Tx.finishTransaction
      case result of
        Left (PG.QueryError "SELECT owned" (PG.ServerFailed _)) -> pure ()
        _ -> failTest "owned collector lost the late statement failure"

  it "returns a pipeline cursor before reading any results" $ do
    readCount <- newIORef (0 :: Int)
    withResponseObserved (const (modifyIORef' readCount (+ 1))) ordinary $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        readIORef readCount `shouldReturn` 0
        run Tx.finishTransaction

  it "pulls rows and borrowed chunks and finishes inside the caller callback" $ do
    owner <- myThreadId
    withResponseObserved (const (myThreadId >>= (`shouldBe` owner))) ordinary $ \context input ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        cursor <- run (Tx.pipeline (Tx.statement () bodyStatement))
        result <- run (Tx.nextRow cursor)
        field <- case result of
          Just (Body (Just value)) -> pure value
          _ -> failTest "missing body field"
        Tx.fieldLength field `shouldBe` BS.length payload
        inside <- newIORef True
        received <- newIORef BS.empty
        drain (run (Tx.nextChunk field)) (collect input inside received)
        readIORef inside `shouldReturn` True
        readIORef received `shouldReturn` payload
        run Tx.finishTransaction
        myThreadId `shouldReturn` owner

  it "preserves owned metadata while the body reuses input" $
    withResponse (command "BEGIN" <> rows [[Just payload, Just payload]] <> readyTx <> command "COMMIT" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let query = Tx.Statement "SELECT metadata, body" (const [])
              (Tx.streamingRow @'[BS.ByteString] Metadata) False
        cursor <- run (Tx.pipeline (Tx.statement () query))
        result <- run (Tx.nextRow cursor)
        case result of
          Just (Metadata metadata (Just field)) -> do
            drain (run (Tx.nextChunk field)) (const (pure ()))
            metadata `shouldBe` payload
          _ -> failTest "missing metadata row"
        run Tx.finishTransaction

  it "drains skipped earlier results before pulling a later cursor" $
    withResponse (command "BEGIN" <> rows [[Just payload]] <> rows [[Just "last"]] <> readyTx <> command "COMMIT" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        (_, cursor) <- run (Tx.pipeline ((,) <$> Tx.statement () bodyStatement <*> Tx.statement () bodyStatement))
        result <- run (Tx.nextRow cursor)
        case result of
          Just (Body (Just field)) -> run (Tx.nextChunk field) `shouldReturn` "last"
          _ -> failTest "missing later row"
        run Tx.finishTransaction

  it "rejects a field after its row advances" $
    withResponse (command "BEGIN" <> rows [[Just "first"], [Just "second"]] <> readyTx <> command "COMMIT" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let query = Tx.Statement "SELECT body" (const []) (Tx.streamingRow @'[] Body) False
        cursor <- run (Tx.pipeline (Tx.statement () query))
        first <- run (Tx.nextRow cursor)
        _ <- run (Tx.nextRow cursor)
        case first of
          Just (Body (Just field)) -> do
            stale <- try @ConnectionError (run (Tx.nextChunk field))
            case stale of Left (RowMisuse _) -> pure (); _ -> failTest "stale field was readable"
          _ -> failTest "missing first field"
        run Tx.finishTransaction

  it "checks extra-row cardinality while finishing an unconsumed result" $
    withResponse (command "BEGIN" <> rows [[Just "one"], [Just "two"]] <> readyTx <> command "ROLLBACK" <> ready) $ \context _ -> do
      result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        run Tx.finishTransaction
      case result of
        Left (PG.QueryError _ (PG.UnexpectedAmountOfRows 2)) -> pure ()
        _ -> failTest "extra row was accepted"

  it "reports a late server failure after field EOF" $
    withResponse (command "BEGIN" <> parse <> bind <> dataRow [Just payload] <> lateError <> readyFailed <> command "ROLLBACK" <> ready) $ \context _ -> do
      delivered <- newIORef False
      result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        cursor <- run (Tx.pipeline (Tx.statement () bodyStatement))
        row <- run (Tx.nextRow cursor)
        case row of
          Just (Body (Just field)) -> drain (run (Tx.nextChunk field)) (const (pure ()))
          _ -> failTest "missing body"
        writeIORef delivered True
        run Tx.finishTransaction
      readIORef delivered `shouldReturn` True
      case result of Left (PG.QueryError _ (PG.ServerFailed _)) -> pure (); _ -> failTest "late error was lost"

  it "reports COMMIT errors synchronously from finishTransaction" $
    withResponse (command "BEGIN" <> rows [[Just payload]] <> readyTx <> parse <> bind <> lateError <> readyFailed <> command "ROLLBACK" <> ready) $ \context _ -> do
      result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        run Tx.finishTransaction
      case result of Left (PG.QueryError "COMMIT" (PG.ServerFailed _)) -> pure (); _ -> failTest "COMMIT error was lost"

  it "observes a final pipeline ending without sending a second ending" $
    withResponse (command "BEGIN" <> rows [[Just payload]] <> command "COMMIT" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.finalPipeline (Tx.statement () bodyStatement))
        run Tx.finishTransaction

  it "rolls back a scope that returns without finishing" $
    withResponse (command "BEGIN" <> rows [[Just payload]] <> readyTx <> command "ROLLBACK" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run ->
        void (run (Tx.pipeline (Tx.statement () bodyStatement)))

  it "uses ROLLBACK after condemnation" $
    withResponse (command "BEGIN" <> rows [[Just payload]] <> readyTx <> command "ROLLBACK" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        run Tx.condemn
        run Tx.finishTransaction

  it "rejects new statements after an explicit finish" $
    withResponse ordinary $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        run Tx.finishTransaction
        result <- try @ConnectionError (run (Tx.sql "BEGIN"))
        case result of Left (SessionUnusable _) -> pure (); _ -> failTest "statement ran after finish"

  it "does not drain again after cancellation while waiting for the first row" $ do
    blocked <- newEmptyMVar
    gate <- newEmptyMVar
    stopped <- newEmptyMVar
    let observe _ = void (tryPutMVar blocked ()) *> readMVar gate
    worker <- forkFinally
      (withResponseObserved observe ordinary $ \context _ ->
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
          cursor <- run (Tx.pipeline (Tx.statement () bodyStatement))
          void (run (Tx.nextRow cursor)))
      (putMVar stopped)
    (do
      timeout 1000000 (readMVar blocked) `shouldReturn` Just ()
      killThread worker
      completed <- timeout 500000 (readMVar stopped)
      isNothing completed `shouldBe` False)
      `finally` killThread worker

  it "does not drain again after cancellation during a field refill" $ do
    blocked <- newEmptyMVar
    gate <- newEmptyMVar
    stopped <- newEmptyMVar
    let observe offset = when (offset >= 64) (void (tryPutMVar blocked ()) *> readMVar gate)
    worker <- forkFinally
      (withResponseObserved observe ordinary $ \context _ ->
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
          cursor <- run (Tx.pipeline (Tx.statement () bodyStatement))
          result <- run (Tx.nextRow cursor)
          case result of
            Just (Body (Just field)) -> drain (run (Tx.nextChunk field)) (const (pure ()))
            _ -> failTest "missing body field")
      (putMVar stopped)
    (do
      timeout 1000000 (readMVar blocked) `shouldReturn` Just ()
      killThread worker
      completed <- timeout 500000 (readMVar stopped)
      isNothing completed `shouldBe` False)
      `finally` killThread worker

  it "propagates cancellation while an unfinished scope is being rolled back" $ do
    blocked <- newEmptyMVar
    gate <- newEmptyMVar
    stopped <- newEmptyMVar
    let observe _ = void (tryPutMVar blocked ()) *> readMVar gate
    worker <- forkFinally
      (withResponseObserved observe ordinary $ \context _ ->
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run ->
          void (run (Tx.pipeline (Tx.statement () bodyStatement))))
      (putMVar stopped)
    (do
      timeout 1000000 (readMVar blocked) `shouldReturn` Just ()
      killThread worker
      completed <- timeout 500000 (readMVar stopped)
      case completed of
        Just (Left _) -> pure ()
        _ -> failTest "rollback cleanup swallowed cancellation")
      `finally` killThread worker

  it "abandons pending input after a callback IO failure" $ do
    readStarted <- newIORef False
    gate <- newEmptyMVar
    stopped <- newEmptyMVar
    connectionRef <- newIORef Nothing
    let observe _ = writeIORef readStarted True *> readMVar gate
    worker <- forkFinally
      (withResponseObserved observe ordinary $ \context _ -> do
        writeIORef connectionRef (Just (contextConnection context))
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
          _ <- run (Tx.finalPipeline (Tx.statement () bodyStatement))
          ioError (userError "HTTP sink disconnected"))
      (putMVar stopped)
    (do
      completed <- timeout 500000 (readMVar stopped)
      case completed of
        Just (Left failure) -> isNothing (fromException @IOException failure) `shouldBe` False
        _ -> failTest "callback IO failure waited for database input"
      readIORef readStarted `shouldReturn` False
      connection <- readIORef connectionRef
      case connection of
        Just value -> Session.isUsable value `shouldReturn` False
        Nothing -> failTest "callback did not start")
      `finally` killThread worker

  it "preserves a finished transaction after a later callback IO failure" $ do
    connection <- withResponse ordinary $ \context _ -> do
      result <- try @IOException $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        run Tx.finishTransaction
        ioError (userError "HTTP tail send failed")
      case result of Left _ -> pure (); Right () -> failTest "callback failure was lost"
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` True

  it "rolls back an ordinary callback exception before releasing the session" $ do
    connection <- withResponse (command "BEGIN" <> rows [[Just payload]] <> readyTx <> command "ROLLBACK" <> ready) $ \context _ -> do
      result <- try @ErrorCall $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        throwIO (ErrorCall "application callback failed")
      case result of
        Left (ErrorCall detail) -> detail `shouldBe` "application callback failed"
        Right () -> failTest "callback failure was lost"
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` True

  it "rolls back an ordinary lifted exception and reuses the same session" $ do
    let rolledBack = command "BEGIN" <> rows [[Just payload]] <> readyTx <> command "ROLLBACK" <> ready
    connection <- withResponse (rolledBack <> ordinary) $ \context _ -> do
      result <- try @ErrorCall $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run ->
        run $ do
          _ <- Tx.pipeline (Tx.statement () bodyStatement)
          liftIO (throwIO (ErrorCall "lifted application failure"))
      case result of
        Left (ErrorCall detail) -> detail `shouldBe` "lifted application failure"
        Right () -> failTest "lifted exception was lost"
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        run Tx.finishTransaction
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` True

  it "still breaks the session when a parameter source throws an ordinary exception" $ do
    opened <- newIORef Nothing
    let source = ParameterStream (ByteStream 8 (throwIO (ErrorCall "parameter source failed")))
        query = Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just source)]) Tx.noResult False
    result <- try @ErrorCall $ withResponse ordinary $ \context _ -> do
      writeIORef opened (Just (contextConnection context))
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () query))
        run Tx.finishTransaction
    case result of
      Left (ErrorCall detail) -> detail `shouldBe` "parameter source failed"
      Right () -> failTest "parameter source failure was lost"
    connection <- readIORef opened
    case connection of
      Just value -> Session.isUsable value `shouldReturn` False
      Nothing -> failTest "connection did not open"

  it "folds streamed rows by pulling their fields inside the same runner" $
    withResponse (command "BEGIN" <> rows [[Just "first"], [Just "second"]] <> readyTx <> command "COMMIT" <> ready) $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let query = Tx.Statement "SELECT bodies" (const []) (Tx.streamingRow @'[] Body) False
            step collected (Body value) = case value of
              Nothing -> liftIO (failTest "unexpected NULL")
              Just field -> do
                chunks <- chunksOf field
                pure (collected <> [BS.concat chunks])
            chunksOf field = do
              chunk <- Tx.nextChunk field
              if BS.null chunk then pure [] else do
                stable <- liftIO (evaluate (BS.copy chunk))
                (stable :) <$> chunksOf field
        cursor <- run (Tx.pipeline (Tx.statement () query))
        run (Tx.foldRows step [] cursor) `shouldReturn` ["first", "second"]
        run Tx.finishTransaction

  it "rejects a runner used on another thread without disturbing its owner" $
    withResponse ordinary $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        cursor <- run (Tx.pipeline (Tx.statement () bodyStatement))
        result <- newEmptyMVar
        _ <- forkFinally (run (Tx.nextRow cursor)) (putMVar result)
        failure <- readMVar result
        case failure of
          Left exception -> case fromException @ConnectionError exception of
            Just (SessionUnusable _) -> pure ()
            _ -> failTest "cross-thread action raised the wrong exception"
          Right _ -> failTest "another thread used the cursor"
        run Tx.finishTransaction

  it "keeps the session reusable after a server failure and rollback" $ do
    let failed = command "BEGIN" <> parse <> bind <> lateError <> readyFailed <> command "ROLLBACK" <> ready
    connection <- withResponse (failed <> ordinary) $ \context _ -> do
      result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        cursor <- run (Tx.pipeline (Tx.statement () bodyStatement))
        void (run (Tx.nextRow cursor))
      case result of Left _ -> pure (); Right () -> failTest "server failure was lost"
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        _ <- run (Tx.pipeline (Tx.statement () bodyStatement))
        run Tx.finishTransaction
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` True

  it "does not resurrect a caught transport failure" $ do
    connection <- withResponse "" $ \context _ -> do
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        cursor <- run (Tx.pipeline (Tx.statement () bodyStatement))
        result <- try @ConnectionError (run (Tx.nextRow cursor))
        case result of Left _ -> pure (); Right _ -> failTest "truncated transport was accepted"
      pure (contextConnection context)
    Session.isUsable connection `shouldReturn` False

  it "keeps lifted IO raw when an unlifted callback outlives its database scope" $ do
    calls <- newIORef (0 :: Int)
    join $ withResponse "" $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run ->
        pure (run (liftIO (modifyIORef' calls (+ 1))))
    readIORef calls `shouldReturn` 1

  it "expires an escaped runner" $
    withResponse "" $ \context _ -> do
      escaped <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run ->
        pure (run Tx.finishTransaction)
      result <- try @ConnectionError escaped
      case result of Left (SessionUnusable _) -> pure (); _ -> failTest "escaped runner remained live"

pipelineAllocation :: Int -> IO Integer
pipelineAllocation count = do
  output <- newBuffer (positiveInt @16)
  pipelineAllocationWith output count

pipelineAllocationWith :: Buffer -> Int -> IO Integer
pipelineAllocationWith output count = do
  let query = Tx.statement () (Tx.Statement "SELECT allocation" (const []) Tx.noResult False)
      plan = foldl' (*>) (pure ()) (replicate count (void query))
      response = command "BEGIN" <> BS.concat (replicate count (command "SELECT 1")) <> readyTx <> command "COMMIT" <> ready
  withResponseCallbacksBuffer output False (const (pure ())) (const (pure ())) response $ \context _ ->
    Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
      before <- getAllocationCounter
      run (Tx.pipeline plan)
      after <- getAllocationCounter
      run Tx.finishTransaction
      pure (toInteger (before - after))

newtype MarkerFailure = MarkerFailure (IORef ())
instance Show MarkerFailure where show _ = "MarkerFailure"
instance Exception MarkerFailure

data Metadata tx = Metadata BS.ByteString (Maybe (Tx.Field tx))

ordinary :: BS.ByteString
ordinary = command "BEGIN" <> rows [[Just payload]] <> readyTx <> command "COMMIT" <> ready

parse, bind, readyTx, readyFailed :: BS.ByteString
parse = message 49 ""
bind = message 50 ""
readyTx = message 90 "T"
readyFailed = message 90 "E"

command :: BS.ByteString -> BS.ByteString
command tag = parse <> bind <> message 67 (tag <> "\0")

rows :: [[Maybe BS.ByteString]] -> BS.ByteString
rows values = parse <> bind <> foldMap dataRow values <> message 67 "SELECT 1\0"

dataRow :: [Maybe BS.ByteString] -> BS.ByteString
dataRow columns = message 68 (BS.pack [0, fromIntegral (length columns)]
  <> foldMap (maybe (number (-1)) (\value -> number (BS.length value) <> value)) columns)

collect :: Buffer -> IORef Bool -> IORef BS.ByteString -> BS.ByteString -> IO ()
collect input inside received chunk = do
  offset <- BSU.unsafeUseAsCString chunk $ \pointer ->
    withForeignPtr (bufferMemory input) (pure . (castPtr pointer `minusPtr`))
  modifyIORef' inside (&& (offset >= 0 && offset + BS.length chunk <= bufferCapacity input))
  stable <- evaluate (BS.copy chunk)
  modifyIORef' received (<> stable)

drain :: IO BS.ByteString -> (BS.ByteString -> IO ()) -> IO ()
drain source sink = do
  chunk <- source
  unless (BS.null chunk) (sink chunk *> drain source sink)

payload :: BS.ByteString
payload = BS.concat (map (BS.replicate 31) [65..72])

withResponse :: BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withResponse = withResponseObserved (const (pure ()))

withResponseObserved :: (Int -> IO ()) -> BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withResponseObserved observe = withResponseCallbacks observe (const (pure ()))

withResponseCallbacks :: (Int -> IO ()) -> ([BS.ByteString] -> IO ()) -> BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withResponseCallbacks = withResponseCallbacksMode False

withDeferredResponse :: BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withDeferredResponse = withResponseCallbacksMode True (const (pure ())) (const (pure ()))

withResponseCallbacksMode :: Bool -> (Int -> IO ()) -> ([BS.ByteString] -> IO ()) -> BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withResponseCallbacksMode defer observe sent response action = do
  output <- newBuffer (positiveInt @16)
  withResponseCallbacksBuffer output defer observe sent response action

withResponseCallbacksBuffer :: Buffer -> Bool -> (Int -> IO ()) -> ([BS.ByteString] -> IO ()) -> BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withResponseCallbacksBuffer output defer observe sent response action = do
  input <- newBuffer (positiveInt @16)
  remaining <- newIORef response
  let receive pointer size = do
        bytes <- readIORef remaining
        observe (BS.length response - BS.length bytes)
        let chunk = BS.take size bytes
        BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
        writeIORef remaining (BS.drop (BS.length chunk) bytes)
        pure (Just (BS.length chunk))
  blocked <- newIORef (if defer then 2 else 0)
  connection <- newConnection (newChannel (Scripted receive sent blocked)) 16
  withContext connection input output (`action` input)

data Scripted = Scripted (Ptr Word8 -> Int -> IO (Maybe Int)) ([BS.ByteString] -> IO ()) (IORef Int)
instance Link Scripted where
  receiveReady (Scripted receive _ blocked) pointer size = do
    state <- readIORef blocked
    if state > 0 then pure Nothing else receive pointer size
  sendReady (Scripted _ sent blocked) chunks = do
    state <- atomicModifyIORef' blocked (\previous -> (if previous == 2 then 1 else 0, previous))
    if state == 2 then pure 0 else do
      let count = sum (map BS.length chunks)
      sent chunks
      pure count
  flushReady _ = pure True
  awaitReadable _ = fail "unexpected read wait"
  awaitWritable _ = pure ()
  awaitReadableOrWritable (Scripted _ _ blocked) = do
    state <- readIORef blocked
    if state > 0 then pure Writable else fail "unexpected combined wait"
  closeLink _ = pure ()

message :: Word8 -> BS.ByteString -> BS.ByteString
message tag body = BS.singleton tag <> number (4 + BS.length body) <> body

number :: Int -> BS.ByteString
number n = BS.pack [fromIntegral (n `div` 16777216), fromIntegral (n `div` 65536), fromIntegral (n `div` 256), fromIntegral n]

ready :: BS.ByteString
ready = message 90 "I"

lateError :: BS.ByteString
lateError = message 69 "SERROR\0C23514\0Mdeferred check failed\0\0"
