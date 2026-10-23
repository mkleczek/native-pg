{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module FieldStreamSpec (spec) where

import Control.Monad.IO.Unlift (withRunInIO)
import Control.Exception (SomeException, evaluate, finally, throwIO, try)
import Control.Concurrent (forkFinally, killThread, threadDelay)
import Control.Concurrent.MVar
import Data.Maybe (isNothing)
import System.Timeout (timeout)
import Control.Monad (unless, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Buffer (Buffer, bufferMemory, bufferCapacity, newBuffer)
import Database.PostgreSQL.Diagnostics (Diagnostics (..))
import Database.PostgreSQL.Internal.Context
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import Database.PostgreSQL.Internal.Protocol (TransactionStatus (..))
import Database.PostgreSQL.Internal.Protocol.Codec (ByteStream (..))
import qualified Database.PostgreSQL.Internal.Execution as PG
import qualified Database.PostgreSQL.Internal.Row as PG
import qualified Database.PostgreSQL.Diagnostics as PG
import qualified Database.PostgreSQL.Transaction as Tx
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr, minusPtr)
import Data.Word (Word8)
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Skeletest hiding (context)

spec :: Spec
spec = describe "borrowed field streams" $ do
  it "finishes an empty row at the following message boundary" $
    withFields [] ready $ \context _ -> do
      result <- PG.executeWithContext context execution (\count row -> do
        PG.rowColumnCount row `shouldBe` 0
        PG.nextColumn row `shouldReturn` Nothing
        pure (count + 1 :: Int)) 0
      result `shouldBe` (Right (1, "SELECT 1"), TxIdle)

  it "skips unread columns without consuming the following message" $
    withFields [Just payload, Nothing, Just "tail"] ready $ \context _ -> do
      result <- PG.executeWithContext context execution (\_ row -> do
        PG.rowColumnCount row `shouldBe` 3
        PG.nextColumn row `shouldReturn` Just (PG.Column (BS.length payload))
        pure ()) ()
      result `shouldBe` (Right ((), "SELECT 1"), TxIdle)

  it "borrows columnChunks directly from the supplied input" $
    withFields [Just payload] ready $ \context input -> do
      inside <- newIORef True
      received <- newIORef BS.empty
      result <- PG.executeWithContext context execution (\_ row -> do
        _ <- PG.nextColumn row
        PG.columnChunks row (collect input inside received)) ()
      result `shouldBe` (Right ((), "SELECT 1"), TxIdle)
      readIORef inside `shouldReturn` True
      readIORef received `shouldReturn` payload

  it "borrows a Source until its next pull or scope end" $
    withFields [Just payload] ready $ \context input -> do
      inside <- newIORef True
      received <- newIORef BS.empty
      result <- PG.executeWithContext context execution (\_ row -> do
        _ <- PG.nextColumn row
        PG.withColumnStream row $ \stream -> do
          byteStreamLength stream `shouldBe` BS.length payload
          drain (byteStreamNext stream) (collect input inside received)) ()
      result `shouldBe` (Right ((), "SELECT 1"), TxIdle)
      readIORef inside `shouldReturn` True
      readIORef received `shouldReturn` payload

  it "expires an escaped Source even before the row callback returns" $
    withFields [Just "abc"] ready $ \context _ -> do
      result <- PG.executeWithContext context execution (\_ row -> do
        _ <- PG.nextColumn row
        next <- PG.withColumnStream row (pure . byteStreamNext)
        failure <- try @ConnectionError next
        case failure of Left (RowMisuse _) -> pure (); _ -> failTest "escaped Source remained readable") ()
      result `shouldBe` (Right ((), "SELECT 1"), TxIdle)

  it "rejects row advancement during a borrowed callback" $
    withFields [Just payload] ready $ \context _ -> do
      _ <- PG.executeWithContext context execution (\_ row -> do
        _ <- PG.nextColumn row
        PG.columnChunks row $ \_ -> do
          failure <- try @ConnectionError (PG.nextColumn row)
          case failure of Left (RowMisuse _) -> pure (); _ -> failTest "row advanced during borrowed callback") ()
      pure ()

  it "keeps a caught borrowed callback failure terminal" $
    withFields [Just payload] ready $ \context _ -> do
      result <- try @ConnectionError $ PG.executeWithContext context execution (\_ row -> do
        _ <- PG.nextColumn row
        _ <- try @SomeException (PG.columnChunks row (\_ -> throwIO (userError "consumer failed")))
        pure ()) ()
      case result of Left (RowMisuse _) -> pure (); _ -> failTest "failed callback left a recoverable row"

  it "invalidates a row after a caught materialization failure" $
    withResponse (message 50 "" <> BS.singleton 68 <> number 106 <> "\0\1" <> number 100 <> "short") $ \context _ -> do
      result <- try @ConnectionError $ PG.executeWithContext context execution (\_ row -> do
        _ <- PG.nextColumn row
        _ <- try @ConnectionError (PG.columnBytes row)
        pure ()) ()
      case result of Left (RowMisuse _) -> pure (); _ -> failTest "failed parser left a recoverable row"

  it "reports protocol failure after field EOF instead of treating EOF as query success" $
    withFields [Just payload] (lateError <> ready) $ \context _ -> do
      delivered <- newIORef False
      result <- PG.executeWithContext context execution (\_ row -> do
        _ <- PG.nextColumn row
        PG.withColumnStream row $ \stream ->
          drain (byteStreamNext stream) (const (pure ()))
        writeIORef delivered True) ()
      readIORef delivered `shouldReturn` True
      case fst result of
        Left (PG.ServerError fields) -> diagnosticCode fields `shouldBe` "23514"
        Right _ -> failTest "late server error was lost after field EOF"

  it "borrows canonical field chunks through its nullable streaming decoder" $
    withTransactionFields [Just payload] $ \context input ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        inside <- newIORef True
        received <- newIORef BS.empty
        let statement = Tx.Statement "SELECT field" (const []) (Tx.singleRow (Tx.streamingRow @'[] Streamed)) False
        cursor <- run (Tx.pipeline (Tx.statement () statement))
        result <- run (Tx.nextRow cursor)
        case result of
          Just (Streamed (Just field)) -> drain (run (Tx.nextChunk field)) (collect input inside received)
          _ -> failTest "unexpected NULL"
        run Tx.finishTransaction
        readIORef inside `shouldReturn` True
        readIORef received `shouldReturn` payload

  it "keeps NULL handling available through streaming decoders" $
    withTransactionFields [Nothing] $ \context _ ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let statement = Tx.Statement "SELECT NULL" (const []) (Tx.singleRow (Tx.streamingRow @'[] Streamed)) False
        cursor <- run (Tx.pipeline (Tx.statement () statement))
        result <- run (Tx.nextRow cursor)
        case result of
          Just (Streamed Nothing) -> pure ()
          _ -> failTest "expected NULL"
        run Tx.finishTransaction

  it "cancels and joins an unfinished field pull before closing its scope" $ do
    blocked <- newEmptyMVar
    stopped <- newEmptyMVar
    entered <- newIORef False
    child <- newIORef Nothing
    let observe offset = when (offset >= 32) $ do
          first <- atomicModifyIORef' entered (\previous -> (True, not previous))
          when first (putMVar blocked () *> threadDelay 10000000)
        cleanup = readIORef child >>= maybe (pure ()) killThread
    (do
      result <- withFieldsObserved observe [Just payload] ready $ \context _ ->
        try @ConnectionError $ PG.executeWithContext context execution (\_ row -> do
          _ <- PG.nextColumn row
          PG.withColumnStream row $ \source -> do
            thread <- forkFinally (drain (byteStreamNext source) (const (pure ()))) (putMVar stopped)
            writeIORef child (Just thread)
            timeout 1000000 (readMVar blocked) `shouldReturn` Just ()) ()
      case result of Left (RowMisuse _) -> pure (); _ -> failTest "abandoned pull left a usable row"
      joined <- timeout 1000000 (readMVar stopped)
      isNothing joined `shouldBe` False) `finally` cleanup

  it "materializes an owned column larger than the supplied block" $
    withFields [Just payload] ready $ \context _ -> do
      result <- PG.executeWithContext context execution (\values row -> do
        _ <- PG.nextColumn row
        value <- PG.columnBytes row
        pure (value : values)) []
      result `shouldBe` (Right ([payload], "SELECT 1"), TxIdle)

  it "assembles leading owned metadata while borrowing the selected body" $
    withTransactionFields [Just payload, Just payload] $ \context input ->
      Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        inside <- newIORef True
        received <- newIORef BS.empty
        let statement = Tx.Statement "SELECT metadata, body" (const []) (Tx.singleRow (Tx.streamingRow @'[BS.ByteString] Metadata)) False
        cursor <- run (Tx.pipeline (Tx.statement () statement))
        result <- run (Tx.nextRow cursor)
        case result of
          Just (Metadata metadata (Just field)) -> do
            metadata `shouldBe` payload
            drain (run (Tx.nextChunk field)) (collect input inside received)
            metadata `shouldBe` payload
          _ -> failTest "unexpected NULL"
        run Tx.finishTransaction
        readIORef inside `shouldReturn` True
        readIORef received `shouldReturn` payload

  it "reports a COMMIT failure after the borrowed body was delivered" $ do
    let row = message 68 ("\0\1" <> number (BS.length payload) <> payload)
        response = command "BEGIN" <> message 49 "" <> message 50 "" <> row <> message 67 "SELECT 1\0" <> message 90 "T"
          <> message 49 "" <> message 50 "" <> lateError <> message 90 "E" <> command "ROLLBACK" <> ready
    withResponse response $ \context _ -> do
      delivered <- newIORef False
      result <- try @PG.QueryError $ Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ withRunInIO $ \run -> do
        let statement = Tx.Statement "SELECT field" (const []) (Tx.singleRow (Tx.streamingRow @'[] Streamed)) False
        cursor <- run (Tx.pipeline (Tx.statement () statement))
        current <- run (Tx.nextRow cursor)
        case current of
          Just (Streamed (Just field)) -> drain (run (Tx.nextChunk field)) (const (pure ()))
          _ -> failTest "unexpected NULL"
        writeIORef delivered True
        run Tx.finishTransaction
      readIORef delivered `shouldReturn` True
      case result of
        Left (Tx.QueryError query (Tx.ServerFailed (PG.ServerError fields))) -> do
          query `shouldBe` "COMMIT"
          diagnosticCode fields `shouldBe` "23514"
        _ -> failTest "deferred COMMIT error was lost after field EOF"

newtype Streamed tx = Streamed (Maybe (Tx.Field tx))
data Metadata tx = Metadata BS.ByteString (Maybe (Tx.Field tx))

command :: BS.ByteString -> BS.ByteString
command tag = message 49 "" <> message 50 "" <> message 67 (tag <> "\0")

withTransactionFields :: [Maybe BS.ByteString] -> (Context -> Buffer -> IO a) -> IO a
withTransactionFields columns =
  let body = BS.pack [0, fromIntegral (length columns)]
        <> foldMap (maybe (number (-1)) (\value -> number (BS.length value) <> value)) columns
  in withResponse (command "BEGIN" <> message 49 "" <> message 50 "" <> message 68 body <> message 67 "SELECT 1\0" <> message 90 "T" <> command "COMMIT" <> ready)

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

execution :: PG.Execution
execution = PG.Execution (PG.Prepared "") [] [] []

withFields :: [Maybe BS.ByteString] -> BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withFields = withFieldsObserved (const (pure ()))

withFieldsObserved :: (Int -> IO ()) -> [Maybe BS.ByteString] -> BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withFieldsObserved observe columns trailing action = do
  let count = length columns
      row = BS.pack [fromIntegral (count `div` 256), fromIntegral count]
        <> foldMap (maybe (number (-1)) (\value -> number (BS.length value) <> value)) columns
      response = message 50 "" <> message 68 row <> message 67 "SELECT 1\0" <> trailing
  withResponseObserved observe response action

withResponse :: BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withResponse = withResponseObserved (const (pure ()))

withResponseObserved :: (Int -> IO ()) -> BS.ByteString -> (Context -> Buffer -> IO a) -> IO a
withResponseObserved observe response action = do
  input <- newBuffer (positiveInt @16)
  output <- newBuffer (positiveInt @16)
  remaining <- newIORef response
  let receive pointer size = do
        bytes <- readIORef remaining
        observe (BS.length response - BS.length bytes)
        let chunk = BS.take size bytes
        BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
        writeIORef remaining (BS.drop (BS.length chunk) bytes)
        pure (Just (BS.length chunk))
  connection <- newConnection (newChannel (Scripted receive)) 16
  withContext connection input output (`action` input)

newtype Scripted = Scripted (Ptr Word8 -> Int -> IO (Maybe Int))
instance Link Scripted where
  receiveReady (Scripted receive) = receive
  sendReady _ = pure . sum . map BS.length
  flushReady _ = pure True
  awaitReadable _ = fail "unexpected read wait"
  awaitWritable _ = pure ()
  awaitReadableOrWritable _ = fail "unexpected combined wait"
  closeLink _ = pure ()

message :: Word8 -> BS.ByteString -> BS.ByteString
message tag body = BS.singleton tag <> number (4 + BS.length body) <> body

number :: Int -> BS.ByteString
number n = BS.pack [fromIntegral (n `div` 16777216), fromIntegral (n `div` 65536), fromIntegral (n `div` 256), fromIntegral n]

ready :: BS.ByteString
ready = message 90 "I"

lateError :: BS.ByteString
lateError = message 69 "SERROR\0C23514\0Mdeferred check failed\0\0"
