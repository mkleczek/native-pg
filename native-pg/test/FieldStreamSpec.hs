{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module FieldStreamSpec (spec) where

import Control.Exception (SomeException, evaluate, finally, throwIO, try)
import Control.Concurrent (forkFinally, killThread, threadDelay)
import Control.Concurrent.MVar
import Data.Maybe (isNothing)
import System.Timeout (timeout)
import Control.Monad (unless, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef
import Database.PostgreSQL.Buffer
import Database.PostgreSQL.Diagnostics (Diagnostics (..))
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Protocol (TransactionStatus (..))
import Database.PostgreSQL.Protocol.Codec (ByteStream (..))
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Transaction as Tx
import qualified Database.PostgreSQL.Pipeline as Pipeline
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr, minusPtr)
import Data.Word (Word8)
import Database.PostgreSQL.Transport
import Skeletest hiding (context)

spec :: Spec
spec = describe "borrowed field streams" $ do
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

  it "borrows Tx.streamChunks directly through its nullable scoped decoder" $
    withFields [Just payload] ready $ \context input -> do
      inside <- newIORef True
      received <- newIORef BS.empty
      let statement = Tx.Statement "SELECT field" (const [])
            (Tx.singleRow (Tx.scopedRow $ \current ->
              Tx.decodeScopedStreamingNullable @'[] current $ \stream ->
                maybe (failTest "unexpected NULL") (\source -> Tx.streamChunks source (collect input inside received)) stream)) True
          prepare _ _ = pure (Pipeline.PreparedAs "")
      Pipeline.runPipelineIn context (Just prepare) (Pipeline.statement () statement) `shouldReturn` Right ()
      readIORef inside `shouldReturn` True
      readIORef received `shouldReturn` payload

  it "keeps NULL handling available through scoped streaming decoders" $
    withFields [Nothing] ready $ \context _ -> do
      let statement = Tx.Statement "SELECT NULL" (const [])
            (Tx.singleRow (Tx.scopedRow $ \current -> Tx.decodeScopedStreamingNullable @'[] current (pure . maybe True (const False)))) True
          prepare _ _ = pure (Pipeline.PreparedAs "")
      Pipeline.runPipelineIn context (Just prepare) (Pipeline.statement () statement) `shouldReturn` Right True

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
    withFields [Just payload, Just payload] ready $ \context input -> do
      inside <- newIORef True
      received <- newIORef BS.empty
      let statement = Tx.Statement "SELECT metadata, body" (const [])
            (Tx.singleRow (Tx.scopedRow $ \current ->
              Tx.decodeScopedStreamingNullable @'[BS.ByteString] current $ \metadata stream -> do
                metadata `shouldBe` payload
                maybe (failTest "unexpected NULL") (\source -> Tx.streamChunks source (collect input inside received)) stream
                metadata `shouldBe` payload)) True
          prepare _ _ = pure (Pipeline.PreparedAs "")
      Pipeline.runPipelineIn context (Just prepare) (Pipeline.statement () statement) `shouldReturn` Right ()
      readIORef inside `shouldReturn` True
      readIORef received `shouldReturn` payload

  it "reports a COMMIT failure after the borrowed body was delivered" $ do
    let parse = message 49 ""
        bind = message 50 ""
        completed tag = message 67 (tag <> "\0")
        row = message 68 ("\0\1" <> number (BS.length payload) <> payload)
        response = parse <> bind <> completed "BEGIN"
          <> parse <> bind <> row <> completed "SELECT 1" <> ready
          <> parse <> bind <> lateError <> ready
    withResponse response $ \context _ -> do
      delivered <- newIORef False
      let statement = Tx.Statement "SELECT field" (const [])
            (Tx.singleRow (Tx.scopedRow $ \current ->
              Tx.decodeScopedStreamingNullable @'[] current $ \stream -> do
                maybe (failTest "unexpected NULL") (\source -> Tx.streamChunks source (const (pure ()))) stream
                writeIORef delivered True)) False
      result <- Tx.transactionNoRetryIn Tx.ReadCommitted Tx.Read (Tx.statement () statement) context
      readIORef delivered `shouldReturn` True
      case result of
        Left (Tx.QueryError query (Tx.ServerFailed (PG.ServerError fields))) -> do
          query `shouldBe` "COMMIT"
          diagnosticCode fields `shouldBe` "23514"
        _ -> failTest "deferred COMMIT error was lost after field EOF"

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
payload = BS.concat (map (\n -> BS.replicate 31 n) [65..72])

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
  input <- newBuffer 16
  output <- newBuffer 16
  remaining <- newIORef response
  let receive pointer size = do
        bytes <- readIORef remaining
        observe (BS.length response - BS.length bytes)
        let chunk = BS.take size bytes
        BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
        writeIORef remaining (BS.drop (BS.length chunk) bytes)
        pure (Just (BS.length chunk))
  connection <- newConnection (newChannel (Scripted receive)) 16
  withContext connection input output (\context -> action context input)

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
