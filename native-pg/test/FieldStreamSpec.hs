{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module FieldStreamSpec (spec) where

import Control.Exception (SomeException, evaluate, throwIO, try)
import Control.Monad (unless)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef
import Database.PostgreSQL.Buffer
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Protocol (TransactionStatus (..))
import Database.PostgreSQL.Protocol.Codec (ByteStream (..))
import qualified Database.PostgreSQL.Session as PG
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
    withFields [Just payload] ready $ \context _ -> do
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
        stream <- PG.columnStream row
        drain (byteStreamNext stream) (const (pure ()))
        writeIORef delivered True) ()
      readIORef delivered `shouldReturn` True
      case fst result of Left _ -> pure (); Right _ -> failTest "late server error was lost after field EOF"

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
withFields columns trailing action = do
  input <- newBuffer 16
  output <- newBuffer 16
  let count = length columns
      row = BS.pack [fromIntegral (count `div` 256), fromIntegral count]
        <> foldMap (maybe (number (-1)) (\value -> number (BS.length value) <> value)) columns
      response = message 50 "" <> message 68 row <> message 67 "SELECT 1\0" <> trailing
  remaining <- newIORef response
  let receive pointer size = do
        bytes <- readIORef remaining
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
