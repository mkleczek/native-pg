{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module OutputBufferSpec (spec) where

import Control.Exception (evaluate, finally)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Functor (($>))
import Data.IORef
import qualified Data.ByteString.Internal as BSI
import qualified Network.Streaming as Streaming
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Control.Monad (forM_)
import qualified Database.PostgreSQL.Internal.Protocol.Codec.Output as Output
import Database.PostgreSQL.Internal.Context
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Buffer (bufferMemory, bufferCapacity, newBuffer)
import Database.PostgreSQL.Internal.Protocol (pattern Sync, pattern Parse, pattern Execute, pattern Query, serialize)
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (castPtr, plusPtr)
import Skeletest hiding (context)

spec :: Spec
spec = describe "caller-owned output storage" $ do
  it "serializes a large batch through the same small buffer" $ do
      buffer <- newBuffer (positiveInt @16)
      addresses <- newIORef True
      sent <- newIORef BS.empty
      let link = Sender $ \chunks -> do
            withForeignPtr (bufferMemory buffer) $ \start ->
              mapM_ (\chunk -> BSU.unsafeUseAsCStringLen chunk $ \(pointer, size) ->
                modifyIORef' addresses (&& (castPtr pointer >= start && castPtr pointer `plusPtr` size <= start `plusPtr` bufferCapacity buffer))) chunks
            accepted <- evaluate (BS.copy (BS.concat chunks))
            modifyIORef' sent (<> accepted)
            pure (sum (map BS.length chunks))
      channelSendBuffered (newChannel link) (outputBufferFrom buffer) batch
      readIORef sent `shouldReturn` wire
      readIORef addresses `shouldReturn` True

  it "sends through the current context's supplied workspace" $ do
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    supplied <- newIORef True
    sent <- newIORef BS.empty
    let link = Sender $ \chunks -> do
          withForeignPtr (bufferMemory output) $ \start ->
            mapM_ (\chunk -> BSU.unsafeUseAsCStringLen chunk $ \(pointer, size) ->
              modifyIORef' supplied (&& (castPtr pointer >= start && castPtr pointer `plusPtr` size <= start `plusPtr` bufferCapacity output))) chunks
          accepted <- evaluate (BS.copy (BS.concat chunks))
          modifyIORef' sent (<> accepted)
          pure (BS.length accepted)
    connection <- newConnection (newChannel link) 16
    withContext connection input output $ \context ->
      runContextOperation context (sendContextOutput context batch)
    readIORef sent `shouldReturn` wire
    readIORef supplied `shouldReturn` True

  it "preserves unsent bytes across partial writes" $ do
    buffer <- newBuffer (positiveInt @16)
    sent <- newIORef BS.empty
    let link = Sender $ \chunks -> do
          prefix <- evaluate (BS.copy (BS.take 3 (BS.concat chunks)))
          modifyIORef' sent (<> prefix)
          pure (BS.length prefix)
    channelSendBuffered (newChannel link) (outputBufferFrom buffer) batch
    readIORef sent `shouldReturn` wire
  it "matches contiguous serialization for complete messages at small capacities" $ do
    let output = serialize (Parse ("" :: BS.ByteString) "SELECT $1" [])
          <> serialize (Query (BS.replicate 9000 65))
          <> serialize (Execute ("" :: BS.ByteString) 17)
          <> serialize Sync
        expected = Output.outputByteString output
    forM_ [positiveInt @4, positiveInt @7, positiveInt @256] $ \capacity -> do
      buffer <- newBuffer capacity
      received <- newIORef BS.empty
      let sender = Sender $ \chunks -> do
            prefix <- evaluate (BS.copy (BS.take 3 (BS.concat chunks)))
            modifyIORef' received (<> prefix)
            pure (BS.length prefix)
      channelSendBuffered (newChannel sender) (outputBufferFrom buffer) output
      readIORef received `shouldReturn` expected

  it "resumes suspended fields in a different output buffer" $ do
    first <- newBuffer (positiveInt @16)
    second <- newBuffer (positiveInt @7)
    let output = Output.word32 0x01020304 <> Output.word32 0x05060708 <> Output.word16 0x090a
    withForeignPtr (bufferMemory first) $ \pointer -> do
      result <- Output.runWriter (Output.outputWriter output) pointer 4 0
      case result of
        Output.NeedSpace used rest -> do
          used `shouldBe` 4
          evaluate (BS.copy (BSI.BS (bufferMemory first) used)) `shouldReturn` "\1\2\3\4"
          -- Spare capacity makes a stale-pointer write observable without overrunning storage.
          fillBytes pointer 0 16
          withForeignPtr (bufferMemory second) $ \next -> do
            resumed <- Output.runWriter rest next 7 0
            case resumed of
              Output.Finished written -> do
                written `shouldBe` 6
                evaluate (BS.copy (BSI.BS (bufferMemory second) written)) `shouldReturn` "\5\6\7\8\9\10"
              _ -> failTest "the remaining fields did not finish in the new buffer"
          evaluate (BS.copy (BSI.BS (bufferMemory first) 16)) `shouldReturn` BS.replicate 16 0
        _ -> failTest "the four-byte buffer did not suspend before the next scalar"

  it "yields suspended bytes that outgrow a replacement buffer" $ do
    first <- newBuffer (positiveInt @32)
    second <- newBuffer (positiveInt @32)
    let bytes = "abcdefghijklmn"
        output = Output.word32 0x01020304 <> Output.bytes bytes
    withForeignPtr (bufferMemory first) $ \pointer -> do
      result <- Output.runWriter (Output.outputWriter output) pointer 16 0
      case result of
        Output.NeedSpace used rest -> do
          used `shouldBe` 4
          evaluate (BS.copy (BSI.BS (bufferMemory first) used)) `shouldReturn` "\1\2\3\4"
          fillBytes pointer 0 32
          withForeignPtr (bufferMemory second) $ \next -> do
            fillBytes next 0 32
            resumed <- Output.runWriter rest next 8 0
            case resumed of
              Output.Yield written (Output.UncopiedBytes direct) remaining -> do
                written `shouldBe` 0
                direct `shouldBe` bytes
                BSU.unsafeUseAsCString direct $ \actual ->
                  BSU.unsafeUseAsCString bytes $ \original -> actual `shouldBe` original
                Output.runWriter remaining next 8 0 >>= \case
                  Output.Finished final -> final `shouldBe` 0
                  _ -> failTest "the direct byte string retained pending output"
              _ -> failTest "the byte string did not bypass the smaller replacement buffer"
          evaluate (BS.copy (BSI.BS (bufferMemory first) 32)) `shouldReturn` BS.replicate 32 0
          evaluate (BS.copy (BSI.BS (bufferMemory second) 32)) `shouldReturn` BS.replicate 32 0
        _ -> failTest "the byte string did not suspend behind the scalar"

  xfail "scoped output implementation follows" $ it "finishes exact-fit scalars across empty and zero-length stream boundaries" $ do
    buffer <- newBuffer (positiveInt @4)
    ordinaryPulls <- newIORef (0 :: Int)
    borrowedPulls <- newIORef (0 :: Int)
    received <- newIORef BS.empty
    calls <- newIORef (0 :: Int)
    let next = modifyIORef' ordinaryPulls (+ 1) $> BS.empty
        source = Streaming.Source $ \use ->
          modifyIORef' borrowedPulls (+ 1) *> use BS.empty
        output = Output.bytes BS.empty
          <> Output.word32 0x01020304
          <> Output.stream (Output.ByteStream 0 next)
          <> Output.borrowedStream 0 source
          <> Output.bytes BS.empty
          <> Output.word16 0x0506 <> Output.word16 0x0708
        sender = Sender $ \chunks -> do
          offered <- evaluate (BS.copy (BS.concat chunks))
          BS.null offered `shouldBe` False
          call <- atomicModifyIORef' calls (\n -> (n + 1, n))
          let prefix = if even call then BS.empty else BS.take 1 offered
          modifyIORef' received (<> prefix)
          pure (BS.length prefix)
    channelSendBuffered (newChannel sender) (outputBufferFrom buffer) output
    readIORef received `shouldReturn` "\1\2\3\4\5\6\7\8"
    readIORef ordinaryPulls `shouldReturn` 1
    readIORef borrowedPulls `shouldReturn` 1
    channelSendBuffered (newChannel sender) (outputBufferFrom buffer) mempty
    readIORef calls `shouldReturn` 16

  xfail "scoped output implementation follows" $ it "resumes mixed fields and scoped streams in four bytes across blocked sends" $ do
    buffer <- newBuffer (positiveInt @4)
    borrowed <- newBuffer (positiveInt @4)
    ordinary <- newIORef ["stream", "!", ""]
    pulls <- newIORef (0 :: Int)
    received <- newIORef BS.empty
    calls <- newIORef (0 :: Int)
    let beforeBorrowed = "\1\2\3\4xyspill\5\6stream!\7"
        expected = beforeBorrowed <> "loan" <> "\8\9\10\11"
        next = atomicModifyIORef' ordinary $ \case
          [] -> ([], BS.empty)
          chunk : rest -> (rest, chunk)
        source = Streaming.Source $ \use -> do
          count <- atomicModifyIORef' pulls (\n -> (n + 1, n))
          if count > 0 then use BS.empty else do
            withForeignPtr (bufferMemory borrowed) $ \pointer ->
              BS.useAsCString "loan" $ \origin -> copyBytes pointer (castPtr origin) 4
            use (BSI.BS (bufferMemory borrowed) 4) `finally` do
              accepted <- readIORef received
              BS.length accepted >= BS.length beforeBorrowed + 4 `shouldBe` True
              withForeignPtr (bufferMemory borrowed) (\pointer -> fillBytes pointer 0 4)
        output = Output.word32 0x01020304 <> Output.bytes "xy" <> Output.bytes "spill" <> Output.word16 0x0506
          <> Output.stream (Output.ByteStream 7 next) <> Output.word8 7
          <> Output.borrowedStream 4 source <> Output.word32 0x08090a0b
        sender = Sender $ \chunks -> do
          offered <- evaluate (BS.copy (BS.concat chunks))
          accepted <- readIORef received
          offered `shouldBe` BS.take (BS.length offered) (BS.drop (BS.length accepted) expected)
          call <- atomicModifyIORef' calls (\n -> (n + 1, n))
          let count = call `mod` 3
          prefix <- evaluate (BS.copy (BS.take count offered))
          modifyIORef' received (<> prefix)
          pure (BS.length prefix)
    channelSendBuffered (newChannel sender) (outputBufferFrom buffer) output
    readIORef received `shouldReturn` expected
    readIORef pulls `shouldReturn` 2
  where
    batch = mconcat (replicate 40 (serialize Sync))
    wire = BS.concat (replicate 40 "S\0\0\0\4")

newtype Sender = Sender ([BS.ByteString] -> IO Int)

instance Link Sender where
  sendReady (Sender send) = send
  flushReady _ = pure True
  awaitWritable _ = pure ()
  receiveReady _ _ _ = fail "unexpected receive"
  awaitReadable _ = fail "unexpected read wait"
  awaitReadableOrWritable _ = fail "unexpected readiness wait"
  closeLink _ = pure ()
