{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module OutputBufferSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Exception (evaluate)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.ByteString.Internal as BSI
import Data.Functor (($>))
import Data.IORef
import Data.Word (Word8)
import Control.Monad (forM_)
import Database.PostgreSQL.Internal.Execution (Execution (..), Statement (..), executionMessages)
import qualified Database.PostgreSQL.Internal.Protocol.Codec.Output as Output
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Database.PostgreSQL.Internal.Context
import Network.Streaming.PositiveInt (PositiveInt, positiveInt)
import Network.Streaming.Buffer (bufferMemory, bufferCapacity, newBuffer)
import Network.Streaming.BufferPool (PoolSettings (..))
import qualified Network.Streaming.BufferPool as BufferPool
import Database.PostgreSQL.Internal.Protocol (pattern Sync, pattern Parse, pattern Execute, pattern Query, serialize)
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import Network.Streaming.Source (Pulled (..))
import Network.Streaming.Watch (readyNow)
import PullSourceTestSupport (PullSource (..), noWatch)
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

  it "limits gathered stream chunks and preserves their order across partial writes" $ do
    remaining <- newIORef (map BS.singleton [1 .. 40])
    offers <- newIORef []
    received <- newIORef BS.empty
    let next = atomicModifyIORef' remaining $ \case
          [] -> ([], BS.empty)
          chunk : rest -> (rest, chunk)
        sender = Sender $ \chunks -> do
          modifyIORef' offers (length chunks :)
          prefix <- evaluate (BS.copy (BS.take 5 (BS.concat chunks)))
          modifyIORef' received (<> prefix)
          pure (BS.length prefix)
    channelSend (newChannel sender) (Output.stream (Output.ByteStream 40 next))
    readIORef received `shouldReturn` BS.pack [1 .. 40]
    offered <- reverse <$> readIORef offers
    take 1 offered `shouldBe` [16]
    all (<= 16) offered `shouldBe` True

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

  it "sizes and sends a list containing empty, copied and direct byte strings" $ do
    buffer <- newBuffer (positiveInt @7)
    received <- newIORef BS.empty
    let values = [BS.empty, "ab", BS.replicate 8192 67, "de", BS.empty]
        output = Output.outputs Output.bytes values <> Output.word16 0x0102
        sender = Sender $ \chunks -> do
          prefix <- evaluate (BS.copy (BS.take 5 (BS.concat chunks)))
          modifyIORef' received (<> prefix)
          pure (BS.length prefix)
    Output.outputLength output `shouldBe` 8198
    Output.outputCopied output `shouldBe` 6
    channelSendBuffered (newChannel sender) (outputBufferFrom buffer) output
    readIORef received `shouldReturn` (BS.concat values <> "\1\2")

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

  it "finishes exact-fit scalars across empty and zero-length stream boundaries" $ do
    buffer <- newBuffer (positiveInt @4)
    ordinaryPulls <- newIORef (0 :: Int)
    borrowedPulls <- newIORef (0 :: Int)
    received <- newIORef BS.empty
    calls <- newIORef (0 :: Int)
    let next = modifyIORef' ordinaryPulls (+ 1) $> BS.empty
        source = PullSource (modifyIORef' borrowedPulls (+ 1) $> Just Ended) noWatch
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

  it "resumes mixed fields and scoped streams in four bytes across blocked sends" $ do
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
        -- The chunk ends with the next pull, by when the link has taken it.
        source = PullSource pullChunk noWatch
        pullChunk = do
          count <- atomicModifyIORef' pulls (\n -> (n + 1, n))
          if count > 0
            then do
              accepted <- readIORef received
              BS.length accepted >= BS.length beforeBorrowed + 4 `shouldBe` True
              withForeignPtr (bufferMemory borrowed) (\pointer -> fillBytes pointer 0 4)
              pure (Just Ended)
            else do
              withForeignPtr (bufferMemory borrowed) $ \pointer ->
                BS.useAsCString "loan" $ \origin -> copyBytes pointer (castPtr origin) 4
              pure (Just (Pulled (BSI.BS (bufferMemory borrowed) 4)))
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

  describe "a request's pipeline" $ do
    forM_ [("sendContextOutput", sendContextOutput), ("queueContextOutput", queueContextOutput)] $ \(label, send) ->
      it ("goes to the link in one send through " <> label) $
        withTestPool (positiveInt @256) $ \access -> do
          sends <- newIORef []
          let link = Sender $ \chunks -> do
                accepted <- evaluate (BS.copy (BS.concat chunks))
                modifyIORef' sends (accepted :)
                pure (BS.length accepted)
          connection <- newConnection (newChannel link) 16
          withRequestContext connection access $ \context ->
            runContextOperation context (send context pipeline)
          reverse <$> readIORef sends `shouldReturn` [Output.outputByteString pipeline]

    it "arrives intact through a link that takes a few bytes at a time" $
      forM_ [1, 7, 4096] $ \accepts -> forM_ [pipeline, bigPipeline] $ \output ->
        withTestPool (positiveInt @256) $ \access -> do
          received <- newIORef BS.empty
          connection <- newConnection (newChannel (takingUpTo accepts received)) 16
          withRequestContext connection access $ \context ->
            runContextOperation context (sendContextOutput context output)
          readIORef received `shouldReturn` Output.outputByteString output

  describe "output continued after a direct value" $ do
    it "goes to the link in one send, from the supplied buffer after what came before" $ do
      buffer <- newBuffer (positiveInt @64)
      sends <- newIORef ([] :: [[(Ptr Word8, Int)]])
      let link = Sender $ \chunks -> do
            pieces <- mapM (\chunk -> BSU.unsafeUseAsCStringLen chunk (\(pointer, size) -> pure (castPtr pointer, size))) chunks
            modifyIORef' sends (pieces :)
            pure (sum (map BS.length chunks))
      channelSendBuffered (newChannel link) (outputBufferFrom buffer) directValue
      calls <- readIORef sends
      map (map snd) calls `shouldBe` [[10, 9000, 6]]
      withForeignPtr (bufferMemory buffer) $ \start -> case calls of
        [[(first, size), _, (second, _)]] -> (first, second) `shouldBe` (start, start `plusPtr` size)
        _ -> failTest "the output did not go in one send of three pieces"

    it "arrives intact through a link that takes a few bytes at a time" $
      forM_ [1, 3, 7, 64] $ \accepts ->
        forM_ [directValue, directValue <> mconcat (replicate 40 (serialize Sync))] $ \output -> do
          buffer <- newBuffer (positiveInt @64)
          received <- newIORef BS.empty
          channelSendBuffered (newChannel (takingUpTo accepts received)) (outputBufferFrom buffer) output
          readIORef received `shouldReturn` Output.outputByteString output
  where
    batch = mconcat (replicate 40 (serialize Sync))
    wire = BS.concat (replicate 40 "S\0\0\0\4")

-- | Framing (10 bytes), a direct value, framing again (6 bytes).
directValue :: Output.Output
directValue = serialize Sync <> serialize (Query (BS.replicate 9000 65)) <> serialize Sync

-- | A request's messages: Parse, Bind with parameters of the given sizes, Execute, and Sync.
request :: [Int] -> Output.Output
request sizes = executionMessages (Execution (Unprepared "SELECT $1" []) [] parameters []) <> serialize Sync
  where
    parameters = [Just (ParameterBytes (BS.replicate size 120)) | size <- sizes]

pipeline, bigPipeline :: Output.Output
pipeline = request [300, 1000, 4000]
-- | More bytes to copy than a request's framing buffer holds.
bigPipeline = request (replicate 12 8000)

-- | A link that takes at most so many bytes per send.
takingUpTo :: Int -> IORef BS.ByteString -> Sender
takingUpTo count received = Sender $ \chunks -> do
  prefix <- evaluate (BS.copy (BS.take count (BS.concat chunks)))
  modifyIORef' received (<> prefix)
  pure (BS.length prefix)

withTestPool :: PositiveInt -> (BufferPool.BufferAccess -> IO a) -> IO a
withTestPool size use = do
  waiting <- getAcquisition
  resources <- BufferPool.newBufferPool waiting PoolSettings
    { bufferSize = size,
      ordinaryBufferCount = positiveInt @1,
      reserveBufferCount = positiveInt @1,
      setupBufferCount = positiveInt @2
    }
  BufferPool.withRequestBuffers resources use

newtype Sender = Sender ([BS.ByteString] -> IO Int)

instance Link Sender where
  sendReady (Sender send) = send
  flushReady _ = pure True
  watchWritable _ = pure readyNow
  receiveReady _ _ _ = fail "unexpected receive"
  watchReadable _ = fail "unexpected read wait"
  closeLink _ = pure ()
