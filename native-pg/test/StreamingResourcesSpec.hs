{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
module StreamingResourcesSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Exception (IOException, bracket, try)
import Control.Monad (forM_)
import Data.Functor (($>))
import Data.IORef
import Data.Maybe (isNothing)
import qualified Database.PostgreSQL.Internal.Context as Context
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import Database.PostgreSQL.Internal.Protocol.Codec (readInputSome, withInputSome)
import qualified Database.PostgreSQL.Internal.Protocol.Codec.Output as Output
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Network.Streaming as Streaming
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (castPtr)
import qualified Foreign.Ptr
import qualified Data.Word
import qualified Database.PostgreSQL.Diagnostics as PG
import qualified Skeletest.Predicate as P
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Streaming as PG
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.BufferPool
import qualified Network.Streaming.BufferPool as BufferPool
import Network.Streaming.Buffer (Buffer, bufferCapacity, bufferMemory, newBuffer)
import Skeletest hiding (context)
import System.IO.Error (ioeGetErrorString)
import System.Timeout (timeout)

spec :: Spec
spec = describe "shared operation resources" $ do
  xfail "Request-context scaffold: lazy shared input follows in the next change" $ it "reserves one primary for the request and releases it after teardown" $
    withTestResources settings $ \resources -> do
      withRequestBuffers resources $ \access -> do
        connection <- Context.newConnection idleChannel 16
        PG.withOperation access connection (const (pure ()))
        unavailable resources `shouldReturn` True
      unavailable resources `shouldReturn` False

  xfail "Request-context scaffold: lazy shared input follows in the next change" $ it "reuses the request primary for upload then response without a body pair" $
    withTestResources settings $ \resources -> withRequestBuffers resources $ \access -> do
      primary <- borrowBuffer access
      let memory = bufferMemory (loanBuffer primary)
      releaseBuffer primary
      received <- newIORef BS.empty
      returned <- newIORef False
      let source = Streaming.Source $ \use -> do
            done <- readIORef returned
            if done then use BS.empty else bracket (borrowBuffer access) releaseBuffer $ \loan -> do
              let buffer = loanBuffer loan
              bufferMemory buffer `shouldBe` memory
              withForeignPtr (bufferMemory buffer) $ \pointer -> fillBytes pointer 65 16
              result <- use (BSI.BS (bufferMemory buffer) 16)
              writeIORef returned True
              pure result
          link = Transfer (\pointer size -> do
            loan <- borrowBuffer access
            -- The parser owns only the primary during this refill. This
            -- second simultaneous loan must therefore use reserve storage.
            (bufferMemory (loanBuffer loan) == memory) `shouldBe` False
            releaseBuffer loan
            let chunk = BS.take size "response"
            BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
            pure (Just (BS.length chunk)))
            (\pieces -> modifyIORef' received (<> BS.concat pieces) $> sum (map BS.length pieces))
      connection <- Context.newConnection (newChannel link) 16
      PG.withOperation access connection $ \(Exchange.Session context) -> do
        Context.sendContextOutput context (Output.word32 16 <> Output.borrowedStream 16 source)
        Context.runContextInput context 8 (withInputSome 8 $ \chunk -> do
          chunk `shouldBe` "response")
        -- Consuming a callback chunk releases input immediately, even though
        -- the operation remains open.
        bracket (borrowBuffer access) releaseBuffer $ \loan ->
          bufferMemory (loanBuffer loan) `shouldBe` memory
      readIORef received `shouldReturn` ("\0\0\0\16" <> BS.replicate 16 65)
      PG.isUsable connection `shouldReturn` True

  xfail "Request-context scaffold: lazy shared input follows in the next change" $ it "keeps unread database bytes loaned while a second owner borrows" $
    withTestResources settings $ \resources -> withRequestBuffers resources $ \access -> do
      wire <- newIORef "abcdefgh"
      let link = Transfer (\pointer size -> do
            bytes <- readIORef wire
            let chunk = BS.take size bytes
            BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
            writeIORef wire (BS.drop (BS.length chunk) bytes)
            pure (Just (BS.length chunk))) (\_ -> fail "unexpected send")
      connection <- Context.newConnection (newChannel link) 16
      Context.withRequestContext connection access $ \context -> do
        Context.runContextInput context 8 (readInputSome 4) `shouldReturn` "abcd"
        bracket (borrowBuffer access) releaseBuffer $ \_ ->
          borrowBuffer access `shouldSatisfy` P.throws @IOException P.anything
        Context.runContextInput context 8 (readInputSome 4) `shouldReturn` "efgh"
        first <- borrowBuffer access
        second <- borrowBuffer access
        releaseBuffer second
        releaseBuffer first

  xfail "Request-context scaffold: lazy shared input follows in the next change" $ it "expires the supplied session without closing its pooled connection" $
    withTestResources settings $ \resources -> withRequestBuffers resources $ \access -> do
      connection <- Context.newConnection idleChannel 16
      session <- PG.withOperation access connection $ \session -> do
        PG.isUsable (PG.connection session) `shouldReturn` False
        PG.close (PG.connection session)
          `shouldSatisfy` P.throws (P.eq (PG.SessionUnusable "the session is busy with another operation"))
        pure session
      PG.closeStatement session "unused"
        `shouldSatisfy` P.throws (P.eq (PG.SessionUnusable "the operation context has expired"))
      PG.isUsable connection `shouldReturn` True
      unavailable resources `shouldReturn` True

  xfail "Request-context scaffold: lazy shared input follows in the next change" $ it "returns storage and invalidates the connection after a callback failure" $ do
    withTestResources settings $ \resources -> withRequestBuffers resources $ \access -> do
      connection <- Context.newConnection idleChannel 16
      result <- try @IOException $ PG.withOperation access connection $ \_ -> ioError (userError "stop")
      either (const (pure ())) (const (failTest "callback exception was lost")) result
      unavailable resources `shouldReturn` True
      PG.isUsable connection `shouldReturn` False

  xfail "Request-context scaffold: lazy shared input follows in the next change" $ it "returns a parser loan when a borrowed consumer throws" $
    withTestResources settings $ \resources -> withRequestBuffers resources $ \access -> do
      let link = Transfer (\pointer size -> do
            let count = min size 8
            fillBytes pointer 65 count
            pure (Just count)) (\_ -> fail "unexpected send")
      connection <- Context.newConnection (newChannel link) 16
      result <- try @IOException $ Context.withRequestContext connection access $ \context ->
        Context.runContextInput context 8 (withInputSome 8 (const (ioError (userError "consumer failed"))))
      either (const (pure ())) (const (failTest "consumer exception was lost")) result
      first <- borrowBuffer access
      second <- borrowBuffer access
      releaseBuffer second
      releaseBuffer first
      PG.isUsable connection `shouldReturn` False

  it "rejects nonpositive capacities in either supplied descriptor before entry" $ do
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    forM_ [0, -1] $ \capacity -> do
      rejectBuffers (input { bufferCapacity = capacity }) output
        "buffer capacity must be positive"
      rejectBuffers input (output { bufferCapacity = capacity })
        "buffer capacity must be positive"

  it "rejects output capacities below the protocol minimum before entry" $ do
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    forM_ [1 .. 3] $ \capacity ->
      rejectBuffers input (output { bufferCapacity = capacity })
        "output buffer capacity must be at least four bytes"

-- Malformed public descriptors must fail before claiming the connection or
-- touching transport, so the caller can retry with valid storage.
rejectBuffers :: Buffer -> Buffer -> String -> IO ()
rejectBuffers input output message = do
  entered <- newIORef False
  touched <- newIORef False
  connection <- Context.newConnection (newChannel (Observed touched)) 16
  result <- try @IOException $
    PG.withBuffers input output connection (\_ -> writeIORef entered True)
  case result of
    Left failure -> ioeGetErrorString failure `shouldBe` message
    Right () -> failTest "invalid supplied capacity was accepted"
  readIORef entered `shouldReturn` False
  readIORef touched `shouldReturn` False
  PG.isUsable connection `shouldReturn` True
  validInput <- newBuffer (positiveInt @16)
  validOutput <- newBuffer (positiveInt @16)
  PG.withBuffers validInput validOutput connection (\_ -> writeIORef entered True)
  readIORef entered `shouldReturn` True
  readIORef touched `shouldReturn` False
  PG.isUsable connection `shouldReturn` True

newtype Observed = Observed (IORef Bool)

instance Link Observed where
  receiveReady link _ _ = unexpectedTransport link
  sendReady link _ = unexpectedTransport link
  flushReady = unexpectedTransport
  awaitReadable = unexpectedTransport
  awaitWritable = unexpectedTransport
  awaitReadableOrWritable = unexpectedTransport
  closeLink = unexpectedTransport

unexpectedTransport :: Observed -> IO a
unexpectedTransport (Observed touched) = do
  writeIORef touched True
  fail "unexpected transport entry"

settings :: PoolSettings
settings = PoolSettings
      { bufferSize = positiveInt @16
      , ordinaryBufferCount = positiveInt @1
      , reserveBufferCount = positiveInt @1
      , setupBufferCount = positiveInt @2
      }

-- Probe admission through its public scope; timeout cancellation also cleans up
-- the blocked acquisition before the enclosing request can return its primary.
unavailable :: BufferPool -> IO Bool
unavailable resources = isNothing <$>
  timeout 200_000 (withRequestBuffers resources (const (pure ())))

data Idle = Idle

instance Link Idle where
  receiveReady _ _ _ = fail "unexpected receive"
  sendReady _ _ = fail "unexpected send"
  flushReady _ = pure True
  awaitReadable _ = fail "unexpected read wait"
  awaitWritable _ = fail "unexpected write wait"
  awaitReadableOrWritable _ = fail "unexpected combined wait"
  closeLink _ = pure ()

idleChannel :: Channel
idleChannel = newChannel Idle

data Transfer = Transfer
  (Foreign.Ptr.Ptr Data.Word.Word8 -> Int -> IO (Maybe Int))
  ([BS.ByteString] -> IO Int)

instance Link Transfer where
  receiveReady (Transfer receive _) = receive
  sendReady (Transfer _ send) = send
  flushReady _ = pure True
  awaitReadable _ = fail "unexpected read wait"
  awaitWritable _ = pure ()
  awaitReadableOrWritable _ = fail "unexpected combined wait"
  closeLink _ = pure ()

withTestResources :: PoolSettings -> (BufferPool.BufferPool -> IO a) -> IO a
withTestResources settings action = getAcquisition >>= \waiting ->
  BufferPool.newBufferPool waiting settings >>= action
