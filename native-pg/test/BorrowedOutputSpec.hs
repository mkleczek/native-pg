{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module BorrowedOutputSpec (spec) where

import Control.Exception (IOException, SomeException, evaluate, finally, bracket, try)
import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar
import Control.Monad (void)
import qualified Skeletest.Predicate as P
import System.Timeout (timeout)
import Database.PostgreSQL.Internal.Protocol.Codec.Output (borrowedStream)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import qualified Foreign.Ptr
import qualified Data.Word
import qualified Network.Streaming as Streaming
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.IORef
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Buffer (bufferMemory, newBuffer)
import Database.PostgreSQL.Internal.Protocol (Parameter (..))
import Database.PostgreSQL.Internal.Protocol.Codec
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (MaybeBytes32Atom))
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import Skeletest

spec :: Spec
spec = describe "borrowed parameter streams" $ do
  it "keeps a reused source buffer unchanged across partial writes" $ do
      buffer <- newBuffer (positiveInt @32)
      pulls <- newIORef (0 :: Int)
      received <- newIORef BS.empty
      let next = do
            count <- atomicModifyIORef' pulls (\n -> (n + 1, n))
            if count >= 3 then pure BS.empty else do
              withForeignPtr (bufferMemory buffer) $ \pointer -> fillBytes pointer (fromIntegral count + 65) 32
              pure (BSI.BS (bufferMemory buffer) 32)
          source = Streaming.Source $ \use -> next >>= \chunk ->
            use chunk `finally` withForeignPtr (bufferMemory buffer) (\pointer -> fillBytes pointer 0 32)
          output = serializeField @(Maybe Parameter) @'MaybeBytes32Atom (Just (ParameterBorrowedStream 96 source))
          sender = Sender $ \pieces -> do
            prefix <- evaluate (BS.copy (BS.take 7 (BS.concat pieces)))
            modifyIORef' received (<> prefix)
            pure (BS.length prefix)
      channelSend (newChannel sender) output
      readIORef received `shouldReturn` ("\0\0\0`" <> BS.replicate 32 65 <> BS.replicate 32 66 <> BS.replicate 32 67)
      readIORef pulls `shouldReturn` 4

  it "keeps earlier large results moving while a later borrowed upload waits" $ do
      let earlierSize = 70000
          uploadSize = 70000
      earlier <- newIORef earlierSize
      earlierRead <- newEmptyMVar
      uploadRead <- newEmptyMVar
      sent <- newIORef (0 :: Int)
      pulled <- newIORef False
      returned <- newIORef False
      let source = Streaming.Source $ \use -> do
            done <- readIORef pulled
            if done then use BS.empty else do
              readMVar earlierRead
              writeIORef pulled True
              use (BS.replicate uploadSize 65) `finally` writeIORef returned True
          receive pointer size = do
            left <- readIORef earlier
            uploaded <- readIORef sent
            if left > 0 then do
              let count = min size left
              fillBytes pointer 66 count
              writeIORef earlier (left - count)
              if left == count then putMVar earlierRead () else pure ()
              pure (Just count)
            else if uploaded == uploadSize then do
              BS.useAsCString "done" $ \bytes -> copyBytes pointer (castPtr bytes) 4
              pure (Just 4)
            else pure Nothing
          send chunks = do
            -- The peer refuses this later parameter until its earlier
            -- response has been drained.
            left <- readIORef earlier
            if left > 0 then pure 0 else do
              let count = min 1024 (sum (map BS.length chunks))
              total <- atomicModifyIORef' sent (\n -> (n + count, n + count))
              if total == uploadSize then putMVar uploadRead () else pure ()
              pure count
          link = Duplex receive send
            (readMVar uploadRead)
            (pure Writable)
          channel = newChannel link
          drain pending = allocaBytes 4096 $ \pointer -> do
            let go 0 = pure ()
                go left = do
                  count <- readInputInto (channelInput channel pending 4096) pointer (min 4096 left)
                  go (left - count)
            go earlierSize
            count <- readInputInto (channelInput channel pending 4096) pointer 4
            BS.packCStringLen (castPtr pointer, count)
      outcome <- timeout 2000000 $ bracket (unsent Nothing (borrowedStream uploadSize source)) discardUnsent $ \initial -> do
        pending <- newIORef =<< channelSendSome channel initial
        value <- drain pending
        -- The output cursor still needs the scoped source's EOF.
        left <- readIORef pending
        channelSendSome channel left >>= writeIORef pending
        pure value
      outcome `shouldBe` Just "done"
      readIORef sent `shouldReturn` uploadSize
      readIORef returned `shouldReturn` True

  it "propagates a source error and joins its cleanup" $ do
      cleaned <- newIORef False
      let source = Streaming.Source $ \_ ->
            ioError (userError "source failed") `finally` writeIORef cleaned True
      channelSend (newChannel (Sender (pure . sum . map BS.length))) (borrowedStream 1 source)
        `shouldSatisfy` P.throws @IOException P.anything
      readIORef cleaned `shouldReturn` True

  it "joins a cancelled source callback before returning borrowed storage" $ do
      entered <- newEmptyMVar
      cleaning <- newEmptyMVar
      allowCleanup <- newEmptyMVar
      stopped <- newEmptyMVar
      let source = Streaming.Source $ \use ->
            (putMVar entered () *> use "borrowed")
              `finally` (putMVar cleaning () *> readMVar allowCleanup)
          sender = Sender (\_ -> pure 0)
      owner <- forkIO $ void (try @SomeException (channelSend (newChannel sender) (borrowedStream 8 source)))
        `finally` putMVar stopped ()
      readMVar entered
      _ <- forkIO (killThread owner)
      timeout 1000000 (readMVar cleaning) `shouldReturn` Just ()
      timeout 20000 (readMVar stopped) `shouldReturn` Nothing
      putMVar allowCleanup ()
      timeout 1000000 (readMVar stopped) `shouldReturn` Just ()

  it "rejects a short and an overlong scoped stream" $ do
      let one chunk = do
            first <- newIORef True
            pure (Streaming.Source $ \use -> do
              current <- atomicModifyIORef' first (\value -> (False, value))
              use (if current then chunk else BS.empty))
      short <- one "a"
      long <- one "abc"
      let channel = newChannel (Sender (pure . sum . map BS.length))
      channelSend channel (borrowedStream 2 short)
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 2) (P.eq 1)))
      channelSend channel (borrowedStream 2 long)
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 2) (P.eq 3)))

newtype Sender = Sender ([BS.ByteString] -> IO Int)

instance Link Sender where
  sendReady (Sender send) = send
  flushReady _ = pure True
  awaitWritable _ = pure ()
  receiveReady _ _ _ = fail "unexpected receive"
  awaitReadable _ = fail "unexpected read wait"
  awaitReadableOrWritable _ = fail "unexpected combined wait"
  closeLink _ = pure ()

data Duplex = Duplex
  (Foreign.Ptr.Ptr Data.Word.Word8 -> Int -> IO (Maybe Int))
  ([BS.ByteString] -> IO Int)
  (IO ())
  (IO Readiness)

instance Link Duplex where
  receiveReady (Duplex receive _ _ _) = receive
  sendReady (Duplex _ send _ _) = send
  flushReady _ = pure True
  awaitWritable _ = pure ()
  awaitReadable (Duplex _ _ readable _) = readable
  awaitReadableOrWritable (Duplex _ _ _ eitherReady) = eitherReady
  closeLink _ = pure ()
