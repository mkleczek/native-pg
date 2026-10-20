{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module TransportSpec (spec) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Word (Word8)
import Database.PostgreSQL.Protocol (Sync (..), SyncType (SyncType))
import Database.PostgreSQL.Protocol.Codec (InputSource (..), Output, serialize)
import Database.PostgreSQL.Transport
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr)
import System.Timeout (timeout)
import Test.Hspec

spec :: Spec
spec = describe "channels over a link without a socket" $ do
  it "retries receiving even when readiness does not yet yield input" $ do
    wakeups <- newIORef (0 :: Int)
    let link = testLink
          { testReceive = \pointer size -> do
              count <- readIORef wakeups
              if count < 2 then pure Nothing else provide "reply" pointer size,
            testReadable = modifyIORef' wakeups (+ 1)
          }
    channel <- channelWith link mempty
    timeout 1000000 (readBytes channel) `shouldReturn` Just "reply"
    readIORef wakeups `shouldReturn` 2

  it "waits for sending to resume after the link takes no bytes" $ do
    ready <- newIORef False
    sent <- newIORef BS.empty
    let link = testLink
          { testSend = \pieces -> do
              writable <- readIORef ready
              if writable then collect sent pieces else pure 0,
            testWritable = writeIORef ready True
          }
    channel <- channelWith link mempty
    timeout 1000000 (channelSend channel sync) `shouldReturn` Just ()
    readIORef sent `shouldReturn` syncBytes

  it "waits until buffered output can be flushed" $ do
    flushed <- newIORef False
    sent <- newIORef BS.empty
    let link = testLink
          { testSend = collect sent,
            testFlush = readIORef flushed,
            testWritable = writeIORef flushed True
          }
    channel <- channelWith link mempty
    timeout 1000000 (channelSend channel sync) `shouldReturn` Just ()
    readIORef sent `shouldReturn` syncBytes
    readIORef flushed `shouldReturn` True

  it "sends pending output while waiting for a response" $ do
    sent <- newIORef BS.empty
    let link = testLink
          { testReceive = \pointer size -> do
              request <- readIORef sent
              if request == syncBytes then provide "reply" pointer size else pure Nothing,
            testSend = collect sent,
            testEither = pure Writable
          }
    channel <- channelWith link sync
    timeout 1000000 (readBytes channel) `shouldReturn` Just "reply"
    readIORef sent `shouldReturn` syncBytes

  it "reads ready input even when output is still pending" $ do
    ready <- newIORef False
    let link = testLink
          { testReceive = \pointer size -> do
              readable <- readIORef ready
              if readable then provide "reply" pointer size else pure Nothing,
            testEither = writeIORef ready True *> pure Readable
          }
    channel <- channelWith link sync
    timeout 1000000 (readBytes channel) `shouldReturn` Just "reply"

-- No socket or file descriptor: the channel must use the link's waits.
-- Unspecified waits and writes fail so that each test exercises the
-- direction its peer permits.
data TestLink = TestLink
  { testReceive :: Ptr Word8 -> Int -> IO (Maybe Int),
    testSend :: [ByteString] -> IO Int,
    testFlush :: IO Bool,
    testReadable :: IO (),
    testWritable :: IO (),
    testEither :: IO Readiness
  }

instance Link TestLink where
  receiveReady = testReceive
  sendReady = testSend
  flushReady = testFlush
  awaitReadable = testReadable
  awaitWritable = testWritable
  awaitReadableOrWritable = testEither
  closeLink _ = pure ()

testLink :: TestLink
testLink =
  TestLink
    (\_ _ -> pure Nothing)
    (\_ -> fail "unexpected send")
    (pure True)
    (fail "unexpected read wait")
    (fail "unexpected write wait")
    (fail "unexpected combined wait")

channelWith :: TestLink -> Output -> IO Channel
channelWith link output = do
  pendingOutput <- newIORef =<< unsent Nothing output
  pure (newChannel link pendingOutput 16)

readBytes :: Channel -> IO ByteString
readBytes channel = allocaBytes 16 $ \pointer -> do
  count <- readInputInto (channelInput channel) pointer 16
  BS.packCStringLen (castPtr pointer, count)

provide :: ByteString -> Ptr Word8 -> Int -> IO (Maybe Int)
provide bytes pointer size =
  BS.useAsCStringLen (BS.take size bytes) $ \(source, count) -> do
    copyBytes pointer (castPtr source) count
    pure (Just count)

collect :: IORef ByteString -> [ByteString] -> IO Int
collect sent pieces = do
  let bytes = BS.concat pieces
  modifyIORef' sent (<> bytes)
  pure (BS.length bytes)

sync :: Output
sync = serialize @Sync @'SyncType Sync

syncBytes :: ByteString
syncBytes = "S\0\0\0\4"
