{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module TransportSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Exception (ErrorCall (..), bracket, finally, throwIO)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Word (Word8)
import Database.PostgreSQL.Internal.Protocol (pattern Sync, serialize)
import Database.PostgreSQL.Internal.Protocol.Codec (ByteStream (..), InputSource (..), Output, serializeField)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (RemainingBytesAtom, MaybeBytes32Atom))
import Database.PostgreSQL.Internal.Protocol (Parameter (..))
import Database.PostgreSQL.Internal.Context (newConnection, newOwnedContext)
import qualified Database.PostgreSQL.Session as Session
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import qualified Database.PostgreSQL.Internal.Execution as Session
import qualified Database.PostgreSQL.Diagnostics as Session
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr)
import System.Timeout (timeout)
import Skeletest
import qualified Skeletest.Predicate as P

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
    (channel, pending) <- channelWith link mempty
    timeout 1000000 (readBytes channel pending) `shouldReturn` Just "reply"
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
    (channel, _) <- channelWith link mempty
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
    (channel, _) <- channelWith link mempty
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
    (channel, pending) <- channelWith link sync
    timeout 1000000 (readBytes channel pending) `shouldReturn` Just "reply"
    readIORef sent `shouldReturn` syncBytes

  it "uses the current operation's pending output instead of the channel creator's" $ do
    sent <- newIORef BS.empty
    let link = testLink
          { testReceive = \pointer size -> do
              request <- readIORef sent
              if request == syncBytes then provide "reply" pointer size else pure Nothing,
            testSend = collect sent,
            testEither = pure Writable
          }
    (channel, _) <- channelWith link mempty
    current <- newIORef =<< unsent Nothing sync
    timeout 1000000 (allocaBytes 16 $ \pointer -> do
      count <- readInputInto (channelInput channel current 16) pointer 16
      BS.packCStringLen (castPtr pointer, count)) `shouldReturn` Just "reply"
    readIORef sent `shouldReturn` syncBytes
    (unsentEmpty <$> readIORef current) `shouldReturn` True

  it "reads ready input even when output is still pending" $ do
    ready <- newIORef False
    let link = testLink
          { testReceive = \pointer size -> do
              readable <- readIORef ready
              if readable then provide "reply" pointer size else pure Nothing,
            testEither = writeIORef ready True *> pure Readable
          }
    (channel, pending) <- channelWith link sync
    timeout 1000000 (readBytes channel pending) `shouldReturn` Just "reply"

  it "bounds each send even for a single large immutable value" $ do
      let payload = BS.replicate 600000 42
      checkGather (serializeField @ByteString @'RemainingBytesAtom payload) payload

  it "splits a large stream chunk without losing its partial-write suffix" $ do
      let payload = BS.replicate 600000 61
      next <- newIORef payload
      let source = ByteStream (BS.length payload) (readIORef next <* writeIORef next BS.empty)
          output = serializeField @(Maybe Parameter) @'MaybeBytes32Atom (Just (ParameterStream source))
      checkGather output ("\0\9'\192" <> payload)

  it "closes the channel once when sending Terminate throws" $
    withCloseHooks (throwIO (ErrorCall "terminate failed")) (pure ()) $ \session ->
      Session.close (Session.connection session) `shouldSatisfy` P.throws (P.con (ErrorCall (P.eq "terminate failed")))

  it "does not retry channel cleanup if it throws" $
    withCloseHooks (pure ()) (throwIO (ErrorCall "cleanup failed")) $ \session ->
      Session.close (Session.connection session) `shouldSatisfy` P.throws (P.con (ErrorCall (P.eq "cleanup failed")))

  it "closes the channel once when sending Terminate is interrupted" $
    withCloseHooks (threadDelay 10000000) (pure ()) $ \session ->
      timeout 100000 (Session.close (Session.connection session)) `shouldReturn` Nothing

withCloseHooks :: IO () -> IO () -> (Session.Session -> IO a) -> IO ()
withCloseHooks beforeSend afterClose check = do
  closes <- newIORef (0 :: Int)
  let link = testLink
        { testSend = \pieces -> do
            BS.concat pieces `shouldBe` "X\0\0\0\4"
            beforeSend
            pure (sum (map BS.length pieces)),
          testClose = (modifyIORef' closes (+ 1)) `finally` afterClose
        }
      open = Exchange.Session <$> (newOwnedContext =<< newConnection (newChannel link) 16)
  bracket open (Session.close . Session.connection) $ \session -> do
    _ <- check session
    Session.isUsable (Session.connection session) `shouldReturn` False
    Session.close (Session.connection session)
    Session.execute session (Session.Execution (Session.Prepared "") [] [] []) (\() _ -> pure ()) ()
      `shouldSatisfy` P.throws (P.eq (Session.SessionUnusable "the session is closed"))
  readIORef closes `shouldReturn` 1

checkGather :: Output -> ByteString -> IO ()
checkGather output expected = do
  calls <- newIORef []
  sent <- newIORef BS.empty
  let link = testLink
        { testSend = \pieces -> do
            let offered = BS.concat pieces
                accepted = BS.take 100003 offered
            modifyIORef' calls (BS.length offered :)
            modifyIORef' sent (<> accepted)
            pure (BS.length accepted),
          testWritable = pure ()
        }
  (channel, _) <- channelWith link mempty
  channelSend channel output
  readIORef sent `shouldReturn` expected
  (all (<= 262144) <$> readIORef calls) `shouldReturn` True

-- No socket or file descriptor: the channel must use the link's waits.
-- Unspecified waits and writes fail so that each test exercises the
-- direction its peer permits.
data TestLink = TestLink
  { testReceive :: Ptr Word8 -> Int -> IO (Maybe Int),
    testSend :: [ByteString] -> IO Int,
    testFlush :: IO Bool,
    testReadable :: IO (),
    testWritable :: IO (),
    testEither :: IO Readiness,
    testClose :: IO ()
  }

instance Link TestLink where
  receiveReady = testReceive
  sendReady = testSend
  flushReady = testFlush
  awaitReadable = testReadable
  awaitWritable = testWritable
  awaitReadableOrWritable = testEither
  closeLink = testClose

testLink :: TestLink
testLink =
  TestLink
    (\_ _ -> pure Nothing)
    (\_ -> fail "unexpected send")
    (pure True)
    (fail "unexpected read wait")
    (fail "unexpected write wait")
    (fail "unexpected combined wait")
    (pure ())

channelWith :: TestLink -> Output -> IO (Channel, IORef Unsent)
channelWith link output = do
  pendingOutput <- newIORef =<< unsent Nothing output
  pure (newChannel link, pendingOutput)

readBytes :: Channel -> IORef Unsent -> IO ByteString
readBytes channel pending = allocaBytes 16 $ \pointer -> do
  count <- readInputInto (channelInput channel pending 16) pointer 16
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
sync = serialize Sync

syncBytes :: ByteString
syncBytes = "S\0\0\0\4"
