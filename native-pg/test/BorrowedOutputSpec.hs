{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module BorrowedOutputSpec (spec) where

import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically, check, newEmptyTMVarIO, newTVarIO, putTMVar, readTMVar, readTVar, readTVarIO, writeTVar)
import Control.Exception (IOException, SomeException, bracket, evaluate, finally, try)
import Control.Monad (void, when)
import Data.Bool (bool)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Functor (($>))
import Data.IORef
import qualified Data.Word
import Database.PostgreSQL.Internal.Channel
import Database.PostgreSQL.Internal.Execution (Execution (..), Statement (..), executionMessages)
import Database.PostgreSQL.Internal.Protocol (Parameter (..))
import Database.PostgreSQL.Internal.Protocol.Codec
import Database.PostgreSQL.Internal.Protocol.Codec.Output (borrowedStream)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (MaybeBytes32Atom))
import Database.PostgreSQL.Transport hiding (newChannel)
import FeedSupport (staticBytes)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (castPtr)
import qualified Foreign.Ptr
import GHC.Conc (retry)
import Network.Streaming.Buffer (bufferMemory, newBuffer)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Source (Pulled (..), Source)
import Network.Streaming.Watch (Watch (..), readyNow)
import PullSourceTestSupport
import Skeletest
import qualified Skeletest.Predicate as P
import System.Timeout (timeout)

spec :: Spec
spec = describe "borrowed parameter streams" $ do
  it "keeps a reused source buffer unchanged across partial writes" $ do
      buffer <- newBuffer (positiveInt @32)
      pulls <- newIORef (0 :: Int)
      received <- newIORef BS.empty
      let next = do
            count <- atomicModifyIORef' pulls (\n -> (n + 1, n))
            -- The chunk before this one ends with the pull: its memory is cleared.
            withForeignPtr (bufferMemory buffer) $ \pointer -> fillBytes pointer 0 32
            if count >= 3 then pure (Just Ended) else do
              withForeignPtr (bufferMemory buffer) $ \pointer -> fillBytes pointer (fromIntegral count + 65) 32
              pure (Just (Pulled (BSI.BS (bufferMemory buffer) 32)))
          source = PullSource next noWatch
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
      earlier <- newTVarIO earlierSize
      uploadRead <- newEmptyTMVarIO
      sent <- newIORef (0 :: Int)
      pulled <- newIORef False
      ended <- newIORef False
      let -- Nothing is ready until the earlier results have been read.
          pulling = readTVarIO earlier >>= \left ->
            if left > 0 then pure Nothing else do
              done <- atomicModifyIORef' pulled (True,)
              if done then writeIORef ended True $> Just Ended else pure (Just (Pulled (BS.replicate uploadSize 65)))
          source = PullSource pulling (pure (Watch (readTVar earlier >>= check . (== 0)) (pure ())))
          receive pointer size = do
            left <- readTVarIO earlier
            uploaded <- readIORef sent
            if left > 0 then do
              let count = min size left
              fillBytes pointer 66 count
              atomically (writeTVar earlier (left - count))
              pure (Just count)
            else if uploaded == uploadSize then do
              BS.useAsCString "done" $ \bytes -> copyBytes pointer (castPtr bytes) 4
              pure (Just 4)
            else pure Nothing
          send chunks = do
            -- The peer refuses this later parameter until its earlier
            -- response has been drained.
            left <- readTVarIO earlier
            if left > 0 then pure 0 else do
              let count = min 1024 (sum (map BS.length chunks))
              total <- atomicModifyIORef' sent (\n -> (n + count, n + count))
              when (total == uploadSize) (atomically (putTMVar uploadRead ()))
              pure count
          link = Duplex receive send
            (pure (Watch (readTMVar uploadRead) (pure ())))
            (pure readyNow)
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
        -- The output cursor still needs the source's end.
        left <- readIORef pending
        channelSendSome channel left >>= writeIORef pending
        pure value
      outcome `shouldBe` Just "done"
      readIORef sent `shouldReturn` uploadSize
      readIORef ended `shouldReturn` True

  describe "a waiting upload forks no thread" $ do
    it "waits for its source or the link's input on one thread, with nothing beside it" $ do
      registered <- newEmptyMVar
      sourceWatched <- newEmptyMVar
      watchedAgain <- newEmptyMVar
      baseline <- newEmptyMVar
      stopped <- newEmptyMVar
      open <- newTVarIO False
      watches <- newIORef (0 :: Int)
      cancels <- newIORef (0 :: Int)
      let source = PullSource
            (bool Nothing (Just Ended) <$> readTVarIO open)
            (putMVar sourceWatched () $> Watch (readTVar open >>= check) (pure ()))
          -- Input never arrives, so only the source's watch ends a wait.
          watchInput = do
            count <- atomicModifyIORef' watches (\n -> (n + 1, n + 1))
            when (count == 1) (putMVar registered ())
            when (count == 2) (putMVar watchedAgain ())
            pure (Watch retry (modifyIORef' cancels (+ 1)))
          link = Duplex (\_ _ -> pure Nothing) (\_ -> fail "unexpected send") watchInput (pure readyNow)
          channel = newChannel link
          upload = do
            liveThreads >>= putMVar baseline
            bracket (unsent Nothing (borrowedStream 0 source)) discardUnsent $ \initial -> do
              pending <- newIORef =<< channelSendSome channel initial
              allocaBytes 16 $ \pointer -> readInputInto (channelInput channel pending 16) pointer 16
      outcome <- timeout 5000000 $ do
        reader <- forkIO (void (try @SomeException upload) `finally` putMVar stopped ())
        before <- takeMVar baseline
        takeMVar registered
        takeMVar sourceWatched
        -- The reader is waiting for the link or the source, on its own.
        during <- liveThreads
        atomically (writeTVar open True)
        -- The source's watch ended the first wait, and the next one began.
        takeMVar watchedAgain
        killThread reader
        takeMVar stopped
        pure (during - before)
      outcome `shouldBe` Just 0
      readIORef cancels `shouldReturn` 2

    it "waits for the link on one thread while a partial send of a chunk is pending" $ do
      registered <- newEmptyMVar
      baseline <- newEmptyMVar
      stopped <- newEmptyMVar
      pulls <- newIORef (0 :: Int)
      takes <- newIORef [3 :: Int]
      let source = PullSource
            (Just . bool Ended (Pulled "0123456789") . (== 0) <$> atomicModifyIORef' pulls (\n -> (n + 1, n)))
            noWatch
          -- The link takes 3 bytes, and then none.
          send pieces = atomicModifyIORef' takes (\case [] -> ([], 0); count : rest -> (rest, count))
            >>= pure . (`min` (sum (map BS.length pieces)))
          link = Duplex (\_ _ -> pure Nothing) send
            (pure (Watch retry (pure ())))
            (putMVar registered () $> Watch retry (pure ()))
          channel = newChannel link
          upload = do
            liveThreads >>= putMVar baseline
            bracket (unsent Nothing (borrowedStream 10 source)) discardUnsent $ \initial -> do
              pending <- newIORef =<< channelSendSome channel initial
              allocaBytes 16 $ \pointer -> readInputInto (channelInput channel pending 16) pointer 16
      outcome <- timeout 5000000 $ do
        reader <- forkIO (void (try @SomeException upload) `finally` putMVar stopped ())
        before <- takeMVar baseline
        takeMVar registered
        -- The reader is waiting for the link, which has the rest of the chunk
        -- to take, and the source isn't pulled for another until it has.
        during <- liveThreads
        killThread reader
        takeMVar stopped
        pure (during - before)
      outcome `shouldBe` Just 0
      readIORef pulls `shouldReturn` 1

  describe "an upload's framing" $ do
    it "goes to the link in one send with a ready first chunk" $ do
      sends <- newIORef []
      source <- chunkSource "hello"
      channelSend (newChannel (recording sends)) (uploadStatement source)
      offers <- reverse <$> readIORef sends
      let (framing, _) = BS.breakSubstring "hello" wire
      map BS.concat offers `shouldBe` [framing <> "hello", BS.drop (BS.length framing + 5) wire]
      case offers of
        first : _ -> last first `shouldBe` "hello"
        [] -> failTest "nothing was sent"

    it "goes at once when the first pull is pending, and the chunk goes when the source's watch fires" $ do
      sends <- newIORef []
      open <- newTVarIO False
      script <- newIORef [Pulled "hello", Ended]
      watching <- newEmptyMVar
      finished <- newEmptyMVar
      let source = PullSource
            (readTVarIO open >>= \ready ->
              if ready then atomicModifyIORef' script (\case [] -> ([], Just Ended); step : rest -> (rest, Just step)) else pure Nothing)
            (putMVar watching () $> Watch (readTVar open >>= check) (pure ()))
          (framing, _) = BS.breakSubstring "hello" wire
      _ <- forkIO (channelSend (newChannel (recording sends)) (uploadStatement source) `finally` putMVar finished ())
      timeout 2000000 (takeMVar watching) `shouldReturn` Just ()
      -- The statement is on its way while the upload has nothing.
      (map BS.concat . reverse <$> readIORef sends) `shouldReturn` [framing]
      atomically (writeTVar open True)
      timeout 2000000 (takeMVar finished) `shouldReturn` Just ()
      (map BS.concat . reverse <$> readIORef sends)
        `shouldReturn` [framing, "hello", BS.drop (BS.length framing + 5) wire]

  it "propagates a source error and a failing watch" $ do
      let channel = newChannel (Sender (pure . sum . map BS.length))
      channelSend channel (borrowedStream 1 (PullSource (ioError (userError "source failed")) noWatch))
        `shouldSatisfy` P.throws @IOException P.anything
      channelSend channel (borrowedStream 1 (PullSource (pure Nothing) (ioError (userError "watch failed"))))
        `shouldSatisfy` P.throws @IOException P.anything

  it "releases the watch of a cancelled wait for its source before the owner unwinds" $ do
      registered <- newEmptyMVar
      stopped <- newEmptyMVar
      cancels <- newIORef (0 :: Int)
      let source = PullSource (pure Nothing) $ do
            putMVar registered ()
            pure (Watch retry (modifyIORef' cancels (+ 1)))
          sender = Sender (\_ -> pure 0)
      owner <- forkIO $ void (try @SomeException (channelSend (newChannel sender) (borrowedStream 8 source)))
        `finally` putMVar stopped ()
      takeMVar registered
      killThread owner
      timeout 1000000 (takeMVar stopped) `shouldReturn` Just ()
      readIORef cancels `shouldReturn` 1

  it "rejects a short and an overlong stream" $ do
      let one chunk = do
            first <- newIORef True
            pure (PullSource (Just . bool Ended (Pulled chunk) <$> atomicModifyIORef' first (False,)) noWatch)
      short <- one "a"
      long <- one "abc"
      let channel = newChannel (Sender (pure . sum . map BS.length))
      channelSend channel (borrowedStream 2 short)
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 2) (P.eq 1)))
      channelSend channel (borrowedStream 2 long)
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 2) (P.eq 3)))

-- | A statement whose only parameter is five bytes from the source.
uploadStatement :: Source source => source -> Output
uploadStatement source = executionMessages (Execution (Unprepared "SELECT $1" []) [] [Just (ParameterBorrowedStream 5 source)] [])

-- | The same statement with its five bytes at hand: what the server gets.
wire :: BS.ByteString
wire = staticBytes (executionMessages (Execution (Unprepared "SELECT $1" []) [] [Just (ParameterBytes "hello")] []))

-- | A source of one chunk.
chunkSource :: BS.ByteString -> IO PullSource
chunkSource chunk = do
  script <- newIORef [Pulled chunk, Ended]
  pure (PullSource (atomicModifyIORef' script (\case [] -> ([], Just Ended); step : rest -> (rest, Just step))) noWatch)

-- | A link that takes everything, and keeps the pieces of each send, the last
-- first.
recording :: IORef [[BS.ByteString]] -> Sender
recording sends = Sender $ \pieces -> do
  offered <- traverse (evaluate . BS.copy) pieces
  modifyIORef' sends (offered :)
  pure (sum (map BS.length offered))

newtype Sender = Sender ([BS.ByteString] -> IO Int)

instance Link Sender where
  sendReady (Sender send) = send
  flushReady _ = pure True
  watchWritable _ = pure readyNow
  receiveReady _ _ _ = fail "unexpected receive"
  watchReadable _ = fail "unexpected read wait"
  closeLink _ = pure ()

data Duplex = Duplex
  (Foreign.Ptr.Ptr Data.Word.Word8 -> Int -> IO (Maybe Int))
  ([BS.ByteString] -> IO Int)
  (IO Watch)
  (IO Watch)

instance Link Duplex where
  receiveReady (Duplex receive _ _ _) = receive
  sendReady (Duplex _ send _ _) = send
  flushReady _ = pure True
  watchWritable (Duplex _ _ _ writable) = writable
  watchReadable (Duplex _ _ readable _) = readable
  closeLink _ = pure ()
