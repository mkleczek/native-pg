{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module ContextSpec (spec) where

import Control.Concurrent (forkIO, forkFinally, killThread, threadDelay)
import Control.Concurrent.MVar
import Control.Exception (SomeException, finally, try)
import Control.Monad (replicateM)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Maybe (isNothing)
import Data.IORef
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Internal.Protocol (TransactionStatus (..))
import qualified Database.PostgreSQL.Session as PG
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Foreign.ForeignPtr (withForeignPtr, plusForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import Data.Word (Word8)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Buffer (Buffer (..), newBuffer)
import Skeletest hiding (context)
import System.Timeout (timeout)

spec :: Spec
spec = describe "explicit operation contexts" $ do
  it "runs consecutive operations using only the current supplied workspaces" $ do
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    remaining <- newIORef (reply <> reply)
    supplied <- newIORef True
    let receive pointer size = do
          withForeignPtr (bufferMemory input) $ \start ->
            modifyIORef' supplied (&& (pointer >= start && pointer `plusPtr` size <= start `plusPtr` bufferCapacity input))
          bytes <- readIORef remaining
          let chunk = BS.take size bytes
          BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
          writeIORef remaining (BS.drop (BS.length chunk) bytes)
          pure (Just (BS.length chunk))
        send chunks = do
          withForeignPtr (bufferMemory output) $ \start ->
            mapM_ (\chunk -> BSU.unsafeUseAsCStringLen chunk $ \(pointer, size) ->
              modifyIORef' supplied (&& (castPtr pointer >= start && castPtr pointer `plusPtr` size <= start `plusPtr` bufferCapacity output))) chunks
          pure (sum (map BS.length chunks))
        link = Scripted receive send
        execution = PG.Execution (PG.Prepared "") [] [] []
        step values row = do
          PG.nextColumn row `shouldReturn` Just (PG.Column 3)
          value <- PG.columnBytes row
          pure (values <> [value])
    connection <- newConnection (newChannel link) 16
    values <- replicateM 2 $ withContext connection input output $ \context ->
      PG.executeWithContext context execution step []
    values `shouldBe` replicate 2 (Right (["abc"], "SELECT 1"), TxIdle)
    readIORef remaining `shouldReturn` BS.empty
    readIORef supplied `shouldReturn` True
    PG.isConnectionUsable connection `shouldReturn` True

  it "expires escaped contexts before the storage can be reused" $ do
    connection <- newConnection idleChannel 16
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    context <- withContext connection input output pure
    failure <- try @ConnectionError (runContextOperation context (pure ()))
    failure `shouldBe` Left (SessionUnusable "the operation context has expired")

  it "rejects overlapping scopes without breaking the outer operation" $ do
    connection <- newConnection idleChannel 16
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    withContext connection input output $ \_ -> do
      result <- try @ConnectionError (withContext connection input output (\_ -> pure ()))
      result `shouldBe` Left (SessionUnusable "the session is busy with another operation")
    state <- readIORef (connectionState connection)
    case state of Idle -> pure (); _ -> failTest "the outer scope did not release the connection"

  it "cancels and joins an unfinished child operation before returning storage" $ do
    connection <- newConnection idleChannel 16
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    started <- newEmptyMVar
    stopped <- newEmptyMVar
    result <- timeout 1000000 $ withContext connection input output $ \context -> do
      _ <- forkIO $ do
        _ <- try @SomeException (runContextOperation context (putMVar started () *> threadDelay 10000000))
          `finally` putMVar stopped ()
        pure ()
      takeMVar started
    result `shouldBe` Just ()
    timeout 1000000 (readMVar stopped) `shouldReturn` Just ()
    state <- readIORef (connectionState connection)
    case state of Broken -> pure (); _ -> failTest "an abandoned protocol operation must break the connection"

  it "keeps storage loaned until a cancelled child's cleanup has finished" $ do
    connection <- newConnection idleChannel 16
    input <- newBuffer (positiveInt @16)
    output <- newBuffer (positiveInt @16)
    started <- newEmptyMVar
    cleaning <- newEmptyMVar
    allowCleanup <- newEmptyMVar
    finished <- newEmptyMVar
    owner <- forkFinally
      (withContext connection input output $ \context -> do
        _ <- forkIO $ do
          _ <- try @SomeException $ runContextOperation context $
            (putMVar started () *> threadDelay 10000000)
              `finally` (putMVar cleaning () *> readMVar allowCleanup)
          pure ()
        readMVar started
        threadDelay 10000000)
      (putMVar finished)
    readMVar started
    _ <- forkIO (killThread owner)
    timeout 1000000 (readMVar cleaning) `shouldReturn` Just ()
    _ <- forkIO (killThread owner)
    early <- timeout 50000 (readMVar finished)
    putMVar allowCleanup ()
    completed <- timeout 1000000 (readMVar finished)
    isNothing early `shouldBe` True
    isNothing completed `shouldBe` False

  it "rejects identical and partially overlapping workspaces before callback entry" $ do
    connection <- newConnection idleChannel 16
    arena <- newBuffer (positiveInt @32)
    let input = Buffer (bufferMemory arena) 16
        overlap = Buffer (plusForeignPtr (bufferMemory arena) 8) 16
    entered <- newIORef False
    let rejected output = do
          result <- try @ConnectionError (withContext connection input output (\_ -> writeIORef entered True))
          result `shouldBe` Left (SessionUnusable "input and output storage overlap")
    rejected input
    rejected overlap
    readIORef entered `shouldReturn` False
    PG.isConnectionUsable connection `shouldReturn` True

reply :: BS.ByteString
reply = "2\0\0\0\4D\0\0\0\13\0\1\0\0\0\3abcC\0\0\0\13SELECT 1\0Z\0\0\0\5I"

data Scripted = Scripted
  (Ptr Word8 -> Int -> IO (Maybe Int))
  ([BS.ByteString] -> IO Int)

instance Link Scripted where
  receiveReady (Scripted receive _) = receive
  sendReady (Scripted _ send) = send
  flushReady _ = pure True
  awaitReadable _ = fail "unexpected read wait"
  awaitWritable _ = pure ()
  awaitReadableOrWritable _ = fail "unexpected combined wait"
  closeLink _ = pure ()

idleChannel :: Channel
idleChannel = newChannel (Scripted (\_ _ -> fail "unexpected receive") (\_ -> fail "unexpected send"))
