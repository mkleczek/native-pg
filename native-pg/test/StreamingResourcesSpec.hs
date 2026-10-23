{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
module StreamingResourcesSpec (spec) where

import Control.Concurrent.Resource.Waiting qualified as Waiting
import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically)
import Control.Exception (IOException, SomeException, bracket, finally, try)
import Control.Monad (forM_, void)
import Data.IORef
import Data.Maybe (isNothing)
import Data.Tuple (Solo)
import qualified Database.PostgreSQL.Internal.Context as Context
import qualified Database.PostgreSQL.Diagnostics as PG
import qualified Skeletest.Predicate as P
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Streaming as PG
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources
import qualified Network.Streaming.Resources as Resources
import Network.Streaming.Buffer (Buffer, bufferCapacity, newBuffer)
import Skeletest
import System.IO.Error (ioeGetErrorString)
import System.Timeout (timeout)

spec :: Spec
spec = describe "shared operation resources" $ do
  it "holds exactly a pair only within the operation" $ do
    withTestResources settings $ \resources -> do
      connection <- Context.newConnection idleChannel 16
      PG.withOperation resources connection $ \_ -> do
        unavailable resources `shouldReturn` True
        Resources.withSetupBuffers @(Buffer, Buffer) resources (\_ -> pure ())
      unavailable resources `shouldReturn` False
      PG.isUsable connection `shouldReturn` True

  it "expires the supplied session without closing its pooled connection" $
    withTestResources settings $ \resources -> do
      connection <- Context.newConnection idleChannel 16
      session <- PG.withOperation resources connection $ \session -> do
        PG.isUsable (PG.connection session) `shouldReturn` False
        PG.close (PG.connection session)
          `shouldSatisfy` P.throws (P.eq (PG.SessionUnusable "the session is busy with another operation"))
        pure session
      PG.closeStatement session "unused"
        `shouldSatisfy` P.throws (P.eq (PG.SessionUnusable "the operation context has expired"))
      PG.isUsable connection `shouldReturn` True
      unavailable resources `shouldReturn` False

  it "returns storage and invalidates the connection after a callback failure" $ do
    withTestResources settings $ \resources -> do
      connection <- Context.newConnection idleChannel 16
      result <- try @IOException $ PG.withOperation resources connection $ \_ -> ioError (userError "stop")
      either (const (pure ())) (const (failTest "callback exception was lost")) result
      unavailable resources `shouldReturn` False
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

  it "does not consume a partial pair while waiting or after cancellation" $ do
    withTestResources settings $ \resources -> Resources.withBuffers @(Solo Buffer) resources $ \_ -> do
      connection <- Context.newConnection idleChannel 16
      entered <- newEmptyMVar
      stopped <- newEmptyMVar
      bracket
        (forkIO (void (try @SomeException (PG.withOperation resources connection (const (putMVar entered ())))) `finally` putMVar stopped ()))
        (\thread -> killThread thread *> readMVar stopped)
        (\_ -> do
          timeout 20000 (readMVar entered) `shouldReturn` Nothing
          timeout 20000 (readMVar stopped) `shouldReturn` Nothing
          -- The other block remains available until the whole pair is free.
          Resources.withBuffers @(Solo Buffer) resources (\_ -> pure ()))
      PG.isUsable connection `shouldReturn` True

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

data Observed = Observed (IORef Bool)

instance Link Observed where
  receiveReady link _ _ = unexpectedTransport link
  sendReady link _ = unexpectedTransport link
  flushReady link = unexpectedTransport link
  awaitReadable link = unexpectedTransport link
  awaitWritable link = unexpectedTransport link
  awaitReadableOrWritable link = unexpectedTransport link
  closeLink link = unexpectedTransport link

unexpectedTransport :: Observed -> IO a
unexpectedTransport (Observed touched) = do
  writeIORef touched True
  fail "unexpected transport entry"

settings :: ResourceSettings
settings = ResourceSettings (positiveInt @48) (positiveInt @32) (positiveInt @16) (positiveInt @1)

unavailable :: Resources -> IO Bool
unavailable resources = atomically $ do
  lease <- Resources.tryAcquire @(Solo Buffer) resources
  mapM_ Resources.release lease
  pure (isNothing lease)

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

withTestResources :: ResourceSettings -> (Resources.Resources -> IO a) -> IO a
withTestResources settings action = Waiting.withWaiting $ \waiting ->
  Resources.withResources waiting settings action
