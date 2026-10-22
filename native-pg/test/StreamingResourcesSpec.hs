{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
module StreamingResourcesSpec (spec) where

import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar
import Control.Concurrent.STM (atomically)
import Control.Exception (IOException, SomeException, bracket, finally, try)
import Control.Monad (void)
import Data.Maybe (isNothing)
import qualified Database.PostgreSQL.Internal.Context as Context
import qualified Database.PostgreSQL.Session as PG
import qualified Database.PostgreSQL.Streaming as PG
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources
import qualified Network.Streaming.Resources.Internal as Resources
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "shared operation resources" $ do
  it "holds exactly a pair only within the operation" $ do
    withResources settings $ \resources -> do
      connection <- Context.newConnection idleChannel 16
      PG.withOperation resources connection $ \_ -> do
        unavailable resources `shouldReturn` True
        Resources.withSetupBuffers resources (positiveInt @2) (\_ -> pure ())
      unavailable resources `shouldReturn` False
      PG.isConnectionUsable connection `shouldReturn` True

  it "returns storage and invalidates the connection after a callback failure" $ do
    withResources settings $ \resources -> do
      connection <- Context.newConnection idleChannel 16
      result <- try @IOException $ PG.withOperation resources connection $ \_ -> ioError (userError "stop")
      either (const (pure ())) (const (failTest "callback exception was lost")) result
      unavailable resources `shouldReturn` False
      PG.isConnectionUsable connection `shouldReturn` False

  it "does not consume a partial pair while waiting or after cancellation" $ do
    withResources settings $ \resources -> Resources.withBuffers resources (positiveInt @1) $ \_ -> do
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
          Resources.withBuffers resources (positiveInt @1) (\_ -> pure ()))
      PG.isConnectionUsable connection `shouldReturn` True

settings :: ResourceSettings
settings = ResourceSettings (positiveInt @32) (positiveInt @32) (positiveInt @16)

unavailable :: Resources -> IO Bool
unavailable resources = atomically $ do
  lease <- Resources.tryAcquire resources (positiveInt @1)
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
