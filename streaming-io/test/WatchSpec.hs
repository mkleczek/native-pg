{-# LANGUAGE OverloadedStrings #-}

module WatchSpec (spec) where

import Control.Concurrent (forkIO, threadDelay, throwTo)
import Control.Concurrent.MVar
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, writeTVar)
import Control.Exception (ErrorCall (..), SomeException, throwIO, try)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef)
import GHC.Conc (retry)
import Network.Streaming.Watch
import Skeletest
import qualified Skeletest.Predicate as P
import System.Timeout (timeout)

spec :: Spec
spec = do
  describe "watches" $ do
    it "fires at once for readyNow, and releasing it is harmless" $ do
      timeout 1000000 (atomically (watchReady readyNow)) `shouldReturn` Just ()
      watchCancel readyNow
      watchCancel readyNow

    it "returns the first of the watches that has fired" $ do
      counts <- newCounts
      fired <- newTVarIO False
      awaitFirst [(pure never, "never"), (fake counts fired, "fake"), (pure readyNow, "now")]
        `shouldReturn` ("now" :: String)

    it "waits for one watch, and releases it once" $ do
      counts <- newCounts
      fired <- newTVarIO False
      _ <- forkIO (threadDelay 20000 *> atomically (writeTVar fired True))
      timeout 2000000 (awaitWatch (fake counts fired)) `shouldReturn` Just ()
      readCounts counts `shouldReturn` (1, 1)

    it "prefers the earlier watch when several have fired" $ do
      awaitFirst [(pure readyNow, 1 :: Int), (pure readyNow, 2)] `shouldReturn` 1
      awaitFirst [(pure never, 0 :: Int), (pure readyNow, 1), (pure readyNow, 2)] `shouldReturn` 1

    it "waits until a watch fires" $ do
      counts <- newCounts
      fired <- newTVarIO False
      _ <- forkIO (threadDelay 20000 *> atomically (writeTVar fired True))
      timeout 2000000 (awaitFirst [(pure never, "never"), (fake counts fired, "fake")])
        `shouldReturn` Just ("fake" :: String)

    it "releases every watch once when one has fired" $ do
      counts <- newCounts
      fired <- newTVarIO False
      ready <- newTVarIO True
      awaitFirst [(fake counts fired, 'a'), (fake counts ready, 'b'), (fake counts fired, 'c')]
        `shouldReturn` 'b'
      readCounts counts `shouldReturn` (3, 3)

    it "releases every watch when the waiting thread is interrupted" $ do
      counts <- newCounts
      fired <- newTVarIO False
      registered <- newEmptyMVar
      finished <- newEmptyMVar
      waiter <- forkIO $
        try @SomeException
          ( awaitFirst
              [ (fake counts fired, ()),
                (fake counts fired, ()),
                (fake counts fired <* putMVar registered (), ())
              ]
          )
          >>= putMVar finished
      takeMVar registered
      throwTo waiter (ErrorCall "interrupted")
      timeout 2000000 (takeMVar finished)
        >>= (`shouldBe` Just "interrupted") . fmap (either show (const "returned"))
      readCounts counts `shouldReturn` (3, 3)

    it "releases the registered watches when registering another fails" $ do
      counts <- newCounts
      fired <- newTVarIO False
      try @ErrorCall (awaitFirst [(fake counts fired, ()), (fake counts fired, ()), (throwIO (ErrorCall "no registration"), ())])
        `shouldSatisfy` P.returns (P.left (P.eq (ErrorCall "no registration")))
      readCounts counts `shouldReturn` (2, 2)

  describe "one watch for several" $ do
    it "fires once any of the watches has" $ do
      counts <- newCounts
      first <- newTVarIO False
      second <- newTVarIO False
      watch <- anyWatch [fake counts first, fake counts second]
      timeout 50000 (atomically (watchReady watch)) `shouldReturn` Nothing
      atomically (writeTVar second True)
      timeout 2000000 (atomically (watchReady watch)) `shouldReturn` Just ()
      watchCancel watch

    it "fires at once when one of the watches has fired already" $ do
      counts <- newCounts
      fired <- newTVarIO False
      watch <- anyWatch [fake counts fired, pure readyNow]
      timeout 2000000 (atomically (watchReady watch)) `shouldReturn` Just ()
      watchCancel watch

    it "registers each watch once, in order, and releases each when it is released" $ do
      counts <- newCounts
      fired <- newTVarIO False
      order <- newIORef []
      let registering name = modifyIORef' order (name :) *> fake counts fired
      watch <- anyWatch [registering 'a', registering 'b', registering 'c']
      readIORef order `shouldReturn` "cba"
      readCounts counts `shouldReturn` (3, 0)
      watchCancel watch
      readCounts counts `shouldReturn` (3, 3)

    it "releases the registered watches when registering another fails" $ do
      counts <- newCounts
      fired <- newTVarIO False
      try @ErrorCall (anyWatch [fake counts fired, fake counts fired, throwIO (ErrorCall "no registration")])
        `shouldSatisfy` P.returns (P.left (P.eq (ErrorCall "no registration")))
      readCounts counts `shouldReturn` (2, 2)

    it "never fires when it has no watches" $ do
      watch <- anyWatch []
      timeout 50000 (atomically (watchReady watch)) `shouldReturn` Nothing
      watchCancel watch

-- | How many watches were registered and how many releases were asked for.
data Counts = Counts (IORef Int) (IORef Int)

newCounts :: IO Counts
newCounts = Counts <$> newIORef 0 <*> newIORef 0

readCounts :: Counts -> IO (Int, Int)
readCounts (Counts registered released) = (,) <$> readIORef registered <*> readIORef released

-- | A watch that fires when the flag is set.
fake :: Counts -> TVar Bool -> IO Watch
fake (Counts registered released) flag = do
  count registered
  pure (Watch (readTVar flag >>= check) (count released))
  where
    count ref = atomicModifyIORef' ref (\n -> (n + 1, ()))

never :: Watch
never = Watch retry (pure ())
