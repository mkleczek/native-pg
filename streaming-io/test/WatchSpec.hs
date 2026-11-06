{-# LANGUAGE OverloadedStrings #-}

module WatchSpec (spec) where

import Control.Concurrent (forkIO, threadDelay, throwTo)
import Control.Concurrent.MVar
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, writeTVar)
import Control.Exception (ErrorCall (..), IOException, SomeException, bracket, throwIO, try)
import Control.Monad (replicateM_, void, when)
import qualified Data.ByteString as BS
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Foreign.Marshal.Alloc (allocaBytes)
import GHC.Conc (retry)
import qualified Network.Socket as Socket
import Network.Streaming.Link
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
      timeout 2000000 (takeMVar finished) >>= \outcome ->
        fmap (either show (const "returned")) outcome `shouldBe` Just "interrupted"
      readCounts counts `shouldReturn` (3, 3)

    it "releases the registered watches when registering another fails" $ do
      counts <- newCounts
      fired <- newTVarIO False
      try @ErrorCall (awaitFirst [(fake counts fired, ()), (fake counts fired, ()), (throwIO (ErrorCall "no registration"), ())])
        `shouldSatisfy` P.returns (P.left (P.eq (ErrorCall "no registration")))
      readCounts counts `shouldReturn` (2, 2)

  describe "the socket link's watches" $ do
    it "fires a readable watch once data has been written" $
      withSocketPair $ \reader writer -> do
        allocaBytes 16 (\buffer -> receiveReady reader buffer 16) `shouldReturn` Nothing
        bracket (watchReadable reader) watchCancel $ \watch -> do
          timeout 100000 (atomically (watchReady watch)) `shouldReturn` Nothing
          sendReady writer ["x"] `shouldReturn` 1
          timeout 2000000 (atomically (watchReady watch)) `shouldReturn` Just ()

    it "fires a writable watch on an empty socket" $
      withSocketPair $ \socket _ ->
        bracket (watchWritable socket) watchCancel $ \watch ->
          timeout 2000000 (atomically (watchReady watch)) `shouldReturn` Just ()

    it "keeps another watch registered when one is released twice" $
      withSocketPair $ \reader writer -> do
        released <- watchReadable reader
        kept <- watchReadable reader
        watchCancel released
        watchCancel released
        sendReady writer ["x"] `shouldReturn` 1
        timeout 2000000 (atomically (watchReady kept)) `shouldReturn` Just ()
        watchCancel kept

  describe "the socket link's wait" $ do
    it "returns at once when it waits for nothing" $
      withSocketPair $ \socket _ ->
        timeout 1000000 (awaitAny socket (Interest False Done)) `shouldReturn` Just ()

    it "waits until readable" $
      withSocketPair $ \reader writer -> do
        timeout 100000 (awaitAny reader inputOnly) `shouldReturn` Nothing
        later (sendReady writer ["x"] >>= (`shouldBe` 1))
        timeout 2000000 (awaitAny reader inputOnly) `shouldReturn` Just ()
        timeout 2000000 (awaitAny reader inputOnly) `shouldReturn` Just ()

    it "waits until writable" $
      withSocketPair $ \writer reader -> do
        timeout 2000000 (awaitAny writer outputOnly) `shouldReturn` Just ()
        fill writer
        timeout 100000 (awaitAny writer outputOnly) `shouldReturn` Nothing
        later (drain reader)
        timeout 2000000 (awaitAny writer outputOnly) `shouldReturn` Just ()

    describe "for both directions" $ do
      it "returns at once when either is ready" $
        withSocketPair $ \socket peer -> do
          timeout 1000000 (awaitAny socket inputAndOutput) `shouldReturn` Just ()
          sendReady peer ["x"] `shouldReturn` 1
          fill socket
          timeout 1000000 (awaitAny socket inputAndOutput) `shouldReturn` Just ()

      it "waits until readable when it can't be written" $
        withSocketPair $ \socket peer -> do
          fill socket
          timeout 100000 (awaitAny socket inputAndOutput) `shouldReturn` Nothing
          later (sendReady peer ["x"] >>= (`shouldBe` 1))
          timeout 2000000 (awaitAny socket inputAndOutput) `shouldReturn` Just ()

      it "waits until writable when nothing can be read" $
        withSocketPair $ \socket peer -> do
          fill socket
          timeout 100000 (awaitAny socket inputAndOutput) `shouldReturn` Nothing
          later (drain peer)
          timeout 2000000 (awaitAny socket inputAndOutput) `shouldReturn` Just ()

      it "can wait again after it was interrupted" $
        withSocketPair $ \socket peer -> do
          fill socket
          replicateM_ 3 $ timeout 20000 (awaitAny socket inputAndOutput) `shouldReturn` Nothing
          later (sendReady peer ["x"] >>= (`shouldBe` 1))
          timeout 2000000 (awaitAny socket inputAndOutput) `shouldReturn` Just ()

      it "fails when the socket is closed while it waits" $
        withSocketPair $ \socket _ -> do
          fill socket
          later (Socket.close socket)
          try @IOException (timeout 2000000 (awaitAny socket inputAndOutput))
            `shouldSatisfy` P.returns (P.left P.anything)

    describe "for a source" $ do
      it "waits until the source's watch fires, and releases it" $
        withSocketPair $ \socket _ -> do
          counts <- newCounts
          fired <- newTVarIO False
          let interest = Interest False (OnSource (fake counts fired))
          timeout 100000 (awaitAny socket interest) `shouldReturn` Nothing
          later (atomically (writeTVar fired True))
          timeout 2000000 (awaitAny socket interest) `shouldReturn` Just ()
          readCounts counts `shouldReturn` (2, 2)

      it "waits for input too" $
        withSocketPair $ \reader writer -> do
          counts <- newCounts
          fired <- newTVarIO False
          let interest = Interest True (OnSource (fake counts fired))
          timeout 100000 (awaitAny reader interest) `shouldReturn` Nothing
          later (sendReady writer ["x"] >>= (`shouldBe` 1))
          timeout 2000000 (awaitAny reader interest) `shouldReturn` Just ()
          readCounts counts `shouldReturn` (2, 2)

  describe "a link's wait" $ do
    it "waits on the watches of its interest, unless it waits more directly" $ do
      let registered interest = do
            readable <- newCounts
            writable <- newCounts
            source <- newCounts
            fired <- newTVarIO True
            awaitAny (WatchedLink readable writable fired) (interest (fake source fired))
            (,,) <$> readCounts readable <*> readCounts writable <*> readCounts source
      registered (const (Interest False Done)) `shouldReturn` ((0, 0), (0, 0), (0, 0))
      registered (const (Interest True Done)) `shouldReturn` ((1, 1), (0, 0), (0, 0))
      registered (const (Interest False OnWritable)) `shouldReturn` ((0, 0), (1, 1), (0, 0))
      registered (const (Interest True OnWritable)) `shouldReturn` ((1, 1), (1, 1), (0, 0))
      registered (Interest False . OnSource) `shouldReturn` ((0, 0), (0, 0), (1, 1))
      registered (Interest True . OnSource) `shouldReturn` ((1, 1), (0, 0), (1, 1))

  describe "awaiting a callback" $ do
    it "gives what the callback is called with, and registers nothing more" $ do
      unregistered <- newIORef (0 :: Int)
      awaitCallback
        (\callback -> void (forkIO (threadDelay 20000 *> callback "called")))
        (\() -> atomicModifyIORef' unregistered (\n -> (n + 1, ())))
        `shouldReturn` ("called" :: String)
      readIORef unregistered `shouldReturn` 0

    it "gives what a callback called during the registration was called with" $
      awaitCallback ($ 'x') (\() -> fail "unregistered") `shouldReturn` 'x'

    it "unregisters what it registered when it is interrupted" $ do
      unregistered <- newIORef []
      timeout 50000 (awaitCallback (\_ -> pure "key") (\key -> writeIORef unregistered [key]))
        `shouldReturn` (Nothing :: Maybe ())
      readIORef unregistered `shouldReturn` ["key" :: String]

    it "unregisters nothing when the registration fails" $
      try @ErrorCall (awaitCallback (\_ -> throwIO (ErrorCall "no registration")) (\() -> fail "unregistered") :: IO ())
        `shouldSatisfy` P.returns (P.left (P.eq (ErrorCall "no registration")))

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

-- | A link that has only watches, which register in the counts and fire when
-- the flag is set.
data WatchedLink = WatchedLink Counts Counts (TVar Bool)

instance Link WatchedLink where
  watchReadable (WatchedLink readable _ fired) = fake readable fired
  watchWritable (WatchedLink _ writable fired) = fake writable fired
  receiveReady _ _ _ = fail "unexpected receive"
  sendReady _ _ = fail "unexpected send"
  flushReady _ = fail "unexpected flush"
  closeLink _ = pure ()

-- | Runs the action soon, so that a wait that starts now has begun by then.
later :: IO () -> IO ()
later action = void (forkIO (threadDelay 20000 *> action))

inputOnly, outputOnly, inputAndOutput :: Interest
inputOnly = Interest True Done
outputOnly = Interest False OnWritable
inputAndOutput = Interest True OnWritable

-- | Fills the socket's send buffer, so that it isn't writable.
fill :: Socket.Socket -> IO ()
fill socket = do
  taken <- sendReady socket [BS.replicate 65536 0]
  when (taken > 0) (fill socket)

-- | Reads what the socket has, so that its peer can write again.
drain :: Socket.Socket -> IO ()
drain socket = allocaBytes 65536 $ \buffer ->
  let go = receiveReady socket buffer 65536 >>= \received -> when (maybe False (> 0) received) go
   in go

-- | A connected pair of sockets, closed afterwards.
withSocketPair :: (Socket.Socket -> Socket.Socket -> IO a) -> IO a
withSocketPair use =
  bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b) (uncurry use)
