{-# LANGUAGE RankNTypes #-}

-- | Readiness as STM, so that waiting for one of several events needs no
-- helper thread: register the events, wait on all of them in one transaction,
-- release them.
module Network.Streaming.Watch
  ( Watch (..),
    readyNow,
    fdReadable,
    fdWritable,
    awaitFirst,
    awaitWatch,
    awaitCallback,
    anyWatch,
  )
where

import Control.Concurrent (threadWaitReadSTM, threadWaitWriteSTM)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (mask, mask_, onException, uninterruptibleMask_)
import Control.Monad (unless)
import Data.IORef (atomicModifyIORef', modifyIORef', newIORef, readIORef)
import GHC.Conc (STM, atomically, orElse, retry)
import System.Posix.Types (Fd)

-- | A registered readiness event. 'watchReady' retries until the event has
-- fired; 'watchCancel' releases the registration and is idempotent. A watch
-- only registers interest: it reads and writes nothing.
data Watch = Watch {watchReady :: STM (), watchCancel :: IO ()}

-- | An event that has fired already, with nothing to release.
readyNow :: Watch
readyNow = Watch (pure ()) (pure ())

-- | Fires when the descriptor is readable, or at its end or an error, as
-- 'threadWaitReadSTM' does.
fdReadable :: Fd -> IO Watch
fdReadable = fdWatch threadWaitReadSTM

-- | Fires when the descriptor is writable, or at an error, as
-- 'threadWaitWriteSTM' does.
fdWritable :: Fd -> IO Watch
fdWritable = fdWatch threadWaitWriteSTM

fdWatch :: (Fd -> IO (STM (), IO ())) -> Fd -> IO Watch
fdWatch register fd = do
  (ready, unregister) <- register fd
  released <- newIORef False
  -- Only the first cancel unregisters.
  pure $ Watch ready $ do
    done <- atomicModifyIORef' released (True,)
    unless done unregister

-- | Registers the events in order, waits until the first has fired, and
-- releases all of them, also if registering one fails or the wait is
-- interrupted. If several have fired, the earliest in the list wins. The
-- list must not be empty.
awaitFirst :: [(IO Watch, a)] -> IO a
awaitFirst entries = mask (awaitFirstMasked entries)
{-# NOINLINE awaitFirst #-}

-- The wait's code is in a function that takes @restore@, and 'awaitFirst'
-- isn't inlined, so that the code exists once, and not once for each masking
-- state at each caller.
awaitFirstMasked :: [(IO Watch, a)] -> (forall b. IO b -> IO b) -> IO a
awaitFirstMasked entries restore = do
  registered <- newIORef []
  let register (make, result) = do
        watch <- make
        modifyIORef' registered (watch :)
        pure (result <$ watchReady watch)
      release = readIORef registered >>= releaseWatches
  fired <- (traverse register entries >>= restore . atomically . foldr orElse retry) `onException` release
  fired <$ release
{-# NOINLINE awaitFirstMasked #-}

-- | 'awaitFirst' of one watch.
awaitWatch :: IO Watch -> IO ()
awaitWatch watch = awaitFirst [(watch, ())]
{-# NOINLINE awaitWatch #-}

-- | Registers a callback with an event source, waits until the source calls
-- it, and gives what it was called with. The registration is released if the
-- wait is interrupted, and by the source once it has called the callback.
-- This is the wait of 'Control.Concurrent.threadWaitRead', for any source.
awaitCallback :: ((a -> IO ()) -> IO key) -> (key -> IO ()) -> IO a
awaitCallback register unregister = mask_ $ do
  called <- newEmptyMVar
  key <- register (putMVar called)
  takeMVar called `onException` unregister key
{-# NOINLINE awaitCallback #-}

-- | Releases the watches, the latest registration first, each even if the
-- one before it fails to be.
releaseWatches :: [Watch] -> IO ()
releaseWatches = uninterruptibleMask_ . go
  where
    go [] = pure ()
    go (watch : earlier) = (watchCancel watch `onException` go earlier) *> go earlier
{-# NOINLINE releaseWatches #-}

anyWatch :: [IO Watch] -> IO Watch
anyWatch _ = pure readyNow
