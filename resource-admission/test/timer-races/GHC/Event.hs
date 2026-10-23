-- A home-module replacement for the timer API used by the Acquisition
-- source. The real backend is exercised separately by resource-admission-spec.
module GHC.Event
  ( TimerManager, TimeoutKey, getSystemTimerManager, registerTimeout, unregisterTimeout
  , withTimerControl, withIndependentTimerControl, registration, tryRegistration, completeRegistration, failRegistration
  , fireTimeout, awaitUnregistered, unregisterCount, pauseUnregistration, completeUnregistration
  ) where

import Control.Concurrent.MVar
import Control.Exception
import Data.IORef
import System.IO.Unsafe (unsafePerformIO)

data TimerManager = TimerManager (MVar TimeoutKey) (MVar (Either SomeException ())) (IORef ())
data TimeoutKey = TimeoutKey (IO ()) (MVar ()) (IORef Int) (IORef ()) (MVar ())

{-# NOINLINE activeManager #-}
activeManager :: MVar TimerManager
activeManager = unsafePerformIO newEmptyMVar

-- Holding this scope serializes tests as well as selecting their timer manager.
withTimerControl :: (TimerManager -> IO a) -> IO a
withTimerControl = bracket acquire release
  where
    acquire = do
      manager <- newManager
      putMVar activeManager manager
      pure manager
    release (TimerManager _ finish _) = do
      -- A failed assertion must not strand a worker behind our test barrier.
      _ <- tryPutMVar finish (Right ())
      _ <- takeMVar activeManager
      pure ()

newManager :: IO TimerManager
newManager = TimerManager <$> newEmptyMVar <*> newEmptyMVar <*> newIORef ()

withIndependentTimerControl :: (TimerManager -> IO a) -> IO a
withIndependentTimerControl = bracket newManager release
  where
    release (TimerManager _ finish _) = do
      _ <- tryPutMVar finish (Right ())
      pure ()

tryRegistration :: TimerManager -> IO (Maybe TimeoutKey)
tryRegistration (TimerManager entered _ _) = tryTakeMVar entered

getSystemTimerManager :: IO TimerManager
getSystemTimerManager = readMVar activeManager

registerTimeout :: TimerManager -> Int -> IO () -> IO TimeoutKey
registerTimeout (TimerManager entered finish owner) _ callback = uninterruptibleMask_ $ do
  key <- TimeoutKey callback <$> newEmptyMVar <*> newIORef 0 <*> pure owner <*> newMVar ()
  putMVar entered key
  -- Model the masked key-publication interval. Widen it without allowing the
  -- test barrier itself to introduce an interruption before the key is returned.
  takeMVar finish >>= either throwIO pure
  pure key

unregisterTimeout :: TimerManager -> TimeoutKey -> IO ()
unregisterTimeout (TimerManager _ _ manager) (TimeoutKey _ done count owner gate) = do
  if manager == owner then pure () else fail "timer cancelled through a different manager"
  atomicModifyIORef' count (\n -> (n + 1, ()))
  _ <- tryPutMVar done ()
  readMVar gate

-- Hold cancellation at an interruptible key-unregistration boundary.
pauseUnregistration :: TimeoutKey -> IO ()
pauseUnregistration (TimeoutKey _ _ _ _ gate) = takeMVar gate

completeUnregistration :: TimeoutKey -> IO ()
completeUnregistration (TimeoutKey _ _ _ _ gate) = putMVar gate ()

registration :: TimerManager -> IO TimeoutKey
registration (TimerManager entered _ _) = takeMVar entered

completeRegistration :: TimerManager -> IO ()
completeRegistration (TimerManager _ finish _) = putMVar finish (Right ())

failRegistration :: TimerManager -> IO ()
failRegistration (TimerManager _ finish _) = putMVar finish (Left (toException (userError "arming failed")))

-- GHC may already have dequeued a callback when unregisterTimeout runs.
fireTimeout :: TimeoutKey -> IO ()
fireTimeout (TimeoutKey callback _ _ _ _) = callback

awaitUnregistered :: TimeoutKey -> IO ()
awaitUnregistered (TimeoutKey _ done _ _ _) = readMVar done

unregisterCount :: TimeoutKey -> IO Int
unregisterCount (TimeoutKey _ _ count _ _) = readIORef count
