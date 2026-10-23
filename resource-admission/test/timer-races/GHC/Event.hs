-- A home-module replacement for the timer API used by the unchanged Waiting
-- source. The real backend is exercised separately by resource-admission-spec.
module GHC.Event
  ( TimerManager, TimeoutKey, getSystemTimerManager, registerTimeout, unregisterTimeout
  , withTimerControl, registration, completeRegistration, failRegistration
  , fireTimeout, awaitUnregistered, unregisterCount
  ) where

import Control.Concurrent.MVar
import Control.Exception
import Data.IORef
import System.IO.Unsafe (unsafePerformIO)

data TimerManager = TimerManager (MVar TimeoutKey) (MVar (Either SomeException ()))
data TimeoutKey = TimeoutKey (IO ()) (MVar ()) (IORef Int)

{-# NOINLINE activeManager #-}
activeManager :: MVar TimerManager
activeManager = unsafePerformIO newEmptyMVar

-- Holding this scope serializes tests as well as selecting their timer manager.
withTimerControl :: (TimerManager -> IO a) -> IO a
withTimerControl action = bracket acquire release action
  where
    acquire = do
      manager <- TimerManager <$> newEmptyMVar <*> newEmptyMVar
      putMVar activeManager manager
      pure manager
    release (TimerManager _ finish) = do
      -- A failed assertion must not strand a worker behind our test barrier.
      _ <- tryPutMVar finish (Right ())
      _ <- takeMVar activeManager
      pure ()

getSystemTimerManager :: IO TimerManager
getSystemTimerManager = readMVar activeManager

registerTimeout :: TimerManager -> Int -> IO () -> IO TimeoutKey
registerTimeout (TimerManager entered finish) _ callback = uninterruptibleMask_ $ do
  key <- TimeoutKey callback <$> newEmptyMVar <*> newIORef 0
  putMVar entered key
  -- Model the masked key-publication interval. Widen it without allowing the
  -- test barrier itself to introduce an interruption before the key is returned.
  takeMVar finish >>= either throwIO pure
  pure key

unregisterTimeout :: TimerManager -> TimeoutKey -> IO ()
unregisterTimeout _ (TimeoutKey _ done count) = do
  atomicModifyIORef' count (\n -> (n + 1, ()))
  _ <- tryPutMVar done ()
  pure ()

registration :: TimerManager -> IO TimeoutKey
registration (TimerManager entered _) = takeMVar entered

completeRegistration :: TimerManager -> IO ()
completeRegistration (TimerManager _ finish) = putMVar finish (Right ())

failRegistration :: TimerManager -> IO ()
failRegistration (TimerManager _ finish) = putMVar finish (Left (toException (userError "arming failed")))

-- GHC may already have dequeued a callback when unregisterTimeout runs.
fireTimeout :: TimeoutKey -> IO ()
fireTimeout (TimeoutKey callback _ _) = callback

awaitUnregistered :: TimeoutKey -> IO ()
awaitUnregistered (TimeoutKey _ done _) = readMVar done

unregisterCount :: TimeoutKey -> IO Int
unregisterCount (TimeoutKey _ _ count) = readIORef count
