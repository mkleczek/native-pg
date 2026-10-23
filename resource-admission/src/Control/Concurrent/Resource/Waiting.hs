{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE StrictData #-}

-- | Private delivery for resources acquired in STM. Adapters own pending
-- requests; each waiting thread owns its RTS timer registration. Timer callbacks
-- signal expiration only, so adapter hooks run on waiter or resource-owner
-- threads. Waiting threads never retry on resource inventory.
module Control.Concurrent.Resource.Waiting
  ( Waiting, Waiter, Deadline(..), WaitError(..)
  , newWaiting, closeWaiting, withWaiting, deadlineAfter, deadlineExpired
  , withWait, tryGrant, reject
  ) where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad (unless, void)
import Data.Time.Clock (DiffTime)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Event qualified as Timer

data Deadline = NoDeadline | At Word64 deriving (Eq, Show)
data WaitError = TimedOut | Closed deriving (Eq, Show)

newtype Waiting = Waiting { closed :: TVar Bool }
data Delivery a = Pending | Offered (Either SomeException a) | Refused WaitError | Taken

data Waiter a = Waiter
  { mailbox :: TVar (Delivery a)
  , serviceClosed :: TVar Bool
  , expiration :: TVar Bool
  , detach :: STM ()
  }

newWaiting :: IO Waiting
newWaiting = Waiting <$> newTVarIO False

-- | Reject later registration and wake pending waiters with Closed.
-- Pending owners detach and cancel their timers through their normal scopes;
-- the process-wide RTS timer manager belongs to the runtime.
closeWaiting :: Waiting -> IO ()
closeWaiting waiting = mask_ (atomically (writeTVar (closed waiting) True))

withWaiting :: (Waiting -> IO a) -> IO a
withWaiting = bracket newWaiting closeWaiting

-- | Saturate rather than wrapping an absolute monotonic deadline.
deadlineAfter :: DiffTime -> IO Deadline
deadlineAfter duration = do
  now <- getMonotonicTimeNSec
  let delta = max 0 (floor (toRational duration * 1000000000) :: Integer)
  pure (At (fromInteger (min (toInteger (maxBound :: Word64)) (toInteger now + delta))))

deadlineExpired :: Deadline -> IO Bool
deadlineExpired NoDeadline = pure False
deadlineExpired (At deadline) = (>= deadline) <$> getMonotonicTimeNSec

-- | Enqueue returns an opaque adapter handle. Removal must not retry or throw;
-- it runs once after a terminal mailbox write, in the winning STM transaction. An
-- immediate grant or rejection defers removal until enqueue returns its handle,
-- still within the registration transaction. Cleanup owns every successful
-- reservation, including cancellation between assignment and callback entry.
-- The callback restores the caller's masking state.
withWait :: Waiting -> Deadline -> (Waiter a -> STM handle) -> (handle -> STM ()) -> (a -> IO ()) -> (a -> IO r) -> IO (Either WaitError r)
withWait waiting deadline enqueue remove cleanup use = mask $ \restore -> do
  expired <- deadlineExpired deadline
  waiter <- atomically $ do
    isClosed <- readTVar (closed waiting)
    box <- newTVar Pending
    signal <- newTVar False
    -- The flag remembers an early request and consumes an installed hook before
    -- invoking it, so a reentrant adapter hook cannot detach or reserve twice.
    removal <- newTVar (False, Nothing)
    let detachOnce = do
          (requested, hook) <- readTVar removal
          unless requested $ do
            writeTVar removal (True, Nothing)
            sequence_ hook
        waiter = Waiter box (closed waiting) signal detachOnce
    if isClosed then writeTVar box (Refused Closed)
    else if expired then writeTVar box (Refused TimedOut)
    else do
      registration <- enqueue waiter
      (requested, _) <- readTVar removal
      if requested then remove registration
      else writeTVar removal (False, Just (remove registration))
    pure waiter
  let abandon = do
        assigned <- atomically $ do
          delivery <- readTVar (mailbox waiter)
          case delivery of
            Pending -> writeTVar (mailbox waiter) Taken >> detach waiter >> pure Nothing
            Offered value -> writeTVar (mailbox waiter) Taken >> pure (Just value)
            _ -> pure Nothing
        case assigned of
          Just (Right value) -> cleanup value
          _ -> pure ()
      await = do
        delivery <- readTVar (mailbox waiter)
        case delivery of
          Pending -> do
            refusal <- pendingRefusal waiter
            case refusal of
              Just reason -> void (reject waiter reason) >> pure (Refused reason)
              Nothing -> retry
          _ -> pure delivery
      waitForDelivery = do
        outcome <- bracket (armTimer deadline waiter) cancelTimer $ \timer ->
          restore $ atomically $
            (Just <$> await) `orElse` (awaitRearm timer >> pure Nothing)
        maybe waitForDelivery pure outcome
  -- Cancellation precedes taking delivery ownership. If registration or
  -- cancellation fails, abandon still owns any concurrently offered resource.
  delivery <- waitForDelivery `onException` abandon
  atomically (writeTVar (mailbox waiter) Taken)
  case delivery of
    Refused reason -> pure (Left reason)
    Offered (Left exception) -> throwIO exception
    Offered (Right value) -> do
      -- Scheduling or a long enqueue transaction may cross the deadline after
      -- the initial clock read. An assigned value still belongs to cleanup.
      expiredAtDelivery <- deadlineExpired deadline
      if expiredAtDelivery then cleanup value >> pure (Left TimedOut)
      else Right <$> (restore (use value) `finally` cleanup value)
    _ -> error "withWait: mailbox ownership invariant"

-- | A retry rolls back the complete reservation and returns False. Closure or
-- expiration detaches without reserving inventory. Exceptions
-- are delivered to the requesting thread, keeping owner scheduling usable.
tryGrant :: Waiter a -> STM a -> STM Bool
tryGrant waiter reserve = do
  delivery <- readTVar (mailbox waiter)
  case delivery of
    Pending -> do
      refusal <- pendingRefusal waiter
      case refusal of
        Just reason -> reject waiter reason >> pure False
        Nothing -> do
          value <- ((Just . Right <$> reserve) `orElse` pure Nothing)
            `catchSTM` (pure . Just . Left @SomeException)
          case value of
            Nothing -> pure False
            Just result -> do
              writeTVar (mailbox waiter) (Offered result)
              detach waiter
              pure True
    _ -> pure False

reject :: Waiter a -> WaitError -> STM Bool
reject waiter reason = do
  delivery <- readTVar (mailbox waiter)
  case delivery of
    Pending -> do
      writeTVar (mailbox waiter) (Refused reason)
      detach waiter
      pure True
    _ -> pure False

-- Both paths check these flags only while Pending, preserving an offer that
-- already won. Closure has precedence when shutdown and expiration coincide.
pendingRefusal :: Waiter a -> STM (Maybe WaitError)
pendingRefusal waiter = do
  isClosed <- readTVar (serviceClosed waiter)
  expired <- readTVar (expiration waiter)
  pure (if isClosed then Just Closed else if expired then Just TimedOut else Nothing)

data TimerRegistration = TimerRegistration Timer.TimerManager Timer.TimeoutKey (Maybe (TVar Bool))

armTimer :: Deadline -> Waiter a -> IO (Maybe TimerRegistration)
armTimer NoDeadline _ = pure Nothing
armTimer (At at) waiter = do
  pending <- atomically $ do
    delivery <- readTVar (mailbox waiter)
    pure (case delivery of Pending -> True; _ -> False)
  if not pending then pure Nothing else do
    manager <- Timer.getSystemTimerManager
    now <- getMonotonicTimeNSec
    let remaining = (max 0 (toInteger at - toInteger now) + 999) `div` 1000
        micros = min (toInteger (maxBound :: Int)) remaining
    -- On 32-bit runtimes a distant deadline needs several bounded intervals.
    -- Their callbacks wake this owner to rearm, without declaring expiration.
    rearm <- if micros < remaining then Just <$> newTVarIO False else pure Nothing
    let -- Evaluate the selector before building the callback: even a timer
        -- already dequeued during cancellation must retain only this signal.
        !signal = maybe (expiration waiter) id rearm
    key <- Timer.registerTimeout manager (fromInteger micros) (expireSignal signal)
    pure (Just (TimerRegistration manager key rearm))

cancelTimer :: Maybe TimerRegistration -> IO ()
cancelTimer Nothing = pure ()
cancelTimer (Just (TimerRegistration manager key _)) = Timer.unregisterTimeout manager key

awaitRearm :: Maybe TimerRegistration -> STM ()
awaitRearm (Just (TimerRegistration _ _ (Just signal))) = readTVar signal >>= check
awaitRearm _ = retry

expireSignal :: TVar Bool -> IO ()
expireSignal !signal = atomically (writeTVar signal True)
