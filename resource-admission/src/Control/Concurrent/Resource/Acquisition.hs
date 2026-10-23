{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE StrictData #-}

-- | Private delivery for resources acquired in STM. Adapters own pending
-- requests; each requesting thread owns its RTS timer registration. Timer callbacks
-- signal expiration only, so adapter hooks run on waiter or resource-owner
-- threads. Requesting threads never retry on resource inventory.
module Control.Concurrent.Resource.Acquisition
  ( Context, Request, Deadline(..)
  , newContext, deadlineAfter, deadlineExpired
  , acquire, tryGrant
  ) where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad (void)
import Data.Time.Clock (DiffTime)
import Data.Maybe (fromMaybe)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Event qualified as Timer

data Deadline = NoDeadline | At Word64 deriving (Eq, Show)

newtype Context = Context Timer.TimerManager
data Delivery a = Pending | Offered (Either SomeException a) | TimedOut | Taken
data Removal = AwaitingHandle | Armed (STM ()) | RemovalRequested

data Request a = Request
  { deliveryState :: TVar (Delivery a)
  , expiration :: TVar Bool
  , detach :: STM ()
  }

-- | Share the application's timer manager without taking ownership of it.
newContext :: Timer.TimerManager -> Context
newContext = Context

-- | Saturate rather than wrapping an absolute monotonic deadline.
deadlineAfter :: DiffTime -> IO Deadline
deadlineAfter duration = do
  now <- getMonotonicTimeNSec
  let delta = max 0 (floor (toRational duration * 1000000000) :: Integer)
  pure (At (fromInteger (min (toInteger (maxBound :: Word64)) (toInteger now + delta))))

deadlineExpired :: Deadline -> IO Bool
deadlineExpired NoDeadline = pure False
deadlineExpired (At deadline) = (>= deadline) <$> getMonotonicTimeNSec

-- | Reserve immediately when inventory and adapter fairness permit it; retry
-- chooses registration in the same transaction, so an intervening release
-- cannot be missed. An expired deadline skips both reservation and registration.
--
-- Enqueue returns an opaque adapter handle. Removal must not retry or throw;
-- it runs once after writing a terminal delivery state, in the winning STM
-- transaction. A grant or error during enqueue defers removal until enqueue
-- returns its handle, still within the registration transaction.
--
-- Cleanup reclaims only assigned values whose ownership was never transferred.
-- A successful return transfers ownership to the caller, which must mask the
-- handoff (for example with bracket) and arrange its own normal release.
acquire :: Context -> Deadline -> STM a -> (Request a -> STM handle) -> (handle -> STM ()) -> (a -> IO ()) -> IO (Maybe a)
acquire (Context manager) deadline reserveNow enqueue remove cleanup = mask $ \restore -> do
  expired <- deadlineExpired deadline
  if expired then pure Nothing else do
    reservation <- atomically $
      (Left <$> reserveNow) `orElse` (Right <$> registerRequest enqueue remove)
    case reservation of
      Left value -> handoff value
      Right waiter -> do
        let abandon = do
              assigned <- atomically $ do
                delivery <- readTVar (deliveryState waiter)
                case delivery of
                  Pending -> writeTVar (deliveryState waiter) Taken >> detach waiter >> pure Nothing
                  Offered value -> writeTVar (deliveryState waiter) Taken >> pure (Just value)
                  _ -> pure Nothing
              case assigned of
                Just (Right value) -> cleanup value
                _ -> pure ()
            await = do
              delivery <- readTVar (deliveryState waiter)
              case delivery of
                Pending -> do
                  expiredWhilePending <- readTVar (expiration waiter)
                  if expiredWhilePending then void (timeoutRequest waiter) >> pure TimedOut
                  else retry
                _ -> pure delivery
            waitForDelivery = do
              outcome <- bracket (armTimer manager deadline waiter) cancelTimer $ \timer ->
                restore $ atomically $
                  (Just <$> await) `orElse` (awaitRearm timer >> pure Nothing)
              maybe waitForDelivery pure outcome
        -- Cancellation precedes taking delivery ownership. If registration or
        -- cancellation fails, abandon still owns any concurrently offered resource.
        delivery <- waitForDelivery `onException` abandon
        atomically (writeTVar (deliveryState waiter) Taken)
        case delivery of
          TimedOut -> pure Nothing
          Offered (Left exception) -> throwIO exception
          Offered (Right value) -> handoff value
          _ -> error "acquire: delivery state ownership invariant"
  where
    handoff value = do
      -- A reservation or timer cancellation may cross the deadline after the
      -- initial clock read. Ownership still belongs to cleanup until this check.
      expiredAtDelivery <- deadlineExpired deadline
      if expiredAtDelivery then cleanup value >> pure Nothing
      else pure (Just value)

registerRequest :: (Request a -> STM handle) -> (handle -> STM ()) -> STM (Request a)
registerRequest enqueue remove = do
  state <- newTVar Pending
  signal <- newTVar False
  -- Remember an early request and consume an installed hook before
  -- invoking it, so a reentrant adapter hook cannot detach or reserve twice.
  removal <- newTVar AwaitingHandle
  let detachOnce = do
        removalState <- readTVar removal
        case removalState of
          AwaitingHandle -> writeTVar removal RemovalRequested
          Armed hook -> do
            writeTVar removal RemovalRequested
            hook
          RemovalRequested -> pure ()
      waiter = Request state signal detachOnce
  registration <- enqueue waiter
  removalState <- readTVar removal
  case removalState of
    RemovalRequested -> remove registration
    _ -> writeTVar removal (Armed (remove registration))
  pure waiter

-- | A retry rolls back the complete reservation and returns False.
-- Expiration detaches without reserving inventory. Exceptions
-- are delivered to the requesting thread, keeping owner scheduling usable.
tryGrant :: Request a -> STM a -> STM Bool
tryGrant waiter reserve = do
  delivery <- readTVar (deliveryState waiter)
  case delivery of
    Pending -> do
      expired <- readTVar (expiration waiter)
      if expired then timeoutRequest waiter >> pure False
      else do
        value <- ((Just . Right <$> reserve) `orElse` pure Nothing)
          `catchSTM` (pure . Just . Left @SomeException)
        case value of
          Nothing -> pure False
          Just result -> do
            writeTVar (deliveryState waiter) (Offered result)
            detach waiter
            pure True
    _ -> pure False

-- Only the deadline signal may complete a request without a reservation.
timeoutRequest :: Request a -> STM Bool
timeoutRequest waiter = do
  delivery <- readTVar (deliveryState waiter)
  case delivery of
    Pending -> do
      writeTVar (deliveryState waiter) TimedOut
      detach waiter
      pure True
    _ -> pure False

data TimerRegistration = TimerRegistration Timer.TimerManager Timer.TimeoutKey (Maybe (TVar Bool))

armTimer :: Timer.TimerManager -> Deadline -> Request a -> IO (Maybe TimerRegistration)
armTimer _ NoDeadline _ = pure Nothing
armTimer manager (At at) waiter = do
  pending <- atomically $ do
    delivery <- readTVar (deliveryState waiter)
    pure (case delivery of Pending -> True; _ -> False)
  if not pending then pure Nothing else do
    now <- getMonotonicTimeNSec
    let remaining = (max 0 (toInteger at - toInteger now) + 999) `div` 1000
        micros = min (toInteger (maxBound :: Int)) remaining
    -- On 32-bit runtimes a distant deadline needs several bounded intervals.
    -- Their callbacks wake this owner to rearm, without declaring expiration.
    rearm <- if micros < remaining then Just <$> newTVarIO False else pure Nothing
    let -- Evaluate the selector before building the callback: even a timer
        -- already dequeued during cancellation must retain only this signal.
        !signal = fromMaybe (expiration waiter) rearm
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
