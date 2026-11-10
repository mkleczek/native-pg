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
  , enroll, delivered, withdraw
  ) where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad (void)
import Data.Bool (bool)
import Data.Time.Clock (DiffTime, diffTimeToPicoseconds)
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
  let delta = max 0 (diffTimeToPicoseconds duration `div` 1000)
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
        let abandon = atomically (withdraw waiter) >>= mapM_ cleanup
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
      bool (pure (Just value)) (cleanup value >> pure Nothing) expiredAtDelivery

-- | The first half of 'acquire' without the wait, for callers that cannot
-- block: reserve now, or register in the same transaction, so a release
-- cannot be missed. The caller owns the request: 'delivered' shows its outcome
-- and 'withdraw' gives it up. There is no deadline and no cleanup hook; a
-- delivered value stays with the request until 'withdraw' returns it, so
-- whoever abandons the request must release that value. An immediate
-- reservation is a request that has been delivered already, and never runs
-- the enqueue or removal hooks. A grant or error during enqueue defers
-- removal as it does for 'acquire'. An exception thrown by the immediate
-- reservation propagates and registers nothing.
enroll :: STM a -> (Request a -> STM handle) -> (handle -> STM ()) -> STM (Request a)
enroll reserveNow enqueue remove = (reserveNow >>= offered) `orElse` registerRequest enqueue remove
  where
    offered value = Request <$> newTVar (Offered (Right value)) <*> newTVar False <*> pure (pure ())

-- | What the request has been given, without taking it: the value, or the
-- exception that the reservation raised in 'tryGrant'. Nothing while the
-- request is pending, and after 'withdraw'.
delivered :: Request a -> STM (Maybe (Either SomeException a))
delivered waiter = offeredResult <$> readTVar (deliveryState waiter)

offeredResult :: Delivery a -> Maybe (Either SomeException a)
offeredResult (Offered result) = Just result
offeredResult _ = Nothing

-- | Gives up the request. A pending one is detached and can no longer be
-- granted; a delivered one returns its value, which the caller must now
-- release. Withdrawing twice, or after expiry, does nothing.
withdraw :: Request a -> STM (Maybe a)
withdraw waiter = do
  state <- readTVar (deliveryState waiter)
  case state of
    Pending -> writeTVar (deliveryState waiter) Taken >> detach waiter >> pure Nothing
    Offered result -> writeTVar (deliveryState waiter) Taken >> pure (either (const Nothing) Just result)
    _ -> pure Nothing

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
        maybe (pure False) grant value
    _ -> pure False
  where
    grant result = writeTVar (deliveryState waiter) (Offered result) >> detach waiter >> pure True

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
  pending <- atomically (isPending <$> readTVar (deliveryState waiter))
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

isPending :: Delivery a -> Bool
isPending Pending = True
isPending _ = False

cancelTimer :: Maybe TimerRegistration -> IO ()
cancelTimer Nothing = pure ()
cancelTimer (Just (TimerRegistration manager key _)) = Timer.unregisterTimeout manager key

awaitRearm :: Maybe TimerRegistration -> STM ()
awaitRearm (Just (TimerRegistration _ _ (Just signal))) = readTVar signal >>= check
awaitRearm _ = retry

expireSignal :: TVar Bool -> IO ()
expireSignal !signal = atomically (writeTVar signal True)
