{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}

-- | A pool of connections of any kind. It's given how to open, close and
-- check its connections, so the same pool serves different clients.
--
-- A use takes an idle connection, or opens one while the pool is below its
-- size, or else waits up to the acquisition timeout for one. The uses that
-- wait form a FIFO belonging to this pool. Independent pools may share one
-- service that expires their deadlines. A connection past its maximum lifetime
-- or idle time is closed instead of used, and a reaper closes idle ones in the
-- background.
--
-- A use owns its connection, or its slot while it opens one, until it gives
-- it back, so an exception, e.g. its thread being killed, leaks neither.
module Database.PostgreSQL.Pool
  ( Pool
  , Config (..)
  , Outcome (..)
  , UsageError (..)
  , newPool
  , release
  , destroy
  , PoolClosedError (..)
  , Policy (..)
  , Admission (..)
  , defaultPolicy
  , overflowPolicy
  , newPoolWithPolicy
  , Stats (..)
  , stats
  , poolPolicyStats
  , use
  , useChecked
  , Reservation
  , ReservationError (..)
  , tryReserveReady
  , validateReservation
  , finishReservation
  , useReservation
  , Opening
  , OpeningError (..)
  , tryReserveOpening
  , prepareOpening
  , abandonOpening
  )
where

import Control.Concurrent (ThreadId, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.STM
  ( STM, TQueue, TVar, atomically, check, flushTQueue, modifyTVar', newTQueueIO
  , newTVar, newTVarIO, readTVar, readTVarIO, orElse, retry, registerDelay, throwSTM, tryReadTQueue, unGetTQueue, writeTQueue, writeTVar
  )
import Control.Concurrent.Resource.Acquisition qualified as Acquire
import Control.Concurrent.Resource.Queue qualified as Queue
import Control.Exception (Exception, SomeException, bracket, displayException, finally, mask, mask_, onException, throwIO, try)
import Control.Monad (forever, unless, void, when)
import Data.Either (fromRight)
import Data.Foldable (for_, traverse_)
import Data.Functor (($>))
import Data.IORef (IORef, mkWeakIORef, newIORef)
import Data.List (partition)
import Data.Maybe (isNothing)
import Data.Text (Text)
import Data.Time.Clock (DiffTime, diffTimeToPicoseconds)
import Data.UUID (UUID)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Event qualified as Timer

import Data.Text qualified as Text
import Data.UUID.V4 qualified as UUID

import Database.PostgreSQL.Internal.PoolLifetime (withLifetime)
import Database.PostgreSQL.Pool.Observation

-- | What the pool needs to know about its connections, and its limits.
data Config connection failure = Config
  { configSize :: Int
  -- ^ The most connections open at once.
  , configAcquisitionTimeout :: DiffTime
  -- ^ How long a use waits for admission. Connection startup and the callback
  -- are outside this waiting timeout.
  , configMaxLifetime :: DiffTime
  -- ^ How long a connection is used for, at most.
  , configMaxIdletime :: DiffTime
  -- ^ How long a connection may stay idle.
  , configConnect :: IO (Either failure connection)
  -- ^ Opens a connection, or tells why it couldn't.
  , configDescribeFailure :: failure -> Maybe Text
  -- ^ Details of a failure to open, for the observations.
  , configClose :: connection -> IO ()
  , configUsableAfterException :: connection -> IO Bool
  -- ^ Whether a connection is usable after a use threw, e.g. because its
  -- thread was killed.
  , configObserve :: Observation -> IO ()
  }

-- | What a use did to its connection.
data Outcome
  = -- | It succeeded, and the connection goes back to the pool.
    Succeeded
  | -- | It failed, but the connection is fine and goes back to the pool.
    Failed SomeException
  | -- | It broke the connection, so the pool closes it. With details if there
    -- are any.
    Broke (Maybe Text)

data UsageError failure
  = -- | No connection became free within the acquisition timeout.
    AcquisitionTimeout
  | -- | Opening a connection failed.
    ConnectionFailed failure
  | -- | The pool was permanently destroyed.
    PoolClosed
  deriving (Eq, Show)

-- | Explicit STM checkout attempted after permanent shutdown.
data PoolClosedError = PoolClosedError deriving (Eq, Show)
instance Exception PoolClosedError

-- | Admission of a new connection. Ready connections do not wait for policy.
data Admission = Admit | AdmitAfter Int | Wait deriving (Eq, Show)

-- | Reusable connection policy. Counts include reserved opening slots. Delay
-- is in microseconds; the pool asks again after waiting without surrendering
-- FIFO position. A failed startup may ask ordinary uses to keep waiting.
data Policy failure = Policy
  { policyAdmit :: Int -> Bool -> STM Admission
  , policyKeep :: Int -> STM Bool
  , policyFailed :: failure -> IO Bool
  , policyTick :: IO ()
  , policyStats :: IO [(Text, Int)]
  }

defaultPolicy :: Policy failure
defaultPolicy = Policy (\_ _ -> pure Admit) (const $ pure True) (const $ pure False) (pure ()) (pure [])

-- | Keep at most the soft size, delay overflow connections by their position,
-- and back off on errors the caller identifies as connection-limit refusals.
-- The caller sets configSize to the hard size including overflow.
overflowPolicy :: Int -> DiffTime -> (failure -> Bool) -> IO (Policy failure)
overflowPolicy size delay isRefused = do
  when (size <= 0) (ioError (userError "policy size must be positive"))
  soft <- newTVarIO size
  refusals <- newTVarIO 0
  lastBackoff <- newTVarIO =<< getMonotonicTimeNSec
  gate <- newTVarIO =<< newTVarIO True
  let micros = fromInteger $ min (toInteger (maxBound :: Int)) (max 0 (diffTimeToPicoseconds delay `div` 1_000_000))
      admit opened waited = do
        available <- readTVar gate >>= readTVar
        limit <- readTVar soft
        pure $ if not available then Wait else if opened < limit || waited then Admit
          else AdmitAfter (fromInteger $ min (toInteger (maxBound :: Int)) (toInteger micros * toInteger (opened - limit + 1)))
      failed err | not (isRefused err) = pure False
                 | otherwise = do
          paused <- registerDelay micros
          now <- getMonotonicTimeNSec
          atomically $ do
            modifyTVar' refusals (+ 1)
            modifyTVar' soft (\n -> max 1 (fromInteger $ toInteger n * 7 `div` 10))
            writeTVar lastBackoff now
            writeTVar gate paused
          pure True
      tick = do
        now <- getMonotonicTimeNSec
        atomically $ do
          limit <- readTVar soft
          previous <- readTVar lastBackoff
          when (limit < size && now - previous >= 5_000_000_000) $ do
            writeTVar soft (limit + 1)
            writeTVar lastBackoff now
      counts = atomically $ do
        limit <- readTVar soft
        refused <- readTVar refusals
        pure [(Text.pack "soft_limit", limit), (Text.pack "refusals", refused)]
  pure (Policy admit (\opened -> (opened <=) <$> readTVar soft) failed tick counts)

-- | Established inventory, active callbacks, and reserved startup slots.
data Stats = Stats {statsIdle :: Int, statsInUse :: Int, statsOpening :: Int} deriving (Eq, Show)

stats :: Pool connection failure -> IO Stats
stats Pool{..} = atomically $ Stats <$> readTVar poolIdleCount <*> readTVar poolInUse <*> readTVar poolOpening

poolPolicyStats :: Pool connection failure -> IO [(Text, Int)]
poolPolicyStats = policyStats . poolPolicy

data Pool connection failure = Pool
  { poolConfig :: Config connection failure
  , poolMaxLifetime :: Word64
  , poolMaxIdletime :: Word64
  -- ^ In nanoseconds.
  , poolIdle :: TQueue (Entry connection)
  -- ^ Open connections not in use, the most recently used first.
  , poolCapacity :: TVar Int
  -- ^ How many more connections may be opened. The size bounds these, the
  -- idle connections and those in use together.
  , poolReuse :: TVar (TVar Bool)
  -- ^ Whether the connections in use go back to the pool when done.
  -- 'release' turns the current flag off, and starts a new one.
  , poolAcquisition :: Acquire.Context
  , poolPending :: Queue.Queue Pending
  , poolClosed :: TVar Bool
  , poolPolicy :: Policy failure
  , poolDelay :: TVar (Maybe (Int, Int, Bool))
  , poolIdleCount :: TVar Int
  , poolInUse :: TVar Int
  , poolOpening :: TVar Int
  , poolWorkers :: [ThreadId]
  , poolReaper :: IORef ()
  -- ^ Stops the reaper once the pool is garbage.
  }

-- Finalizer rejection remains distinct internally from an expired deadline.
data PoolUnavailable = PoolUnavailable deriving (Show)

instance Exception PoolUnavailable

-- The queue retains the pool only while a use is waiting. Its dispatcher and
-- the reaper capture inventory alone, so an idle pool can still be collected.
data Pending = Pending
  { grantPending :: STM Bool
  , rejectPending :: STM ()
  }

-- | A connection, with when it was opened and last taken.
data Entry connection = Entry
  { entryConnection :: connection
  , entryId :: UUID
  , entryOpened :: Word64
  , entryTaken :: Word64
  }

-- | A tentative exclusive claim on an already open connection. Acquire it
-- under 'mask', together with any other STM resources; then validate it and
-- eventually call 'finishReservation', even when other acquisition work throws.
data Reservation connection failure = Reservation
  { reservationPool :: Pool connection failure
  , reservationReuse :: TVar Bool
  , reservationEntry :: Entry connection
  , reservationState :: TVar ReservationState
  }

data ReservationState = Tentative | Validated Word64 | InUse Word64 | Finished

data ReservationError = ReservationFinished | ReservationNotValidated | ReservationAlreadyInUse
  deriving (Eq, Show)

instance Exception ReservationError

-- | An exclusive claim on capacity, independent of a request reservation.
-- Acquire under 'mask' and call 'prepareOpening' or 'abandonOpening'.
data Opening connection failure = OpeningSlot
  { openingPool :: Pool connection failure
  , openingReuse :: TVar Bool
  , openingFinished :: TVar Bool
  }

data OpeningError = OpeningFinished
  deriving (Eq, Show)

instance Exception OpeningError

-- | Takes ready inventory without delaying it for opening policy. Validate in IO
-- immediately after the STM transaction; a tentative reservation may have aged or crossed a 'release' generation.
tryReserveReady :: Pool connection failure -> STM (Maybe (Reservation connection failure))
tryReserveReady pool@Pool{..} = do
  ensureOpen poolClosed
  entry <- tryReadTQueue poolIdle
  for_ entry (const $ modifyTVar' poolIdleCount (subtract 1))
  traverse (\owned -> Reservation pool <$> readTVar poolReuse <*> pure owned <*> newTVar Tentative) entry

-- | Checks expiration against a fresh monotonic time, closing an invalid
-- tentative claim. A validated claim remains valid until finished. Returns
-- False for invalid or finished reservations. Repeated validation is safe.
validateReservation :: Reservation connection failure -> IO Bool
validateReservation Reservation{..} = withPoolLifetime reservationPool $ mask_ $ do
  now <- getMonotonicTimeNSec
  result <- atomically $ readTVar reservationState >>= \case
    Finished -> pure (False, Nothing)
    Validated _ -> pure (True, Nothing)
    InUse _ -> pure (True, Nothing)
    Tentative -> do
      keep <- readTVar reservationReuse
      let pool = reservationPool
          reason
            | not keep = Just ReleaseConnectionTerminationReason
            | isAged (poolMaxLifetime pool) now reservationEntry = Just AgingConnectionTerminationReason
            | isIdle (poolMaxIdletime pool) now reservationEntry = Just IdlenessConnectionTerminationReason
            | otherwise = Nothing
      writeTVar reservationState (maybe (Validated now) (const Finished) reason)
      pure (isNothing reason, reason)
  traverse_ (closePoolEntry reservationPool reservationEntry) (snd result)
  pure (fst result)

-- | Returns or closes a reservation once. Duplicate calls do nothing. Cleanup
-- and observations run masked; observer/close exceptions propagate after
-- ownership has transferred or the connection's capacity has been freed.
finishReservation :: Reservation connection failure -> Outcome -> IO ()
finishReservation Reservation{..} outcome = withPoolLifetime reservationPool $ mask_ $ do
  entry <- atomically $ readTVar reservationState >>= \case
    Finished -> pure Nothing
    state -> do
      case state of InUse _ -> modifyTVar' (poolInUse reservationPool) (subtract 1); _ -> pure ()
      writeTVar reservationState Finished
      pure . Just $ case state of
        Validated taken -> reservationEntry{entryTaken = taken}
        InUse taken -> reservationEntry{entryTaken = taken}
        _ -> reservationEntry
  for_ entry $ \owned -> case outcome of
    Succeeded -> returnPoolEntry reservationPool reservationReuse owned SessionSucceededConnectionReadyForUseReason
    Failed failure -> returnPoolEntry reservationPool reservationReuse owned (SessionFailedConnectionReadyForUseReason failure)
    Broke details -> closePoolEntry reservationPool owned (NetworkErrorConnectionTerminationReason details)

-- | Runs an already validated reservation, with 'use'-style exception cleanup.
-- A tentative or finished claim throws 'ReservationError' before the action.
useReservation :: Reservation connection failure -> (connection -> IO (Outcome, a)) -> IO a
useReservation reservation@Reservation{..} action = withPoolLifetime reservationPool $ mask $ \restore -> do
  rejected <- atomically $ readTVar reservationState >>= \case
    Finished -> pure (Just ReservationFinished)
    Tentative -> pure (Just ReservationNotValidated)
    InUse _ -> pure (Just ReservationAlreadyInUse)
    Validated taken -> do
      modifyTVar' (poolInUse reservationPool) (+ 1)
      writeTVar reservationState (InUse taken) $> Nothing
  traverse_ throwIO rejected
  let config = poolConfig reservationPool
  result <- try @SomeException $ do
    configObserve config (ConnectionObservation (entryId reservationEntry) InUseConnectionStatus)
    restore (action (entryConnection reservationEntry))
  case result of
    Right (outcome, value) -> finishReservation reservation outcome $> value
    Left exception -> do
      usable <- fromRight False <$> try @SomeException (configUsableAfterException config (entryConnection reservationEntry))
      finishReservation reservation $
        if usable then Failed exception else Broke (Just (Text.pack (displayException exception)))
      throwIO exception

-- | Claims spare inventory when the connection policy allows it. A delayed
-- attempt returns Nothing and schedules one pool-level wakeup. Startup can then
-- run under an independent setup-resource budget.
tryReserveOpening :: Pool connection failure -> STM (Maybe (Opening connection failure))
tryReserveOpening pool@Pool{..} = do
  ensureOpen poolClosed
  requestOpeningDelay pool
  free <- readTVar poolCapacity
  decision <- openingAdmission pool
  if free <= 0 || decision /= Admit then pure Nothing else do
    writeTVar poolCapacity (free - 1)
    modifyTVar' poolOpening (+ 1)
    writeTVar poolDelay Nothing
    Just <$> (OpeningSlot pool <$> readTVar poolReuse <*> newTVar False)

ensureOpen :: TVar Bool -> STM ()
ensureOpen closed = readTVar closed >>= (\stopped -> when stopped (throwSTM PoolClosedError))

openingAdmission :: Pool connection failure -> STM Admission
openingAdmission Pool{..} = do
  free <- readTVar poolCapacity
  delayed <- readTVar poolDelay
  let opened = configSize poolConfig - free
      waited = case delayed of Just (count, _, True) -> count == opened; _ -> False
  policyAdmit poolPolicy opened waited `orElse` pure Wait

-- Kept outside tryGrant's retried reservation, so requesting a delay commits
-- even when inventory cannot yet be delivered to the FIFO head.
requestOpeningDelay :: Pool connection failure -> STM ()
requestOpeningDelay pool@Pool{..} = do
  closed <- readTVar poolClosed
  free <- readTVar poolCapacity
  unless (closed || free <= 0) $ openingAdmission pool >>= \case
    AdmitAfter micros -> do
      previous <- readTVar poolDelay
      let opened = configSize poolConfig - free
      case previous of
        Just (count, duration, _) | count == opened && duration == micros -> pure ()
        _ -> writeTVar poolDelay (Just (opened, max 0 micros, False))
    _ -> pure ()

-- | Opens under the claimed capacity and returns the connection to the pool,
-- making it available for a later connection checkout. On
-- cancellation or startup failure the slot is freed. Each token is one-shot;
-- reuse throws 'OpeningFinished'. A release during startup closes the result.
prepareOpening :: Opening connection failure -> IO (Either (UsageError failure) ())
prepareOpening opening = withPoolLifetime (openingPool opening) $ mask_ $ do
  prepared <- prepareReservation opening
  traverse (\Reservation{..} -> do
    atomically (writeTVar reservationState Finished)
    void (putPoolEntryBack reservationPool reservationReuse reservationEntry)) prepared

-- An ordinary use keeps its opening claim through startup and the callback.
-- Explicit startup can instead publish the result through 'prepareOpening'.
prepareReservation :: Opening connection failure -> IO (Either (UsageError failure) (Reservation connection failure))
prepareReservation OpeningSlot{..} = mask_ $ do
  claimed <- atomically (consume openingFinished)
  unless claimed (throwIO OpeningFinished)
  let pool@Pool{..} = openingPool
      giveSlotBack = atomically $ modifyTVar' poolOpening (subtract 1) >> freeSlot poolPending poolCapacity
      observe connectionId = configObserve poolConfig . ConnectionObservation connectionId
  closedBefore <- readTVarIO poolClosed
  if closedBefore then giveSlotBack $> Left PoolClosed else do
    prepareConnection pool openingReuse giveSlotBack observe

prepareConnection :: Pool connection failure -> TVar Bool -> IO () -> (UUID -> ConnectionStatus -> IO ()) -> IO (Either (UsageError failure) (Reservation connection failure))
prepareConnection pool@Pool{..} openingReuse giveSlotBack observe = do
  opened <- (do
    connectionId <- UUID.nextRandom
    observe connectionId ConnectingConnectionStatus
    (connectionId,) <$> configConnect poolConfig) `onException` giveSlotBack
  case opened of
    (connectionId, Left failure) -> do
      giveSlotBack
      observe connectionId . TerminatedConnectionStatus . NetworkErrorConnectionTerminationReason $
        configDescribeFailure poolConfig failure
      pure (Left (ConnectionFailed failure))
    (connectionId, Right connection) -> do
      now <- getMonotonicTimeNSec
      let entry = Entry connection connectionId now now
      observe connectionId (ReadyForUseConnectionStatus EstablishedConnectionReadyForUseReason)
        `onException` (atomically (modifyTVar' poolOpening (subtract 1)) >> closePoolEntry pool entry (NetworkErrorConnectionTerminationReason Nothing))
      closed <- atomically $ do
        modifyTVar' poolOpening (subtract 1)
        readTVar poolClosed
      if closed then closePoolEntry pool entry ReleaseConnectionTerminationReason $> Left PoolClosed
      else do
        reservation <- atomically $ Reservation pool openingReuse entry <$> newTVar (Validated now)
        pure (Right reservation)

-- | Abandons an opening slot once. Does nothing after prepareOpening took it.
abandonOpening :: Opening connection failure -> IO ()
abandonOpening OpeningSlot{..} = withPoolLifetime openingPool $ mask_ $ atomically $ do
  claimed <- consume openingFinished
  when claimed $ let Pool{..} = openingPool in modifyTVar' poolOpening (subtract 1) >> freeSlot poolPending poolCapacity

consume :: TVar Bool -> STM Bool
consume finished = do
  consumed <- readTVar finished
  if consumed then pure False else writeTVar finished True $> True

-- | A pool with its own FIFO, using the caller's acquisition context.
-- The caller cancels or drains its tasks before releasing application resources.
newPool :: Acquire.Context -> Config connection failure -> IO (Pool connection failure)
newPool acquisition = newPoolWithPolicy acquisition defaultPolicy

newPoolWithPolicy :: Acquire.Context -> Policy failure -> Config connection failure -> IO (Pool connection failure)
newPoolWithPolicy acquisition policy config = mask_ $ do
  when (configSize config <= 0) (ioError (userError "pool size must be positive"))
  idle <- newTQueueIO
  capacity <- newTVarIO (configSize config)
  reuse <- newTVarIO =<< newTVarIO True
  pending <- atomically Queue.newQueue
  closed <- newTVarIO False
  delayed <- newTVarIO Nothing
  idleCount <- newTVarIO 0
  inUse <- newTVarIO 0
  opening <- newTVarIO 0
  reaperRef <- newIORef ()
  let maxLifetime = nanoseconds (configMaxLifetime config)
      maxIdletime = nanoseconds (configMaxIdletime config)
      freed = freeSlot pending capacity
      -- Rejection can dispatch and detach later heads, so read each head anew.
      stopPending = Queue.first pending >>= traverse_ (\entry ->
        rejectPending (Queue.value entry) >> stopPending)
      stopQueue = atomically stopPending
  reaper <- forkIOWithUnmask (\unmask -> unmask . forever $ do
    threadDelay 1_000_000
    policyTick policy
    reap config freed maxLifetime maxIdletime idle idleCount)
  policyWorker <- forkIOWithUnmask $ \unmask -> unmask $
    watchPolicy policy config capacity delayed closed pending
  let pool = Pool
        { poolConfig = config, poolMaxLifetime = maxLifetime, poolMaxIdletime = maxIdletime
        , poolIdle = idle, poolCapacity = capacity, poolReuse = reuse
        , poolAcquisition = acquisition, poolPending = pending, poolReaper = reaperRef
        , poolClosed = closed, poolPolicy = policy, poolDelay = delayed
        , poolIdleCount = idleCount, poolInUse = inUse, poolOpening = opening
        , poolWorkers = [reaper, policyWorker]
        }
  -- Capture inventory and policy only; retaining pool here would defeat its
  -- weak lifetime key. The watcher owns at most one cancellable delay timer.
  void . mkWeakIORef reaperRef $
    (killThread reaper `finally` killThread policyWorker) `finally` stopQueue
  pure pool

watchPolicy :: Policy failure -> Config connection failure -> TVar Int -> TVar (Maybe (Int, Int, Bool)) -> TVar Bool -> Queue.Queue Pending -> IO ()
watchPolicy policy config capacity delayed closed pending = do
  manager <- Timer.getSystemTimerManager
  let snapshot = do
        stopped <- readTVar closed
        check (not stopped)
        free <- readTVar capacity
        delay <- readTVar delayed
        let opened = configSize config - free
            waited = case delay of Just (count, _, True) -> count == opened; _ -> False
        admission <- policyAdmit policy opened waited `orElse` pure Wait
        pure (free, delay, admission)
      loop = do
        previous@(_, delay, _) <- atomically snapshot
        signal <- newTVarIO False
        let arm = case delay of
              Just (_, micros, False) -> Just <$> Timer.registerTimeout manager micros (atomically $ writeTVar signal True)
              _ -> pure Nothing
            cancel = traverse_ (Timer.unregisterTimeout manager)
        bracket arm cancel $ \_ -> atomically $ do
          current <- snapshot
          expired <- readTVar signal
          check (current /= previous || expired)
          when (expired && current == previous) $ case delay of
            Just (count, micros, False) -> writeTVar delayed (Just (count, micros, True))
            _ -> pure ()
          dispatchPool pending
        loop
  loop

-- | Closes the idle connections, and the ones in use once they're done. Later
-- uses open new connections, so this also resets the pool.
release :: Pool connection failure -> IO ()
release pool@Pool{..} = withPoolLifetime pool $ mask_ $ do
  entries <- atomically $ do
    current <- readTVar poolReuse
    writeTVar current False
    writeTVar poolReuse =<< newTVar True
    writeTVar poolIdleCount 0
    flushTQueue poolIdle
  closeAll poolConfig (freeSlot poolPending poolCapacity) [(entry, ReleaseConnectionTerminationReason) | entry <- entries]

-- | Permanently reject new and waiting checkouts, close idle inventory, and
-- retire already owned connections when their owners return them. Idempotent.
destroy :: Pool connection failure -> IO ()
destroy pool@Pool{..} = withPoolLifetime pool $ mask_ $ do
  entries <- atomically $ do
    writeTVar poolClosed True
    readTVar poolReuse >>= (`writeTVar` False)
    writeTVar poolIdleCount 0
    entries <- flushTQueue poolIdle
    dispatchPool poolPending
    pure entries
  traverse_ killThread poolWorkers `finally`
    closeAll poolConfig (freeSlot poolPending poolCapacity) [(entry, ReleaseConnectionTerminationReason) | entry <- entries]

-- | Runs the action with a connection, and gives the connection back as the
-- action's outcome says. If the action throws, the connection goes back to
-- the pool if it's still usable and is closed if not, and the exception is
-- rethrown.
use :: Pool connection failure -> (connection -> IO (Outcome, a)) -> IO (Either (UsageError failure) a)
use pool = useChecked pool (const $ pure $ Right ())

-- | Like 'use', but validate a claimed connection before starting the callback.
-- A Left result retires it with the optional failure details and retries under
-- the original admission deadline. Startup and the callback keep their existing
-- ownership and timeout boundaries. A thrown preflight exception retires the
-- connection and propagates without running the callback.
useChecked :: Pool connection failure -> (connection -> IO (Either (Maybe Text) ())) -> (connection -> IO (Outcome, a)) -> IO (Either (UsageError failure) a)
useChecked pool@Pool{..} preflight action = withPoolLifetime pool $ mask $ \restore -> do
  deadline <- Acquire.deadlineAfter (configAcquisitionTimeout poolConfig)
  let claim = do
        -- Preserve the existing public timeout result for finalizer rejection.
        admitted <- try @PoolClosedError . try @PoolUnavailable $
          awaitPool poolAcquisition poolPending deadline (requestOpeningDelay pool) reserve cleanup run
        case admitted of
          Left PoolClosedError -> pure (Left PoolClosed)
          Right (Left PoolUnavailable) -> pure (Left AcquisitionTimeout)
          Right (Right Nothing) -> pure (Left AcquisitionTimeout)
          Right (Right (Just Nothing)) -> claim
          Right (Right (Just (Just result))) -> pure result
      reserve = tryReserveReady pool >>= \case
        Just reservation -> pure (Left reservation)
        Nothing -> Right <$> (tryReserveOpening pool >>= maybe retry pure)
      cleanup = either (`finishReservation` Succeeded) abandonOpening
      run = \case
        Left reservation -> do
          valid <- validateReservation reservation
          if valid then checked reservation else pure Nothing
        Right opening -> do
          prepared <- prepareReservation opening
          case prepared of
            Left (ConnectionFailed failure) -> policyFailed poolPolicy failure >>= \waitInstead ->
              pure $ if waitInstead then Nothing else Just (Left (ConnectionFailed failure))
            Left failure -> pure (Just $ Left failure)
            Right reservation -> checked reservation
      checked reservation@Reservation{reservationEntry} = do
        ready <- restore (preflight $ entryConnection reservationEntry)
          `onException` finishReservation reservation (Broke Nothing)
        case ready of
          Left details -> finishReservation reservation (Broke details) $> Nothing
          Right () -> Just . Right <$> useReservation reservation (restore . action)
  closed <- readTVarIO poolClosed
  if closed then pure (Left PoolClosed) else claim

-- | Return ownership before observations, so an observer exception cannot leak.
putPoolEntryBack :: Pool connection failure -> TVar Bool -> Entry connection -> IO Bool
putPoolEntryBack pool@Pool{..} reuse entry = do
  kept <- atomically $ do
    generation <- readTVar reuse
    free <- readTVar poolCapacity
    keep <- if generation then policyKeep poolPolicy (configSize poolConfig - free) else pure False
    when keep $ do
      modifyTVar' poolIdleCount (+ 1)
      unGetTQueue poolIdle entry
      dispatchPool poolPending
    pure keep
  if kept then pure True else closePoolEntry pool entry ReleaseConnectionTerminationReason $> False

returnPoolEntry :: Pool connection failure -> TVar Bool -> Entry connection -> ConnectionReadyForUseReason -> IO ()
returnPoolEntry pool@Pool{..} reuse entry reason = do
  kept <- putPoolEntryBack pool reuse entry
  when kept $ configObserve poolConfig (ConnectionObservation (entryId entry) (ReadyForUseConnectionStatus reason))

closePoolEntry :: Pool connection failure -> Entry connection -> ConnectionTerminationReason -> IO ()
closePoolEntry Pool{..} entry reason = closeAll poolConfig (freeSlot poolPending poolCapacity) [(entry, reason)]

-- Registration, cancellation and inventory returns dispatch only this FIFO.
-- tryGrant consumes inventory before detaching; removal can therefore grant the
-- next head in the same transaction without overtaking or a dispatcher thread.
awaitPool :: Acquire.Context -> Queue.Queue Pending -> Acquire.Deadline -> STM () -> STM a -> (a -> IO ()) -> (a -> IO r) -> IO (Maybe r)
awaitPool acquisition pending deadline requestDelay reserve cleanup action =
  -- Queue emptiness and inventory reservation commit together, so immediate
  -- admission cannot pass a queued use.
  let fast = Queue.isEmpty pending >>= check >> reserve
      detach entry = do
        Queue.remove entry
        dispatchPool pending
      enqueue waiter = do
        entry <- Queue.append pending Pending
          { grantPending = requestDelay >> Acquire.tryGrant waiter reserve
          , rejectPending = void (Acquire.tryGrant waiter (throwSTM PoolUnavailable))
          }
        dispatchPool pending
        pure entry
  -- acquire only cleans up undelivered grants. After delivery this owner keeps
  -- the claim masked through startup and releases it on every exit. The
  -- reservation/opening cleanup is idempotent after the normal use path.
  in mask_ $ bracket
    (Acquire.acquire acquisition deadline fast enqueue detach cleanup)
    (traverse_ cleanup)
    (traverse action)

dispatchPool :: Queue.Queue Pending -> STM ()
dispatchPool pending = Queue.first pending >>= traverse_ (void . grantPending . Queue.value)

-- | Inventory is available to the next queued use before the return commits.
freeSlot :: Queue.Queue Pending -> TVar Int -> STM ()
freeSlot pending capacity = do
  modifyTVar' capacity (+ 1)
  dispatchPool pending

-- | Closes the idle connections past their lifetime or idle time.
reap :: Config connection failure -> STM () -> Word64 -> Word64 -> TQueue (Entry connection) -> TVar Int -> IO ()
reap config slotFreed maxLifetime maxIdletime idle idleCount = mask_ $ do
  now <- getMonotonicTimeNSec
  expired <- atomically $ do
    entries <- flushTQueue idle
    let
      (aged, unaged) = partition (isAged maxLifetime now) entries
      (idled, live) = partition (isIdle maxIdletime now) unaged
    traverse_ (writeTQueue idle) live
    writeTVar idleCount (length live)
    pure $
      [(entry, AgingConnectionTerminationReason) | entry <- aged]
        <> [(entry, IdlenessConnectionTerminationReason) | entry <- idled]
  closeAll config slotFreed expired

-- | Closes each connection and gives its slot back, even if closing another
-- one throws.
closeAll :: Config connection failure -> STM () -> [(Entry connection, ConnectionTerminationReason)] -> IO ()
closeAll config slotFreed = foldr (\(entry, reason) rest -> close entry reason `finally` rest) (pure ())
  where
    close entry reason = do
      configClose config (entryConnection entry) `finally` atomically slotFreed
      configObserve config (ConnectionObservation (entryId entry) (TerminatedConnectionStatus reason))

isAged :: Word64 -> Word64 -> Entry connection -> Bool
isAged maxLifetime now entry = now > entryOpened entry + maxLifetime

isIdle :: Word64 -> Word64 -> Entry connection -> Bool
isIdle maxIdletime now entry = now > entryTaken entry + maxIdletime

nanoseconds :: DiffTime -> Word64
nanoseconds time = fromInteger (max 0 (min (toInteger (maxBound :: Word64) `div` 2) (diffTimeToPicoseconds time `div` 1_000)))

-- Optimisation may unpack a pool into inventory fields while a caller waits.
-- Keep its finalizer key alive until all ownership cleanup has completed.
withPoolLifetime :: Pool connection failure -> IO a -> IO a
withPoolLifetime pool = withLifetime (poolReaper pool)
