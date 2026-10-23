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
  , use
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

import Control.Concurrent (forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.STM
  ( STM, TQueue, TVar, atomically, flushTQueue, modifyTVar', newTQueueIO
  , newTVar, newTVarIO, readTVar, retry, tryReadTQueue, writeTQueue, writeTVar
  )
import Control.Concurrent.Resource.Waiting qualified as Waiting
import Control.Concurrent.Resource.Waiting.Queue qualified as Queue
import Control.Exception (Exception, SomeException, displayException, finally, mask, mask_, onException, throwIO, try)
import Control.Monad (forever, unless, void, when)
import Data.Either (fromRight)
import Data.Foldable (for_, traverse_)
import Data.Functor (($>))
import Data.IORef (IORef, mkWeakIORef, newIORef)
import Data.List (partition)
import Data.Text (Text)
import Data.Time.Clock (DiffTime, diffTimeToPicoseconds)
import Data.UUID (UUID)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)

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
  deriving (Eq, Show)

data Pool connection failure = Pool
  { poolConfig :: Config connection failure
  , poolMaxLifetime :: Word64
  , poolMaxIdletime :: Word64
  -- ^ In nanoseconds.
  , poolIdle :: TQueue (Entry connection)
  -- ^ Open connections not in use, the least recently used first.
  , poolCapacity :: TVar Int
  -- ^ How many more connections may be opened. The size bounds these, the
  -- idle connections and those in use together.
  , poolReuse :: TVar (TVar Bool)
  -- ^ Whether the connections in use go back to the pool when done.
  -- 'release' turns the current flag off, and starts a new one.
  , poolWaiting :: Waiting.Waiting
  , poolPending :: Queue.Queue Pending
  , poolReaper :: IORef ()
  -- ^ Stops the reaper once the pool is garbage.
  }

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

-- | Takes ready inventory without applying admission policy. Validate in IO
-- immediately after the STM transaction; a tentative reservation may have aged or crossed a 'release' generation.
tryReserveReady :: Pool connection failure -> STM (Maybe (Reservation connection failure))
tryReserveReady pool@Pool{..} = do
  entry <- tryReadTQueue poolIdle
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
      pure (maybe True (const False) reason, reason)
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
    Validated taken -> writeTVar reservationState (InUse taken) $> Nothing
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

-- | Claims spare inventory without applying admission policy.
-- Startup can then run under an independent setup-resource budget.
tryReserveOpening :: Pool connection failure -> STM (Maybe (Opening connection failure))
tryReserveOpening pool@Pool{..} = do
  free <- readTVar poolCapacity
  if free <= 0 then pure Nothing else do
    writeTVar poolCapacity (free - 1)
    Just <$> (OpeningSlot pool <$> readTVar poolReuse <*> newTVar False)

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
      giveSlotBack = atomically (freeSlot poolPending poolCapacity)
      observe connectionId = configObserve poolConfig . ConnectionObservation connectionId
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
        `onException` closePoolEntry pool entry (NetworkErrorConnectionTerminationReason Nothing)
      reservation <- atomically $ Reservation pool openingReuse entry <$> newTVar (Validated now)
      pure (Right reservation)

-- | Abandons an opening slot once. Does nothing after prepareOpening took it.
abandonOpening :: Opening connection failure -> IO ()
abandonOpening OpeningSlot{..} = withPoolLifetime openingPool $ mask_ $ atomically $ do
  claimed <- consume openingFinished
  when claimed $ let Pool{..} = openingPool in freeSlot poolPending poolCapacity

consume :: TVar Bool -> STM Bool
consume finished = do
  consumed <- readTVar finished
  if consumed then pure False else writeTVar finished True $> True

-- | A pool with its own FIFO, using the caller's deadline service. The caller
-- keeps the service open through pool use and closes it after the last use.
-- Releasing or collecting this pool never closes the supplied service.
newPool :: Waiting.Waiting -> Config connection failure -> IO (Pool connection failure)
newPool waiting config = mask_ $ do
  when (configSize config <= 0) (ioError (userError "pool size must be positive"))
  idle <- newTQueueIO
  capacity <- newTVarIO (configSize config)
  reuse <- newTVarIO =<< newTVarIO True
  pending <- atomically Queue.newQueue
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
    reap config freed maxLifetime maxIdletime idle)
  void . mkWeakIORef reaperRef $
    killThread reaper `finally` stopQueue
  pure Pool
    { poolConfig = config
    , poolMaxLifetime = maxLifetime
    , poolMaxIdletime = maxIdletime
    , poolIdle = idle
    , poolCapacity = capacity
    , poolReuse = reuse
    , poolWaiting = waiting
    , poolPending = pending
    , poolReaper = reaperRef
    }

-- | Closes the idle connections, and the ones in use once they're done. Later
-- uses open new connections, so this also resets the pool.
release :: Pool connection failure -> IO ()
release pool@Pool{..} = withPoolLifetime pool $ mask_ $ do
  entries <- atomically $ do
    current <- readTVar poolReuse
    writeTVar current False
    writeTVar poolReuse =<< newTVar True
    entries <- flushTQueue poolIdle
    pure entries
  closeAll poolConfig (freeSlot poolPending poolCapacity) [(entry, ReleaseConnectionTerminationReason) | entry <- entries]

-- | Runs the action with a connection, and gives the connection back as the
-- action's outcome says. If the action throws, the connection goes back to
-- the pool if it's still usable and is closed if not, and the exception is
-- rethrown.
use :: Pool connection failure -> (connection -> IO (Outcome, a)) -> IO (Either (UsageError failure) a)
use pool@Pool{..} action = withPoolLifetime pool $ mask $ \restore -> do
  deadline <- Waiting.deadlineAfter (configAcquisitionTimeout poolConfig)
  let claim = do
        admitted <- awaitPool poolWaiting poolPending deadline reserve cleanup run
        case admitted of
          Left _ -> pure (Left AcquisitionTimeout)
          Right Nothing -> claim
          Right (Just result) -> pure result
      reserve = tryReserveReady pool >>= \case
        Just reservation -> pure (Left reservation)
        Nothing -> Right <$> (tryReserveOpening pool >>= maybe retry pure)
      cleanup = either (`finishReservation` Succeeded) abandonOpening
      run = \case
        Left reservation -> do
          valid <- validateReservation reservation
          if valid then Just . Right <$> useReservation reservation (restore . action) else pure Nothing
        Right opening -> do
          prepared <- prepareReservation opening
          traverse (\reservation -> useReservation reservation (restore . action)) prepared
            >>= pure . Just
  claim

-- | Return ownership before observations, so an observer exception cannot leak.
putPoolEntryBack :: Pool connection failure -> TVar Bool -> Entry connection -> IO Bool
putPoolEntryBack pool@Pool{..} reuse entry = do
  kept <- atomically $ do
    keep <- readTVar reuse
    when keep $ do
      writeTQueue poolIdle entry
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
awaitPool :: Waiting.Waiting -> Queue.Queue Pending -> Waiting.Deadline -> STM a -> (a -> IO ()) -> (a -> IO r) -> IO (Either Waiting.WaitError r)
awaitPool waiting pending deadline reserve cleanup action =
  let detach entry = do
        Queue.remove entry
        dispatchPool pending
      enqueue waiter = do
        entry <- Queue.append pending Pending
          { grantPending = Waiting.tryGrant waiter reserve
          , rejectPending = void (Waiting.reject waiter Waiting.Closed)
          }
        dispatchPool pending
        pure entry
  in Waiting.withWait waiting deadline enqueue detach cleanup action

dispatchPool :: Queue.Queue Pending -> STM ()
dispatchPool pending = Queue.first pending >>= traverse_ (\entry ->
  void (grantPending (Queue.value entry)))

-- | Inventory is available to the next queued use before the return commits.
freeSlot :: Queue.Queue Pending -> TVar Int -> STM ()
freeSlot pending capacity = do
  modifyTVar' capacity (+ 1)
  dispatchPool pending

-- | Closes the idle connections past their lifetime or idle time.
reap :: Config connection failure -> STM () -> Word64 -> Word64 -> TQueue (Entry connection) -> IO ()
reap config slotFreed maxLifetime maxIdletime idle = mask_ $ do
  now <- getMonotonicTimeNSec
  expired <- atomically $ do
    entries <- flushTQueue idle
    let
      (aged, unaged) = partition (isAged maxLifetime now) entries
      (idled, live) = partition (isIdle maxIdletime now) unaged
    traverse_ (writeTQueue idle) live
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
