{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RecordWildCards #-}

-- | A pool of connections of any kind. It's given how to open, close and
-- check its connections, so the same pool serves different clients.
--
-- A use takes an idle connection, or opens one while the pool is below its
-- size, or else waits up to the acquisition timeout for one. The uses that
-- wait get the connections freed meanwhile in the order they came, and a later
-- use doesn't take one while others wait. One thread times them out, as the
-- one that waited longest times out first. A connection
-- past its maximum lifetime or idle time is closed instead of used, and a
-- reaper closes idle ones in the background.
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
  ( STM
  , TMVar
  , TQueue
  , TVar
  , atomically
  , flushTQueue
  , modifyTVar'
  , newEmptyTMVar
  , newTQueueIO
  , newTVar
  , newTVarIO
  , peekTQueue
  , putTMVar
  , readTQueue
  , readTVar
  , retry
  , takeTMVar
  , tryPeekTQueue
  , tryReadTQueue
  , tryTakeTMVar
  , unGetTQueue
  , writeTQueue
  , writeTVar
  )
import Control.Exception (Exception, SomeException, displayException, finally, mask, mask_, onException, throwIO, try)
import Control.Monad (forever, unless, void, when)
import Data.Either (fromRight)
import Data.Foldable (asum, for_, traverse_)
import Data.Functor (($>))
import Data.IORef (IORef, mkWeakIORef, newIORef)
import Data.List (partition)
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Data.Time.Clock (DiffTime, diffTimeToPicoseconds)
import Data.UUID (UUID)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)

import Data.Text qualified as Text
import Data.UUID.V4 qualified as UUID

import Control.Concurrent.Resource.Waiting qualified as Waiting

import Database.PostgreSQL.Pool.Observation

-- | What the pool needs to know about its connections, and its limits.
data Config connection failure = Config
  { configSize :: Int
  -- ^ The most connections open at once.
  , configAcquisitionTimeout :: DiffTime
  -- ^ How long a use waits for a connection.
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
  , poolAcquisitionTimeout :: Word64
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
  , poolWaiting :: TQueue (Waiter connection)
  -- ^ The uses waiting for a connection, the longest waiting first.
  , poolReaper :: IORef ()
  -- ^ Stops the reaper and the expirer once the pool is garbage.
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

-- | Takes a ready connection without waiting or opening one. Existing 'use'
-- waiters retain priority. Validate in IO immediately after the STM transaction;
-- a tentative reservation may have aged or crossed a 'release' generation.
tryReserveReady :: Pool connection failure -> STM (Maybe (Reservation connection failure))
tryReserveReady pool@Pool{..} = do
  waiting <- someoneWaiting poolWaiting
  if waiting then pure Nothing else
    tryReadTQueue poolIdle >>= traverse (\entry -> Reservation pool <$> readTVar poolReuse <*> pure entry <*> newTVar Tentative)

-- | Checks expiration against a fresh monotonic time, closing an invalid
-- tentative claim. A validated claim remains valid until finished. Returns
-- False for invalid or finished reservations. Repeated validation is safe.
validateReservation :: Reservation connection failure -> IO Bool
validateReservation Reservation{..} = mask_ $ do
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
finishReservation Reservation{..} outcome = mask_ $ do
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
useReservation reservation@Reservation{..} action = mask $ \restore -> do
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

-- | Claims spare capacity without waiting; queued 'use' calls retain priority.
-- Startup can then run under an independent setup-resource budget.
tryReserveOpening :: Pool connection failure -> STM (Maybe (Opening connection failure))
tryReserveOpening pool@Pool{..} = do
  waiting <- someoneWaiting poolWaiting
  free <- readTVar poolCapacity
  if waiting || free <= 0 then pure Nothing else do
    writeTVar poolCapacity (free - 1)
    Just <$> (OpeningSlot pool <$> readTVar poolReuse <*> newTVar False)

-- | Opens under the claimed capacity and returns the connection to the pool,
-- making it available for a later combined request-resource reservation. On
-- cancellation or startup failure the slot is freed. Each token is one-shot;
-- reuse throws 'OpeningFinished'. A release during startup closes the result.
prepareOpening :: Opening connection failure -> IO (Either (UsageError failure) ())
prepareOpening OpeningSlot{..} = mask_ $ do
  claimed <- atomically (consume openingFinished)
  unless claimed (throwIO OpeningFinished)
  let pool@Pool{..} = openingPool
      giveSlotBack = atomically (freeSlot poolReuse poolWaiting poolCapacity)
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
      void (putPoolEntryBack pool openingReuse entry)
      pure (Right ())

-- | Abandons an opening slot once. Does nothing after prepareOpening took it.
abandonOpening :: Opening connection failure -> IO ()
abandonOpening OpeningSlot{..} = mask_ $ atomically $ do
  claimed <- consume openingFinished
  when claimed $ let Pool{..} = openingPool in freeSlot poolReuse poolWaiting poolCapacity

consume :: TVar Bool -> STM Bool
consume finished = do
  consumed <- readTVar finished
  if consumed then pure False else writeTVar finished True $> True

data Claim connection
  = Reused (TVar Bool) (Entry connection)
  | Opening (TVar Bool)
  | Queued (Waiter connection)

-- | A use waiting for a connection.
data Waiter connection = Waiter
  { waiterDeadline :: Word64
  -- ^ When it times out, on the monotonic clock.
  , waiterGiven :: TMVar (Given connection)
  , waiterGone :: TVar Bool
  -- ^ Whether it stopped waiting, so it's passed over.
  }

-- | What a waiting use is given: an idle connection, or the slot of a
-- closed one to open a connection with, with the reuse flag then, or a
-- timeout.
data Given connection
  = Given (TVar Bool) (Maybe (Entry connection))
  | TimedOut

-- | A pool with no connections yet. They're opened as uses need them.
newPool :: Waiting.Waiting -> Config connection failure -> IO (Pool connection failure)
newPool _ config = do
  idle <- newTQueueIO
  capacity <- newTVarIO (configSize config)
  reuse <- newTVarIO =<< newTVarIO True
  waiting <- newTQueueIO
  reaperRef <- newIORef ()
  let
    maxLifetime = nanoseconds (configMaxLifetime config)
    maxIdletime = nanoseconds (configMaxIdletime config)
  -- The reaper mustn't hold the IORef that the finalizer below waits on.
  reaper <- forkIOWithUnmask $ \unmask -> unmask . forever $ do
    threadDelay 1_000_000
    reap config (freeSlot reuse waiting capacity) maxLifetime maxIdletime idle
  expirer <- forkIOWithUnmask $ \unmask -> unmask (expire waiting)
  void . mkWeakIORef reaperRef $ killThread reaper *> killThread expirer
  pure
    Pool
      { poolConfig = config
      , poolAcquisitionTimeout = nanoseconds (configAcquisitionTimeout config)
      , poolMaxLifetime = maxLifetime
      , poolMaxIdletime = maxIdletime
      , poolIdle = idle
      , poolCapacity = capacity
      , poolReuse = reuse
      , poolWaiting = waiting
      , poolReaper = reaperRef
      }

-- | Closes the idle connections, and the ones in use once they're done. Later
-- uses open new connections, so this also resets the pool.
release :: Pool connection failure -> IO ()
release Pool{..} = mask_ $ do
  entries <- atomically $ do
    current <- readTVar poolReuse
    writeTVar current False
    writeTVar poolReuse =<< newTVar True
    flushTQueue poolIdle
  closeAll poolConfig (freeSlot poolReuse poolWaiting poolCapacity) [(entry, ReleaseConnectionTerminationReason) | entry <- entries]

-- | Runs the action with a connection, and gives the connection back as the
-- action's outcome says. If the action throws, the connection goes back to
-- the pool if it's still usable and is closed if not, and the exception is
-- rethrown.
use :: Pool connection failure -> (connection -> IO (Outcome, a)) -> IO (Either (UsageError failure) a)
use Pool{..} action =
  mask $ \restore ->
    let
      -- A use that waited already, and was given a connection it had to
      -- close, keeps its deadline
      claim deadline = do
        now <- getMonotonicTimeNSec
        claimed <- atomically $ do
          reuse <- readTVar poolReuse
          let queued = Queued <$> enqueue (fromMaybe (now + poolAcquisitionTimeout) deadline) (isJust deadline)
          -- Behind the uses already waiting, which get what's freed first
          waited <- someoneWaiting poolWaiting
          if waited then
            queued
          else
            asum [Reused reuse <$> readTQueue poolIdle, Opening reuse <$ takeSlot, queued]
        case claimed of
          Queued waiter -> do
            given <- atomically (takeTMVar (waiterGiven waiter)) `onException` abandon waiter
            case given of
              TimedOut -> pure (Left AcquisitionTimeout)
              Given reuse Nothing -> open reuse
              Given reuse (Just entry) -> reused (Just (waiterDeadline waiter)) reuse entry
          Opening reuse -> open reuse
          Reused reuse entry -> reused deadline reuse entry

      reused deadline reuse entry = do
        now <- getMonotonicTimeNSec
        if
          | isAged poolMaxLifetime now entry -> closeEntry entry AgingConnectionTerminationReason *> claim deadline
          | isIdle poolMaxIdletime now entry -> closeEntry entry IdlenessConnectionTerminationReason *> claim deadline
          | otherwise -> run reuse entry{entryTaken = now} Nothing

      -- Owns the slot until the connection is open.
      open reuse = do
        opened <-
          ( do
              connectionId <- UUID.nextRandom
              observe connectionId ConnectingConnectionStatus
              (connectionId,) <$> configConnect poolConfig
          )
            `onException` giveSlotBack
        case opened of
          (connectionId, Left failure) -> do
            giveSlotBack
            observe connectionId . TerminatedConnectionStatus . NetworkErrorConnectionTerminationReason $
              configDescribeFailure poolConfig failure
            pure (Left (ConnectionFailed failure))
          (connectionId, Right connection) -> do
            now <- getMonotonicTimeNSec
            run reuse (Entry connection connectionId now now) (Just EstablishedConnectionReadyForUseReason)

      -- Owns the connection until it's given back or closed.
      run reuse entry ready = do
        result <-
          try @SomeException $ do
            traverse_ (observe (entryId entry) . ReadyForUseConnectionStatus) ready
            observe (entryId entry) InUseConnectionStatus
            restore (action (entryConnection entry))
        case result of
          Left exception -> do
            usable <- fromRight False <$> try @SomeException (configUsableAfterException poolConfig (entryConnection entry))
            if usable then
              giveBack reuse entry (SessionFailedConnectionReadyForUseReason exception)
            else
              closeEntry entry (NetworkErrorConnectionTerminationReason (Just (Text.pack (displayException exception))))
            throwIO exception
          Right (outcome, value) -> do
            case outcome of
              Succeeded -> giveBack reuse entry SessionSucceededConnectionReadyForUseReason
              Failed failure -> giveBack reuse entry (SessionFailedConnectionReadyForUseReason failure)
              Broke details -> closeEntry entry (NetworkErrorConnectionTerminationReason details)
            pure (Right value)
    in
      claim Nothing
  where
    takeSlot = do
      free <- readTVar poolCapacity
      if free > 0 then writeTVar poolCapacity (free - 1) else retry
    giveSlotBack = atomically (freeSlot poolReuse poolWaiting poolCapacity)
    -- At the back, or at the front for a use that waited longest already,
    -- so the deadlines stay in order
    enqueue deadline waited = do
      waiter <- Waiter deadline <$> newEmptyTMVar <*> newTVar False
      (if waited then unGetTQueue else writeTQueue) poolWaiting waiter
      pure waiter
    -- A waiter that stops waiting because it's interrupted gives back what
    -- it was given meanwhile
    abandon waiter = do
      given <- atomically $ writeTVar (waiterGone waiter) True *> tryTakeTMVar (waiterGiven waiter)
      for_ given $ \case
        Given _ Nothing -> giveSlotBack
        Given reuse (Just entry) -> void (putBack reuse entry)
        TimedOut -> pure ()
    -- Back to the idle connections, or to the use that waited longest, unless
    -- the pool was released meanwhile.
    putBack = putPoolEntryBack Pool{..}
    giveBack = returnPoolEntry Pool{..}
    closeEntry = closePoolEntry Pool{..}
    observe connectionId = configObserve poolConfig . ConnectionObservation connectionId

-- | Return ownership before observations, so an observer exception cannot leak.
putPoolEntryBack :: Pool connection failure -> TVar Bool -> Entry connection -> IO Bool
putPoolEntryBack pool@Pool{..} reuse entry = do
  kept <- atomically $ do
    keep <- readTVar reuse
    when keep $
      nextWaiter poolWaiting >>= \case
        Just waiter -> readTVar poolReuse >>= \current -> putTMVar (waiterGiven waiter) (Given current (Just entry))
        Nothing -> writeTQueue poolIdle entry
    pure keep
  if kept then pure True else closePoolEntry pool entry ReleaseConnectionTerminationReason $> False

returnPoolEntry :: Pool connection failure -> TVar Bool -> Entry connection -> ConnectionReadyForUseReason -> IO ()
returnPoolEntry pool@Pool{..} reuse entry reason = do
  kept <- putPoolEntryBack pool reuse entry
  when kept $ configObserve poolConfig (ConnectionObservation (entryId entry) (ReadyForUseConnectionStatus reason))

closePoolEntry :: Pool connection failure -> Entry connection -> ConnectionTerminationReason -> IO ()
closePoolEntry Pool{..} entry reason = closeAll poolConfig (freeSlot poolReuse poolWaiting poolCapacity) [(entry, reason)]

-- | Whether a use is waiting. The ones that stopped waiting are dropped.
someoneWaiting :: TQueue (Waiter connection) -> STM Bool
someoneWaiting waiting =
  tryPeekTQueue waiting >>= \case
    Nothing -> pure False
    Just waiter ->
      readTVar (waiterGone waiter) >>= \case
        True -> readTQueue waiting *> someoneWaiting waiting
        False -> pure True

-- | Takes the use that waited longest, passing over the ones that stopped.
nextWaiter :: TQueue (Waiter connection) -> STM (Maybe (Waiter connection))
nextWaiter waiting =
  tryReadTQueue waiting >>= \case
    Nothing -> pure Nothing
    Just waiter ->
      readTVar (waiterGone waiter) >>= \case
        True -> nextWaiter waiting
        False -> pure (Just waiter)

-- | Gives the slot of a closed connection to the use that waited longest, or
-- back to the pool if none waits.
freeSlot :: TVar (TVar Bool) -> TQueue (Waiter connection) -> TVar Int -> STM ()
freeSlot reuse waiting capacity =
  nextWaiter waiting >>= \case
    Just waiter -> readTVar reuse >>= \current -> putTMVar (waiterGiven waiter) (Given current Nothing)
    Nothing -> modifyTVar' capacity (+ 1)

-- | Times out the uses that wait too long. All wait as long, so the one that
-- waited longest times out first: this sleeps until its deadline, rather than
-- every use registering a timer, which GHC can't cancel, of its own.
expire :: TQueue (Waiter connection) -> IO ()
expire waiting = forever $ do
  deadline <- atomically $ do
    waited <- someoneWaiting waiting
    if waited then waiterDeadline <$> peekTQueue waiting else retry
  now <- getMonotonicTimeNSec
  -- In microseconds, an hour at most at a time
  when (deadline > now) $ threadDelay (fromIntegral (min 3_600_000_000 ((deadline - now) `div` 1000 + 1)))
  atomically . timeOut =<< getMonotonicTimeNSec
  where
    timeOut now =
      tryPeekTQueue waiting >>= \case
        Just waiter | waiterDeadline waiter <= now -> do
          _ <- readTQueue waiting
          gone <- readTVar (waiterGone waiter)
          unless gone $ putTMVar (waiterGiven waiter) TimedOut
          timeOut now
        _ -> pure ()

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
nanoseconds time = fromIntegral (min (toInteger (maxBound :: Word64) `div` 2) (diffTimeToPicoseconds time `div` 1_000))
