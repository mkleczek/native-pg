{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
-- | Application-owned storage pools with scoped, exclusive buffer loans.
module Network.Streaming.Resources
  ( Resources, ResourceSettings (..), withResources
  , BufferAccess, BufferLoan, withRequestBuffers, borrowBuffer, loanBuffer, releaseBuffer, withBuffer
  , validateSettings
  , BufferGrant, Lease, tryAcquire, tryAcquireSetup, release, leaseBuffers
  , withBuffers, withSetupBuffers
  ) where

import Control.Concurrent.STM
  ( STM, TVar, atomically, newTVar, newTVarIO, readTVar, writeTVar
  , retry, throwSTM, check )
import Control.Exception (bracket, mask, mask_, onException)
import Control.Concurrent.Resource.Waiting.Queue qualified as Queue
import Control.Concurrent.Resource.Waiting (Deadline (NoDeadline), WaitError (Closed), Waiting, Waiter, tryGrant, reject, withWait)
import Control.Monad (when, unless, void, foldM)
import Data.IntSet qualified as IntSet
import Data.Map.Strict qualified as Map
import Data.Tuple (Solo (MkSolo))
import Network.Streaming.Internal.ListPrefix (ListPrefix (..))
import Network.Streaming.Buffer (Buffer, newBuffers)
import Network.Streaming.PositiveInt (PositiveInt, positiveIntValue)

data ResourceSettings = ResourceSettings
  { streamMemoryBytes :: !PositiveInt
  , setupMemoryBytes :: !PositiveInt
  , ioBufferBytes :: !PositiveInt
  , reserveBufferCount :: !PositiveInt
  } deriving (Eq, Show)

data Pool = Pool
  { poolCapacity :: !Int
  , poolReserve :: !Int
  , poolFree :: !(TVar (Int, [Buffer]))
  , poolActive :: !(TVar Bool)
  , poolWaiting :: !Waiting
  , poolLanes :: !(TVar (Map.Map Int (Queue.Queue Request)))
  , poolNextArrival :: !(TVar Integer)
  , poolDispatching :: !(TVar Bool)
  }
data Resources = Resources !Pool !Pool
type BufferGrant grant = ListPrefix grant Buffer

data Lease grant = Lease !Pool grant !(TVar Bool)

-- Each required inventory count has a FIFO. Selecting among eligible heads
-- costs the number of grant sizes, independently of blocked request count.
-- Arrival tickets preserve oldest-eligible order across this buffer pool alone.
data Request = Request !Integer !(STM Bool) !(STM ())

dispatch :: Pool -> STM ()
dispatch pool = do
  running <- readTVar (poolDispatching pool)
  alive <- readTVar (poolActive pool)
  when (alive && not running) $ do
    writeTVar (poolDispatching pool) True
    pump IntSet.empty
    writeTVar (poolDispatching pool) False
  where
    pump attempted = do
      (available, _) <- readTVar (poolFree pool)
      lanes <- Map.toAscList <$> readTVar (poolLanes pool)
      chosen <- foldM (candidate attempted) Nothing
        (takeWhile ((<= available) . fst) lanes)
      case chosen of
        Nothing -> pure ()
        Just (required, _, grant) -> do
          granted <- grant
          pump (if granted then IntSet.empty else IntSet.insert required attempted)
    candidate attempted best (required, lane)
      | IntSet.member required attempted = pure best
      | otherwise = do
          first <- Queue.first lane
          pure $ case Queue.value <$> first of
            Nothing -> best
            Just (Request ticket grant _) -> case best of
              Just (_, earlier, _) | earlier < ticket -> best
              _ -> Just (required, ticket, grant)

-- Registration, exact removal, local delivery and inventory changes all commit
-- with scheduling. The guard lets terminal hooks dispatch without recursion.
admit :: Pool -> Int -> STM a -> (Waiter a -> STM ()) -> STM () -> (a -> IO ()) -> (a -> IO r) -> IO (Either WaitError r)
admit pool required reserve register unregister cleanup use =
  let detach current =
        case current of
          Nothing -> pure ()
          Just (lane, entry) -> do
            Queue.remove entry
            empty <- Queue.isEmpty lane
            when empty $ do
              lanes <- readTVar (poolLanes pool)
              writeTVar (poolLanes pool) (Map.delete required lanes)
            unregister
            dispatch pool
      enqueue waiter = do
        alive <- readTVar (poolActive pool)
        if not alive then reject waiter Closed >> pure Nothing
        else do
          lanes <- readTVar (poolLanes pool)
          lane <- case Map.lookup required lanes of
            Just existing -> pure existing
            Nothing -> do
              created <- Queue.newQueue
              writeTVar (poolLanes pool) (Map.insert required created lanes)
              pure created
          ticket <- readTVar (poolNextArrival pool)
          writeTVar (poolNextArrival pool) (ticket + 1)
          entry <- Queue.append lane
            (Request ticket (tryGrant waiter reserve) (void (reject waiter Closed)))
          register waiter
          dispatch pool
          pure (Just (lane, entry))
  in withWait (poolWaiting pool) NoDeadline enqueue detach cleanup use

-- Validate before queueing: an impossible demand would never be selected.
checkedGrantSize :: forall grant. BufferGrant grant => Pool -> STM Int
checkedGrantSize pool = do
  let count = prefixSize @grant
  when (count > poolCapacity pool - poolReserve pool) $
    throwSTM (userError "buffer count must fit capacity")
  alive <- readTVar (poolActive pool)
  when (not alive) $ throwSTM (userError "resources are retired")
  pure count

-- | Only complete blocks are lent from each budget. Storage is initialized
-- here so admission and return never allocate or clear body storage. The
-- caller keeps the deadline service open through this scope; retirement only
-- closes these inventories, leaving other users of that service unaffected.
withResources :: Waiting -> ResourceSettings -> (Resources -> IO a) -> IO a
withResources waiting settings action = do
  either (ioError . userError) pure (validateSettings settings)
  bracket (newPool (positiveIntValue (reserveBufferCount settings)) (streamMemoryBytes settings)) retire $ \streams ->
    bracket (newPool 0 (setupMemoryBytes settings)) retire $ \setup ->
      action (Resources streams setup)
  where
    newPool reserve budget = do
      buffers <- newBuffers budget (ioBufferBytes settings)
      let capacity = length buffers
      free <- newTVarIO (capacity, buffers)
      active <- newTVarIO True
      lanes <- newTVarIO Map.empty
      nextArrival <- newTVarIO 0
      dispatching <- newTVarIO False
      pure (Pool capacity reserve free active waiting lanes nextArrival dispatching)
    retire pool = atomically $ do
      writeTVar (poolActive pool) False
      writeTVar (poolFree pool) (0, [])
      lanes <- readTVar (poolLanes pool)
      mapM_ retireLane lanes
    retireLane lane = do
      first <- Queue.first lane
      case first of
        Nothing -> pure ()
        Just entry -> do
          let Request _ _ refuse = Queue.value entry
          refuse
          retireLane lane

-- | Validate configuration without allocating resource storage.
validateSettings :: ResourceSettings -> Either String ()
validateSettings settings
  | streamMemoryBytes settings < ioBufferBytes settings = Left "stream budget must fit a block"
  | positiveIntValue (streamMemoryBytes settings) `div` positiveIntValue (ioBufferBytes settings) <= positiveIntValue (reserveBufferCount settings) =
      Left "stream budget must fit an ordinary block plus reserve"
  | setupMemoryBytes settings < ioBufferBytes settings = Left "setup budget must fit a block"
  | toInteger (positiveIntValue (streamMemoryBytes settings)) + toInteger (positiveIntValue (setupMemoryBytes settings)) > toInteger (maxBound :: Int) =
      Left "combined memory budget exceeds Int capacity"
  | otherwise = Right ()

-- | Ownership transfers only when the surrounding transaction commits.
-- The caller must mask the interval between admission and installing cleanup.
tryAcquire :: forall grant. BufferGrant grant => Resources -> STM (Maybe (Lease grant))
tryAcquire (Resources streams _) = acquireFrom streams

tryAcquireSetup :: forall grant. BufferGrant grant => Resources -> STM (Maybe (Lease grant))
tryAcquireSetup (Resources _ setup) = acquireFrom setup

acquireFrom :: forall grant. BufferGrant grant => Pool -> STM (Maybe (Lease grant))
acquireFrom pool = do
  count <- checkedGrantSize @grant pool
  (available, buffers) <- readTVar (poolFree pool)
  case if available - count < poolReserve pool then Nothing else takePrefix buffers of
    Nothing -> pure Nothing
    Just (selected, remaining) -> do
      writeTVar (poolFree pool) (available - count, remaining)
      returned <- newTVar False
      pure (Just (Lease pool selected returned))

-- | Per-lease state prevents a stale return from duplicating a later loan.
release :: forall grant. BufferGrant grant => Lease grant -> STM ()
release (Lease pool buffers returned) = do
  alreadyReturned <- readTVar returned
  when (not alreadyReturned) $ do
    writeTVar returned True
    alive <- readTVar (poolActive pool)
    when alive $ do
      (count, available) <- readTVar (poolFree pool)
      writeTVar (poolFree pool) (count + prefixSize @grant, prepend buffers available)
      dispatch pool

leaseBuffers :: Lease grant -> grant
leaseBuffers (Lease _ buffers _) = buffers

withBuffers :: forall grant a. BufferGrant grant => Resources -> (grant -> IO a) -> IO a
withBuffers (Resources streams _) = withLease @grant streams

withSetupBuffers :: forall grant a. BufferGrant grant => Resources -> (grant -> IO a) -> IO a
withSetupBuffers (Resources _ setup) = withLease @grant setup

withLease :: forall grant a. BufferGrant grant => Pool -> (grant -> IO a) -> IO a
withLease pool action = do
  count <- atomically (checkedGrantSize @grant pool)
  result <- admit pool (count + poolReserve pool)
    (acquireFrom @grant pool >>= maybe retry pure) (const (pure ())) (pure ())
    (atomically . release)
    (action . leaseBuffers)
  either (const (ioError (userError "resources are retired"))) pure result


-- | One request owns a primary block throughout its scope. An additional loan
-- may use global inventory (including the reserve), but is returned immediately.
-- Local returns use a private Waiting mailbox, never resource-wide retry wakeups.
data BufferAccess = BufferAccess !Pool !(Lease (Solo Buffer)) !(TVar AccessState)
data AccessState = AccessState
  { accessActive :: !Bool
  , primaryLoan :: !(Maybe BufferLoan)
  , extraLoan :: !(Maybe BufferLoan)
  , extraBusy :: !Bool
  , activeCallbacks :: !Int
  , extraWaiter :: !(Maybe (Waiter BufferLoan))
  }
data LoanKind = Primary | Extra !(Lease (Solo Buffer))
data BufferLoan = BufferLoan !BufferAccess !LoanKind !Buffer !(TVar Bool) !(TVar Bool)

loanBuffer :: BufferLoan -> Buffer
loanBuffer (BufferLoan _ _ buffer _ _) = buffer

withRequestBuffers :: Resources -> (BufferAccess -> IO a) -> IO a
withRequestBuffers (Resources pool _) action = do
  -- Waiting owns an unclaimed primary. Once installed, the access owns it so
  -- cancellation during teardown cannot reclaim a running child's callback.
  owner <- newTVarIO Nothing
  let cleanup primary = atomically $ readTVar owner >>= maybe (release primary) expireAccess
      use primary = bracket
        (atomically $ do
          state <- newTVar (AccessState True Nothing Nothing False 0 Nothing)
          let access = BufferAccess pool primary state
          writeTVar owner (Just access)
          pure access)
        closeAccess action
  result <- admit pool (1 + poolReserve pool)
    (acquireFrom @(Solo Buffer) pool >>= maybe retry pure) (const (pure ())) (pure ()) cleanup use
  either (const (ioError (userError "resources are retired"))) pure result

closeAccess :: BufferAccess -> IO ()
closeAccess access@(BufferAccess _ _ state) = mask_ $ do
  atomically (expireAccess access)
  -- Only the owner waits on its callback count. If interrupted, active callbacks
  -- retain ownership and return their loans themselves; no uninterruptible wait.
  atomically $ readTVar state >>= check . (== 0) . activeCallbacks

expireAccess :: BufferAccess -> STM ()
expireAccess (BufferAccess _ primary state) = do
  current <- readTVar state
  when (accessActive current) $ do
    writeTVar state current {accessActive = False}
    mapM_ (void . (`reject` Closed)) (extraWaiter current)
    -- Raw loans have the request scope as their lifetime. Callback loans keep
    -- storage exclusive until the corresponding withBuffer callback completes.
    let finishManual loan@(BufferLoan _ _ _ _ inUse) = do
          running <- readTVar inUse
          unless running (releaseLoan loan)
    mapM_ finishManual (primaryLoan current)
    mapM_ finishManual (extraLoan current)
    case primaryLoan current of
      Nothing -> release primary
      Just _ -> pure ()

-- | At most two overlapping loans, including an acquisition still waiting.
-- Manual users must stop using every alias before the request callback exits.
-- Use 'withBuffer' to register a callback whose completion teardown must await.
borrowBuffer :: BufferAccess -> IO BufferLoan
borrowBuffer access@(BufferAccess pool _ state) = mask $ \_ -> do
  immediate <- atomically $ do
    current <- checkedAccess access
    case primaryLoan current of
      Nothing -> do
        -- A pending acquisition owns the next primary return, including the
        -- small interval before its private mailbox has been registered.
        case extraLoan current of
          Nothing -> when (extraBusy current) $
            throwSTM (userError "request already has a pending buffer loan")
          Just _ -> pure ()
        Just <$> newPrimary access current
      Just _ -> do
        when (extraBusy current) $ throwSTM (userError "request already has two buffer loans")
        writeTVar state current {extraBusy = True}
        pure Nothing
  case immediate of
    Just loan -> pure loan
    Nothing -> do
      handedOff <- newTVarIO False
      let reserve = do
            current <- checkedAccess access
            case primaryLoan current of
              Nothing -> newPrimary access current
              Just _ -> do
                lease <- acquireExtra pool
                let MkSolo buffer = leaseBuffers lease
                returned <- newTVar False
                inUse <- newTVar False
                let loan = BufferLoan access (Extra lease) buffer returned inUse
                writeTVar state current {extraLoan = Just loan}
                pure loan
          register waiter = do
            current <- checkedAccess access
            writeTVar state current {extraWaiter = Just waiter}
            -- A local return may have happened between deciding to queue and
            -- registration. Resolve that race without waiting for global supply.
            case primaryLoan current of
              Nothing -> void (tryGrant waiter reserve)
              Just _ -> pure ()
          unregister = do
            current <- readTVar state
            writeTVar state current {extraWaiter = Nothing}
          cleanup loan = atomically $ do
            transferred <- readTVar handedOff
            unless transferred (releaseLoan loan)
          failed = atomically $ do
            current <- readTVar state
            case extraLoan current of
              Nothing -> writeTVar state current {extraBusy = False}
              Just _ -> pure ()
      result <- admit pool 1
        reserve register unregister cleanup
        (\loan -> atomically (writeTVar handedOff True) >> pure loan)
        `onException` failed
      case result of
        Left _ -> failed >> ioError (userError "request buffer access is closed")
        Right loan -> pure loan

checkedAccess :: BufferAccess -> STM AccessState
checkedAccess (BufferAccess pool _ state) = do
  current <- readTVar state
  alive <- readTVar (poolActive pool)
  unless (accessActive current && alive) $ throwSTM (userError "request buffer access is closed")
  pure current

newPrimary :: BufferAccess -> AccessState -> STM BufferLoan
newPrimary access@(BufferAccess _ primary state) current = do
  let MkSolo buffer = leaseBuffers primary
  returned <- newTVar False
  inUse <- newTVar False
  let loan = BufferLoan access Primary buffer returned inUse
  writeTVar state current {primaryLoan = Just loan, extraBusy = maybe False (const True) (extraLoan current)}
  pure loan

-- Extra loans can consume the protected reserve; ordinary grants cannot.
acquireExtra :: Pool -> STM (Lease (Solo Buffer))
acquireExtra pool = do
  (available, buffers) <- readTVar (poolFree pool)
  case buffers of
    [] -> retry
    buffer : remaining -> do
      writeTVar (poolFree pool) (available - 1, remaining)
      Lease pool (MkSolo buffer) <$> newTVar False

releaseBuffer :: BufferLoan -> IO ()
releaseBuffer = atomically . releaseLoan

releaseLoan :: BufferLoan -> STM ()
releaseLoan (BufferLoan access@(BufferAccess pool primary state) kind _ returned _) = do
  alreadyReturned <- readTVar returned
  unless alreadyReturned $ do
    writeTVar returned True
    current <- readTVar state
    case kind of
      Extra lease -> do
        writeTVar state current {extraLoan = Nothing, extraBusy = False}
        release lease
      Primary -> do
        writeTVar state current {primaryLoan = Nothing}
        alive <- readTVar (poolActive pool)
        unless (accessActive current) (release primary)
        when (accessActive current && alive) $
          mapM_ (\waiter -> void $ tryGrant waiter $ do
            latest <- checkedAccess access
            newPrimary access latest) (extraWaiter current)

withBuffer :: BufferAccess -> (Buffer -> IO a) -> IO a
withBuffer access@(BufferAccess _ _ state) action = bracket acquire finish (action . loanBuffer)
  where
    acquire = do
      loan@(BufferLoan _ _ _ _ inUse) <- borrowBuffer access
      (atomically $ do
        current <- checkedAccess access
        writeTVar inUse True
        writeTVar state current {activeCallbacks = activeCallbacks current + 1})
        `onException` releaseBuffer loan
      pure loan
    finish loan@(BufferLoan _ _ _ _ inUse) = atomically $ do
      current <- readTVar state
      writeTVar inUse False
      writeTVar state current {activeCallbacks = activeCallbacks current - 1}
      releaseLoan loan
