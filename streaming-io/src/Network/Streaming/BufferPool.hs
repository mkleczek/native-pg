{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
-- | Application-owned storage pools with scoped, exclusive buffer loans.
module Network.Streaming.BufferPool
  ( BufferPool, PoolSettings (..), newBufferPool
  , BufferAccess, withRequestBuffers
  , BufferLoan, borrowBuffer, loanBuffer, releaseBuffer
  , BufferGrant, withSetupBuffers
  ) where

import Control.Concurrent.STM
  ( STM, TVar, atomically, newTVar, newTVarIO, readTVar, writeTVar
  , retry, throwSTM, check )
import Control.Exception (bracket, mask, onException)
import Control.Concurrent.Resource.Queue qualified as Queue
import Control.Concurrent.Resource.Acquisition (Deadline (NoDeadline), Context, tryGrant)
import Control.Concurrent.Resource.Acquisition qualified as Acquire
import Control.Monad (when, unless, void, foldM)
import Data.IntSet qualified as IntSet
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Tuple (Solo (MkSolo))
import Network.Streaming.Internal.ListPrefix (ListPrefix (..))
import Network.Streaming.Buffer (Buffer, newBuffers)
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt, positiveIntValue)

data PoolSettings = PoolSettings
  { bufferSize :: !PositiveInt
  , ordinaryBufferCount :: !PositiveInt
  , reserveBufferCount :: !PositiveInt
  , setupBufferCount :: !PositiveInt
  } deriving (Eq, Show)

data Pool = Pool
  { poolCapacity :: !Int
  , poolReserve :: !Int
  , poolFree :: !(TVar (Int, [Buffer]))
  , poolAcquisition :: !Context
  , poolLanes :: !(TVar (Map.Map Int (Queue.Queue Request)))
  , poolNextArrival :: !(TVar Integer)
  , poolDispatching :: !(TVar Bool)
  }
data BufferPool = BufferPool !Pool !Pool
type BufferGrant grant = ListPrefix grant Buffer

data Lease grant = Lease !Pool grant !(TVar Bool)

-- Each required inventory count has a FIFO. Selecting among eligible heads
-- costs the number of grant sizes, independently of blocked request count.
-- Arrival tickets preserve oldest-eligible order across this buffer pool alone.
data Request = Request !Integer !(STM Bool)

dispatch :: Pool -> STM ()
dispatch pool = do
  running <- readTVar (poolDispatching pool)
  unless running $ do
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
            Just (Request ticket grant) -> case best of
              Just (_, earlier, _) | earlier < ticket -> best
              _ -> Just (required, ticket, grant)

-- Registration, exact removal, local delivery and inventory changes all commit
-- with scheduling. The guard lets terminal hooks dispatch without recursion.
admit :: Pool -> Int -> STM a -> (Acquire.Request a -> STM ()) -> STM () -> (a -> IO ()) -> IO a
admit pool required reserve register unregister cleanup =
  let reserveNow = do
        lanes <- readTVar (poolLanes pool)
        -- Queued requests must retain first claim on returned inventory.
        check (Map.null lanes)
        reserve
      detach (lane, entry) = do
        Queue.remove entry
        empty <- Queue.isEmpty lane
        when empty $ do
          lanes <- readTVar (poolLanes pool)
          writeTVar (poolLanes pool) (Map.delete required lanes)
        unregister
        dispatch pool
      enqueue waiter = do
        lanes <- readTVar (poolLanes pool)
        lane <- case Map.lookup required lanes of
          Just existing -> pure existing
          Nothing -> do
            created <- Queue.newQueue
            writeTVar (poolLanes pool) (Map.insert required created lanes)
            pure created
        ticket <- readTVar (poolNextArrival pool)
        writeTVar (poolNextArrival pool) (ticket + 1)
        entry <- Queue.append lane (Request ticket (tryGrant waiter reserve))
        register waiter
        dispatch pool
        pure (lane, entry)
  in do
    result <- Acquire.acquire (poolAcquisition pool) NoDeadline reserveNow enqueue detach cleanup
    -- Only deadlines produce Nothing; buffer admission never sets one.
    maybe (ioError (userError "buffer admission without a deadline expired")) pure result

-- Validate before queueing: an impossible demand would never be selected.
checkedGrantSize :: forall grant. BufferGrant grant => Pool -> STM Int
checkedGrantSize pool = do
  let count = prefixSize @grant
  when (count > poolCapacity pool - poolReserve pool) $
    throwSTM (userError "buffer count must fit capacity")
  pure count

-- | Counts specify exact pool inventories. Storage is initialized here so
-- admission and return never allocate or clear body storage. Retained resources
-- remain usable; the caller owns stopping, cancelling and joining its tasks.
-- The backing storage is collected when its final pool or buffer alias is gone.
newBufferPool :: Context -> PoolSettings -> IO BufferPool
newBufferPool acquisition settings =
  BufferPool
    <$> newPool reserve (count (ordinaryBufferCount settings) + toInteger reserve)
    <*> newPool 0 (count (setupBufferCount settings))
  where
    reserve = positiveIntValue (reserveBufferCount settings)
    count = toInteger . positiveIntValue
    newPool protected bufferCount = do
      -- Check the full byte count before narrowing for the allocator.
      budget <- maybe (ioError (userError "buffer allocation exceeds Int capacity")) pure
        (mkPositiveInt (bufferCount * count (bufferSize settings)))
      allocated@(capacity, _) <- newBuffers budget (bufferSize settings)
      Pool capacity protected
        <$> newTVarIO allocated
        <*> pure acquisition
        <*> newTVarIO Map.empty
        <*> newTVarIO 0
        <*> newTVarIO False

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
  unless alreadyReturned $ do
    writeTVar returned True
    (count, available) <- readTVar (poolFree pool)
    writeTVar (poolFree pool) (count + prefixSize @grant, prepend buffers available)
    dispatch pool

leaseBuffers :: Lease grant -> grant
leaseBuffers (Lease _ buffers _) = buffers

withSetupBuffers :: forall grant a. BufferGrant grant => BufferPool -> (grant -> IO a) -> IO a
withSetupBuffers (BufferPool _ setup) = withLease @grant setup

withLease :: forall grant a. BufferGrant grant => Pool -> (grant -> IO a) -> IO a
withLease pool action = bracket obtain (atomically . release) (action . leaseBuffers)
  where
    obtain = do
      count <- atomically (checkedGrantSize @grant pool)
      admit pool (count + poolReserve pool)
        (acquireFrom @grant pool >>= maybe retry pure) (const (pure ())) (pure ())
        (atomically . release)


-- | One request owns a primary block throughout its scope. An additional loan
-- may use global inventory (including the reserve), but is returned immediately.
-- Local returns update private acquisition delivery state, avoiding resource-wide retry wakeups.
data BufferAccess = BufferAccess !Pool !(Lease (Solo Buffer)) !(TVar AccessState)
data AccessState = AccessState
  { accessActive :: !Bool
  , primaryLoan :: !(Maybe BufferLoan)
  , extraLoan :: !(Maybe BufferLoan)
  , extraBusy :: !Bool
  , extraWaiter :: !(Maybe (Acquire.Request BufferLoan))
  }
data LoanKind = Primary | Extra !(Lease (Solo Buffer))
data BufferLoan = BufferLoan !BufferAccess !LoanKind !Buffer !(TVar Bool)

loanBuffer :: BufferLoan -> Buffer
loanBuffer (BufferLoan _ _ buffer _) = buffer

-- | Every loan and buffer alias expires with this scope. Callers must finish
-- their children and stop accessing those aliases before returning.
withRequestBuffers :: BufferPool -> (BufferAccess -> IO a) -> IO a
withRequestBuffers (BufferPool pool _) = bracket obtain (atomically . expireAccess)
  where
    -- The bracket masks the handoff from acquisition to access ownership.
    obtain = do
      primary <- admit pool (1 + poolReserve pool)
        (acquireFrom @(Solo Buffer) pool >>= maybe retry pure) (const (pure ())) (pure ())
        (atomically . release)
      atomically (do
        state <- newTVar (AccessState True Nothing Nothing False Nothing)
        pure (BufferAccess pool primary state))
        `onException` atomically (release primary)

expireAccess :: BufferAccess -> STM ()
expireAccess (BufferAccess _ primary state) = do
  current <- readTVar state
  when (accessActive current) $ do
    writeTVar state current {accessActive = False}
    mapM_ (\waiter -> void (tryGrant waiter (throwSTM (userError "request buffer access is closed")))) (extraWaiter current)
    mapM_ releaseLoan (primaryLoan current)
    mapM_ releaseLoan (extraLoan current)
    release primary

-- | Loans share the lifetime of 'withRequestBuffers'. A pending borrow also
-- occupies one of the two loan slots, so reserve demand stays bounded.
borrowBuffer :: BufferAccess -> IO BufferLoan
-- Keep local loan state and acquisition delivery under the same mask.
borrowBuffer access@(BufferAccess pool _ state) = mask $ \_ -> do
  immediate <- atomically $ do
    current <- checkedAccess access
    case primaryLoan current of
      Nothing -> do
        -- A pending acquisition owns the next primary return, including the
        -- small interval before its waiter has been registered.
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
      let reserve = do
            current <- checkedAccess access
            case primaryLoan current of
              Nothing -> newPrimary access current
              Just _ -> do
                lease <- acquireExtra pool
                let MkSolo buffer = leaseBuffers lease
                returned <- newTVar False
                let loan = BufferLoan access (Extra lease) buffer returned
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
          cleanup = atomically . releaseLoan
          failed = atomically $ do
            current <- readTVar state
            case extraLoan current of
              Nothing -> writeTVar state current {extraBusy = False}
              Just _ -> pure ()
      admit pool 1 reserve register unregister cleanup `onException` failed

checkedAccess :: BufferAccess -> STM AccessState
checkedAccess (BufferAccess _ _ state) = do
  current <- readTVar state
  unless (accessActive current) $ throwSTM (userError "request buffer access is closed")
  pure current

newPrimary :: BufferAccess -> AccessState -> STM BufferLoan
newPrimary access@(BufferAccess _ primary state) current = do
  let MkSolo buffer = leaseBuffers primary
  returned <- newTVar False
  let loan = BufferLoan access Primary buffer returned
  writeTVar state current {primaryLoan = Just loan, extraBusy = isJust (extraLoan current)}
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
releaseLoan (BufferLoan access@(BufferAccess _ primary state) kind _ returned) = do
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
        unless (accessActive current) (release primary)
        when (accessActive current) $
          mapM_ (\waiter -> void $ tryGrant waiter $ do
            latest <- checkedAccess access
            newPrimary access latest) (extraWaiter current)

