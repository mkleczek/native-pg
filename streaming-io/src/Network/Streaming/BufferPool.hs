{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
-- | Application-owned storage pools with scoped, exclusive buffer loans.
module Network.Streaming.BufferPool
  ( BufferPool, PoolSettings (..), newBufferPool
  , BufferAccess, withRequestBuffers
  , BufferLoan, borrowBuffer, tryBorrowBuffer, watchBorrow, loanBuffer, releaseBuffer
  , BufferGrant, withSetupBuffers
  ) where

import Control.Concurrent.STM
  ( STM, TVar, atomically, newTVar, newTVarIO, readTVar, writeTVar
  , retry, throwSTM, check, orElse )
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
import Network.Streaming.Watch (Watch (..))

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

-- Queued requests must retain first claim on returned inventory.
reserveUnqueued :: Pool -> STM a -> STM a
reserveUnqueued pool reserve = do
  lanes <- readTVar (poolLanes pool)
  check (Map.null lanes)
  reserve

type Queued = (Queue.Queue Request, Queue.Entry Request)

-- Registration, exact removal, local delivery and inventory changes all commit
-- with scheduling. The guard lets terminal hooks dispatch without recursion.
-- The hooks serve both the blocking 'admit' and a registration nobody waits on.
admission :: Pool -> Int -> STM a -> (Acquire.Request a -> STM ()) -> STM ()
  -> (STM a, Acquire.Request a -> STM Queued, Queued -> STM ())
admission pool required reserve register unregister =
  let detach (lane, entry) = do
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
  in (reserveUnqueued pool reserve, enqueue, detach)

admit :: Pool -> Int -> STM a -> (Acquire.Request a -> STM ()) -> STM () -> (a -> IO ()) -> IO a
admit pool required reserve register unregister cleanup = do
  let (reserveNow, enqueue, detach) = admission pool required reserve register unregister
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
  , extraWatch :: !(Maybe Registration)
  }
-- A 'watchBorrow' registration. Its request holds the grant until a try claims
-- it or the watch is cancelled; the flag makes either happen once.
data Registration = Registration !(Acquire.Request BufferLoan) !(TVar Bool)
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
        state <- newTVar (AccessState True Nothing Nothing False Nothing Nothing)
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
    primaryFree <- enterSlot access current
    if primaryFree then Just <$> newPrimary access current else pure Nothing
  case immediate of
    Just loan -> pure loan
    Nothing -> do
      let failed = atomically $ do
            current <- readTVar state
            case extraLoan current of
              Nothing -> writeTVar state current {extraBusy = False}
              Just _ -> pure ()
      admit pool 1 (grantExtra access) (registerExtra access) (unregisterExtra access)
        (atomically . releaseLoan) `onException` failed

-- | Takes a loan if one is available now: the free primary, or an extra loan
-- that overtakes no waiting borrower. Never waits. Fails promptly, like
-- 'borrowBuffer', on a third overlapping loan or an expired access. A loan
-- granted to this access's 'watchBorrow' registration is claimed first.
tryBorrowBuffer :: BufferAccess -> IO (Maybe BufferLoan)
tryBorrowBuffer access@(BufferAccess pool _ state) = atomically $ do
  current <- checkedAccess access
  claimed <- claimWatch state current
  case claimed of
    Just _ -> pure claimed
    Nothing ->
      (do
        primaryFree <- enterSlot access current
        Just <$> if primaryFree
          then newPrimary access current
          else reserveUnqueued pool (grantExtra access))
        -- Without supply, or behind an earlier borrower, the extra slot is
        -- given back and the caller can watch instead.
        `orElse` pure Nothing

-- | Registers as this access's pending extra borrower, in the same lane and
-- FIFO order as 'borrowBuffer'. 'watchReady' fires once a loan has been
-- granted to the registration; the next 'tryBorrowBuffer' on this access
-- claims it. 'watchCancel' withdraws, and returns a granted loan nobody
-- claimed. Expiry fails the registration, as it fails a pending borrow:
-- 'watchReady' still fires, and the next 'tryBorrowBuffer' raises the error of
-- an expired access.
--
-- A loan that is available now is granted to the registration at once, so the
-- watch is ready when it is returned and holds that loan until it is claimed
-- or cancelled: nothing can overtake it in between. A registration occupies
-- the extra slot, so an access has at most one, and a third overlapping loan
-- fails promptly. Register under a mask and cancel on exceptions, as
-- 'Network.Streaming.Watch.awaitWatch' does.
watchBorrow :: BufferAccess -> IO Watch
watchBorrow access@(BufferAccess pool _ state) = do
  registration@(Registration request _) <- atomically $ do
    current <- checkedAccess access
    when (isJust (extraWatch current)) $
      throwSTM (userError "request already has a pending buffer loan")
    void (enterSlot access current)
    let (reserveNow, enqueue, detach) =
          admission pool 1 (grantExtra access) (registerExtra access) (unregisterExtra access)
    request <- Acquire.enroll reserveNow enqueue detach
    settled <- newTVar False
    -- Granting or removing during registration has written the state.
    latest <- readTVar state
    let registration = Registration request settled
    writeTVar state latest {extraWatch = Just registration}
    pure registration
  pure Watch
    { watchReady = Acquire.delivered request >>= maybe retry (const (pure ()))
    , watchCancel = atomically (cancelWatch state registration)
    }

-- The two-loan rule. A free primary is the next loan. Otherwise the extra
-- slot must be free, and the caller takes it. A pending acquisition owns the
-- next primary return, including the small interval before its waiter has
-- been registered.
enterSlot :: BufferAccess -> AccessState -> STM Bool
enterSlot (BufferAccess _ _ state) current = case primaryLoan current of
  Nothing -> do
    case extraLoan current of
      Nothing -> when (extraBusy current) $
        throwSTM (userError "request already has a pending buffer loan")
      Just _ -> pure ()
    pure True
  Just _ -> do
    when (extraBusy current) $ throwSTM (userError "request already has two buffer loans")
    writeTVar state current {extraBusy = True}
    pure False

-- The loan of a pending extra borrow: the primary if it came back, else global inventory.
grantExtra :: BufferAccess -> STM BufferLoan
grantExtra access@(BufferAccess pool _ state) = do
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

registerExtra :: BufferAccess -> Acquire.Request BufferLoan -> STM ()
registerExtra access@(BufferAccess _ _ state) waiter = do
  current <- checkedAccess access
  writeTVar state current {extraWaiter = Just waiter}
  -- A local return may have happened between deciding to queue and
  -- registration. Resolve that race without waiting for global supply.
  case primaryLoan current of
    Nothing -> void (tryGrant waiter (grantExtra access))
    Just _ -> pure ()

unregisterExtra :: BufferAccess -> STM ()
unregisterExtra (BufferAccess _ _ state) = do
  current <- readTVar state
  writeTVar state current {extraWaiter = Nothing}

-- A try takes the loan its watch was granted. A grant is already in the loan
-- slots; claiming only hands it over. A registration still pending yields
-- nothing, and the two-loan rule then rejects the try.
claimWatch :: TVar AccessState -> AccessState -> STM (Maybe BufferLoan)
claimWatch state current = case extraWatch current of
  Nothing -> pure Nothing
  Just (Registration request settled) -> do
    outcome <- Acquire.delivered request
    case outcome of
      Nothing -> pure Nothing
      Just (Left failure) -> throwSTM failure
      Just (Right loan) -> do
        writeTVar settled True
        writeTVar state current {extraWatch = Nothing}
        pure (Just loan)

-- Withdraws a pending registration or returns a grant nobody claimed. Settling
-- first makes repeated cancels, and a cancel after a claim, do nothing.
cancelWatch :: TVar AccessState -> Registration -> STM ()
cancelWatch state (Registration request settled) = do
  done <- readTVar settled
  unless done $ do
    writeTVar settled True
    Acquire.withdraw request >>= mapM_ releaseLoan
    current <- readTVar state
    writeTVar state current {extraWatch = Nothing, extraBusy = isJust (extraLoan current)}

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

