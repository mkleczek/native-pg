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
dispatch _ = pure ()

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


-- | Validate configuration without allocating resource storage.
validateSettings :: ResourceSettings -> Either String ()
validateSettings settings
  | streamMemoryBytes settings < ioBufferBytes settings = Left "stream budget must fit a block"
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
withLease pool action = bracket
  (atomically (acquireFrom @grant pool >>= maybe retry pure))
  (atomically . release)
  (action . leaseBuffers)


data BufferAccess = BufferAccess
data BufferLoan = BufferLoan Buffer
withRequestBuffers :: Resources -> (BufferAccess -> IO a) -> IO a
withRequestBuffers _ _ = error "pending request primary implementation"
borrowBuffer :: BufferAccess -> IO BufferLoan
borrowBuffer _ = error "pending request buffer loan"
loanBuffer :: BufferLoan -> Buffer
loanBuffer (BufferLoan buffer) = buffer
releaseBuffer :: BufferLoan -> IO ()
releaseBuffer _ = error "pending request return"
withBuffer :: BufferAccess -> (Buffer -> IO a) -> IO a
withBuffer _ _ = error "pending scoped callback implementation"
