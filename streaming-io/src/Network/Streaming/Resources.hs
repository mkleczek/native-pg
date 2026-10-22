{-# LANGUAGE ConstraintKinds #-}
-- | Application-owned storage pools with scoped, exclusive buffer loans.
module Network.Streaming.Resources
  ( Resources, ResourceSettings (..), withResources
  , validateSettings
  , BufferGrant, Lease, tryAcquire, tryAcquireSetup, release, leaseBuffers
  , withBuffers, withSetupBuffers
  ) where

import Control.Concurrent.STM
  ( STM, TVar, atomically, newTVar, newTVarIO, readTVar, writeTVar
  , retry, throwSTM )
import Control.Exception (bracket)
import Control.Monad (when)
import Network.Streaming.Internal.ListPrefix (ListPrefix (..))
import Network.Streaming.Buffer (Buffer, newBuffers)
import Network.Streaming.PositiveInt (PositiveInt, positiveIntValue)

data ResourceSettings = ResourceSettings
  { streamMemoryBytes :: !PositiveInt
  , setupMemoryBytes :: !PositiveInt
  , ioBufferBytes :: !PositiveInt
  } deriving (Eq, Show)

data Pool = Pool !Int !(TVar [Buffer]) !(TVar Bool)
data Resources = Resources !Pool !Pool
type BufferGrant grant = ListPrefix grant Buffer

data Lease grant = Lease !Pool grant !(TVar Bool)

-- | Only complete blocks are lent from each budget. Storage is initialized
-- here so admission and return never allocate or clear body storage.
withResources :: ResourceSettings -> (Resources -> IO a) -> IO a
withResources settings action = do
  either (ioError . userError) pure (validateSettings settings)
  bracket create retire action
  where
    create = Resources <$> newPool (streamMemoryBytes settings) <*> newPool (setupMemoryBytes settings)
    newPool budget = do
      buffers <- newBuffers budget (ioBufferBytes settings)
      Pool (length buffers) <$> newTVarIO buffers <*> newTVarIO True
    retire (Resources streams setup) = atomically (close streams >> close setup)
    close (Pool _ free active) = writeTVar active False >> writeTVar free []

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
acquireFrom pool@(Pool capacity free active)
  | prefixSize @grant > capacity = throwSTM (userError "buffer count must fit capacity")
  | otherwise = do
      alive <- readTVar active
      when (not alive) $ throwSTM (userError "resources are retired")
      buffers <- readTVar free
      case takePrefix buffers of
        Nothing -> pure Nothing
        Just (selected, remaining) -> do
          writeTVar free remaining
          returned <- newTVar False
          pure (Just (Lease pool selected returned))

-- | Per-lease state prevents a stale return from duplicating a later loan.
release :: BufferGrant grant => Lease grant -> STM ()
release (Lease (Pool _ free active) buffers returned) = do
  alreadyReturned <- readTVar returned
  when (not alreadyReturned) $ do
    writeTVar returned True
    alive <- readTVar active
    when alive $ do
      available <- readTVar free
      writeTVar free (prepend buffers available)

leaseBuffers :: Lease grant -> grant
leaseBuffers (Lease _ buffers _) = buffers

withBuffers :: forall grant a. BufferGrant grant => Resources -> (grant -> IO a) -> IO a
withBuffers resources = withLease (tryAcquire @grant resources)

withSetupBuffers :: forall grant a. BufferGrant grant => Resources -> (grant -> IO a) -> IO a
withSetupBuffers resources = withLease (tryAcquireSetup @grant resources)

withLease :: BufferGrant grant => STM (Maybe (Lease grant)) -> (grant -> IO a) -> IO a
withLease acquire action =
  bracket wait (atomically . release) (action . leaseBuffers)
  where
    wait = atomically (acquire >>= maybe retry pure)
