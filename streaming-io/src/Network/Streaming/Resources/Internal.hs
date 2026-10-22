{-# LANGUAGE ConstraintKinds #-}
-- | Integration API. Buffer aliases expire when their lease or resource scope ends.
module Network.Streaming.Resources.Internal
  ( Resources, ResourceSettings (..), withResources
  , validateSettings
  , Buffer (..), BufferGrant, Lease, tryAcquire, tryAcquireSetup, release, leaseBuffers
  , withBuffers, withSetupBuffers
  ) where

import Control.Concurrent.STM
  ( STM, TVar, atomically, newTVar, newTVarIO, readTVar, writeTVar
  , retry, throwSTM )
import Control.Exception (bracket)
import Control.Monad (replicateM, when)
import Data.Word (Word8)
import Network.Streaming.Internal.ListPrefix (ListPrefix (..))
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrBytes, withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import Network.Streaming.PositiveInt (PositiveInt, positiveIntValue)

data ResourceSettings = ResourceSettings
  { streamMemoryBytes :: !PositiveInt
  , setupMemoryBytes :: !PositiveInt
  , ioBufferBytes :: !PositiveInt
  } deriving (Eq, Show)

data Buffer = Buffer
  { bufferMemory :: !(ForeignPtr Word8)
  , bufferCapacity :: !Int
  }

data Pool = Pool !Int !(TVar [Buffer]) !(TVar Bool)
data Resources = Resources !Pool !Pool
type BufferGrant grant = ListPrefix grant Buffer

data Lease grant = Lease !Pool grant !(TVar Bool)

-- | Budgets are rounded down to complete blocks. Every block is initialized
-- here so admission and return never allocate or clear body storage.
withResources :: ResourceSettings -> (Resources -> IO a) -> IO a
withResources settings action = do
  either (ioError . userError) pure (validateSettings settings)
  bracket create retire action
  where
    create = Resources <$> newPool (streamMemoryBytes settings) <*> newPool (setupMemoryBytes settings)
    newPool budget = do
      let blockBytes = positiveIntValue (ioBufferBytes settings)
          count = positiveIntValue budget `div` blockBytes
      buffers <- replicateM count $ do
        memory <- mallocForeignPtrBytes blockBytes
        withForeignPtr memory $ \pointer -> fillBytes pointer 0 blockBytes
        pure (Buffer memory blockBytes)
      Pool count <$> newTVarIO buffers <*> newTVarIO True
    retire (Resources streams setup) = atomically (close streams >> close setup)
    close (Pool _ free active) = writeTVar active False >> writeTVar free []

-- | Validate configuration without allocating resource storage.
validateSettings :: ResourceSettings -> Either String ()
validateSettings settings
  | streamMemoryBytes settings < ioBufferBytes settings = Left "stream budget must fit a block"
  | setupMemoryBytes settings < ioBufferBytes settings = Left "setup budget must fit a block"
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
