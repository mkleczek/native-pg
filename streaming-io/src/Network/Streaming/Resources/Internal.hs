-- | Integration API. Buffer aliases expire when their lease or resource scope ends.
module Network.Streaming.Resources.Internal
  ( Resources, ResourceSettings (..), withResources
  , Buffer (..), Lease, tryAcquire, tryAcquireSetup, release, leaseBuffers
  , withBuffers, withSetupBuffers
  ) where

import Control.Concurrent.STM
  ( STM, TVar, atomically, newTVarIO, readTVar, writeTVar, retry )
import Control.Exception (bracket)
import Control.Monad (replicateM)
import Foreign.Marshal.Utils (fillBytes)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrBytes, withForeignPtr)
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

data Resources = Resources !(TVar [Buffer])
data Lease = Lease !(TVar [Buffer]) [Buffer]

withResources :: ResourceSettings -> (Resources -> IO a) -> IO a
withResources settings action
  | streamMemoryBytes settings < ioBufferBytes settings = invalid "stream budget must fit a block"
  | setupMemoryBytes settings < ioBufferBytes settings = invalid "setup budget must fit a block"
  | otherwise = do
      let blockBytes = positiveIntValue (ioBufferBytes settings)
      buffers <- replicateM (positiveIntValue (streamMemoryBytes settings) `div` blockBytes) $ do
        memory <- mallocForeignPtrBytes blockBytes
        withForeignPtr memory $ \pointer -> fillBytes pointer 0 blockBytes
        pure (Buffer memory blockBytes)
      free <- newTVarIO buffers
      action (Resources free)
  where
    invalid = ioError . userError

tryAcquire :: Resources -> PositiveInt -> STM (Maybe Lease)
tryAcquire (Resources free) _ = do
  buffers <- readTVar free
  case buffers of
    [] -> pure Nothing
    buffer : remaining -> do
      writeTVar free remaining
      pure (Just (Lease free [buffer]))

tryAcquireSetup :: Resources -> PositiveInt -> STM (Maybe Lease)
tryAcquireSetup = tryAcquire

release :: Lease -> STM ()
release (Lease free buffers) = do
  available <- readTVar free
  writeTVar free (buffers ++ available)

leaseBuffers :: Lease -> [Buffer]
leaseBuffers (Lease _ buffers) = buffers

withBuffers :: Resources -> PositiveInt -> ([Buffer] -> IO a) -> IO a
withBuffers resources count action =
  bracket acquire (atomically . release) (action . leaseBuffers)
  where
    acquire = atomically (tryAcquire resources count >>= maybe retry pure)

withSetupBuffers :: Resources -> PositiveInt -> ([Buffer] -> IO a) -> IO a
withSetupBuffers = withBuffers
