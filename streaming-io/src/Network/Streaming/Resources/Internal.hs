-- | Integration API. Buffer aliases expire when their lease or resource scope ends.
module Network.Streaming.Resources.Internal
  ( Resources, ResourceSettings (..), withResources
  , Buffer (..), Lease, tryAcquire, tryAcquireSetup, release, leaseBuffers
  , withBuffers, withSetupBuffers
  ) where

import Control.Concurrent.STM (STM)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr)
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

data Resources = Resources
data Lease = Lease [Buffer]

withResources :: ResourceSettings -> (Resources -> IO a) -> IO a
withResources settings action
  | streamMemoryBytes settings < ioBufferBytes settings = invalid "stream budget must fit a block"
  | setupMemoryBytes settings < ioBufferBytes settings = invalid "setup budget must fit a block"
  | otherwise = action Resources
  where
    invalid = ioError . userError

tryAcquire :: Resources -> PositiveInt -> STM (Maybe Lease)
tryAcquire _ _ = pure Nothing

tryAcquireSetup :: Resources -> PositiveInt -> STM (Maybe Lease)
tryAcquireSetup = tryAcquire

release :: Lease -> STM ()
release _ = pure ()

leaseBuffers :: Lease -> [Buffer]
leaseBuffers (Lease buffers) = buffers

withBuffers :: Resources -> PositiveInt -> ([Buffer] -> IO a) -> IO a
withBuffers _ _ _ = ioError (userError "buffer loans are not implemented")

withSetupBuffers :: Resources -> PositiveInt -> ([Buffer] -> IO a) -> IO a
withSetupBuffers = withBuffers
