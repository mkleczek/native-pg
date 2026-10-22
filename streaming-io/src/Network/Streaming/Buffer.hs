-- | Initialized, fixed-capacity storage lent exclusively to one operation.
-- Aliases expire when the loan or owning allocation scope ends.
module Network.Streaming.Buffer
  ( Buffer (..), newBuffer, newBuffers ) where

import Control.Exception (mask_, onException)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, newForeignPtr, plusForeignPtr)
import Foreign.Marshal.Alloc (callocBytes, finalizerFree, free)
import Network.Streaming.PositiveInt (PositiveInt, positiveIntValue)

data Buffer = Buffer
  { bufferMemory :: !(ForeignPtr Word8)
  , bufferCapacity :: !Int
  }

newBuffer :: PositiveInt -> IO Buffer
newBuffer size = mask_ $ do
  let capacity = positiveIntValue size
  -- Transfer ownership before asynchronous cancellation can abandon the allocation.
  pointer <- callocBytes capacity
  memory <- newForeignPtr finalizerFree pointer `onException` free pointer
  pure (Buffer memory capacity)

-- | Allocate the budget once and expose only complete blocks.
newBuffers :: PositiveInt -> PositiveInt -> IO [Buffer]
newBuffers budget size =
  -- Each view shares the allocation's owner, even after other slices expire.
  split <$> newBuffer budget
  where
    capacity = positiveIntValue size
    count = positiveIntValue budget `div` capacity
    slice index storage = Buffer (plusForeignPtr (bufferMemory storage) (index * capacity)) capacity
    split storage = slice <$> [0 .. count - 1] <*> [storage]
