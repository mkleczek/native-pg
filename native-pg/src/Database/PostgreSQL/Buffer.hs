-- | Initialized storage owned by the caller. A buffer may be lent to only
-- one operation at a time; returning from that operation ends the loan.
module Database.PostgreSQL.Buffer
  ( Buffer,
    newBuffer,
    initializeBuffer,
    bufferFromInitializedMemory,
    bufferMemory,
    bufferCapacity,
  )
where

import Control.Monad (when)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)

data Buffer = Buffer
  { bufferMemory :: !(ForeignPtr Word8),
    bufferCapacity :: !Int
  }

newBuffer :: Int -> IO Buffer
newBuffer size = do
  validateCapacity size
  memory <- mallocPlainForeignPtrBytes size
  initializeBuffer memory size

-- | The allocation must contain at least this many writable bytes. Call
-- once when adding storage to a pool, not whenever a borrower acquires it:
-- rustls requires initialized destinations even though it only writes them.
initializeBuffer :: ForeignPtr Word8 -> Int -> IO Buffer
initializeBuffer memory size = do
  validateCapacity size
  withForeignPtr memory $ \pointer -> fillBytes pointer 0 size
  pure (Buffer memory size)

-- | Adapts storage that its owner has already initialized. Unlike
-- 'initializeBuffer', this only creates a descriptor; the caller guarantees
-- the allocation contains at least the given number of initialized writable
-- bytes and keeps each loan exclusive until its operation returns.
bufferFromInitializedMemory :: ForeignPtr Word8 -> Int -> IO Buffer
bufferFromInitializedMemory memory size = do
  validateCapacity size
  pure (Buffer memory size)

validateCapacity :: Int -> IO ()
validateCapacity size = when (size <= 0) (ioError (userError "buffer capacity must be positive"))
