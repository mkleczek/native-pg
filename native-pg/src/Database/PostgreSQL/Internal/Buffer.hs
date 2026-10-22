-- | Initialized storage owned by the caller. A buffer may be lent to only
-- one operation at a time; returning from that operation ends the loan.
module Database.PostgreSQL.Internal.Buffer
  ( Buffer (..),
    bufferFromInitializedMemory,
    newBuffer,
    initializeBuffer,
  )
where

import Control.Monad (when)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Network.Streaming.Resources.Internal (Buffer (..))

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

validateCapacity :: Int -> IO ()
validateCapacity size = when (size <= 0) (ioError (userError "buffer capacity must be positive"))

-- Compatibility name for fixtures predating the initialized-buffer API.
bufferFromInitializedMemory :: ForeignPtr Word8 -> Int -> IO Buffer
bufferFromInitializedMemory = initializeBuffer
