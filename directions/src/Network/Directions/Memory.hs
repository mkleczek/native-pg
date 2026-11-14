-- | A link over bytes in memory, which never waits: a receive copies what
-- comes next, up to the room and up to a limit that stands for how much a
-- network gives at once. For measuring parsers apart from the waiting.
module Network.Directions.Memory
  ( Memory,
    newMemory,
    memoryReceive,
  )
where

import Control.Arrow (Arrow, (>>>))
import Control.Exception (throwIO)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peek, poke)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes

-- | The bytes, where the next receive starts (unboxed), and the most that one
-- receive gives.
data Memory = Memory !ByteString !(ForeignPtr Int) !Int

-- | A link that gives the bytes from their start, at most the limit at a time.
newMemory :: ByteString -> Int -> IO Memory
newMemory wire limit = do
  position <- mallocForeignPtrArray 1
  unsafeWithForeignPtr position (`poke` 0)
  pure (Memory wire position limit)

-- | Receives into the address, up to the size: how many bytes, 0 at the end.
memoryReceive :: Memory -> Ptr Word8 -> Int -> IO Int
memoryReceive (Memory (BSI.BS wire total) position limit) target room = unsafeWithForeignPtr position $ \pointer -> do
  at <- peek pointer
  let count = minimum [room, limit, total - at]
  unsafeWithForeignPtr wire (\from -> copyBytes target (from `plusPtr` at) count)
  poke pointer (at + count)
  pure count

-- | The link is for reading only. A receive never waits, but is a point to give
-- way at, as the receive of a link that does wait is, and the link always has
-- input.
instance (Arrow arr, Effect arr, GiveWay arr) => Link arr Memory where
  receive = giveWay >>> now (\(memory, Room (Buffer target _) offset size) -> unsafeWithForeignPtr target (\pointer -> memoryReceive memory (pointer `plusPtr` offset) size))
  {-# INLINE receive #-}
  sendAll = now (const (throwIO (userError "a memory link is for reading only")))
  awaitInput = now (const (pure ()))
  shutdownSend = now (const (pure ()))
  close = now (const (pure ()))
