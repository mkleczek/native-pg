-- | The byte-stream interface implemented by transport links.
module Database.PostgreSQL.Transport.Link
  ( Link (..),
    Readiness (..),
    StreamLengthError (..),
  )
where

import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.Word (Word8)
import Foreign.Ptr (Ptr)

-- | A stream of bytes to the server, such as a socket or TLS over it.
-- Reading, sending and flushing take what's ready without waiting.
-- Readiness waits suspend the calling thread until an operation can be
-- retried; they don't read or write, and don't guarantee progress. For
-- example, socket input may contain only part of a TLS record.
--
-- Callers must ensure exclusive access to each connection. Receiving,
-- sending, flushing, waiting and closing must not overlap, whether called
-- on the link directly or through any @Channel@ built from it. Ownership
-- may pass to another thread between calls. Different connections may be
-- used concurrently.
class Link link where
  -- | Waits until receiving can be retried after 'receiveReady' returns
  -- 'Nothing'. The wait must be interruptible.
  awaitReadable :: link -> IO ()

  -- | Waits until sending or flushing can be retried after stalling.
  -- The wait must be interruptible.
  awaitWritable :: link -> IO ()

  -- | Waits for either direction, so pending output can be sent while
  -- waiting for input. If both readiness events have been observed,
  -- prefers 'Readable'. The wait must be interruptible and release its
  -- registrations when it returns or is interrupted.
  awaitReadableOrWritable :: link -> IO Readiness

  -- | Reads what's ready into the buffer, up to its size, without waiting:
  -- 'Nothing' if nothing is, and 0 at the end of the stream.
  receiveReady :: link -> Ptr Word8 -> Int -> IO (Maybe Int)

  -- | Takes what it can of the pieces without waiting, and gives how many
  -- bytes it took.
  sendReady :: link -> [ByteString] -> IO Int

  -- | Writes what it took but hasn't written yet, without waiting. True if
  -- nothing is left.
  flushReady :: link -> IO Bool

  -- | Closes the link and releases its underlying connection.
  closeLink :: link -> IO ()

-- | Which operation to retry after waiting for either direction.
data Readiness = Readable | Writable
  deriving (Eq, Show)

-- | A parameter stream gave a different number of bytes than it declared.
-- The message it belongs to is incomplete, so the connection is unusable.
data StreamLengthError = StreamLengthError
  { declaredLength :: Int,
    -- | At least this many.
    deliveredLength :: Int
  }
  deriving (Show)

instance Exception StreamLengthError
