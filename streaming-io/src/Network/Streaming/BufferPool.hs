-- | Application-owned storage pools with scoped, exclusive buffer loans: the pool, as settings and a constructor. The
-- loans are those of "Network.Streaming.Loans".
module Network.Streaming.BufferPool
  ( BufferPool,
    BufferPoolSettings (..),
    newBufferPool,
    BufferGrant,
  )
where

import Network.Streaming.Buffer (Buffer, newBuffers)
import Network.Streaming.Internal.BufferPool (BufferPool (..))
import Network.Streaming.Internal.ListPrefix (ListPrefix)
import Network.Streaming.Pool (newPool)
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt, positiveIntValue)

data BufferPoolSettings = BufferPoolSettings
  { bufferSize :: !PositiveInt,
    ordinaryBufferCount :: !PositiveInt,
    reserveBufferCount :: !PositiveInt,
    setupBufferCount :: !PositiveInt
  }
  deriving (Eq, Show)

-- | A setup grant: all of its buffers at once, as many as the tuple has.
type BufferGrant grant = ListPrefix grant Buffer

-- | Counts specify exact pool inventories. Storage is initialized here so
-- admission and return never allocate or clear body storage. Retained resources
-- remain usable; the caller owns stopping, cancelling and joining its tasks.
-- The backing storage is collected when its final pool or buffer alias is gone.
newBufferPool :: BufferPoolSettings -> IO BufferPool
newBufferPool settings = do
  (_, requests) <- inventory (count (ordinaryBufferCount settings) + toInteger reserve)
  (setups, setup) <- inventory (count (setupBufferCount settings))
  BufferPool <$> newPool requests <*> pure reserve <*> newPool setup <*> pure setups
  where
    reserve = positiveIntValue (reserveBufferCount settings)
    count = toInteger . positiveIntValue
    inventory bufferCount = do
      -- Check the full byte count before narrowing for the allocator.
      budget <- maybe (ioError (userError "buffer allocation exceeds Int capacity")) pure (mkPositiveInt (bufferCount * count (bufferSize settings)))
      newBuffers budget (bufferSize settings)
