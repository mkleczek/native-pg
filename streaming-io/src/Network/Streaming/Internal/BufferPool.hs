-- | What the buffer pool is made of, which "Network.Streaming.BufferPool" and "Network.Streaming.Loans" share. The
-- package doesn't expose this module: a program works with the pool's loans and has no use for its parts.
module Network.Streaming.Internal.BufferPool
  ( BufferPool (..),
  )
where

import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Pool (Pool)

-- | The buffers that requests in flight work in, and apart from them those that connections are set up with, so that
-- requests that wait for connections can't starve a setup of its buffers.
data BufferPool = BufferPool
  { -- | The ordinary buffers and the reserve, which only extra loans may take.
    requestBuffers :: !(Pool Buffer),
    -- | How many of the request buffers are the reserve.
    reserveCount :: !Int,
    -- | The buffers of the setup.
    setupBuffers :: !(Pool Buffer),
    -- | How many there are of them: the most a grant can be.
    setupCount :: !Int
  }
