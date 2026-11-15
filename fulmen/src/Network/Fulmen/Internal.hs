{-# LANGUAGE DataKinds #-}

-- | What a call is made of, which the server makes and the operations of
-- "Network.Fulmen.Request" and "Network.Fulmen.Response" take apart. The public
-- modules export these types without their constructors.
module Network.Fulmen.Internal
  ( Settings (..),
    Call (..),
    Body (..),
    Answering (..),
    Cores (..),
    Shared (..),
    Free (..),
  )
where

import Network.Fulmen.Internal.Body (Body (..))
import Network.Fulmen.Internal.Date (Pulse)
import Network.Fulmen.Internal.Response (Exchange)
import Network.Socket (Socket)
import Network.Streaming.BufferPool (BufferPool, BufferPoolSettings)
import Network.Streaming.Classes (Address, Duration, Event, Reach (..))
import Network.Streaming.Loans (Loans)
import Network.Streaming.Pool (Pool)

-- | How the server is configured: the size of the pool of request buffers, how many connections it serves, and its
-- limits and timeouts.
data Settings = Settings
  { -- | The size of the pool of the buffers that a request in flight reads and writes in, which
    -- 'Network.Fulmen.newCores' makes once for all the cores.
    bufferPool :: BufferPoolSettings,
    -- | How many workers each core has at startup, which is how many connections
    -- it serves at once: its share of the connection cap.
    connectionsPerCore :: Int,
    -- | The bytes that a request line or a header line may take. A longer request line is answered with 414, and a longer
    -- header line with 431. A line may be longer than a buffer: the head is read in as many fills of the buffer as it
    -- takes, up to its limit. A chunk-size line and a trailer line of a chunked body must fit a buffer as well.
    lineLimit :: Int,
    -- | The bytes that the whole head, the request line and the header fields,
    -- may take, however many buffers they come in. A longer head is answered with 431.
    headLimit :: Int,
    -- | The bytes that a request body may have, if limited. A longer body is
    -- answered with 413.
    bodyLimit :: Maybe Int,
    -- | How long a client may be silent in the middle of a head or a body before
    -- the server closes the connection.
    clientTimeout :: Maybe Duration,
    -- | The event that starts a graceful shutdown, if the server has one: when it fires, the listener closes, a connection
    -- that waits for its next request closes, a request in flight finishes (until the 'drainTimeout' has passed) and says that
    -- its connection ends, and the server returns when none is left.
    shutdown :: Maybe Event,
    -- | How long a request in flight has, once the shutdown has begun, to finish before it is cancelled: its handler's scopes are
    -- released, its transactions roll back and its connection closes. 'Nothing' waits for ever, as Warp does unless it is given a
    -- timeout. It counts from the start of the shutdown, and only a server that has a 'shutdown' event has a drain.
    drainTimeout :: Maybe Duration
  }

-- | One request in flight: its loans, its body and its answer.
data Call = Call !Loans !Body !Exchange

-- | A response whose head has been sent, or is held for the first of its body.
newtype Answering = Answering Exchange

-- | The cores that a server runs on, and what its parts share across them. A value is one core's: the core it is for
-- is the one that runs the acceptor, if it is the first.
data Cores = Cores !Int !Shared

-- | What the cores share: the free workers, which the acceptor hands connections to, the date that responses carry and
-- whether the server is shutting down, and the buffers that requests in flight work in.
data Shared = Shared
  { sharedWorkers :: !(Pool Free),
    sharedPulse :: !Pulse,
    sharedBuffers :: !BufferPool
  }

-- | A free worker: the core that it is on, and the address of the inbox that it collects its connections from.
data Free = Free
  { freeCore :: !Int,
    freeAddress :: !(Address "connections" 'Anywhere Socket)
  }
