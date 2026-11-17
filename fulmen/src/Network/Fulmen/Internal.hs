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
    Waiting (..),
    Count (..),
  )
where

import Data.IORef (IORef)
import Data.IntMap.Strict (IntMap)
import Network.Fulmen.Internal.Body (Body (..))
import Network.Fulmen.Internal.Date (Pulse)
import Network.Fulmen.Internal.Response (Exchange)
import Network.Socket (Socket)
import Network.Streaming.BufferPool (BufferPool, BufferPoolSettings)
import Network.Streaming.Classes (Duration, Event, Promise, Reach (..))
import Network.Streaming.Loans (Loans)
import Network.Streaming.Pool (Pool)

-- | How the server is configured: the size of the pool of request buffers, how many connections it serves, and its
-- limits and timeouts.
data Settings = Settings
  { -- | The size of the pool of the buffers that a request in flight reads and writes in, which
    -- 'Network.Fulmen.newCores' makes once for all the cores.
    bufferPool :: BufferPoolSettings,
    -- | How many connection workers each core runs at most, which is how many connections
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
    -- | How long a connection may wait for its next request, from when it is accepted or when it has served one, before the server
    -- closes it and its connection worker ends, so that its core has room for another connection and one that waits in the listen
    -- backlog is served. It is the wait for the first byte of a request, so that a client that is gone without closing the
    -- connection, or that connects and says nothing, doesn't hold a connection worker for ever; a connection that is serving a
    -- request, the time its handler takes included, is not timed. 'Nothing' waits for ever. A connection that has an idle timeout has a watcher, a part of its own that sleeps until the
    -- connection can have waited that long and looks at it then, so that the timeout costs a request a clock read and two changes of a mark,
    -- a few hundred bytes, and a connection a timer for each timeout that it lasts, and not a deadline for each wait for a request.
    idleTimeout :: Maybe Duration,
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

-- | What the cores share: the pool of the cores that wait for connections, which the acceptor gives batches of connections to,
-- how many connection workers each core has, the date that responses carry and whether the server is shutting down, and
-- the pools of the buffers that requests in flight work in, of which a value of one core has its own.
data Shared = Shared
  { sharedWaiting :: !(Pool Waiting),
    -- | The count of each core, by its number.
    sharedCounts :: !(IntMap (IORef Count)),
    sharedPulse :: !Pulse,
    -- | The pool of request buffers of the core that the value is for: a request is admitted from its own core's pool, so that
    -- the buffer that a request gives back goes to a request of the same core, and not to one that waits on another core while
    -- this core has none to run.
    sharedBuffers :: !BufferPool,
    -- | The pools of request buffers of all the cores, by the number of the core.
    sharedCorePools :: !(IntMap BufferPool)
  }

-- | A core that waits for connections: its number, and the promise that the acceptor gives the sockets of its batch of
-- connections with.
data Waiting = Waiting
  { waitingCore :: !Int,
    waitingPromise :: !(Promise 'Anywhere [Socket])
  }

-- | What a core has of its connection workers: how many are alive, and the promise of the part that waits for room, if
-- it does, which the end of a worker fulfils.
data Count = Count !Int !(Maybe (Promise 'Anywhere ()))
