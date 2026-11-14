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

import Data.IORef (IORef)
import Network.Fulmen.Internal.Body (Body (..))
import Network.Fulmen.Internal.Date (Date)
import Network.Fulmen.Internal.Response (Exchange)
import Network.Socket (Socket)
import Network.Streaming.BufferPool (BufferPool)
import Network.Streaming.Classes (Duration, Giver, Reach (..))
import Network.Streaming.Loans (Loans)
import Network.Streaming.Pool (Pool)

-- | Where the server accepts, how many connections it serves, and its limits and
-- timeouts.
data Settings = Settings
  { -- | A socket that is bound and listening, which the acceptor accepts on. Past
    -- the connection cap, connections wait in its backlog.
    listener :: Socket,
    -- | The pool of the buffers that a request in flight reads and writes in.
    buffers :: BufferPool,
    -- | How many workers each core has at startup, which is how many connections
    -- it serves at once: its share of the connection cap.
    connectionsPerCore :: Int,
    -- | The bytes that a request line or a header line may take, as well as a
    -- chunk-size line and a trailer line: the size of the line buffer. A longer
    -- request line is answered with 414, and a longer header line with 431.
    lineLimit :: Int,
    -- | The bytes that the whole head, the request line and the header fields,
    -- may take. A longer head is answered with 431.
    headLimit :: Int,
    -- | The bytes that a request body may have, if limited. A longer body is
    -- answered with 413.
    bodyLimit :: Maybe Int,
    -- | How long a client may be silent in the middle of a head or a body before
    -- the server closes the connection.
    clientTimeout :: Maybe Duration
  }

-- | One request in flight: its loans, its body and its answer.
data Call = Call !Loans !Body !Exchange

-- | A response whose head has been sent, or is held for the first of its body.
newtype Answering = Answering Exchange

-- | The cores that a server runs on, and what its parts share across them. A value is one core's: the core it is for
-- is the one that runs the acceptor, if it is the first.
data Cores = Cores !Int !Shared

-- | What the cores share: the free workers, which the acceptor hands connections to, and the date that responses
-- carry.
data Shared = Shared
  { sharedWorkers :: !(Pool Free),
    sharedDate :: !(IORef Date)
  }

-- | A free worker: the core that it is on, and the giver of the ticket that it waits at for a connection.
data Free = Free
  { freeCore :: !Int,
    freeGiver :: !(Giver 'Anywhere Socket)
  }
