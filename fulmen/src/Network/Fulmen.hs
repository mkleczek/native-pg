-- | An HTTP/1.1 server that is a fixed network of parts, made at startup, per core:
-- each core has as many workers as its share of the connection cap, and an acceptor
-- hands the connections that it accepts to free workers as remote tickets. A worker
-- serves its connection with a request direction, which parses the head and hands
-- the body on piece by piece, and an answer direction, which runs the handler; a
-- connection that waits for its next request holds no buffer.
--
-- The handler is a program for any arrow that has what 'Runtime' bundles. It
-- sends its own answer, so a request is one lexical scope: whatever the handler
-- acquires, the loans of the call included, is released when it returns, after
-- 'finish'.
--
-- Serving is a stub, which fails with 'Network.Streaming.NotImplemented.NotImplemented'
-- until the server is there.
module Network.Fulmen
  ( -- * Serving
    Settings (..),
    defaultSettings,
    Cores,
    newCores,
    serve,

    -- * A call
    Call,
    loans,
    module Network.Fulmen.Request,
    module Network.Fulmen.Response,
  )
where

import Control.Arrow (Arrow, arr)
import Network.Fulmen.Internal (Call (..), Cores (..))
import Network.Fulmen.Request
import Network.Fulmen.Response
import Network.Socket (Socket)
import Network.Streaming.BufferPool (BufferPool)
import Network.Streaming.Classes (Duration, Runtime, secs)
import Network.Streaming.Loans (Loans)
import Network.Streaming.NotImplemented (notImplemented)

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

-- | A server of one worker for each core, with a line limit of 16 KiB, a head limit
-- of 64 KiB, no limit on bodies, and a silent client given 30 seconds.
defaultSettings :: Socket -> BufferPool -> Settings
defaultSettings socket pool = Settings socket pool 1 16384 65536 Nothing (Just (secs 30))

-- | The cores that a server runs on, from how many there are.
newCores :: Int -> IO Cores
newCores = pure . Cores

-- | The server: its workers and its acceptor, made once, for each core. The handler
-- is given each request, with its call, once its head is parsed.
serve :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
serve _ _ = notImplemented "serve"

-- | The loans of the call, which native-pg takes, so that the replies of the
-- database and the response share a buffer.
loans :: (Arrow arr) => arr Call Loans
loans = arr (\(Call callLoans _) -> callLoans)
