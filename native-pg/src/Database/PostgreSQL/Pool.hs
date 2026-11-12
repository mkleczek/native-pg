-- | A pool of connections. Making one is a stub, which fails with
-- 'Network.Streaming.NotImplemented.NotImplemented' until the code is there.
module Database.PostgreSQL.Pool
  ( ConnectionPoolSettings (..),
    connectionPoolSettings,
    newConnectionPool,
    withConnection,
    Observation (..),
    ConnectionStatus (..),
    ConnectionReadyForUseReason (..),
    ConnectionTerminationReason (..),
    PoolError (..),
  )
where

import Database.PostgreSQL.Connection (ConnectSettings, Connection)
import Database.PostgreSQL.Pool.Observation
import Network.Streaming.Classes (Duration, Runtime)
import Network.Streaming.NotImplemented (notImplementedIO)
import Network.Streaming.Pool (Pool, PoolError (..), urgently, withLoan)

data ConnectionPoolSettings = ConnectionPoolSettings
  { -- | How many connections the pool has at most.
    poolSize :: Int,
    -- | How its connections are made.
    poolConnect :: ConnectSettings,
    -- | How long a checkout waits for a free connection before it gives
    -- 'CheckoutTimedOut'.
    poolCheckoutTimeout :: Maybe Duration,
    -- | The age at which a connection is closed, and the idle time at which it
    -- is.
    poolLifetime :: Maybe Duration,
    poolIdleTime :: Maybe Duration,
    -- | Told of each connection's events, for logs and metrics.
    poolObserver :: Observation -> IO ()
  }

-- | A pool of this size that makes its connections as the settings say, and has
-- no deadlines, and observes nothing.
connectionPoolSettings :: Int -> ConnectSettings -> ConnectionPoolSettings
connectionPoolSettings size settings = ConnectionPoolSettings size settings Nothing Nothing Nothing (const (pure ()))

-- | The pool, which keeps its connections and maintains them (lifetime, idle
-- time) in parts of its own.
newConnectionPool :: ConnectionPoolSettings -> IO (Pool Connection)
newConnectionPool _ = notImplementedIO "newConnectionPool"

-- | The body, with a connection of the pool for as long as it runs, as 'withLoan'
-- does, and as a holder that is served first ('urgently'). The checkout waits
-- inside the deadline of the arrow's 'Network.Streaming.Classes.within'.
withConnection :: (Runtime arr) => Pool Connection -> arr (Connection, a) b -> arr a (Either PoolError b)
withConnection pool body = withLoan pool (urgently body)
