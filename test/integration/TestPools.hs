-- | The bracket that the tests use for a pool, which native-pg doesn't have: its pools are values that are made and closed
-- without waiting, and its maintenance is an arrow that a program runs.
module TestPools
  ( withPool,
  )
where

import Control.Concurrent (forkIO, killThread)
import Control.Exception (SomeException, bracket, try)
import Control.Monad (void)
import Control.Arrow ((^>>))
import Database.PostgreSQL.Pool (ConnectionPool, ConnectionPoolSettings, closeConnectionPool, maintain, newConnectionPool, newMaintenance)
import Network.Streaming.BufferPool (BufferPool)
import TestPrograms (Runner, runOn)

-- | The use, with a pool of the settings and a maintenance that a thread runs under the runner's arrow, over the buffers: the
-- pool is closed when the use ends, however it ends, and the maintenance stops.
withPool :: Runner -> BufferPool -> ConnectionPoolSettings -> (ConnectionPool -> IO a) -> IO a
withPool runner buffers settings use = do
  maintenance <- newMaintenance
  bracket (newConnectionPool maintenance settings) closeConnectionPool $ \pool ->
    bracket (forkIO (void (try @SomeException (runOn runner buffers (const maintenance ^>> maintain))))) killThread (const (use pool))
