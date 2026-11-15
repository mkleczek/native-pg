{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}

-- | A pool of connections, as slots that a request checks out, one connection each, and that a connection fills
-- when a request first has the slot. The pool is streaming-io's, which serves any items that are loaned: what a
-- request waits for is a slot, in the order that requests came, and its deadline is the pool's checkout timeout or
-- the arrow's own. The maintenance of connections by their age and their idleness, and the observer, are to come.
module Database.PostgreSQL.Pool
  ( ConnectionPoolSettings (..),
    connectionPoolSettings,
    ConnectionPool,
    withConnectionPool,
    withConnection,
    Observation (..),
    ConnectionStatus (..),
    ConnectionReadyForUseReason (..),
    ConnectionTerminationReason (..),
    PoolError (..),
  )
where

import Control.Arrow (arr, second, (***), (>>>), (>>^), (^>>), (|||))
import Control.Exception (finally, throwIO)
import Control.Monad (replicateM)
import Data.Foldable (traverse_)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe)
import Data.Tuple (swap)
import Database.PostgreSQL.Connection (ConnectSettings (connectReadSize), connect)
import Database.PostgreSQL.Connection.Internal (Connection (..), Session (..), Status (..), breakSession, leased)
import Database.PostgreSQL.Internal.Carrier (isQuiet)
import Database.PostgreSQL.Pool.Observation
import Network.Streaming.BufferPool (BufferPool, BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes (Duration, Effect (..), Runtime, Scope (..))
import Network.Streaming.Combinators (keeping, tap)
import Network.Streaming.Loans (withRequestBuffers)
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.Pool (Pool, PoolError (..), closePoolWith, newPool, urgently, withLoan, withLoanWithin)
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt, positiveInt)

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

-- | A pool of connections: its slots, which are empty until a request first has them, and the buffers that its
-- connections are set up with.
data ConnectionPool = ConnectionPool
  { poolSlots :: !(Pool Slot),
    poolBuffers :: !BufferPool,
    poolSettings :: !ConnectionPoolSettings
  }

-- | A place for a connection, which is empty until it is made, and empty again once it is broken.
newtype Slot = Slot (IORef (Maybe Connection))

-- | The body, with a pool that keeps its connections. The pool is closed when the body ends, however it ends: the
-- connections that are free close, and each of the others closes as the request that has it gives it back, and a checkout
-- after that is 'PoolClosed'. A request that waits for a connection when the pool closes is served by one that comes
-- back, and its connection closes when it gives it back.
--
-- A connection is made under the arrow of the request that first has its slot, and is set up with the buffers of a pool
-- of the pool's own, which needn't be the request's: for each slot an ordinary buffer, so that every connection can be
-- made at once, and a setup buffer, and one buffer that is kept in reserve, as a request's pool does. They are the size
-- of a connection's read buffer, 16 KiB with the usual settings, so a pool of 10 connections has 21 of them, which is
-- 336 KiB.
withConnectionPool :: ConnectionPoolSettings -> (ConnectionPool -> IO a) -> IO a
withConnectionPool settings use = do
  buffers <-
    newBufferPool
      BufferPoolSettings
        { bufferSize = positive (connectReadSize (poolConnect settings)),
          ordinaryBufferCount = positive size,
          reserveBufferCount = positive 1,
          setupBufferCount = positive size
        }
  slots <- replicateM size (Slot <$> newIORef Nothing)
  pool <- newPool slots
  use (ConnectionPool pool buffers settings) `finally` closePoolWith closeSlot pool
  where
    size = max 1 (poolSize settings)
    positive :: Int -> PositiveInt
    positive number = fromMaybe (positiveInt @1) (mkPositiveInt (toInteger number))
    closeSlot (Slot ref) = readIORef ref >>= traverse_ (\connection -> breakSession (connectionSession connection) "the pool is closed")

-- | The body, with a connection of the pool for as long as it runs, as 'withLoan'
-- does, and as a holder that is served first ('urgently'). The checkout waits
-- inside the deadline of the arrow's 'Network.Streaming.Classes.within', and
-- the pool's own checkout timeout, if it has one. A slot that is empty is
-- filled by making a connection, which fails with
-- 'Database.PostgreSQL.Errors.ConnectFailure' if it can't be made. The connection
-- is the body's alone: used after the body ended, it fails with
-- 'Database.PostgreSQL.Errors.ConnectionReleased'. One that the body broke, as a
-- cancelled statement does, is closed as the body ends, and its slot is empty.
withConnection :: (Runtime arr) => ConnectionPool -> arr (Connection, a) b -> arr a (Either PoolError b)
withConnection pool body = checkingOut (poolSlots pool) (urgently (using pool body))
  where
    checkingOut = maybe withLoan withLoanWithin (poolCheckoutTimeout (poolSettings pool))

-- | The body on the connection of the slot: the one that the slot holds if it is as it was left, and else one that is
-- made. What the body leaves of the connection stays in the slot, unless the body broke it.
using :: forall arr a b. (Runtime arr) => ConnectionPool -> arr (Connection, a) b -> arr (Slot, a) b
using pool body = scope acquire release run
  where
    acquire =
      now (\(Slot ref, _) -> either (const (Left ref)) (Right . (ref,)) <$> existing @(ModeOf arr) ref)
        >>> (making ||| arr id)
        >>> second (now leased)
    making =
      keeping ((poolBuffers pool,) ^>> withRequestBuffers (fst ^>> connect (poolConnect (poolSettings pool))) >>> now (either throwIO pure))
        >>> tap (now (\(connection, ref) -> writeIORef ref (Just connection)))
        >>^ swap
    release = now $ \((ref, connection), _) -> do
      writeIORef (connectionLease connection) False
      status <- readIORef (sessionStatus (connectionSession connection))
      case status of
        Idle -> pure ()
        Busy -> breakSession (connectionSession connection) "the connection was still in use when its request ended" *> writeIORef ref Nothing
        Broken _ -> writeIORef ref Nothing
    run = (snd *** snd) ^>> body

-- | The connection that the slot holds, if it is as it was left: idle, with nothing from the server to read, which is
-- so until the server ends it. One that isn't is closed, and dropped.
existing :: forall m. (Instrumented m) => IORef (Maybe Connection) -> IO (Either () Connection)
existing ref =
  readIORef ref >>= \case
    Nothing -> pure (Left ())
    Just connection -> do
      status <- readIORef (sessionStatus (connectionSession connection))
      quiet <- if status == Idle then isQuiet @m (sessionCarrier (connectionSession connection)) else pure False
      if quiet
        then pure (Right connection)
        else do
          breakSession (connectionSession connection) "the connection was not as it was left"
          writeIORef ref Nothing
          pure (Left ())
