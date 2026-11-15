{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}

-- | A pool of connections, as slots that a request checks out, one connection each, and that a connection fills
-- when a request first has the slot. The pool is streaming-io's, which serves any items that are loaned: what a
-- request waits for is a slot, in the order that requests came, and its deadline is the pool's checkout timeout or
-- the arrow's own. What happens to each connection is told to the pool's observer, if it has one.
--
-- A connection is closed when it is older than the pool's lifetime, and when nobody has used it for the pool's idle time, by
-- a thread of the pool's own, which looks at the connections that are free, a few times in the shortest of the two, and
-- never at one that a request has. It is the one thread that waits in IO, to sleep between its looks: a request waits only
-- through the arrow that runs it.
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

import Control.Arrow (arr, (***), (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (ThreadId, forkIOWithUnmask, killThread, threadDelay)
import Control.Exception (ErrorCall (..), SomeException, bracket, finally, mask_, throwIO, toException, try)
import Control.Monad (forever, replicateM, void, when)
import System.IO (fixIO)
import Data.Tuple (swap)
import Data.Foldable (traverse_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (catMaybes, fromMaybe, isJust)
import qualified Data.Text as Text
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import Database.PostgreSQL.Connection (ConnectSettings (connectReadSize), connect)
import Database.PostgreSQL.Connection.Internal (Connection (..), Session (..), Status (..), breakSession, leased)
import Database.PostgreSQL.Errors (ConnectFailure)
import Database.PostgreSQL.Internal.Carrier (isQuiet)
import Database.PostgreSQL.Pool.Observation
import Network.Streaming.BufferPool (BufferPool, BufferPoolSettings (..), newBufferPool)
import Data.Word (Word64)
import Network.Streaming.Classes (Duration (..), Effect (..), Exit (..), GiveWay (..), Runtime, Scope (..), Within (..), giving, monotonicNanoseconds, taking)
import Network.Streaming.Combinators (alongside, done, step, tap)
import Network.Streaming.Loans (withRequestBuffers)
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.Pool (Pool, PoolError (..), closePoolWith, isClosed, newPool, queueAt, releaseNow, takeNow, urgently)
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
    -- | Told of each connection's events, for logs and metrics. A pool that has none builds no events.
    poolObserver :: Maybe (Observation -> IO ())
  }

-- | A pool of this size that makes its connections as the settings say, and has
-- no deadlines, and observes nothing.
connectionPoolSettings :: Int -> ConnectSettings -> ConnectionPoolSettings
connectionPoolSettings size settings = ConnectionPoolSettings size settings Nothing Nothing Nothing Nothing

-- | A pool of connections: its slots, which are empty until a request first has them, and the buffers that its
-- connections are set up with.
data ConnectionPool = ConnectionPool
  { poolSlots :: !(Pool Slot),
    poolBuffers :: !BufferPool,
    poolSettings :: !ConnectionPoolSettings,
    -- | The count of the looks that the pool's thread has made at its connections, which a request that gives a connection
    -- back stamps it with, if the pool has an idle time.
    poolEpoch :: !(IORef Int)
  }

-- | A place for a connection, which is empty until it is made, and empty again once it is broken, and the identifier that the
-- observer knows the connection by. It knows its pool, so that a request that has it needs nothing else of the pool.
data Slot = Slot
  { slotOwner :: ConnectionPool,
    slotConnection :: !(IORef (Maybe Connection)),
    slotIdentifier :: !(IORef UUID),
    -- | When the connection was made, on the monotonic clock, in nanoseconds.
    slotOpened :: !(IORef Word64),
    -- | The epoch of the pool when the connection was last given back.
    slotUsed :: !(IORef Int)
  }

-- | Tells the observer of the event, if the pool has one.
observe :: ConnectionPoolSettings -> Observation -> IO ()
observe settings event = maybe (pure ()) ($ event) (poolObserver settings)

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
  -- The slots know the pool that they are of, so the pool is made by the knot: nothing looks at it until it is made.
  connectionPool <-
    fixIO $ \self -> do
      slots <- replicateM size (Slot self <$> newIORef Nothing <*> newIORef UUID.nil <*> newIORef 0 <*> newIORef 0)
      ConnectionPool <$> newPool slots <*> pure buffers <*> pure settings <*> newIORef 0
  let pool = poolSlots connectionPool
  bracket
    (maintaining connectionPool)
    (\maintenance -> traverse_ killThread maintenance *> closePoolWith (closeSlot settings ReleaseConnectionTerminationReason "the pool is closed") pool)
    (const (use connectionPool))
  where
    size = max 1 (poolSize settings)
    positive :: Int -> PositiveInt
    positive number = fromMaybe (positiveInt @1) (mkPositiveInt (toInteger number))

-- | Closes the connection that the slot holds, if it has one, for the reason, which is then told to the observer. The slot is
-- empty.
closeSlot :: ConnectionPoolSettings -> ConnectionTerminationReason -> String -> Slot -> IO ()
closeSlot settings reason because slot =
  readIORef (slotConnection slot)
    >>= traverse_
      ( \connection -> do
          breakSession (connectionSession connection) because
          writeIORef (slotConnection slot) Nothing
          identifier <- readIORef (slotIdentifier slot)
          observe settings (ConnectionObservation identifier (TerminatedConnectionStatus reason))
      )

-- | The thread that closes the connections that are free when they are old or idle, if the pool has a lifetime or an idle time.
-- It sleeps between its looks, and a look is not interrupted: it is masked, so that the slots it took are given back.
maintaining :: ConnectionPool -> IO (Maybe ThreadId)
maintaining pool = case maintenancePeriod (poolSettings pool) of
  Nothing -> pure Nothing
  Just period -> Just <$> mask_ (forkIOWithUnmask (\unmask -> forever (unmask (threadDelay period) *> (void (try @SomeException (look pool))))))

-- | How often the pool looks at its connections, in microseconds: four times in the shortest of the lifetime and the idle
-- time, and not more often than every 10 milliseconds, or less often than every second.
maintenancePeriod :: ConnectionPoolSettings -> Maybe Int
maintenancePeriod settings = case catMaybes [poolLifetime settings, poolIdleTime settings] of
  [] -> Nothing
  durations -> Just (max 10000 (min 1000000 (fromIntegral (minimum (map nanoseconds durations) `div` 4000))))

nanoseconds :: Duration -> Word64
nanoseconds (Duration count) = count

-- | One look at the connections that are free: those that are older than the lifetime, or that were given back more than the
-- idle time ago, are closed. A slot is taken from the pool for the look, and is the looker's alone while it is, so that no
-- request has the connection that is closed; they are given back in the order that they were taken in, so that the pool lends
-- them as it did.
look :: ConnectionPool -> IO ()
look pool = do
  now' <- monotonicNanoseconds
  epoch <- atomicModifyIORef' (poolEpoch pool) (\count -> (count + 1, count + 1))
  taken <- takeAll []
  traverse_ (expire now' epoch) taken `finally` traverse_ (\slot -> releaseNow (poolSlots pool) [slot]) (reverse taken)
  where
    settings = poolSettings pool
    takeAll taken = takeNow (poolSlots pool) 1 >>= maybe (pure (reverse taken)) (\slot -> takeAll (slot : taken))
    expire now' epoch slot = do
      opened <- readIORef (slotOpened slot)
      used <- readIORef (slotUsed slot)
      case () of
        _
          | maybe False (\lifetime -> now' - opened > nanoseconds lifetime) (poolLifetime settings) ->
              closeSlot settings AgingConnectionTerminationReason "the connection reached the pool's lifetime" slot
          | maybe False (\idle -> epoch - used > idleEpochs idle) (poolIdleTime settings) ->
              closeSlot settings IdlenessConnectionTerminationReason "the connection was idle for the pool's idle time" slot
          | otherwise -> pure ()
    -- How many looks make the idle time, rounded up.
    idleEpochs idle = maybe 0 (\period -> fromIntegral ((nanoseconds idle + period * 1000 - 1) `div` (period * 1000))) (fromIntegral <$> maintenancePeriod settings)

-- | The body, with a connection of the pool that is the arrow's input, for as long as it runs, and with the input of the
-- body after the pool: one program, built once, serves the pool of each run, as a tenant's pool is. The connection is lent as
-- a pool lends its items, and the request is a holder that is served first ('urgently'). The checkout waits inside the
-- deadline of the arrow's 'Network.Streaming.Classes.within', and the pool's own checkout timeout, if it has one. A slot that
-- is empty is filled by making a connection, with the pool's own connect settings, which fails with
-- 'Database.PostgreSQL.Errors.ConnectFailure' if it can't be made. The connection
-- is the body's alone: used after the body ended, it fails with
-- 'Database.PostgreSQL.Errors.ConnectionReleased'. One that the body broke, as a
-- cancelled statement does, is closed as the body ends, and its slot is empty.
withConnection :: (Runtime arr) => arr (Connection, a) b -> arr (ConnectionPool, a) (Either PoolError b)
withConnection body = loaned (urgently (using body))

-- | The scope around the checkout of a slot, which is the arrow that gives the slot or says why not, and the release of what it
-- gave: it goes back to the pool that it came from. A slot that is free is taken at once, and the pool's being closed is
-- looked at only if there is none: a closed pool keeps none free.
loaned :: (Runtime arr) => arr (Slot, a) b -> arr (ConnectionPool, a) (Either PoolError b)
loaned body = scope acquire returning run
  where
    acquire =
      giveWay
        >>> step "claim from free items" (\(pool, _) -> maybe (Left pool) Right <$> takeNow (poolSlots pool) 1) `giving` "item free?"
        >>> (taking "no" waiting ||| taking "yes" (arr Right))
    returning = fst ^>> (done ||| step "return item" (\slot -> releaseNow (poolSlots (slotOwner slot)) [slot]))
    run = withInput ^>> (arr Left ||| (body >>^ Right))
    withInput (acquired, (_, input)) = fmap (,input) acquired

-- | The request has to wait for a slot: until the pool's checkout timeout, if it has one, with its place given up if the wait
-- ends any other way. A pool that is closed has none to give.
waiting :: (Runtime arr) => arr ConnectionPool (Either PoolError Slot)
waiting = now (\pool -> (\closed -> if closed then Left PoolClosed else Right pool) <$> isClosed (poolSlots pool)) >>> (arr Left ||| queueing)
  where
    queueing = limited ^>> ((queueAt >>^ Right) ||| (withinIn queueAt >>^ maybe (Left CheckoutTimedOut) Right))
    limited pool = case poolCheckoutTimeout (poolSettings pool) of
      Nothing -> Left (1, poolSlots pool)
      Just limit -> Right (limit, (1, poolSlots pool))

-- | The body on the connection of the slot: the one that the slot holds if it is as it was left, and else one that is
-- made. What the body leaves of the connection stays in the slot, unless the body broke it. The observer is told that the
-- connection is in use, and what became of it, if the pool has one.
using :: forall arr a b. (Runtime arr) => arr (Connection, a) b -> arr (Slot, a) b
using body = scope acquire release run
  where
    acquire =
      now (\(slot, _) -> either (const (Left slot)) (Right . (slot,)) <$> existing @(ModeOf arr) (poolSettings (slotOwner slot)) slot)
        >>> (making ||| arr id)
        >>> now lending
    -- A connection is made under the arrow of the request, with the settings and the buffers of the pool, and the observer is
    -- told that it is being made, and that it is ready, or that it could not be made.
    making =
      tap (now (\slot -> UUID.nextRandom >>= \identifier -> writeIORef (slotIdentifier slot) identifier *> observeNow slot ConnectingConnectionStatus))
        >>> alongside ((\slot -> (poolBuffers (slotOwner slot), poolConnect (poolSettings (slotOwner slot)))) ^>> withRequestBuffers (swap ^>> connect))
        >>> now (uncurry made)
    made :: Either ConnectFailure Connection -> Slot -> IO (Slot, Connection)
    made result slot = case result of
      Left failure -> observeNow slot (TerminatedConnectionStatus (NetworkErrorConnectionTerminationReason (Just (Text.pack (show failure))))) *> throwIO failure
      Right connection -> do
        writeIORef (slotConnection slot) (Just connection)
        monotonicNanoseconds >>= writeIORef (slotOpened slot)
        readIORef (poolEpoch (slotOwner slot)) >>= writeIORef (slotUsed slot)
        observeNow slot (ReadyForUseConnectionStatus EstablishedConnectionReadyForUseReason)
        pure (slot, connection)
    lending (slot, connection) = do
      lent <- leased connection
      observeNow slot InUseConnectionStatus
      pure (slot, lent)
    -- A connection that is given back is stamped with the epoch of the pool, if the pool has an idle time to tell it by, and
    -- the observer is told what became of it, if the pool has one.
    release = now $ \((slot, connection), exit) -> do
      let pool = slotOwner slot
      when (isJust (poolIdleTime (poolSettings pool))) (readIORef (poolEpoch pool) >>= writeIORef (slotUsed slot))
      ended <- giveBack slot connection
      case poolObserver (poolSettings pool) of
        Nothing -> pure ()
        Just _ -> observeNow slot (afterUse exit ended)
    run = (snd *** snd) ^>> body

-- | Tells the observer of the event of the slot's connection, if the pool has one: the event is not built if it has none.
observeNow :: Slot -> ConnectionStatus -> IO ()
observeNow slot status = case poolObserver (poolSettings (slotOwner slot)) of
  Nothing -> pure ()
  Just tell -> readIORef (slotIdentifier slot) >>= \identifier -> tell (ConnectionObservation identifier status)

-- | What became of a connection when its request ended: it is idle in its slot again, or it was closed, and why.
data Ended = Kept | Closed String

-- | Gives the slot's connection back after its use: it stays in the slot if it is idle, and else it is closed, and the slot is
-- empty.
giveBack :: Slot -> Connection -> IO Ended
giveBack slot connection = do
  writeIORef (connectionLease connection) False
  status <- readIORef (sessionStatus (connectionSession connection))
  case status of
    Idle -> pure Kept
    Busy -> do
      let reason = "the connection was still in use when its request ended"
      breakSession (connectionSession connection) reason
      writeIORef (slotConnection slot) Nothing
      pure (Closed reason)
    Broken reason -> Closed reason <$ writeIORef (slotConnection slot) Nothing

-- | What the observer is told when a request has ended: its connection is ready again, as the use ended, or it was closed.
afterUse :: Exit -> Ended -> ConnectionStatus
afterUse exit = \case
  Kept -> ReadyForUseConnectionStatus $ case exit of
    Done -> SessionSucceededConnectionReadyForUseReason
    Failed failure -> SessionFailedConnectionReadyForUseReason failure
    Cancelled' -> SessionFailedConnectionReadyForUseReason (toException (ErrorCall "the request was cancelled"))
  Closed reason -> TerminatedConnectionStatus (NetworkErrorConnectionTerminationReason (Just (Text.pack reason)))

-- | The connection that the slot holds, if it is as it was left: idle, with nothing from the server to read, which is
-- so until the server ends it. One that isn't is closed, and dropped, and the observer is told.
existing :: forall m. (Instrumented m) => ConnectionPoolSettings -> Slot -> IO (Either () Connection)
existing settings slot =
  readIORef (slotConnection slot) >>= \case
    Nothing -> pure (Left ())
    Just connection -> do
      status <- readIORef (sessionStatus (connectionSession connection))
      quiet <- if status == Idle then isQuiet @m (sessionCarrier (connectionSession connection)) else pure False
      if quiet
        then pure (Right connection)
        else do
          let reason = "the connection was not as it was left"
          breakSession (connectionSession connection) reason
          writeIORef (slotConnection slot) Nothing
          readIORef (slotIdentifier slot) >>= \identifier -> observe settings (ConnectionObservation identifier (TerminatedConnectionStatus (NetworkErrorConnectionTerminationReason (Just (Text.pack reason)))))
          pure (Left ())
