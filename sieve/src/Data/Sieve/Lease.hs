{-# LANGUAGE DataKinds #-}

-- | A lease on a value that must not be destroyed while a request uses it: a count of the requests in flight, and a flag that
-- says the value is retired, which no request enters after.
--
-- A request 'enter's before it uses the value and 'leave's when it is done, and neither waits. 'retire' marks the lease
-- retired, and waits for the requests that entered to leave, so that a teardown that follows it never meets a request that
-- uses the value, and no request enters a value after its teardown began: it is the guarantee of a count kept in the same
-- transaction that finds the value, without composing with the cache.
module Data.Sieve.Lease
  ( Lease,
    newLease,
    newSharedLease,
    enter,
    leave,
    Retirement (..),
    retire,
    inFlight,
    isRetired,
  )
where

import Data.Sieve (Reaching, Sharing (..))
import Network.Streaming.Classes
import Network.Streaming.NotImplemented (notImplemented, notImplementedIO)

-- | The lease of a value, for the cores of the sharing.
data Lease (c :: Sharing)

-- | A lease for the parts of one core, which switch only at waits. Not yet.
newLease :: IO (Lease 'OneCore)
newLease = notImplementedIO "newLease"

-- | A lease that the parts of every core share. Not yet.
newSharedLease :: IO (Lease 'AllCores)
newSharedLease = notImplementedIO "newSharedLease"

-- | Counts a request in flight, unless the lease is retired: then it is 'False', and nothing is counted. Never waits. Not yet.
enter :: Lease c -> IO Bool
enter _ = notImplementedIO "enter"

-- | A request in flight is done. The last one to leave a retired lease wakes the 'retire' that waits for it. Never waits.
-- Not yet.
leave :: Lease c -> IO ()
leave _ = notImplementedIO "leave"

-- | How a retirement ended.
data Retirement
  = -- | The requests in flight have left.
    Drained
  | -- | Some were still in flight at the deadline.
    TimedOut
  | -- | The lease was retired before: the part that retired it first does the waiting.
    AlreadyRetired
  deriving (Eq, Show)

-- | Marks the lease retired, so that no request enters after it, and waits for the requests that did to leave, until the
-- deadline. Not yet.
retire :: (Runtime arr, Reaching c arr) => Duration -> arr (Lease c) Retirement
retire _ = notImplemented "retire"

-- | How many requests are in flight.
inFlight :: Lease c -> IO Int
inFlight _ = notImplementedIO "inFlight"

-- | Whether the lease is retired.
isRetired :: Lease c -> IO Bool
isRetired _ = notImplementedIO "isRetired"
