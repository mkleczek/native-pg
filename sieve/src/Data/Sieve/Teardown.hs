{-# LANGUAGE DataKinds #-}

-- | The teardown of values that are evicted from a cache while requests may still use them: a queue of the values, which the
-- eviction listener fills without waiting, and a part of the program that retires each value's lease, and then runs the
-- owner's teardown arrow on it.
module Data.Sieve.Teardown
  ( Graveyard,
    newGraveyard,
    newSharedGraveyard,
    bury,
    outstanding,
    teardown,
  )
where

import Data.Sieve (Reaching, Sharing (..))
import Data.Sieve.Lease (Lease, Retirement)
import Data.Kind (Type)
import Network.Streaming.Classes
import Network.Streaming.NotImplemented (notImplemented, notImplementedIO)

-- | The values that wait to be torn down, for the cores of the sharing.
data Graveyard (c :: Sharing) (v :: Type)

-- | A graveyard for the parts of one core, which switch only at waits. Not yet.
newGraveyard :: IO (Graveyard 'OneCore v)
newGraveyard = notImplementedIO "newGraveyard"

-- | A graveyard that the parts of every core share. Not yet.
newSharedGraveyard :: IO (Graveyard 'AllCores v)
newSharedGraveyard = notImplementedIO "newSharedGraveyard"

-- | Queues the value to be torn down, and wakes the teardown part if it waits for one: it is what an eviction listener does.
-- Never waits. Not yet.
bury :: Graveyard c v -> v -> IO ()
bury _ _ = notImplementedIO "bury"

-- | How many values were buried and are not torn down yet, the one that the part works on included.
outstanding :: Graveyard c v -> IO Int
outstanding _ = notImplementedIO "outstanding"

-- | The part that tears the values down, one after the other and for ever: it retires the lease of the value, within the
-- deadline, and then runs the owner's arrow on the value and how the retirement ended, unless the lease was retired before,
-- when whoever retired it tears the value down. Not yet.
teardown :: (Runtime arr, Reaching c arr) => (v -> Lease c) -> Duration -> arr (v, Retirement) () -> arr (Graveyard c v) ()
teardown _ _ _ = notImplemented "teardown"
