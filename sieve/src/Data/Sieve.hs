{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | A SIEVE cache: a table of a fixed number of entries, replaced as SIEVE says, whose lookups take no lock.
--
-- A cache is for one core or for all cores, which its type says ('Sharing'). One that is for one core has no lock and no
-- atomic operation, because the steps of an arrow that never wait are atomic on a core whose parts switch only at waits. One
-- that is for all cores serialises its writes with one lock, held for a few hundred nanoseconds by a miss, and still takes none
-- for a hit.
--
-- A program looks a key up with 'cached', which runs a loader, an arrow fixed when the program is built, for a miss, so that
-- the callers that miss the same key share one load. The value that is found or loaded is checked by a hook of the caller's,
-- which can ask for it to be loaded again.
module Data.Sieve
  ( Sharing (..),
    ReachOf,
    Cooperating,
    Reaching,
    Key (..),
    Config (..),
    config,
    Cache,
    newCache,
    newSharedCache,
    Use (..),
    cached,
    peek,
    store,
    remove,
    flush,
    entries,
    resize,
    size,
  )
where

import Data.Kind (Type)
import GHC.TypeLits (ErrorMessage (..), TypeError)
import Network.Streaming.Classes
import Network.Streaming.NotImplemented (notImplemented, notImplementedIO)

-- | Which cores use a cache.
data Sharing
  = -- | The parts of one core, which switch only at waits.
    OneCore
  | -- | The parts of every core.
    AllCores

-- | The reach of the tickets that a cache's programs make, for an arrow: that of the arrow's own core for a cache of one core, and
-- any core's for a cache of all.
type family ReachOf (c :: Sharing) (arr :: Type -> Type -> Type) :: Reach where
  ReachOf 'OneCore arr = Local arr
  ReachOf 'AllCores arr = 'Anywhere

-- | What the parts of a core must do for a cache of one core to be used by them: switch only at waits. An arrow whose parts are
-- preempted, as the threads of 'Network.Streaming.Threads.Threads' are, can't: a step that never waits is atomic there only if
-- no other thread can run in it. A class, and not a type family of constraints, so that a program that has it as a given
-- doesn't have its inferred types made ambiguous.
class Cooperating (c :: Sharing) (r :: Reach)

instance Cooperating 'AllCores r

instance Cooperating 'OneCore ('Here s)

instance
  (TypeError ('Text "A cache of one core needs an arrow whose parts of a core switch only at waits, and the arrow of threads preempts them: use a shared cache")) =>
  Cooperating 'OneCore 'Anywhere

-- | What an arrow needs to run the programs of a cache of the sharing: the tickets of its reach, and parts that suit it.
type Reaching c arr = (Exchange arr (ReachOf c arr), Cooperating c (Local arr))

-- | A key: equality, and a hash that equal keys share.
class (Eq k) => Key k where
  hashKey :: k -> Int

instance Key Int where
  hashKey = id

-- | How a cache is made: its capacity, what it does with a key that it stores, and what it tells.
data Config k v = Config
  { -- | How many entries it holds, at least one.
    capacity :: Int,
    -- | Prepares a key for storage, for example by copying a view of a buffer. Equal keys stay equal, and hash the same. It is
    -- evaluated when a key is stored.
    prepareKey :: k -> k,
    -- | Told of each lookup of 'cached': whether it found the key. It never waits.
    requested :: Bool -> IO (),
    -- | Told of each entry that is evicted, flushed or dropped by a resize, and not of one that is removed. It never waits.
    evicted :: k -> v -> IO (),
    -- | Told of a miss of a key that is among the last evicted ones, as many as the capacity. It never waits.
    thrashed :: Maybe (IO ())
  }

-- | A config of this capacity that prepares no key and tells nothing.
config :: Int -> Config k v
config capacity = Config {capacity, prepareKey = id, requested = const (pure ()), evicted = \_ _ -> pure (), thrashed = Nothing}

-- | A cache of the sharing, of keys and values.
data Cache (c :: Sharing) (k :: Type) (v :: Type)

-- | A cache of one core, for programs whose parts switch only at waits. Not yet.
newCache :: (Key k) => Config k v -> IO (Cache 'OneCore k v)
newCache _ = notImplementedIO "newCache"

-- | A cache that the parts of every core share. Not yet.
newSharedCache :: (Key k) => Config k v -> IO (Cache 'AllCores k v)
newSharedCache _ = notImplementedIO "newSharedCache"

-- | What the hook of 'cached' says about a value: the caller's result, or that the value is to be loaded again.
data Use u = Use u | Reload

-- | The result of the hook for the value of a key, loading it if the cache has none. The loader runs as part of the caller's
-- own program, so it may wait, and the callers that miss the same key while it does wait for its value, and don't load it again.
-- If the loader fails or is cancelled, one of them loads it. Not yet.
cached :: (Runtime arr, Key k, Reaching c arr) => arr k v -> (Instant -> v -> IO (Use u)) -> arr (Cache c k v, k) u
cached _ _ = notImplemented "cached"

-- | The value of a key, if it is cached, which counts as its use. It neither runs a hook nor tells 'requested'. Not yet.
peek :: (Key k) => Cache c k v -> k -> IO (Maybe v)
peek _ _ = notImplementedIO "peek"

-- | Caches the value of a key, unless it is cached already, and says whether it did. It may evict an entry. Not yet.
store :: (Key k) => Cache c k v -> k -> v -> IO Bool
store _ _ _ = notImplementedIO "store"

-- | Removes a key, giving its value if it was cached. 'evicted' is not told: the caller has the value. Not yet.
remove :: (Key k) => Cache c k v -> k -> IO (Maybe v)
remove _ _ = notImplementedIO "remove"

-- | Removes every entry, telling 'evicted' of each, the oldest first. Not yet.
flush :: Cache c k v -> IO ()
flush _ = notImplementedIO "flush"

-- | The keys and values of the cache, the oldest first. Not yet.
entries :: Cache c k v -> IO [(k, v)]
entries _ = notImplementedIO "entries"

-- | Changes the capacity, to at least one. A cache that holds more than that evicts, as an insertion does, and tells 'evicted'
-- of each entry. Not yet.
resize :: Cache c k v -> Int -> IO ()
resize _ _ = notImplementedIO "resize"

-- | How many entries the cache holds. Not yet.
size :: Cache c k v -> IO Int
size _ = notImplementedIO "size"
