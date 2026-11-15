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

import Control.Concurrent (yield)
import Control.Exception (onException)
import Control.Monad (when)
import Data.Foldable (for_, traverse_)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef, atomicWriteIORef)
import Data.Kind (Type)
import Data.Sieve.Ghosts (Ghosts, addGhost, noGhosts, takeGhost)
import Data.Sieve.Memory (Ints, casInt, newInts, readInt, writeInt, writeIntAtomic)
import Data.Sieve.Table
import GHC.TypeLits (ErrorMessage (..), TypeError)
import Network.Streaming.Classes
import Network.Streaming.NotImplemented (notImplemented)

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
data Cache (c :: Sharing) (k :: Type) (v :: Type) = Cache
  { cacheShared :: !Bool,
    -- | The table that readers read, which a resize or a flush replaces by a new one.
    cacheTable :: !(IORef (Table k v)),
    -- | The lock of the writers of a shared cache, and the number that the next entry gets.
    cacheControl :: !Ints,
    -- | The keys evicted last, if the cache tells of thrashing.
    cacheGhosts :: !(Maybe (IORef Ghosts)),
    cacheConfig :: !(Config k v)
  }

-- | The word of 'cacheControl' that is the lock: 0 when it is free, and 1 when a writer holds it.
lockWord, serialWord :: Int
lockWord = 0
serialWord = 1

make :: Bool -> Config k v -> IO (Cache c k v)
make shared settings = do
  table <- newTable shared (capacity settings)
  Cache shared <$> newIORef table <*> newInts 2 0 <*> traverse (const (newIORef noGhosts)) (thrashed settings) <*> pure settings

-- | A cache of one core, for programs whose parts switch only at waits.
newCache :: (Key k) => Config k v -> IO (Cache 'OneCore k v)
newCache = make False

-- | A cache that the parts of every core share.
newSharedCache :: (Key k) => Config k v -> IO (Cache 'AllCores k v)
newSharedCache = make True

-- | What the hook of 'cached' says about a value: the caller's result, or that the value is to be loaded again.
data Use u = Use u | Reload

-- | The result of the hook for the value of a key, loading it if the cache has none. The loader runs as part of the caller's
-- own program, so it may wait, and the callers that miss the same key while it does wait for its value, and don't load it again.
-- If the loader fails or is cancelled, one of them loads it. Not yet.
cached :: (Runtime arr, Key k, Reaching c arr) => arr k v -> (Instant -> v -> IO (Use u)) -> arr (Cache c k v, k) u
cached _ _ = notImplemented "cached"

-- | Runs the action as the only writer: a cache of one core needs no lock, since the steps that never wait are atomic on a core
-- whose parts switch only at waits, and a shared cache has a flag that a writer takes with a compare-and-swap, and that a writer
-- that finds it taken yields at, so that the holder, which runs for a few hundred nanoseconds, runs on: a spin that neither
-- allocates nor yields would stall the garbage collector's stop-the-world.
withWriter :: Cache c k v -> IO a -> IO a
withWriter cache body
  | cacheShared cache = acquire *> (body `onException` release) <* release
  | otherwise = body
  where
    acquire = casInt (cacheControl cache) lockWord 0 1 >>= \before -> when (before /= 0) (yield *> acquire)
    release = writeIntAtomic (cacheControl cache) lockWord 0

-- | Makes a table the readers read.
publishTable :: Cache c k v -> Table k v -> IO ()
publishTable cache table
  | cacheShared cache = atomicWriteIORef (cacheTable cache) table
  | otherwise = writeIORef (cacheTable cache) table

-- | Evicts while the test of the number of entries holds, and gives the entries that went, in the order that they did. The ghosts
-- remember them, up to the limit.
evictWhile :: Cache c k v -> Int -> Table k v -> (Int -> Bool) -> IO [Entry k v]
evictWhile cache limit table over = go []
  where
    go gone = do
      entries' <- tableSize table
      if over entries'
        then do
          gone' <- evictOne table
          for_ (cacheGhosts cache) (\ghosts -> modifyIORef' ghosts (addGhost limit (entryHash gone')))
          go (gone' : gone)
        else pure (reverse gone)

-- | Tells the eviction listener of the entries, in order, once the writer has done.
told :: Cache c k v -> [Entry k v] -> IO ()
told cache = traverse_ (\gone -> case gone of Entry _ _ key value -> evicted (cacheConfig cache) key value; Vacant -> pure ())

-- | A key stored is no longer one that was evicted lately.
forget :: Cache c k v -> Int -> IO ()
forget cache h = for_ (cacheGhosts cache) (\ghosts -> modifyIORef' ghosts (snd . takeGhost h))

-- | A new entry, with the number that no other of the cache has. The key is prepared, and the value evaluated, before the writer
-- is in: a copy of a key is not made while the others wait.
newEntry :: Cache c k v -> Int -> k -> v -> IO (Entry k v)
newEntry cache h key value = do
  ident <- readInt (cacheControl cache) serialWord
  writeInt (cacheControl cache) serialWord (ident + 1)
  pure $! Entry h ident key value

-- | The value of a key, if it is cached, which counts as its use. It neither runs a hook nor tells 'requested'. A shared cache
-- that finds nothing looks again where no other writer is, since a lookup that a writer passes can miss.
peek :: (Key k) => Cache c k v -> k -> IO (Maybe v)
peek cache k = do
  let h = hashKey k
  readIORef (cacheTable cache) >>= \table -> findEntry table h k >>= \found -> case found of
    Entry _ _ _ value -> pure (Just value)
    Vacant
      | cacheShared cache -> withWriter cache (readIORef (cacheTable cache) >>= \table' -> findEntry table' h k) >>= \again -> pure (valueOf again)
      | otherwise -> pure Nothing

valueOf :: Entry k v -> Maybe v
valueOf (Entry _ _ _ value) = Just value
valueOf Vacant = Nothing

-- | Caches the value of a key, unless it is cached already, and says whether it did. It may evict an entry.
store :: (Key k) => Cache c k v -> k -> v -> IO Bool
store cache k value = do
  let h = hashKey k
      !prepared = prepareKey (cacheConfig cache) k
  !_ <- pure value
  (stored, gone) <- withWriter cache $ do
    table <- readIORef (cacheTable cache)
    present <- locateEntry table h k
    if present >= 0
      then pure (False, [])
      else do
        gone <- evictWhile cache (tableCapacity table) table (>= tableCapacity table)
        newEntry cache h prepared value >>= insertEntry table
        forget cache h
        pure (True, gone)
  told cache gone
  pure stored

-- | Removes a key, giving its value if it was cached. 'evicted' is not told: the caller has the value.
remove :: (Key k) => Cache c k v -> k -> IO (Maybe v)
remove cache k = withWriter cache $ do
  table <- readIORef (cacheTable cache)
  ident <- locateEntry table (hashKey k) k
  if ident < 0 then pure Nothing else valueOf <$> removeEntry table ident

-- | Removes every entry, telling 'evicted' of each, the oldest first.
flush :: Cache c k v -> IO ()
flush cache = do
  gone <- withWriter cache $ do
    table <- readIORef (cacheTable cache)
    gone <- entriesOf table
    newTable (cacheShared cache) (tableCapacity table) >>= publishTable cache
    pure gone
  told cache gone

-- | The keys and values of the cache, the oldest first.
entries :: Cache c k v -> IO [(k, v)]
entries cache = withWriter cache $ do
  table <- readIORef (cacheTable cache)
  found <- entriesOf table
  pure [(key, value) | Entry _ _ key value <- found]

-- | Changes the capacity, to at least one. A cache that holds more than that evicts, as an insertion does, and tells 'evicted'
-- of each entry. The readers that are in the old table finish in it.
resize :: Cache c k v -> Int -> IO ()
resize cache wanted = do
  gone <- withWriter cache $ do
    table <- readIORef (cacheTable cache)
    let capacity' = max 1 wanted
    gone <- evictWhile cache capacity' table (> capacity')
    when (capacity' /= tableCapacity table) (rebuild (cacheShared cache) capacity' table >>= publishTable cache)
    pure gone
  told cache gone

-- | How many entries the cache holds.
size :: Cache c k v -> IO Int
size cache = readIORef (cacheTable cache) >>= tableSize
