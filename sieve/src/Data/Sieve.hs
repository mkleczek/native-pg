{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PatternSynonyms #-}
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

import Control.Arrow (returnA, (>>>), (^>>), (|||))
import Control.Concurrent (yield)
import Control.Exception (evaluate, onException)
import Control.Monad (when)
import Data.Foldable (for_, traverse_)
import Data.IORef (IORef, atomicWriteIORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Kind (Type)
import Data.Sieve.Ghosts (Ghosts, addGhost, noGhosts, takeGhost)
import Data.Sieve.Memory (Ints, casInt, newInts, readInt, writeInt, writeIntAtomic)
import Data.Sieve.Table
import GHC.TypeLits (ErrorMessage (..), TypeError)
import Network.Streaming.Classes
import Network.Streaming.Combinators (Step, alongside, done, iterateA, keeping, step, pattern Again, pattern Finish)

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

-- | A caller that missed on its first look: the cache, the key, and whether the caller told 'requested' already, which it does
-- once.
data Slow c k v = Slow !(Cache c k v) !k !Bool

-- | The result of the hook for the value of a key, loading it if the cache has none.
--
-- * **The look:** it takes no lock. A key that is found is given to the hook, with the time of the arrow's clock, and the hook
--   says what to return, or that the value is to be loaded again: the entry is then removed, if it is still that entry, and the
--   look goes on as a miss. A miss that a writer may have caused is looked at again under the writer's lock, where it is a miss
--   for certain.
-- * **The load:** the loader is an arrow, fixed when the program is built, that runs as part of the caller's own program, so it
--   may wait. What it gives is cached, unless the key was cached meanwhile, and then that is what the caller has. The hook runs
--   once on the value, as it does on one that is found.
-- * **The listeners:** 'requested' is told once for each call, and the eviction listener of what the load evicts.
cached ::
  forall c arr k v u.
  (Runtime arr, Key k, Reaching c arr) =>
  arr k v ->
  (Instant -> v -> IO (Use u)) ->
  arr (Cache c k v, k) u
cached loader hook =
  named (Label "cached" "Looks the key up without a lock, and loads it if the cache has no value that the hook takes.") $
    keeping clock
      >>> step "look up" firstLook `giving` "found a value that the hook took?"
      >>> (taking "no" slow ||| taking "yes" returnA)
  where
    -- The lookup that takes no lock, and the hook on what it finds.
    firstLook (instant, (cache, k)) = do
      table <- readIORef (cacheTable cache)
      found <- findEntry table (hashKey k) k
      case found of
        Vacant -> pure (Left (Slow cache k False))
        Entry _ _ _ value -> do
          requested (cacheConfig cache) True
          hook instant value >>= \result -> case result of
            Use taken -> pure (Right taken)
            Reload -> Left (Slow cache k True) <$ dropSame cache found

    slow :: arr (Slow c k v) u
    slow = named (Label "claim and load" "Looks the key up where no other writer is, and loads it if it is not cached.") (iterateA claimed)

    claimed :: arr (Slow c k v) (Step (Slow c k v) u)
    claimed = step "claim" claim `giving` "cached?" >>> (taking "yes" applying ||| taking "no" loading)

    loading :: arr (Slow c k v) (Step (Slow c k v) u)
    loading = alongside ((\(Slow _ k _) -> k) ^>> loader) >>> step "insert" insertLoaded >>> applying

    -- Runs the hook, on the entry, with the time of the arrow's clock.
    applying :: arr (Slow c k v, Entry k v) (Step (Slow c k v) u)
    applying = alongside (done >>> clock) >>> step "hook" (\(instant, (caller, entry)) -> use instant caller entry)

    use instant caller@(Slow cache k _) entry = case entry of
      Vacant -> pure (Again caller)
      Entry _ _ _ value -> hook instant value >>= \result -> case result of
        Use taken -> pure (Finish taken)
        Reload -> Again (Slow cache k True) <$ dropSame cache entry

    insertLoaded (value, caller@(Slow cache k _)) = do
      loaded <- evaluate value
      (entry, _) <- insertUnlessCached cache (hashKey k) (prepareKey (cacheConfig cache) k) loaded
      pure (caller, entry)
{-# INLINE cached #-}

-- | The look of a caller that missed, where no other writer is: the entry, if the key is cached after all, or the caller, which
-- has told 'requested', and 'thrashed' if the key was evicted lately.
claim :: (Key k) => Slow c k v -> IO (Either (Slow c k v, Entry k v) (Slow c k v))
claim (Slow cache k recorded) = do
  (found, notices) <- withWriter cache $ do
    table <- readIORef (cacheTable cache)
    entry <- findEntry table h k
    case entry of
      Vacant -> (,) entry <$> missed
      _ -> pure (entry, [requested settings True | not recorded])
  sequence_ notices
  pure $ case found of
    Vacant -> Right (Slow cache k True)
    _ -> Left (Slow cache k True, found)
  where
    h = hashKey k
    settings = cacheConfig cache
    missed
      | recorded = pure []
      | otherwise = do
          thrashing <- maybe (pure False) (\ghosts -> readIORef ghosts >>= \before -> let (was, after) = takeGhost h before in was <$ writeIORef ghosts after) (cacheGhosts cache)
          pure (requested settings False : [tell | Just tell <- [thrashed settings], thrashing])

-- | Removes the entry from the cache if the key still has this entry, and not one that took its place. It is not an eviction:
-- the listener isn't told, and the key is not remembered as evicted.
dropSame :: (Key k) => Cache c k v -> Entry k v -> IO ()
dropSame cache entry = case entry of
  Vacant -> pure ()
  Entry h ident k _ -> withWriter cache $ do
    table <- readIORef (cacheTable cache)
    at <- locateEntry table h k
    when (at >= 0) $
      entryAt table at >>= \current -> case current of
        Entry _ identNow _ _ | identNow == ident -> () <$ removeEntry table at
        _ -> pure ()

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
  let !prepared = prepareKey (cacheConfig cache) k
  evaluated <- evaluate value
  snd <$> insertUnlessCached cache (hashKey k) prepared evaluated

-- | Caches the value of the prepared key, unless the key is cached, and gives the entry that is, and whether it is the new one.
-- The key is prepared and the value evaluated before the writer is in: a copy of a key is not made while the others wait. It
-- evicts as an insertion does, and tells the eviction listener once the writer has done.
insertUnlessCached :: (Key k) => Cache c k v -> Int -> k -> v -> IO (Entry k v, Bool)
insertUnlessCached cache h key value = do
  (result, gone) <- withWriter cache $ do
    table <- readIORef (cacheTable cache)
    present <- locateEntry table h key
    if present >= 0
      then (\entry -> ((entry, False), [])) <$> entryAt table present
      else do
        gone <- evictWhile cache (tableCapacity table) table (>= tableCapacity table)
        entry <- newEntry cache h key value
        insertEntry table entry
        forget cache h
        pure ((entry, True), gone)
  told cache gone
  pure result

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
