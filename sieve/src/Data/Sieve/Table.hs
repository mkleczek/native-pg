{-# LANGUAGE BangPatterns #-}

-- | The table of a cache: go-sieve's layout. The entries are a dense array of pointers to immutable 'Entry's, with the SIEVE
-- queue as two arrays of entry numbers and a byte for each entry's visited bit, which only the writer reads and changes, but for
-- the visited bit, which a reader sets. The index is open addressing over twice as many slots as there are entries, each the
-- number of an entry or empty, with linear probing, and deletion by shifting the chain back, so that it has no tombstones.
--
-- A lookup takes no lock and writes nothing but, once, the visited byte. Writes go one at a time, which the caller sees to: in
-- place, and in the order that a reader needs.
--
-- * An entry is written to its place in the array before its number is published in the index, and its number leaves the index
--   before the entry leaves the array, so that a reader that finds a number in the index finds that entry, or one that took its
--   number later, which it compares with the key that it looks for, and never takes for the key's.
-- * A reader that is passed by a chain being shifted back can miss a key that is there. It never finds one that isn't: a miss
--   from a lookup is only a hint, and the caller that depends on it looks again where no writer changes the table.
module Data.Sieve.Table
  ( Entry (..),
    Table,
    newTable,
    tableCapacity,
    tableSize,
    findEntry,
    entryAt,
    locateEntry,
    insertEntry,
    removeEntry,
    evictOne,
    entriesOf,
    rebuild,
  )
where

import Control.Monad (when)
import Data.Bits (countLeadingZeros, finiteBitSize, unsafeShiftR, (.&.))
import Data.Sieve.Memory
import Data.Word (Word64)

-- | What a key has in the table: its hash, a number of its own that no other entry of the cache has, the key and the value.
-- It is immutable, so that a reader that holds it has a key and a value that belong together, and the garbage collector keeps
-- it alive for as long as the reader does.
data Entry k v
  = -- | The place of an entry that isn't there.
    Vacant
  | Entry
      { entryHash :: !Int,
        entryIdent :: !Int,
        entryKey :: !k,
        entryValue :: !v
      }

data Table k v = Table
  { -- | Whether readers run while the writer does, on other cores, so that the index is published with atomic writes.
    tableShared :: !Bool,
    tableCapacity :: !Int,
    -- | What the multiplied hash is shifted right by to give a slot of the index.
    tableShift :: !Int,
    tableMask :: !Int,
    tableEntries :: !(Pointers (Entry k v)),
    tableIndex :: !Ints,
    -- | For each entry, the next newer in the queue and the next older, or -1. A free entry's next is the next free one.
    tableNext :: !Ints,
    tablePrev :: !Ints,
    tableVisited :: !Bytes,
    -- | The hand, the newest entry, the oldest entry, how many entries there are, and the first free one: each an entry
    -- number, or -1, but the count.
    tableState :: !Ints
  }

hand, newest, oldest, count, free :: Int
hand = 0
newest = 1
oldest = 2
count = 3
free = 4

-- | An empty table for this many entries, at least one.
newTable :: Bool -> Int -> IO (Table k v)
newTable shared wanted = do
  let capacity = max 1 wanted
      slots = indexSlots capacity
      bits = finiteBitSize slots - 1 - countLeadingZeros slots
  entries <- newPointers capacity Vacant
  index <- newInts slots (-1)
  next <- newInts capacity (-1)
  prev <- newInts capacity (-1)
  visited <- newBytes capacity
  state <- newInts 5 (-1)
  chain next capacity 0
  writeInt state free 0
  writeInt state count 0
  pure (Table shared capacity (64 - bits) (slots - 1) entries index next prev visited state)
  where
    -- The entries are free one after the other, which a tail loop links.
    chain next capacity i = when (i < capacity) (writeInt next i (if i + 1 == capacity then -1 else i + 1) *> chain next capacity (i + 1))

-- | The least power of two over twice the capacity, and at least four: the index is never more than half full.
indexSlots :: Int -> Int
indexSlots capacity = go 4
  where
    go n = if n >= 2 * capacity then n else go (n * 2)

-- | How many entries there are.
tableSize :: Table k v -> IO Int
tableSize table = readInt (tableState table) count

-- | The first slot of the chain of a hash: its multiple by an odd number near the golden ratio, taking the high bits, so that
-- hashes that are close, or that share their low bits, are spread.
homeOf :: Table k v -> Int -> Int
homeOf table h = fromIntegral ((fromIntegral h * 0x9E3779B97F4A7C15 :: Word64) `unsafeShiftR` tableShift table)
{-# INLINE homeOf #-}

nextSlot :: Table k v -> Int -> Int
nextSlot table slot = (slot + 1) .&. tableMask table
{-# INLINE nextSlot #-}

-- | Stores in the index, with the order that a reader needs when the readers run at once.
publish :: Table k v -> Int -> Int -> IO ()
publish table slot ident
  | tableShared table = writeIntAtomic (tableIndex table) slot ident
  | otherwise = writeInt (tableIndex table) slot ident
{-# INLINE publish #-}

-- | The entry of the key, if the table has it, which counts as a use of it: it takes no lock, and the only thing it writes
-- is the entry's visited byte, once. It can miss a key that a writer is moving, and it never finds another key's entry.
findEntry :: (Eq k) => Table k v -> Int -> k -> IO (Entry k v)
findEntry table h k = go (homeOf table h)
  where
    go !slot = do
      ident <- readInt (tableIndex table) slot
      if ident < 0
        then pure Vacant
        else do
          found <- readPointer (tableEntries table) ident
          case found of
            Entry h' _ k' _ | h' == h && k' == k -> found <$ visit table ident
            _ -> go (nextSlot table slot)
{-# INLINE findEntry #-}

-- | The entry that has the number: it is the writer's to ask, for an entry that is there.
entryAt :: Table k v -> Int -> IO (Entry k v)
entryAt table = readPointer (tableEntries table)

-- | Sets the entry's visited byte, unless it is set: a hit that finds it set writes nothing.
visit :: Table k v -> Int -> IO ()
visit table ident = readByte (tableVisited table) ident >>= \seen -> when (seen == 0) (writeByte (tableVisited table) ident 1)
{-# INLINE visit #-}

-- | The number of the entry of the key, or -1. For the writer, which no other writer runs beside, and which sets no visited byte.
locateEntry :: (Eq k) => Table k v -> Int -> k -> IO Int
locateEntry table h k = go (homeOf table h)
  where
    go !slot = do
      ident <- readInt (tableIndex table) slot
      if ident < 0
        then pure (-1)
        else do
          found <- readPointer (tableEntries table) ident
          case found of
            Entry h' _ k' _ | h' == h && k' == k -> pure ident
            _ -> go (nextSlot table slot)

-- | Adds the entry, as the newest of the queue, for a key that the table doesn't have, and when it has room.
insertEntry :: Table k v -> Entry k v -> IO ()
insertEntry table entry = do
  let state = tableState table
  ident <- readInt state free
  readInt (tableNext table) ident >>= writeInt state free
  writePointer (tableEntries table) ident entry
  previous <- readInt state newest
  writeInt (tablePrev table) ident previous
  writeInt (tableNext table) ident (-1)
  if previous >= 0 then writeInt (tableNext table) previous ident else writeInt state oldest ident
  writeInt state newest ident
  writeByte (tableVisited table) ident 0
  readInt state count >>= writeInt state count . (+ 1)
  -- The entry is in its place: now readers find it.
  emptySlot (homeOf table (entryHash entry)) >>= \slot -> publish table slot ident
  where
    emptySlot !slot = readInt (tableIndex table) slot >>= \taken -> if taken < 0 then pure slot else emptySlot (nextSlot table slot)

-- | Removes the entry that has the number, and gives it: it leaves the index, with the chain shifted back, and then the queue
-- and the array. The hand goes to the next newer entry if it was at this one.
removeEntry :: Table k v -> Int -> IO (Entry k v)
removeEntry table ident = do
  let state = tableState table
  removed <- readPointer (tableEntries table) ident
  slotOfEntry (homeOf table (entryHash removed)) >>= shiftBack table
  newer <- readInt (tableNext table) ident
  older <- readInt (tablePrev table) ident
  at <- readInt state hand
  when (at == ident) (writeInt state hand newer)
  if older >= 0 then writeInt (tableNext table) older newer else writeInt state oldest newer
  if newer >= 0 then writeInt (tablePrev table) newer older else writeInt state newest older
  writePointer (tableEntries table) ident Vacant
  readInt state free >>= writeInt (tableNext table) ident
  writeInt state free ident
  readInt state count >>= writeInt state count . subtract 1
  pure removed
  where
    slotOfEntry !slot = do
      holding <- readInt (tableIndex table) slot
      if holding == ident
        then pure slot
        else
          if holding < 0
            then ioError (userError "sieve: an entry is not in the index")
            else slotOfEntry (nextSlot table slot)

-- | Empties the slot of the index, which leaves a hole in its chain, and fills the hole with the entry of the chain that can
-- move into it, until a slot that is empty ends the chain. Each entry that moves is written to the hole before it is erased
-- from its old slot, so that a reader that passes neither finds a hole in the middle of a chain nor misses the entry for good.
shiftBack :: Table k v -> Int -> IO ()
shiftBack table = go
  where
    go !hole = look hole (nextSlot table hole)
    look !hole !slot = do
      ident <- readInt (tableIndex table) slot
      if ident < 0
        then publish table hole (-1)
        else do
          entry <- readPointer (tableEntries table) ident
          let home = homeOf table (entryHash entry)
          -- The entry can move into the hole if its home isn't between the hole, after it, and its slot.
          if (slot - home) .&. tableMask table >= (slot - hole) .&. tableMask table
            then publish table hole ident *> go slot
            else look hole (nextSlot table slot)

-- | Evicts as SIEVE does and gives the entry: from the hand, or the oldest, each entry that was visited is passed over and loses
-- its bit, the queue taken round to the oldest after the newest, and the first that wasn't is removed, with the hand on the next.
-- There must be an entry.
evictOne :: Table k v -> IO (Entry k v)
evictOne table = do
  let state = tableState table
  at <- readInt state hand
  first' <- readInt state oldest
  look first' (if at >= 0 then at else first')
  where
    look first' !ident = do
      seen <- readByte (tableVisited table) ident
      newer <- readInt (tableNext table) ident
      if seen /= 0
        then writeByte (tableVisited table) ident 0 *> look first' (if newer >= 0 then newer else first')
        else writeInt (tableState table) hand newer *> removeEntry table ident

-- | The entries, the oldest first.
entriesOf :: Table k v -> IO [Entry k v]
entriesOf table = readInt (tableState table) newest >>= collect []
  where
    -- From the newest to the oldest, which puts the oldest first, with a tail loop.
    collect found !ident
      | ident < 0 = pure found
      | otherwise = do
          entry <- readPointer (tableEntries table) ident
          older <- readInt (tablePrev table) ident
          collect (entry : found) older

-- | A table for this many entries, shared or not, with the entries of this one, which are at most that many, in the same order,
-- with their visited bits and the hand: for a resize, which publishes the new table when it is built.
rebuild :: Bool -> Int -> Table k v -> IO (Table k v)
rebuild shared capacity old = do
  new <- newTable shared capacity
  at <- readInt (tableState old) hand
  first' <- readInt (tableState old) oldest
  copy new at first'
  pure new
  where
    copy new at !ident = when (ident >= 0) $ do
      entry <- readPointer (tableEntries old) ident
      insertEntry new entry
      placed <- readInt (tableState new) newest
      readByte (tableVisited old) ident >>= writeByte (tableVisited new) placed
      when (ident == at) (writeInt (tableState new) hand placed)
      readInt (tableNext old) ident >>= copy new at
