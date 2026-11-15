-- | The keys that were evicted last, which a cache remembers to tell when it misses one of them: a miss that a cache a little
-- larger would have had. A key is remembered by its hash, not as itself: a key can be large, and a fixed-size hash keeps the memory
-- of the tracking bounded by the number of keys that it tracks. A hash that two keys share may rarely tell of a miss of a key
-- that was not evicted.
module Data.Sieve.Ghosts
  ( Ghosts,
    noGhosts,
    addGhost,
    takeGhost,
  )
where

import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap

-- | The hashes, by when each was evicted, and by hash, with the number of the next eviction.
data Ghosts = Ghosts !(IntMap Int) !(IntMap Int) !Int

noGhosts :: Ghosts
noGhosts = Ghosts IntMap.empty IntMap.empty 0

-- | Remembers the hash of an evicted key, forgetting the oldest ones over the limit. A key that is evicted again keeps only its
-- latest eviction.
addGhost :: Int -> Int -> Ghosts -> Ghosts
addGhost limit h ghosts = trim (IntMap.size byHash') bySequence' byHash'
  where
    Ghosts bySequence byHash next = snd (takeGhost h ghosts)
    bySequence' = IntMap.insert next h bySequence
    byHash' = IntMap.insert h next byHash
    -- A tail loop, since the limit can fall by a lot.
    trim size sequences hashes
      | size > max 0 limit,
        Just ((_, oldest), rest) <- IntMap.minViewWithKey sequences =
          trim (size - 1) rest (IntMap.delete oldest hashes)
      | otherwise = Ghosts sequences hashes (next + 1)

-- | Forgets the hash of a key that was missed, and says whether it was remembered.
takeGhost :: Int -> Ghosts -> (Bool, Ghosts)
takeGhost h ghosts@(Ghosts bySequence byHash next) = case IntMap.lookup h byHash of
  Nothing -> (False, ghosts)
  Just evictedAt -> (True, Ghosts (IntMap.delete evictedAt bySequence) (IntMap.delete h byHash) next)
