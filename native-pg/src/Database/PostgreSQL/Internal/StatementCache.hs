{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The statements that a connection has prepared for the program that caches them, by the key that the program gives: which
-- name the connection has a statement under, and which it is to close. A connection has one user at a time, so the cache is
-- changed without a lock.
--
-- A key is any value that has a hash and an equality, of any type: keys of different types are different keys. The cache has
-- a number of statements at most, and when it is full, the one that was used least recently goes: its name is not forgotten but
-- put among the names that the connection is still to close, one of which each statement that is run takes, so that the Close
-- goes with a statement that is sent, and a name that was not closed, since the pipeline that was to close it failed, is
-- closed by a later one.
--
-- A statement is put in the cache when it is laid out, before it is sent, and is kept only once the server has accepted its
-- Parse: the part that reads the answers says so ('memoryParsed'), or that the pipeline failed ('memoryFailed'), which undoes
-- what was not accepted. The server forgetting its statements ('forget') puts all the names among those to close.
module Database.PostgreSQL.Internal.StatementCache
  ( Statements,
    newStatements,
    Looked (..),
    Reserved (..),
    Memory (..),
    find,
    insert,
    forget,
  )
where

import Control.Monad (unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import Data.Hashable (Hashable, hash)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.List as List
import Data.Typeable (Typeable, cast)

-- | A key of any type, as the cache keeps it.
data Key = forall k. (Typeable k, Hashable k) => Key k

-- | Whether two keys are the same: of the same type, and equal.
sameKey :: Key -> Key -> Bool
sameKey (Key left) (Key right) = maybe False (== left) (cast right)

-- | A statement that the cache has: its key, its name, and when it was last used.
data Entry = Entry {entryKey :: !Key, entryName :: !ByteString, entryUsed :: !(IORef Int)}

-- | What the cache holds: the statements by the hash of their keys, how many there are, the number of the next name, and the
-- names of statements that the connection has and is to close.
data State = State
  { stateEntries :: !(IntMap [Entry]),
    stateCount :: !Int,
    stateNext :: !Int,
    stateGarbage :: ![ByteString]
  }

-- | The statements of a connection: how many it may keep, what it keeps, and the clock that says which was used last.
data Statements = Statements !Int !(IORef State) !(IORef Int)

-- | An empty cache of this many statements at most. None keeps nothing.
newStatements :: Int -> IO Statements
newStatements limit = Statements (max 0 limit) <$> newIORef (State IntMap.empty 0 0 []) <*> newIORef 0

-- | What the connection has for a key.
data Looked
  = -- | The cache keeps nothing: the statement is sent as an unnamed one.
    Disabled
  | -- | The statement is there.
    Has !Reserved
  | -- | It is not.
    Lacks

-- | The name that the statement has, a name that is to be closed with it, if the connection has one, and what the parts that
-- read the answers tell the cache, if there is anything to tell: the Close was answered, the Parse was accepted, or the
-- pipeline failed.
data Reserved = Reserved {reservedName :: !ByteString, reservedClose :: !(Maybe ByteString), reservedMemory :: !(Maybe Memory)}

-- | What the part that reads the answers tells the cache.
data Memory = Memory {memoryParsed :: IO (), memoryClosed :: IO (), memoryFailed :: IO ()}


-- | Looks the key up. A statement that is there is marked as used now, and takes a name to close if there is one.
find :: (Typeable k, Hashable k) => Statements -> k -> IO Looked
find (Statements limit ref clock) key
  | limit <= 0 = pure Disabled
  | otherwise = do
      current <- readIORef ref
      let wrapped = Key key
      case IntMap.lookup (hash key) (stateEntries current) >>= List.find (sameKey wrapped . entryKey) of
        Nothing -> pure Lacks
        Just entry -> do
          tick clock >>= writeIORef (entryUsed entry)
          Has . (\closing -> Reserved (entryName entry) (fst <$> closing) (snd <$> closing)) <$> claim ref

-- | Puts a statement for the key in the cache, under a name that is new, and has the least recently used go if the cache is full,
-- which is then to be closed. It is not kept unless the server accepts its Parse.
insert :: (Typeable k, Hashable k) => Statements -> k -> IO Reserved
insert (Statements limit ref clock) key = do
  current <- readIORef ref
  stamp <- tick clock
  used <- newIORef stamp
  accepted <- newIORef False
  let number = stateNext current
      name = "npg_" <> Char8.pack (show number)
      code = hash key
      entry = Entry (Key key) name used
  evicting <- if stateCount current >= limit then oldest (stateEntries current) else pure Nothing
  let (entries, count, garbage) = case evicting of
        Nothing -> (stateEntries current, stateCount current, stateGarbage current)
        Just (evictedCode, evicted) -> (withoutEntry evictedCode (entryName evicted) (stateEntries current), stateCount current - 1, entryName evicted : stateGarbage current)
  writeIORef ref State {stateEntries = IntMap.insertWith (++) code [entry] entries, stateCount = count + 1, stateNext = number + 1, stateGarbage = garbage}
  closing <- claim ref
  let undo = do
        kept <- readIORef accepted
        unless kept (modifyIORef' ref (withoutStatement code name))
      memory = case closing of
        Nothing -> Memory {memoryParsed = writeIORef accepted True, memoryClosed = pure (), memoryFailed = undo}
        Just (_, closed) -> closed {memoryParsed = writeIORef accepted True, memoryFailed = undo *> memoryFailed closed}
  pure Reserved {reservedName = name, reservedClose = fst <$> closing, reservedMemory = Just memory}

-- | Forgets every statement, as the server has forgotten them, or is not to be asked for them: their names are among those that
-- the connection closes.
forget :: Statements -> IO ()
forget (Statements _ ref _) = modifyIORef' ref $ \state ->
  state {stateEntries = IntMap.empty, stateCount = 0, stateGarbage = [entryName entry | entries <- IntMap.elems (stateEntries state), entry <- entries] <> stateGarbage state}

-- | A name that the connection is to close, if it has one, and what the part that reads the answers tells the cache about it: it
-- is the connection's no more, but is back among those that it is to close if the pipeline that was to close it failed first.
claim :: IORef State -> IO (Maybe (ByteString, Memory))
claim ref = do
  state <- readIORef ref
  case stateGarbage state of
    [] -> pure Nothing
    name : others -> do
      writeIORef ref state {stateGarbage = others}
      closed <- newIORef False
      let reinstate = readIORef closed >>= \done -> unless done (modifyIORef' ref (\now -> now {stateGarbage = name : stateGarbage now}))
      pure (Just (name, Memory {memoryParsed = pure (), memoryClosed = writeIORef closed True, memoryFailed = reinstate}))

-- | The statement that was used least recently, with the hash of its key. It looks at every statement, as a loop that allocates only
-- when it finds an older one.
oldest :: IntMap [Entry] -> IO (Maybe (Int, Entry))
oldest entries = groups (IntMap.toList entries) Nothing
  where
    groups [] best = pure (fmap (\(code, _, entry) -> (code, entry)) best)
    groups ((code, group) : others) best = within code group best >>= groups others
    within _ [] best = pure best
    within code (entry : others) best = do
      !used <- readIORef (entryUsed entry)
      let !older = case best of
            Just (_, kept, _) | kept <= used -> best
            _ -> Just (code, used, entry)
      within code others older

-- | The statements without the one that has the name.
withoutEntry :: Int -> ByteString -> IntMap [Entry] -> IntMap [Entry]
withoutEntry code name = IntMap.update (\group -> case filter ((/= name) . entryName) group of [] -> Nothing; others -> Just others) code

-- | The cache without the statement that has the name, if it has it.
withoutStatement :: Int -> ByteString -> State -> State
withoutStatement code name state
  | any ((== name) . entryName) (IntMap.findWithDefault [] code (stateEntries state)) = state {stateEntries = withoutEntry code name (stateEntries state), stateCount = stateCount state - 1}
  | otherwise = state

-- | The time now, which the clock counts in uses.
tick :: IORef Int -> IO Int
tick clock = do
  now <- readIORef clock
  writeIORef clock $! now + 1
  pure now
