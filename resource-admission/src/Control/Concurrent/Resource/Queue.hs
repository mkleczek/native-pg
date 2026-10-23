-- | An STM FIFO with exact removal. Entries are opaque handles that retain
-- their references; removal invalidates the handle, which callers must discard.
-- Each owner supplies its own scheduling policy and keeps a separate queue.
module Control.Concurrent.Resource.Queue
  ( Queue, Entry, newQueue, append, remove, first, value, isEmpty
  ) where

import Control.Concurrent.STM
import Data.Maybe (isNothing)

data Queue a = Queue !(TVar (Maybe (Entry a))) !(TVar (Maybe (Entry a)))
data Entry a = Entry !(Queue a) a !(TVar (Maybe (Entry a))) !(TVar (Maybe (Entry a)))

newQueue :: STM (Queue a)
newQueue = Queue <$> newTVar Nothing <*> newTVar Nothing

append :: Queue a -> a -> STM (Entry a)
append queue@(Queue headVar tailVar) payload = do
  previous <- readTVar tailVar
  entry <- Entry queue payload <$> newTVar previous <*> newTVar Nothing
  case previous of
    Nothing -> writeTVar headVar (Just entry)
    Just (Entry _ _ _ following) -> writeTVar following (Just entry)
  writeTVar tailVar (Just entry)
  pure entry

-- | Remove a live entry exactly once. No operation may use its handle afterward.
remove :: Entry a -> STM ()
remove (Entry (Queue headVar tailVar) _ beforeVar afterVar) = do
  before <- readTVar beforeVar
  after <- readTVar afterVar
  case before of
    Nothing -> writeTVar headVar after
    Just (Entry _ _ _ following) -> writeTVar following after
  case after of
    Nothing -> writeTVar tailVar before
    Just (Entry _ _ preceding _) -> writeTVar preceding before

first :: Queue a -> STM (Maybe (Entry a))
first (Queue headVar _) = readTVar headVar

value :: Entry a -> a
value (Entry _ payload _ _) = payload

isEmpty :: Queue a -> STM Bool
isEmpty queue = isNothing <$> first queue
