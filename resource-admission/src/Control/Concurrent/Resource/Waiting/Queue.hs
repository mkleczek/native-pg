module Control.Concurrent.Resource.Waiting.Queue
  (Queue, Entry, newQueue, append, remove, first, value, isEmpty) where
import Control.Concurrent.STM

data Queue a = Queue
data Entry a = Entry
newQueue :: STM (Queue a)
newQueue = pure Queue
append :: Queue a -> a -> STM (Entry a)
append _ _ = error "pending FIFO append"
remove :: Entry a -> STM ()
remove _ = error "pending exact FIFO removal"
first :: Queue a -> STM (Maybe (Entry a))
first _ = error "pending FIFO head"
value :: Entry a -> a
value _ = error "pending FIFO payload"
isEmpty :: Queue a -> STM Bool
isEmpty _ = error "pending FIFO emptiness"
