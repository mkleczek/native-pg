-- | Local work staged to run in the next scheduler round, in insertion order.
module Network.Streaming.Internal.RoundQueue
  ( Later,
    empty,
    push,
    pushBatch,
    takeReady,
  )
where

newtype Later a = Later [a]

empty :: Later a
empty = Later []
{-# INLINE empty #-}

push :: Later a -> a -> Later a
push (Later newest) value = Later (value : newest)
{-# INLINE push #-}

pushBatch :: Later a -> [a] -> (Later a -> r) -> r
pushBatch (Later newest) values resume = resume (Later (foldl (flip (:)) newest values))
{-# INLINE pushBatch #-}

takeReady :: Later a -> [a]
takeReady (Later newest) = reverse newest
{-# INLINE takeReady #-}
