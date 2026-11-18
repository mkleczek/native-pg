-- | Local work staged to run in the next scheduler round, in insertion order.
module Network.Streaming.Internal.RoundQueue
  ( Later,
    empty,
    push,
    pushBatch,
    takeReady,
  )
where

import qualified Data.List as List (foldl')

newtype Later a = Later [a]

empty :: Later a
empty = Later []
{-# INLINE empty #-}

push :: Later a -> a -> Later a
push (Later newest) value = Later (value : newest)
{-# INLINE push #-}

-- | Stage every child before returning to the scheduler, without forcing the children themselves.
pushBatch :: Later a -> [a] -> (Later a -> r) -> r
pushBatch (Later newest) values resume = resume $! Later (List.foldl' (flip (:)) newest values)
{-# INLINE pushBatch #-}

takeReady :: Later a -> [a]
takeReady (Later newest) = reverse newest
{-# INLINE takeReady #-}
