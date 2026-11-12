{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Changing a shared reference from several cores. 'atomicModifyIORef'' installs the new value as a thunk, and
-- the cores that race for the reference then wait on each other's black holes to evaluate it; 'update' evaluates
-- the new value before it installs it by compare-and-swap, and a core that loses the race computes it again from
-- the value that won. The reference must always hold an evaluated value, which its first one is if it is a
-- constructor, and everything 'update' installs is.
module Network.Streaming.Internal.Atomic
  ( update,
  )
where

import Data.IORef (IORef)
import GHC.Exts (casMutVar#, readMutVar#)
import GHC.IO (IO (..))
import GHC.IORef (IORef (..))
import GHC.STRef (STRef (..))

-- | Changes the value, and gives what the change says.
update :: IORef a -> (a -> (a, r)) -> IO r
update (IORef (STRef var)) change = IO loop
  where
    loop s = case readMutVar# var s of
      (# s1, old #) -> case change old of
        (!new, !result) -> case casMutVar# var old new s1 of
          (# s2, 0#, _ #) -> (# s2, result #)
          (# s2, _, _ #) -> loop s2
{-# INLINE update #-}
