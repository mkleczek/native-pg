{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Changing a shared reference from several cores. 'atomicModifyIORef'' installs the new value as a thunk, and
-- the cores that race for the reference then wait on each other's black holes to evaluate it; 'update' evaluates
-- the new value before it installs it by compare-and-swap, and a core that loses the race computes it again from
-- the value that won. The reference must always hold an evaluated value, which its first one is if it is a
-- constructor, and everything 'update' installs is meant to be. GHC can still install a thunk it considers cheap to
-- evaluate. A reader's compare-and-swap then compares against the evaluated value's pointer, not the thunk's, and fails
-- until a garbage collection removes the thunk. So after a failed compare-and-swap, 'update' replaces the stored pointer
-- with the evaluated value when the two differ, then retries.
module Network.Streaming.Internal.Atomic
  ( update,
  )
where

import Data.IORef (IORef)
import GHC.Exts (casMutVar#, readMutVar#, reallyUnsafePtrEquality#, seq#)
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
          (# s2, _, _ #) -> case readMutVar# var s2 of
            (# s3, stored #) -> case seq# stored s3 of
              (# s4, value #) -> case reallyUnsafePtrEquality# stored value of
                0# -> case casMutVar# var stored value s4 of (# s5, _, _ #) -> loop s5
                _ -> loop s4
{-# INLINE update #-}
