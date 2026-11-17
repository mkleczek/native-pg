{-# LANGUAGE MagicHash #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Changing a shared reference from several cores by compare-and-swap against the pointer that was read, whether it is a value or a thunk.
module Network.Streaming.Internal.Atomic
  ( update,
    Change,
    changeHere,
  )
where

import Data.IORef (IORef, readIORef, writeIORef)
import GHC.Exts (casMutVar#, readMutVar#)
import GHC.IO (IO (..))
import GHC.IORef (IORef (..))
import GHC.STRef (STRef (..))

-- | Changes the value, and gives what the change says; the new value is evaluated before it is installed, and a core that loses the race changes the value that won.
update :: IORef a -> (a -> (a, r)) -> IO r
update (IORef (STRef var)) change = IO loop
  where
    -- The change is given a copy of the pointer and the swap keeps the original, since the compiler replaces what a change evaluates by its value, and the value is not what a thunk in the reference is.
    loop s = case readMutVar# var s of
      (# s1, stored #) -> case copied stored of
        (# old #) -> case change old of
          (!new, !result) -> case casMutVar# var stored new s1 of
            (# s2, 0#, _ #) -> (# s2, result #)
            (# s2, _, _ #) -> loop s2
{-# INLINE update #-}

-- | The same pointer, from a function that the compiler does not look into, so that what it learns of the copy it does not learn of the original.
copied :: a -> (# a #)
copied x = (# x #)
{-# OPAQUE copied #-}

-- | How a reference is changed: by a function that gives the new value and what the change says. 'update' is the change for the parts of
-- any core, and 'changeHere' for the parts of one core, so that one set of changes serves both reaches.
type Change = forall x r. IORef x -> (x -> (x, r)) -> IO r

-- | The change by plain reads and writes, for the parts of one core. It evaluates what it installs, as 'update' does.
changeHere :: Change
changeHere ref change = readIORef ref >>= \old -> case change old of (!new, !result) -> result <$ writeIORef ref new
{-# INLINE changeHere #-}
