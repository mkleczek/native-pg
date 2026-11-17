-- | A scheduler-owned table of descriptor waits. The first page handles small
-- descriptor numbers directly, and other pages are allocated as descriptors
-- are used. Empty slots retain no waiters.
module Network.Streaming.Internal.DescriptorWaiters
  ( WaiterTable,
    Waiters (..),
    newWaiterTable,
    withWaiters,
    packWaiters,
    pushWaiter,
  )
where

import Data.Bits (shiftL, shiftR, (.&.))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import GHC.IOArray (IOArray, newIOArray, unsafeReadIOArray, unsafeWriteIOArray)

-- | The singleton cases avoid allocating a list node for each descriptor that
-- one part waits on. Lists in the general case are newest first.
data Waiters a
  = NoWaiters
  | OneReader !a
  | OneWriter !a
  | Several ![a] ![a]

type Page a = IOArray Int (Waiters a)

data WaiterTable a = WaiterTable !(Page a) !(IORef (IntMap.IntMap (Page a)))

pageBits :: Int
pageBits = 8

pageMask :: Int
pageMask = (1 `shiftL` pageBits) - 1

newPage :: IO (Page a)
newPage = newIOArray (0, pageMask) NoWaiters

newWaiterTable :: IO (WaiterTable a)
newWaiterTable = WaiterTable <$> newPage <*> newIORef IntMap.empty

-- | Reads one slot and writes its replacement if there is one. A lookup of a
-- descriptor whose page was never used can leave the page unallocated.
withWaiters :: WaiterTable a -> Int -> (Waiters a -> IO (Maybe (Waiters a), b)) -> IO b
withWaiters (WaiterTable first others) descriptor change
  | page == 0 = present first
  | otherwise = do
      directory <- readIORef others
      case IntMap.lookup page directory of
        Just slots -> present slots
        Nothing -> do
          (replacement, result) <- change NoWaiters
          case replacement of
            Nothing -> pure ()
            Just NoWaiters -> pure ()
            Just entry -> do
              slots <- newPage
              unsafeWriteIOArray slots index $! entry
              writeIORef others $! IntMap.insert page slots directory
          pure result
  where
    page = descriptor `shiftR` pageBits
    index = descriptor .&. pageMask
    present slots = do
      current <- unsafeReadIOArray slots index
      (replacement, result) <- change current
      case replacement of
        Nothing -> pure ()
        Just entry -> unsafeWriteIOArray slots index $! entry
      pure result
{-# INLINE withWaiters #-}

-- | Packs lists that may have shrunk after a cancellation or readiness report.
packWaiters :: [a] -> [a] -> Waiters a
packWaiters [] [] = NoWaiters
packWaiters [entry] [] = OneReader entry
packWaiters [] [entry] = OneWriter entry
packWaiters readers writers = Several readers writers

pushWaiter :: Bool -> a -> Waiters a -> Waiters a
pushWaiter True entry NoWaiters = OneReader entry
pushWaiter False entry NoWaiters = OneWriter entry
pushWaiter True entry (OneReader previous) = Several [entry, previous] []
pushWaiter False entry (OneReader previous) = Several [previous] [entry]
pushWaiter True entry (OneWriter previous) = Several [entry] [previous]
pushWaiter False entry (OneWriter previous) = Several [] [entry, previous]
pushWaiter True entry (Several readers writers) = Several (entry : readers) writers
pushWaiter False entry (Several readers writers) = Several readers (entry : writers)
{-# INLINE pushWaiter #-}
