-- | A simulated connection: an in-memory byte stream each way, whose lanes
-- hold at most a set number of bytes, as a socket's buffers do. Its events
-- depend only on what was sent and received, so a simulation of directions
-- over it is deterministic.
module Network.Streaming.Simulated
  ( SimEnd,
    connect,
  )
where

import Control.Arrow (ArrowChoice, (>>>))
import Control.Concurrent.STM (STM, TVar, atomically, check, newTVarIO, readTVar, writeTVar)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Word (Word8)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr)
import Network.Streaming.Classes
import Network.Streaming.Watch (Watch (..))

-- | One way of a connection: the bytes sent and not yet received, whether the
-- sender has ended it, and how many bytes it holds at most.
data Lane = Lane
  { laneBytes :: !(TVar BS.ByteString),
    laneEnded :: !(TVar Bool),
    laneCapacity :: !Int
  }

-- | One end of a simulated connection.
data SimEnd = SimEnd {endIn :: !Lane, endOut :: !Lane}

-- | Two connected ends, each of whose lanes holds at most this many bytes.
connect :: Int -> IO (SimEnd, SimEnd)
connect capacity = do
  there <- newLane
  back <- newLane
  pure (SimEnd back there, SimEnd there back)
  where
    newLane = Lane <$> newTVarIO BS.empty <*> newTVarIO False <*> pure capacity

-- | Takes what the lane has into the memory, up to the size, without waiting:
-- 'Nothing' if it has nothing, and 0 at the end of the stream.
receiveReady :: SimEnd -> Ptr Word8 -> Int -> IO (Maybe Int)
receiveReady end pointer size = do
  taken <- atomically $ do
    bytes <- readTVar (laneBytes lane)
    if BS.null bytes
      then (\ended -> if ended then Just BS.empty else Nothing) <$> readTVar (laneEnded lane)
      else do
        let (part, rest) = BS.splitAt size bytes
        Just part <$ writeTVar (laneBytes lane) rest
  case taken of
    Nothing -> pure Nothing
    Just chunk -> Just (BS.length chunk) <$ BSU.unsafeUseAsCStringLen chunk (\(from, count) -> copyBytes pointer (castPtr from) count)
  where
    lane = endIn end

-- | Puts as much of the pieces in the lane as it has room for, without waiting,
-- and gives how many bytes it took.
sendReady :: SimEnd -> [BS.ByteString] -> IO Int
sendReady end pieces = atomically $ do
  bytes <- readTVar (laneBytes lane)
  -- The pieces may be views of a direction's buffer, so the lane keeps a copy,
  -- made now.
  let !chunk = BS.copy (BS.take (laneCapacity lane - BS.length bytes) (BS.concat pieces))
  BS.length chunk <$ (writeTVar (laneBytes lane) $! bytes <> chunk)
  where
    lane = endOut end

-- | Ends the end's output.
halfClose :: SimEnd -> IO ()
halfClose end = atomically (writeTVar (laneEnded (endOut end)) True)

-- | A simulated end's events are its lanes' watches.
readable, writable :: SimEnd -> IO Event
readable end = pure (Watched (pure (Watch (laneReadable (endIn end)) (pure ()))))
writable end = pure (Watched (pure (Watch (laneWritable (endOut end)) (pure ()))))

-- | A simulated end, in any arrow that can run its tries, block, and give way.
-- It waits for input as its receive does, on its lane's watch.
instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr) => Link arr SimEnd where
  receive = receiveWith receiveReady readable
  {-# INLINE receive #-}
  sendAll = sendAllWith sendReady (\_ -> pure True) writable
  {-# INLINE sendAll #-}
  awaitInput = now readable >>> block
  shutdownSend = now halfClose
  close = now halfClose

laneReadable :: Lane -> STM ()
laneReadable lane = do
  bytes <- readTVar (laneBytes lane)
  ended <- readTVar (laneEnded lane)
  check (not (BS.null bytes) || ended)

laneWritable :: Lane -> STM ()
laneWritable lane = readTVar (laneBytes lane) >>= check . (< laneCapacity lane) . BS.length
