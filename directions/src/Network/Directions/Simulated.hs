-- | A simulated connection: an in-memory byte stream each way, whose lanes
-- hold at most a set number of bytes, as a socket's buffers do. Its events
-- depend only on what was sent and received, so a simulation of directions
-- over it is deterministic.
module Network.Directions.Simulated
  ( SimEnd,
    connect,
  )
where

import Control.Concurrent.STM (STM, TVar, atomically, check, newTVarIO, readTVar, writeTVar)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Network.Directions.Ends (HalfClose (..))
import Network.Streaming.Link (Link (..))
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

instance Link SimEnd where
  watchReadable end = pure (Watch (readable (endIn end)) (pure ()))
  watchWritable end = pure (Watch (writable (endOut end)) (pure ()))
  receiveReady end pointer size = do
    taken <- atomically $ do
      bytes <- readTVar (laneBytes lane)
      if BS.null bytes
        then (\ended -> if ended then Just BS.empty else Nothing) <$> readTVar (laneEnded lane)
        else do
          let (now, rest) = BS.splitAt size bytes
          Just now <$ writeTVar (laneBytes lane) rest
    case taken of
      Nothing -> pure Nothing
      Just chunk -> Just (BS.length chunk) <$ BSU.unsafeUseAsCStringLen chunk (\(from, count) -> copyBytes pointer (castPtr from) count)
    where
      lane = endIn end
  sendReady end pieces = atomically $ do
    bytes <- readTVar (laneBytes lane)
    -- The pieces may be views of a stage's buffer, so the lane keeps a copy,
    -- made now.
    let !chunk = BS.copy (BS.take (laneCapacity lane - BS.length bytes) (BS.concat pieces))
    BS.length chunk <$ (writeTVar (laneBytes lane) $! bytes <> chunk)
    where
      lane = endOut end
  flushReady _ = pure True
  closeLink = shutdownSend

instance HalfClose SimEnd where
  shutdownSend end = atomically (writeTVar (laneEnded (endOut end)) True)

readable :: Lane -> STM ()
readable lane = do
  bytes <- readTVar (laneBytes lane)
  ended <- readTVar (laneEnded lane)
  check (not (BS.null bytes) || ended)

writable :: Lane -> STM ()
writable lane = readTVar (laneBytes lane) >>= check . (< laneCapacity lane) . BS.length
