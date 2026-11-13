-- | What the runtime knows about a descriptor, by its number, in memory that the whole process shares: the run that
-- armed it last, and what the last calls on it say of whether the next receive would find anything. A receive that gives
-- fewer bytes than it asked for has drained the socket, and if the socket sent something after that, the other end can't
-- have answered yet: the next receive waits before it tries. A number that the table has no room for is treated as one that
-- nothing is known of. The operating system gives a closed descriptor's number to the next socket, and nothing here is told
-- of a close, so what the table holds for a number may be of an earlier socket: whoever makes a socket says so with 'fresh'.
module Network.Streaming.Internal.Descriptors
  ( newRunNumber,
    armedBy,
    skipsReceive,
    receiveNoted,
    sendNoted,
    receivesFirst,
    fresh,
  )
where

import Control.Monad (when)
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Word (Word8)
import Foreign.Marshal.Alloc (callocBytes)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peekByteOff, pokeByteOff)
import Network.Socket (Socket, withFdSocket)
import System.IO.Unsafe (unsafePerformIO)

-- | How many descriptor numbers the table has room for. The operating system gives numbers from 0 up, so a server
-- that holds a few thousand connections is far inside it, and the pages of the table that no descriptor reaches are
-- never touched.
limit :: Int
limit = 1048576

-- | Two bytes for each descriptor number: what the receive and send calls say (none, the last receive was short, or the next
-- receive waits first), and the number of the run that armed it last, or 0.
table :: Ptr Word8
table = unsafePerformIO (callocBytes (2 * limit))
{-# NOINLINE table #-}

runs :: IORef Int
runs = unsafePerformIO (newIORef 0)
{-# NOINLINE runs #-}

-- | A number for a new run, from 1 to 255: the numbers of two runs that are alive at once differ unless there are more than 255.
newRunNumber :: IO Word8
newRunNumber = atomicModifyIORef' runs (\n -> (n + 1, fromIntegral (n `mod` 255) + 1))

-- | The run arms the descriptor: whether another run armed it before, as a descriptor that moved between cores is.
armedBy :: Int -> Word8 -> IO Bool
armedBy descriptor run
  | descriptor < 0 || descriptor >= limit = pure False
  | otherwise = do
      previous <- peekByteOff table (2 * descriptor + 1) :: IO Word8
      pokeByteOff table (2 * descriptor + 1) run
      pure (previous /= 0 && previous /= run)

-- | Whether the next receive on the descriptor should wait before it tries, because the one before was short and a send
-- followed. It says so once: the receive after that one tries first. Waiting first is right whatever the table holds, since
-- arming a descriptor reports readiness that is already there, so a flag that is stale costs a wait that a try would have
-- saved, and nothing else.
skipsReceive :: Int -> IO Bool
skipsReceive descriptor
  | descriptor < 0 || descriptor >= limit = pure False
  | otherwise = do
      state <- peekByteOff table (2 * descriptor) :: IO Word8
      if state /= waitsFirst then pure False else True <$ pokeByteOff table (2 * descriptor) none

-- Not inlined: its range check would split into several calls of the receive's retry loop, which is then allocated as a closure for each receive.
{-# NOINLINE skipsReceive #-}

-- | Notes a receive that asked for this many bytes at most and got this many, none if it found nothing: a stream socket that
-- gives fewer than were asked for has nothing more to give.
receiveNoted :: Int -> Int -> Int -> IO ()
receiveNoted descriptor asked got
  | descriptor < 0 || descriptor >= limit = pure ()
  | otherwise = pokeByteOff table (2 * descriptor) (if got > 0 && got < asked then drained else none)

-- | Notes a send that gave bytes: after a short receive, the other end has been given something to answer.
sendNoted :: Int -> IO ()
sendNoted descriptor
  | descriptor < 0 || descriptor >= limit = pure ()
  | otherwise = do
      state <- peekByteOff table (2 * descriptor) :: IO Word8
      when (state == drained) (pokeByteOff table (2 * descriptor) waitsFirst)

none, drained, waitsFirst :: Word8
none = 0
drained = 1
waitsFirst = 2

-- | The next receive on the descriptor tries before it waits.
receivesFirst :: Int -> IO ()
receivesFirst descriptor
  | descriptor < 0 || descriptor >= limit = pure ()
  | otherwise = pokeByteOff table (2 * descriptor) none

-- | The socket is new: nothing that the table holds for its number is of it.
fresh :: Socket -> IO ()
fresh sock = withFdSocket sock $ \fd ->
  let descriptor = fromIntegral fd
   in if descriptor < 0 || descriptor >= limit then pure () else pokeByteOff table (2 * descriptor) (0 :: Word8) *> pokeByteOff table (2 * descriptor + 1) (0 :: Word8)
