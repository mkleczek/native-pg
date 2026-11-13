-- | What the runtime knows about a descriptor, by its number, in memory that the whole process shares: the run that
-- armed it last. A descriptor with a number that the table has no room for is not known: every call here treats it as one
-- that nothing has been noted of. The operating system gives a closed descriptor's number to the next socket, and nothing
-- here is told of a close, so whatever the table holds for a number may be of an earlier socket: whoever makes a socket
-- says so with 'fresh'.
module Network.Streaming.Internal.Descriptors
  ( newRunNumber,
    armedBy,
    fresh,
  )
where

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

-- | For each descriptor number, the number of the run that armed it last, or 0.
table :: Ptr Word8
table = unsafePerformIO (callocBytes limit)
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
      previous <- peekByteOff table descriptor :: IO Word8
      pokeByteOff table descriptor run
      pure (previous /= 0 && previous /= run)

-- | The socket is new: nothing that the table holds for its number is of it.
fresh :: Socket -> IO ()
fresh sock = withFdSocket sock $ \fd ->
  let descriptor = fromIntegral fd
   in if descriptor < 0 || descriptor >= limit then pure () else pokeByteOff table descriptor (0 :: Word8)
