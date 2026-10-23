module TestSupport (within, forkResult, await, eventually, awaitBlocked, collected) where
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import GHC.Conc (ThreadStatus(..), BlockReason(..), threadStatus)
import System.Mem (performGC)
import System.Timeout (timeout)

within :: IO a -> IO a
within action = timeout 3000000 action >>= maybe (fail "test exceeded three seconds") pure
forkResult :: IO a -> IO (ThreadId, MVar (Either SomeException a))
forkResult action = do
  result <- newEmptyMVar
  tid <- forkIO (try action >>= putMVar result)
  pure (tid, result)
await :: MVar a -> IO a
await = within . takeMVar
eventually :: STM Bool -> IO ()
eventually condition = within (atomically (condition >>= check))

-- A blocked private mailbox proves registration committed. Successful immediate
-- grants may already have finished, which also satisfies the startup barrier.
awaitBlocked :: ThreadId -> IO ()
awaitBlocked tid = within loop
  where
    loop = threadStatus tid >>= \status -> case status of
      ThreadBlocked BlockedOnSTM -> pure ()
      ThreadBlocked BlockedOnMVar -> pure ()
      ThreadFinished -> pure ()
      ThreadDied -> pure ()
      _ -> yield >> loop

-- Retry collection because a just-completed worker may still be unwinding its
-- stack during the first GC. This does not depend on timer deadlines elapsing.
collected :: MVar () -> IO ()
collected finalized = within loop
  where
    loop = do
      performGC
      done <- tryReadMVar finalized
      case done of
        Just () -> pure ()
        Nothing -> threadDelay 1000 >> loop
