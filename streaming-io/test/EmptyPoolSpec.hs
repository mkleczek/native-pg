{-# LANGUAGE DataKinds #-}

-- | A pool that has nothing free when it is made, as the arrows' programs use it: the same programs for every arrow.
-- Every change of a pool is a compare-and-swap of its state, so the first change of a pool that has just been made
-- is the one that shows whether the state it was made with is a value or still to be worked out.
module EmptyPoolSpec (spec) where

import Control.Arrow ((^>>))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad (forM_)
import Data.IORef (newIORef, readIORef)
import Network.Streaming.Classes hiding (within)
import Network.Streaming.Combinators (concurrently)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Pool
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

type Program a = forall arr. (Runtime arr) => arr () a

data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners =
  [ Runner "threads" onThreads,
    Runner "delimited" onDelimited,
    Runner "compiled" onCompiled
  ]

onThreads, onDelimited, onCompiled :: Program a -> IO a
onThreads program = runThreads program ()
onDelimited program = fst <$> runDelimited program ()
onCompiled program = fst <$> runCompiled program ()

spec :: Spec
spec = describe "a pool made empty" $
  forM_ runners $ \(Runner name run) -> describe name $ do
    it "gives nothing to a part that takes at once, when it was made of a list that is empty only at run time" $ do
      pool <- newPool =<< emptyAtRunTime
      within (run (now (const (takeNow pool 1)))) `shouldReturn` Just Nothing

    it "frees what a part releases, when it was made of []" $ do
      pool <- newPool []
      within (run (now (const (releaseNow pool [7 :: Int] >> free pool)))) `shouldReturn` Just [7]

    it "gives a part that checks out an item that another part releases, when it was made of []" $ do
      pool <- newPool []
      within (run (concurrently (checkout pool) (const 7 ^>> release pool))) `shouldReturn` Just (7 :: Int, ())

-- | A list that is empty, and that nothing at compile time says is empty: the state of a pool that is made of it is
-- built when the pool is made.
emptyAtRunTime :: IO [Int]
emptyAtRunTime = newIORef [] >>= readIORef

-- | A test that waits for ever, or spins, fails instead. The program runs on a thread of its own: a scheduler runs its
-- parts masked, so a part that spins can't be interrupted, and the thread that waits for it gives up instead (the spinning
-- thread is left behind).
within :: IO a -> IO (Maybe a)
within action = do
  result <- newEmptyMVar
  _ <- forkIO (action >>= putMVar result)
  timeout 3000000 (takeMVar result)
