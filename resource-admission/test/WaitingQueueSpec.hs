module WaitingQueueSpec (spec) where

import Control.Concurrent (killThread)
import Control.Concurrent.Resource.Waiting
import Control.Exception (throwIO)
import Data.Either (isLeft)
import Data.List (inits)
import Control.Concurrent.Resource.Waiting.Queue qualified as Q
import Control.Concurrent.STM
import Control.Monad (foldM, forM_, forM)
import Skeletest
import TestSupport (within, forkResult, await)

data Operation = Append | Remove Int

data Model = Model Int [(Int, Q.Entry Int)]

spec :: Spec
spec = xfail "FIFO API scaffold: ordered links follow in the next change" $ describe "opaque FIFO" $ do
  it "preserves FIFO grant order after cancelling a middle opaque registration" $ withWaiting $ \waiting -> do
    queue <- atomically Q.newQueue
    registered <- newEmptyTMVarIO
    removed <- newTVarIO (0 :: Int)
    cleaned <- newTVarIO (0 :: Int)
    workers <- forM [1 .. 3 :: Int] $ \index -> do
      worker <- forkResult $ withWait waiting NoDeadline
        (\waiter -> do
          entry <- Q.append queue (index, waiter)
          putTMVar registered waiter
          pure entry)
        (\entry -> Q.remove entry >> modifyTVar' removed (+ 1))
        (const (atomically (modifyTVar' cleaned (+ 1)))) pure
      waiter <- within (atomically (takeTMVar registered))
      pure (worker, waiter)
    case workers of
      [((_, firstResult), firstWaiter), ((middle, middleResult), middleWaiter), ((_, lastResult), lastWaiter)] -> do
        killThread middle
        await middleResult >>= (\outcome -> isLeft outcome `shouldBe` True)
        assertTerminal middleWaiter
        readTVarIO removed >>= (`shouldBe` 1)
        atomically (fmap fst <$> firstValue queue) >>= (`shouldBe` Just 1)
        atomically (grantFirst queue) >>= (`shouldBe` Just 1)
        assertTerminal firstWaiter
        readTVarIO removed >>= (`shouldBe` 2)
        atomically (fmap fst <$> firstValue queue) >>= (`shouldBe` Just 3)
        atomically (grantFirst queue) >>= (`shouldBe` Just 3)
        assertTerminal lastWaiter
        await firstResult >>= either throwIO (`shouldBe` Right 1)
        await lastResult >>= either throwIO (`shouldBe` Right 3)
      _ -> fail "expected three registered waiters"
    atomically (Q.isEmpty queue) >>= (`shouldBe` True)
    readTVarIO removed >>= (`shouldBe` 3)
    readTVarIO cleaned >>= (`shouldBe` 2)
  it "appends immediately and removes arbitrary entries without changing FIFO order" $
    runOperations $ replicate 16 Append
      ++ map Remove [7, 0, 15, 4, 1, 14, 8, 2, 13, 3, 12, 5, 11, 6, 10, 9]
      ++ [Append, Remove 16, Append, Remove 17, Append]
  it "removes entries from independent queues with interleaved head, tail and singleton removal" $ do
    (left, right, a, b, c, x, y) <- atomically $ do
      left <- Q.newQueue
      right <- Q.newQueue
      a <- Q.append left (1 :: Int)
      x <- Q.append right (10 :: Int)
      b <- Q.append left 2
      y <- Q.append right 20
      c <- Q.append left 3
      pure (left, right, a, b, c, x, y)
    let assertQueues expectedLeft expectedRight = do
          atomically (firstValue left) >>= (`shouldBe` expectedLeft)
          atomically (firstValue right) >>= (`shouldBe` expectedRight)
    atomically (Q.remove a)
    assertQueues (Just 2) (Just 10)
    atomically (Q.remove y)
    assertQueues (Just 2) (Just 10)
    atomically (Q.remove c)
    assertQueues (Just 2) (Just 10)
    atomically (Q.remove x)
    assertQueues (Just 2) Nothing
    atomically (Q.remove b)
    assertQueues Nothing Nothing
    z <- atomically (Q.append right 30)
    assertQueues Nothing (Just 30)
    atomically (Q.remove z)
    assertQueues Nothing Nothing
  it "keeps an append linked when the sole existing entry is removed concurrently" $ within $ do
    queue <- atomically Q.newQueue
    old <- atomically (Q.append queue (1 :: Int))
    (added, ()) <- together (Q.append queue 2) (Q.remove old)
    atomically (firstValue queue) >>= (`shouldBe` Just 2)
    atomically (Q.remove added)
    atomically (Q.isEmpty queue) >>= (`shouldBe` True)
    final <- atomically (Q.append queue 3)
    atomically (firstValue queue) >>= (`shouldBe` Just 3)
    atomically (Q.remove final)
    atomically (Q.isEmpty queue) >>= (`shouldBe` True)
  it "preserves both survivors when adjacent entries are removed concurrently" $ within $ do
    queue <- atomically Q.newQueue
    [headEntry, left, right, tailEntry] <- atomically $ mapM (Q.append queue) [1 .. 4 :: Int]
    _ <- together (Q.remove left) (Q.remove right)
    atomically (firstValue queue) >>= (`shouldBe` Just 1)
    atomically (Q.remove headEntry)
    atomically (firstValue queue) >>= (`shouldBe` Just 4)
    atomically (Q.remove tailEntry)
    atomically (Q.isEmpty queue) >>= (`shouldBe` True)

together :: STM a -> STM b -> IO (a, b)
together left right = do
  ready <- newTVarIO (0 :: Int)
  start <- newEmptyTMVarIO
  let worker action = forkResult $ do
        atomically (modifyTVar' ready (+ 1))
        atomically (readTMVar start >> action)
  (_, leftResult) <- worker left
  (_, rightResult) <- worker right
  atomically $ do
    readTVar ready >>= check . (== 2)
    putTMVar start ()
  a <- await leftResult >>= either throwIO pure
  b <- await rightResult >>= either throwIO pure
  pure (a, b)

firstValue :: Q.Queue a -> STM (Maybe a)
firstValue queue = fmap Q.value <$> Q.first queue

grantFirst :: Q.Queue (Int, Waiter Int) -> STM (Maybe Int)
grantFirst queue = do
  entry <- Q.first queue
  case Q.value <$> entry of
    Nothing -> pure Nothing
    Just (index, waiter) -> do
      granted <- tryGrant waiter (pure index)
      pure (if granted then Just index else Nothing)

-- Only the Waiter survives removal. Terminal operations must never run its
-- queue-removal hook again, even when an adapter retains that Waiter.
assertTerminal :: Waiter Int -> IO ()
assertTerminal waiter = do
  outcome <- atomically $ do
    rejected <- reject waiter Closed
    granted <- tryGrant waiter (pure 99)
    rejectedAgain <- reject waiter TimedOut
    pure (rejected, granted, rejectedAgain)
  outcome `shouldBe` (False, False, False)

runOperations :: [Operation] -> IO ()
runOperations operations = within $
  forM_ (inits operations) $ \prefix -> do
    queue <- atomically Q.newQueue
    Model _ live <- foldM (step queue) (Model 0 []) prefix
    let expected = map fst live
    actual <- drain (length live) queue
    actual `shouldBe` expected
    atomically (Q.isEmpty queue) >>= (`shouldBe` True)

step :: Q.Queue Int -> Model -> Operation -> IO Model
step queue (Model ticket live) operation = do
  updated@(Model _ remaining) <- case operation of
    Append -> do
      entry <- atomically $ Q.append queue ticket
      pure (Model (ticket + 1) (live ++ [(ticket, entry)]))
    Remove index -> case lookup index live of
      Nothing -> pure (Model ticket live)
      Just entry -> do
        atomically $ Q.remove entry
        pure (Model ticket (filter ((/= index) . fst) live))
  atomically (Q.isEmpty queue) >>= (`shouldBe` null remaining)
  pure updated

drain :: Int -> Q.Queue a -> IO [a]
drain remaining queue = do
  entry <- atomically (Q.first queue)
  case entry of
    Nothing -> pure []
    Just current
      | remaining <= 0 -> fail "queue exceeds live model size"
      | otherwise -> do
          value <- atomically $ do
            let value = Q.value current
            value `seq` Q.remove current
            pure value
          (value :) <$> drain (remaining - 1) queue
