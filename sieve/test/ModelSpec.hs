{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | The table with the operations that never wait, for the cache of one core and the one that all cores share: its eviction
-- order is that of a plain reference model of SIEVE, for any sequence of operations and for keys that collide, and what it
-- tells its listeners is what the model evicts.
module ModelSpec (spec) where

import Control.Monad (forM_, unless, when)
import Data.Bits (shiftR, xor)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Sieve
import Data.Word (Word64)
import Model
import Skeletest
import Support

-- | A cache of the sharing, made as its constructor makes it.
type Maker c = forall k v. (Key k) => Config k v -> IO (Cache c k v)

-- | Runs the action with each sharing's constructor.
sharings :: (forall c. Maker c -> IO ()) -> IO ()
sharings action = action newCache >> action newSharedCache

-- | Keys of which only two, and only five, hashes are told apart: the table has to follow its chains.
newtype Two = Two Int deriving (Eq, Show)

newtype Five = Five Int deriving (Eq, Show)

instance Key Two where hashKey (Two k) = k `mod` 2

instance Key Five where hashKey (Five k) = k `mod` 5

data Op = Store Int Int | Peek Int | Remove Int | Resize Int | Flush
  deriving (Show)

-- | A sequence of operations on keys below the bound, from a seed: stores and peeks mostly, with the others now and then.
operations :: Int -> Int -> Word64 -> [Op]
operations count bound = take count . go
  where
    go seed =
      let a = step seed
          b = step a
          c = step b
          key = fromIntegral (b `shiftR` 20) `mod` bound
          value = fromIntegral (c `shiftR` 20) `mod` 1000
          op = case fromIntegral (a `shiftR` 24) `mod` (100 :: Int) of
            n
              | n < 45 -> Store key value
              | n < 80 -> Peek key
              | n < 92 -> Remove key
              | n < 98 -> Resize (1 + fromIntegral (c `shiftR` 30) `mod` (bound * 2 `div` 3 + 1))
              | otherwise -> Flush
       in op : go c
    -- A linear congruential generator, scrambled by an xor so that its low bits are usable.
    step s = let t = s * 6364136223846793005 + 1442695040888963407 in t `xor` (t `shiftR` 29)

-- | Runs the operations on the cache and on the model, and compares what each says, what the cache tells its eviction listener,
-- and the entries in their order of the queue, after each of them.
agrees :: (Key k) => Maker c -> (Int -> k) -> (k -> Int) -> Int -> [Op] -> IO ()
agrees make toKey fromKey cap ops = do
  told <- newIORef []
  cache <- make ((config cap) {evicted = \k v -> modifyIORef' told ((fromKey k, v) :)})
  let run _ [] = pure ()
      run model (op : rest) = do
        writeIORefEmpty told
        (model', expected) <- case op of
          Store k v -> do
            stored <- store cache (toKey k) v
            let (stored', evicted, m) = modelStore k v model
            when (stored /= stored') (failTest ("store " <> show (k, v) <> " said " <> show stored))
            pure (m, evicted)
          Peek k -> do
            found <- peek cache (toKey k)
            let (found', m) = modelPeek k model
            when (found /= found') (failTest ("peek " <> show k <> " said " <> show found))
            pure (m, [])
          Remove k -> do
            removed <- remove cache (toKey k)
            let (removed', m) = modelRemove k model
            when (removed /= removed') (failTest ("remove " <> show k <> " said " <> show removed))
            pure (m, [])
          Resize n -> resize cache n >> let (evicted, m) = modelResize n model in pure (m, evicted)
          Flush -> flush cache >> let (evicted, m) = modelFlush model in pure (m, evicted)
        evictedNow <- reverse <$> readIORef told
        unless (evictedNow == expected) (failTest (show op <> " told the eviction listener " <> show evictedNow <> ", and the model evicts " <> show expected))
        listed <- map (\(k, v) -> (fromKey k, v)) <$> entries cache
        unless (listed == modelEntries model') (failTest (show op <> " left " <> show listed <> ", and the model has " <> show (modelEntries model')))
        size cache >>= \n -> unless (n == modelSize model') (failTest ("the size is " <> show n))
        run model' rest
  run (newModel cap) ops
  where
    writeIORefEmpty ref = modifyIORef' ref (const [])

spec :: Spec
spec = describe "the table" $ do
  itNeeds [Table, Shared] "evicts as the reference model of SIEVE does, for sequences of stores, peeks, removals, resizes and flushes" $
      sharings $ \make ->
        forM_ [1, 2, 3, 5, 8, 16] $ \cap -> forM_ [1 .. 40] $ \seed -> agrees make id id cap (operations 300 (cap * 3) (fromIntegral (seed * 7919 + cap)))

  itNeeds [Table, Shared] "does the same for keys that collide, so that the chains of the table are followed and shifted back" $
      sharings $ \make ->
        forM_ [1, 2, 3, 5, 8, 16, 32] $ \cap -> forM_ [1 .. 40] $ \seed -> do
          agrees make Two (\(Two k) -> k) cap (operations 300 (cap * 3) (fromIntegral (seed * 104729 + cap)))
          agrees make Five (\(Five k) -> k) cap (operations 300 (cap * 3) (fromIntegral (seed * 1299709 + cap)))

  describe "one by one" $ do
    itNeeds [Table] "finds only what was stored, and keeps the value that is there" $ do
        cache <- newCache (config 4)
        peek cache (1 :: Int) `shouldReturn` (Nothing :: Maybe Int)
        store cache 1 10 `shouldReturn` True
        store cache 1 20 `shouldReturn` False
        peek cache 1 `shouldReturn` Just 10
        size cache `shouldReturn` 1

    itNeeds [Table] "evicts the oldest entry that was not visited, and keeps the ones that were peeked" $ do
        cache <- newCache (config 2)
        mapM_ (\k -> store cache k (k * 10)) [1, 2 :: Int]
        peek cache 1 `shouldReturn` Just 10
        store cache 3 30 `shouldReturn` True
        peek cache 1 `shouldReturn` Just 10
        peek cache 2 `shouldReturn` Nothing
        entries cache `shouldReturn` [(1, 10), (3, 30)]

    itNeeds [Table] "takes the hand on after an eviction, and back to the oldest once it is past the newest" $ do
        cache <- newCache (config 3)
        mapM_ (\k -> store cache k k) [1, 2, 3 :: Int]
        mapM_ (peek cache) [1, 3]
        -- 1 is passed over and loses its bit, and 2 goes. The hand stands at 3.
        store cache 4 4 `shouldReturn` True
        entries cache `shouldReturn` [(1, 1), (3, 3), (4, 4)]
        -- 3 is passed over and loses its bit, and 4 goes, the newest: the hand is past it.
        store cache 5 5 `shouldReturn` True
        entries cache `shouldReturn` [(1, 1), (3, 3), (5, 5)]
        -- The hand starts at the oldest again: 1 was not visited since, and goes.
        store cache 6 6 `shouldReturn` True
        entries cache `shouldReturn` [(3, 3), (5, 5), (6, 6)]

    itNeeds [Table] "keeps evicting after a removal, also of the entry that the hand looks at" $ do
        cache <- newCache (config 2)
        mapM_ (\k -> store cache k k) [1, 2 :: Int]
        remove cache 1 `shouldReturn` Just 1
        remove cache 1 `shouldReturn` Nothing
        mapM_ (\k -> store cache k k) [3, 4, 5]
        entries cache `shouldReturn` [(4, 4), (5, 5)]

    itNeeds [Table] "has room again for every entry that was removed" $ do
        cache <- newCache (config 4)
        forM_ [1 .. 50 :: Int] $ \round' -> do
          mapM_ (\k -> store cache (round' * 10 + k) k) [1 .. 4]
          mapM_ (\k -> remove cache (round' * 10 + k)) [1 .. 4]
        size cache `shouldReturn` 0
        mapM_ (\k -> store cache k k) [1 .. 4 :: Int]
        size cache `shouldReturn` 4

  describe "its listeners" $ do
    itNeeds [Table] "tells the eviction listener of each entry that is evicted, flushed or dropped by a resize, and not of one that is removed" $ do
        told <- newLog
        cache <- newCache ((config 2) {evicted = \k v -> noteIn told (show (k :: Int, v :: Int))})
        mapM_ (\k -> store cache k (k * 10)) [1, 2, 3]
        readLog told `shouldReturn` ["(1,10)"]
        remove cache 2 `shouldReturn` Just 20
        readLog told `shouldReturn` ["(1,10)"]
        mapM_ (\k -> store cache k (k * 10)) [4, 5]
        flush cache
        readLog told `shouldReturn` ["(1,10)", "(3,30)", "(4,40)", "(5,50)"]
        mapM_ (\k -> store cache k (k * 10)) [6, 7]
        resize cache 1
        readLog told `shouldReturn` ["(1,10)", "(3,30)", "(4,40)", "(5,50)", "(6,60)"]

  describe "resizing" $ do
    itNeeds [Table] "keeps every entry of a cache that grows, and has room for more" $ do
        cache <- newCache (config 2)
        mapM_ (\k -> store cache k k) [1, 2 :: Int]
        resize cache 5
        mapM_ (\k -> store cache k k) [3, 4, 5]
        entries cache `shouldReturn` [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]

    itNeeds [Table] "evicts what is over the capacity at once, in the order of SIEVE, and stays at one entry at least" $ do
        cache <- newCache (config 4)
        mapM_ (\k -> store cache k k) [1 .. 4 :: Int]
        peek cache 1 `shouldReturn` Just 1
        resize cache 2
        entries cache `shouldReturn` [(1, 1), (4, 4)]
        resize cache 0
        size cache `shouldReturn` 1
        resize cache 3
        mapM_ (\k -> store cache k k) [7, 8]
        size cache `shouldReturn` 3
