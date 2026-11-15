-- | A plain reference model of SIEVE, as the paper has it, on lists: the queue holds the entries from the oldest to the newest,
-- each with its visited bit, and the hand is the key of the entry that the next eviction looks at first, or nothing, which means
-- the oldest.
module Model
  ( Model,
    newModel,
    modelStore,
    modelPeek,
    modelRemove,
    modelResize,
    modelFlush,
    modelEntries,
    modelSize,
  )
where

import Data.List (findIndex)

data Slot = Slot {slotKey :: Int, slotValue :: Int, slotVisited :: Bool}

data Model = Model {capacity :: Int, queue :: [Slot], hand :: Maybe Int}

-- | An empty model of this capacity, at least one.
newModel :: Int -> Model
newModel n = Model (max 1 n) [] Nothing

modelEntries :: Model -> [(Int, Int)]
modelEntries model = [(slotKey s, slotValue s) | s <- queue model]

modelSize :: Model -> Int
modelSize = length . queue

-- | Stores the value unless the key is there, evicting as SIEVE does if the queue is full: whether it did, and the entries
-- that were evicted, in order.
modelStore :: Int -> Int -> Model -> (Bool, [(Int, Int)], Model)
modelStore key value model
  | any ((== key) . slotKey) (queue model) = (False, [], model)
  | otherwise = (True, evicted, made {queue = queue made ++ [Slot key value False]})
  where
    (evicted, made) = evictWhile (\m -> length (queue m) >= capacity m) model

-- | The value of the key, which counts as a use.
modelPeek :: Int -> Model -> (Maybe Int, Model)
modelPeek key model = case findIndex ((== key) . slotKey) (queue model) of
  Nothing -> (Nothing, model)
  Just i -> (Just (slotValue (queue model !! i)), model {queue = [if j == i then s {slotVisited = True} else s | (j, s) <- zip [0 ..] (queue model)]})

-- | Removes the key. If the hand looks at it, the hand moves on to the next entry, as the entry after it in the queue, or to the
-- oldest if there is none.
modelRemove :: Int -> Model -> (Maybe Int, Model)
modelRemove key model = case findIndex ((== key) . slotKey) (queue model) of
  Nothing -> (Nothing, model)
  Just i ->
    let after = take i (queue model) ++ drop (i + 1) (queue model)
        moved = if hand model == Just key then slotKey <$> safeIndex i after else hand model
     in (Just (slotValue (queue model !! i)), model {queue = after, hand = moved})

-- | The capacity, at least one: the entries over it are evicted at once, as an insertion evicts them.
modelResize :: Int -> Model -> ([(Int, Int)], Model)
modelResize n model = let (evicted, made) = evictWhile (\m -> length (queue m) > max 1 n) model in (evicted, made {capacity = max 1 n})

-- | Removes every entry, the oldest first.
modelFlush :: Model -> ([(Int, Int)], Model)
modelFlush model = (modelEntries model, model {queue = [], hand = Nothing})

evictWhile :: (Model -> Bool) -> Model -> ([(Int, Int)], Model)
evictWhile full model
  | full model = let (entry, evicted) = evictOne model; (more, done) = evictWhile full evicted in (entry : more, done)
  | otherwise = ([], model)

-- | One eviction: from the hand, or the oldest, each entry that was visited is passed over and loses its bit, with the queue
-- taken round to the oldest after the newest, and the first that wasn't is removed.
evictOne :: Model -> ((Int, Int), Model)
evictOne model = look start (queue model)
  where
    start = maybe 0 id (hand model >>= \key -> findIndex ((== key) . slotKey) (queue model))
    look i slots =
      let slot = slots !! i
       in if slotVisited slot
            then look (if i + 1 >= length slots then 0 else i + 1) [if j == i then s {slotVisited = False} else s | (j, s) <- zip [0 ..] slots]
            else
              let after = take i slots ++ drop (i + 1) slots
               in ((slotKey slot, slotValue slot), model {queue = after, hand = slotKey <$> safeIndex i after})

safeIndex :: Int -> [a] -> Maybe a
safeIndex i xs = case drop i xs of
  x : _ -> Just x
  [] -> Nothing
