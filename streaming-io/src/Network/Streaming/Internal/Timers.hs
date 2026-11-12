{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

-- | A hashed timer wheel for one scheduler thread: no locks, and inserting and cancelling a timer cost the same
-- however many there are, since most timers are cancelled before they fire. Time is in nanoseconds, from a
-- monotonic clock; a slot is a millisecond.
--
-- Three levels hold the timers by how far off they are: the slots of the first are a millisecond each, and cover
-- the next 1,024 ms; the slots of the second are 1,024 ms each, and cover about 17 minutes; the rest are in an
-- ordered map by blocks of those. As time passes the timers of a block of the next level move into the one
-- before. A cancelled timer is marked and dropped when its slot is reached, or when there are more of those than
-- of the live ones.
--
-- A timer never fires before its time, and fires at the first 'expire' that is at or past it: the millisecond is
-- where a timer is kept, not how exactly it is timed.
module Network.Streaming.Internal.Timers
  ( Wheel,
    Timer,
    newWheel,
    addTimer,
    cancelTimer,
    expire,
    nextDeadline,
    pending,
  )
where

import Control.Monad (forM_, when)
import Data.Bits (shiftL, shiftR, (.&.))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.Word (Word64)
import GHC.IOArray (IOArray, newIOArray, unsafeReadIOArray, unsafeWriteIOArray)

-- | A timer: whether it is still to fire, when, and what it does.
data Timer = Timer !(IORef Bool) !Word64 (IO ())

-- | The slots of each level, the timers beyond them by block, the millisecond to expire next, how many timers
-- are live and how many are in the wheel, live or not.
data Wheel = Wheel
  { wheelNear :: !(IOArray Int [Timer]),
    wheelFar :: !(IOArray Int [Timer]),
    wheelBeyond :: !(IORef (IntMap.IntMap [Timer])),
    wheelTick :: !(IORef Int),
    wheelLive :: !(IORef Int),
    wheelHeld :: !(IORef Int)
  }

slots :: Int
slots = 1024

-- | A wheel that starts at the time.
newWheel :: Word64 -> IO Wheel
newWheel now = do
  near <- newIOArray (0, slots - 1) []
  far <- newIOArray (0, slots - 1) []
  Wheel near far <$> newIORef IntMap.empty <*> newIORef (millisecondOf now) <*> newIORef 0 <*> newIORef 0

-- | The millisecond slot that the time is in.
millisecondOf :: Word64 -> Int
millisecondOf nanoseconds = fromIntegral (nanoseconds `div` 1000000)

-- | How many timers are live.
pending :: Wheel -> IO Int
pending = readIORef . wheelLive

-- | A timer that does the action at the time, or at the next expiry if that has passed.
addTimer :: Wheel -> Word64 -> IO () -> IO Timer
addTimer wheel deadline action = do
  live <- newIORef True
  let timer = Timer live deadline action
  tick <- readIORef (wheelTick wheel)
  place wheel tick (max tick (millisecondOf deadline)) timer
  count <- readIORef (wheelLive wheel)
  writeIORef (wheelLive wheel) $! count + 1
  held <- readIORef (wheelHeld wheel)
  writeIORef (wheelHeld wheel) $! held + 1
  pure timer

-- | Puts the timer in the slot that the millisecond belongs to, from the millisecond that is next.
place :: Wheel -> Int -> Int -> Timer -> IO ()
place wheel tick millisecond timer
  | millisecond - tick < slots = push (wheelNear wheel) (millisecond .&. (slots - 1))
  | (millisecond `shiftR` 10) - (tick `shiftR` 10) < slots = push (wheelFar wheel) ((millisecond `shiftR` 10) .&. (slots - 1))
  | otherwise = do
      beyond <- readIORef (wheelBeyond wheel)
      writeIORef (wheelBeyond wheel) $! IntMap.insertWith (++) (millisecond `shiftR` 20) [timer] beyond
  where
    push array index = unsafeReadIOArray array index >>= unsafeWriteIOArray array index . (timer :)

-- | The timer won't fire. It says whether it was still to: cancelling it again, or after it fired, changes
-- nothing.
cancelTimer :: Wheel -> Timer -> IO Bool
cancelTimer wheel (Timer live _ _) = do
  wasLive <- readIORef live
  when wasLive $ do
    writeIORef live False
    count <- readIORef (wheelLive wheel)
    writeIORef (wheelLive wheel) $! count - 1
    held <- readIORef (wheelHeld wheel)
    -- When most of what the wheel holds is cancelled, it drops that.
    when (held > 4 * (count - 1) + 4096) (compact wheel)
  pure wasLive

-- | Drops the timers that were cancelled.
compact :: Wheel -> IO ()
compact wheel = do
  let sweep array = forM_ [0 .. slots - 1] $ \index -> unsafeReadIOArray array index >>= \timers -> if null timers then pure () else filterLive timers >>= unsafeWriteIOArray array index
  sweep (wheelNear wheel)
  sweep (wheelFar wheel)
  beyond <- readIORef (wheelBeyond wheel)
  beyond' <- IntMap.filter (not . null) <$> traverse filterLive beyond
  writeIORef (wheelBeyond wheel) $! beyond'
  count <- readIORef (wheelLive wheel)
  writeIORef (wheelHeld wheel) $! count

filterLive :: [Timer] -> IO [Timer]
filterLive = go []
  where
    go kept [] = pure (reverse kept)
    go kept (timer@(Timer live _ _) : others) = readIORef live >>= \alive -> go (if alive then timer : kept else kept) others

-- | Fires the timers that are due at the time, in the order of their milliseconds, and in the order they were
-- added within one. A timer that one of them adds is due at the next expiry at the soonest.
expire :: Wheel -> Word64 -> IO ()
expire wheel now = loop
  where
    target = millisecondOf now
    loop = do
      tick <- readIORef (wheelTick wheel)
      live <- readIORef (wheelLive wheel)
      case () of
        _
          | live == 0 -> writeIORef (wheelTick wheel) $! max tick target
          | tick < target -> do
              -- A whole millisecond has passed: all that it holds is due. The wheel goes on to the next before
              -- they fire, so that those they add are for the slots to come.
              let index = tick .&. (slots - 1)
              due <- unsafeReadIOArray (wheelNear wheel) index
              unsafeWriteIOArray (wheelNear wheel) index []
              writeIORef (wheelTick wheel) $! tick + 1
              when ((tick + 1) .&. (slots - 1) == 0) (cascade wheel (tick + 1))
              fire (reverse due)
              loop
          | otherwise -> do
              -- This millisecond is not over: what is due is what has a time at or before now.
              let index = tick .&. (slots - 1)
              here <- unsafeReadIOArray (wheelNear wheel) index
              (due, later) <- split here
              unsafeWriteIOArray (wheelNear wheel) index later
              fire (reverse due)
    -- The timers that are due, and the others that are live; the cancelled are dropped.
    split = go [] []
      where
        go due later [] = pure (reverse due, reverse later)
        go due later (timer@(Timer alive deadline _) : others) =
          readIORef alive >>= \isLive ->
            if not isLive
              then do
                held <- readIORef (wheelHeld wheel)
                writeIORef (wheelHeld wheel) $! held - 1
                go due later others
              else if deadline <= now then go (timer : due) later others else go due (timer : later) others
    fire = mapM_ $ \(Timer alive _ action) -> do
      stillLive <- readIORef alive
      held <- readIORef (wheelHeld wheel)
      writeIORef (wheelHeld wheel) $! held - 1
      when stillLive $ do
        writeIORef alive False
        count <- readIORef (wheelLive wheel)
        writeIORef (wheelLive wheel) $! count - 1
        action

-- | At the start of a block of a level: its timers go to the level before.
cascade :: Wheel -> Int -> IO ()
cascade wheel tick = do
  when (tick .&. ((1 `shiftL` 20) - 1) == 0) $ do
    beyond <- readIORef (wheelBeyond wheel)
    case IntMap.lookup (tick `shiftR` 20) beyond of
      Nothing -> pure ()
      Just timers -> do
        writeIORef (wheelBeyond wheel) $! IntMap.delete (tick `shiftR` 20) beyond
        forM_ (reverse timers) (relocate wheel tick)
  let index = (tick `shiftR` 10) .&. (slots - 1)
  timers <- unsafeReadIOArray (wheelFar wheel) index
  unsafeWriteIOArray (wheelFar wheel) index []
  forM_ (reverse timers) (relocate wheel tick)

-- | Moves a timer that came down a level to the slot it belongs to now, if it is live.
relocate :: Wheel -> Int -> Timer -> IO ()
relocate wheel tick timer@(Timer live deadline _) = do
  alive <- readIORef live
  if alive
    then place wheel tick (max tick (millisecondOf deadline)) timer
    else do
      held <- readIORef (wheelHeld wheel)
      writeIORef (wheelHeld wheel) $! held - 1

-- | When the next live timer is due, if there is one.
nextDeadline :: Wheel -> IO (Maybe Word64)
nextDeadline wheel = do
  live <- readIORef (wheelLive wheel)
  if live == 0
    then pure Nothing
    else do
      tick <- readIORef (wheelTick wheel)
      nearest <- firstLive (wheelNear wheel) [tick .. tick + slots - 1]
      case nearest of
        Just deadline -> pure (Just deadline)
        Nothing -> do
          farther <- firstLive (wheelFar wheel) [(tick `shiftR` 10) .. (tick `shiftR` 10) + slots - 1]
          case farther of
            Just deadline -> pure (Just deadline)
            Nothing -> readIORef (wheelBeyond wheel) >>= earliest . concat . IntMap.elems
  where
    -- The earliest live deadline of the first of the slots that has one.
    firstLive array = go
      where
        go [] = pure Nothing
        go (position : others) =
          unsafeReadIOArray array (position .&. (slots - 1)) >>= earliest >>= \case
            Just deadline -> pure (Just deadline)
            Nothing -> go others

-- | The earliest deadline of the live timers.
earliest :: [Timer] -> IO (Maybe Word64)
earliest = go Nothing
  where
    go !found [] = pure found
    go !found (Timer alive deadline _ : others) =
      readIORef alive >>= \isLive -> go (if isLive then Just (maybe deadline (min deadline) found) else found) others
