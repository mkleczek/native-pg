{-# LANGUAGE InterruptibleFFI #-}
{-# LANGUAGE LambdaCase #-}

-- | The scheduler of the arrows that run every part of a program on one thread
-- as code: "Network.Directions.Compiled" and "Network.Directions.Delimited". A
-- part is an action that runs until it ends, waits, forks or yields, and says
-- which. The scheduler owns a poller (kqueue, or epoll on Linux): a descriptor
-- is registered with it once, for edges of both readability and writability,
-- and the scheduler asks it what became ready when no part can go on, or once
-- a round otherwise, so that a wait costs neither a registration nor a thread
-- of GHC's IO manager. A part that waits for a ticket is held by the ticket.
module Network.Directions.Scheduler
  ( Step (..),
    Run,
    newRun,
    spent,
    stepped,
    counted,
    wakeTicket,
    Slot (..),
    schedule,
  )
where

import Control.Concurrent.STM (atomically, check, orElse, readTVar, registerDelay, retry)
import Control.Exception (finally)
import Control.Monad ((>=>))
import Data.Bits ((.&.))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.List (partition)
import Foreign.C.Error (eINTR, getErrno, throwErrno, throwErrnoIfMinus1, throwErrnoIfMinus1_)
import Foreign.C.Types (CInt (..))
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peekElemOff, pokeElemOff)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Directions.Classes (Event (..))
import Network.Directions.Loop (Report (..))
import Network.Streaming.Watch (Watch (..))
import System.Posix.Types (Fd (..))

-- | What a part comes to.
data Step
  = -- | It ended, at a join or with the program's result.
    Ended
  | -- | It waits for the event, and goes on with the action once it fires.
    Suspended !Event (IO Step)
  | -- | Two parts that go on at once.
    Forked (IO Step) (IO Step)
  | -- | It waits for a ticket, whose slot holds it.
    Parked
  | -- | It used its steps, and can go on.
    Yielded (IO Step)

-- | A ticket's slot: empty, given its value, or holding the part that waits
-- for it, as what goes on with the value.
data Slot a = Empty | Given a | Waiter (a -> IO Step)

-- | What the parts of a run share.
data Run = Run
  { -- | Parts that tickets woke.
    runTickets :: !(IORef [IO Step]),
    -- | The steps the running part has left, the steps taken, and the waits:
    -- unboxed, so that counting allocates nothing.
    runCounters :: !(ForeignPtr Int),
    runPoller :: !CInt,
    -- | Where the poller writes what became ready: descriptors, and kinds.
    runReady :: !(ForeignPtr CInt),
    runKinds :: !(ForeignPtr CInt),
    -- | The parts that wait on each registered descriptor: to read, to write.
    runWaiters :: !(IORef (IntMap.IntMap Waiters))
  }

data Waiters = Waiters [IO Step] [IO Step]

-- | How many readinesses one poll gives at most.
batch :: Int
batch = 64

newRun :: IO Run
newRun = do
  counters <- mallocForeignPtrArray 3
  unsafeWithForeignPtr counters $ \pointer -> mapM_ (\index -> pokeElemOff pointer index 0) [0, 1, 2]
  poller <- throwErrnoIfMinus1 "directions_poller_new" c_pollerNew
  Run <$> newIORef [] <*> pure counters <*> pure poller <*> mallocForeignPtrArray batch <*> mallocForeignPtrArray batch <*> newIORef IntMap.empty

counter :: Run -> Int -> IO Int
counter run index = unsafeWithForeignPtr (runCounters run) (`peekElemOff` index)
{-# INLINE counter #-}

setCounter :: Run -> Int -> Int -> IO ()
setCounter run index value = unsafeWithForeignPtr (runCounters run) (\pointer -> pokeElemOff pointer index value)
{-# INLINE setCounter #-}

-- | Counts a step of a primitive, and says whether the part has used its steps
-- and yields.
spent :: Run -> IO Bool
spent run = do
  counter run 1 >>= setCounter run 1 . (+ 1)
  remaining <- counter run 0
  if remaining <= 1 then pure True else False <$ setCounter run 0 (remaining - 1)
{-# INLINE spent #-}

-- | Counts a step of a primitive, and goes on, or yields once the part has
-- used its steps.
stepped :: Run -> IO Step -> IO Step
stepped run continue = spent run >>= \yields -> if yields then pure (Yielded continue) else continue
{-# INLINE stepped #-}

-- | Counts a step of a primitive that suspends its part.
counted :: Run -> Step -> IO Step
counted run step = step <$ (counter run 1 >>= setCounter run 1 . (+ 1))
{-# INLINE counted #-}

-- | Puts a part that a ticket woke back among those that can go on.
wakeTicket :: Run -> IO Step -> IO ()
wakeTicket run part = modifyIORef' (runTickets run) (part :)

-- | Runs the first part, and the parts it forks, until the result is there:
-- each part that can go on in turn, entered with the given action, for up to
-- 64 steps. It closes the run's poller at the end.
schedule :: Run -> (IO Step -> IO Step) -> IORef (Maybe b) -> IO Step -> IO (b, Report)
schedule run enter result start = go [start] [] [] 0 0 `finally` c_close (runPoller run)
  where
    -- The parts that can go on in this round, and in the next; the parts that
    -- wait on watches; how many wait on descriptors; and how many tickets hold.
    go ready later watching descriptors parked = case ready of
      part : others -> do
        setCounter run 0 64
        step <- enter part
        readIORef result >>= \case
          Just value -> (\steps waits -> (value, Report steps waits)) <$> counter run 1 <*> counter run 2
          Nothing -> case step of
            Ended -> go others later watching descriptors parked
            Parked -> go others later watching descriptors (parked + 1)
            Yielded part' -> go others (part' : later) watching descriptors parked
            Forked one two -> go others (two : one : later) watching descriptors parked
            Suspended (Readable fd) part' -> waitOn fd True part' *> go others later watching (descriptors + 1) parked
            Suspended (Writable fd) part' -> waitOn fd False part' *> go others later watching (descriptors + 1) parked
            Suspended (Watched register) part' -> register >>= \watch -> go others later ((watch, part') : watching) descriptors parked
      [] -> do
        -- Each round, the parts that tickets woke and that descriptors became
        -- ready for join.
        tickets <- readIORef (runTickets run)
        writeIORef (runTickets run) []
        polled <- if descriptors > (0 :: Int) then poll c_pollerPoll 0 else pure []
        case reverse later <> reverse tickets <> polled of
          [] -> idle watching descriptors parked
          next -> go next [] watching (descriptors - length polled) (parked - length tickets)
    idle watching descriptors parked
      | null watching && descriptors == 0 =
          ioError . userError $
            if parked > (0 :: Int)
              then "the parts wait for tickets that nothing will give a value"
              else "the program ended without a result"
      | null watching = do
          countWait
          polled <- poll c_pollerWait (-1)
          go polled [] watching (descriptors - length polled) parked
      | otherwise = do
          countWait
          -- Watches, and descriptors too: the watches for a millisecond at
          -- most, then the descriptors without waiting.
          delay <- if descriptors > 0 then Just <$> registerDelay 1000 else pure Nothing
          atomically $ maybe retry (readTVar >=> check) delay `orElse` foldr (orElse . watchReady . fst) retry watching
          checked <- traverse (\entry -> (,) entry <$> hasFired (fst entry)) watching
          let (fired, still) = partition snd checked
          mapM_ (watchCancel . fst . fst) fired
          polled <- if descriptors > 0 then poll c_pollerPoll 0 else pure []
          go (map (snd . fst) fired <> polled) [] (map fst still) (descriptors - length polled) parked
    countWait = counter run 2 >>= setCounter run 2 . (+ 1)
    -- Registers the descriptor with the poller the first time a part waits on
    -- it, and adds the part to its waiters.
    waitOn (Fd fd) reading part = do
      waiters <- readIORef (runWaiters run)
      let key = fromIntegral fd
          Waiters readers writers = IntMap.findWithDefault (Waiters [] []) key waiters
      if IntMap.member key waiters then pure () else throwErrnoIfMinus1_ "directions_poller_add" (c_pollerAdd (runPoller run) fd)
      writeIORef (runWaiters run) $
        IntMap.insert key (if reading then Waiters (part : readers) writers else Waiters readers (part : writers)) waiters
    -- Asks the poller what became ready, waiting as the timeout says, and gives
    -- the parts that wait on it.
    poll call timeout = do
      count <-
        unsafeWithForeignPtr (runReady run) $ \fds ->
          unsafeWithForeignPtr (runKinds run) $ \kinds ->
            call (runPoller run) fds kinds (fromIntegral batch) timeout
      if count < 0
        then getErrno >>= \errno -> if errno == eINTR then pure [] else throwErrno "directions_poller_wait"
        else do
          ready <-
            unsafeWithForeignPtr (runReady run) $ \fds ->
              unsafeWithForeignPtr (runKinds run) $ \kinds ->
                traverse (\index -> (,) <$> peekElemOff fds index <*> peekElemOff kinds index) [0 .. fromIntegral count - 1]
          waiters <- readIORef (runWaiters run)
          let (woken, waiters') = foldl wake ([], waiters) ready
          woken <$ writeIORef (runWaiters run) waiters'
    wake (woken, waiters) (fd, kind) =
      let key = fromIntegral fd
          Waiters readers writers = IntMap.findWithDefault (Waiters [] []) key waiters
          (fromReaders, readers') = if kind .&. 1 /= (0 :: CInt) then (readers, []) else ([], readers)
          (fromWriters, writers') = if kind .&. 2 /= 0 then (writers, []) else ([], writers)
       in (woken <> reverse fromReaders <> reverse fromWriters, IntMap.insert key (Waiters readers' writers') waiters)

-- | Whether the watch has fired, without waiting.
hasFired :: Watch -> IO Bool
hasFired watch = atomically ((True <$ watchReady watch) `orElse` pure False)

foreign import ccall unsafe "directions_poller_new"
  c_pollerNew :: IO CInt

foreign import ccall unsafe "directions_poller_add"
  c_pollerAdd :: CInt -> CInt -> IO CInt

-- | Asks without waiting.
foreign import ccall unsafe "directions_poller_wait"
  c_pollerPoll :: CInt -> Ptr CInt -> Ptr CInt -> CInt -> CInt -> IO CInt

-- | Waits, and can be interrupted, so that an asynchronous exception reaches
-- the scheduler.
foreign import ccall interruptible "directions_poller_wait"
  c_pollerWait :: CInt -> Ptr CInt -> Ptr CInt -> CInt -> CInt -> IO CInt

foreign import ccall unsafe "close"
  c_close :: CInt -> IO ()
