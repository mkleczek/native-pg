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
--
-- Each scheduler also has a mailbox that other threads, the schedulers of other
-- cores, put parts in: a part that waited for a ticket that another core gives
-- is put there by the giver, which wakes the poller if the scheduler waits in
-- it. The scheduler takes the parts from its mailbox every round, and before
-- it waits.
module Network.Directions.Scheduler
  ( Step (..),
    Run,
    newRun,
    freeRun,
    withRun,
    spent,
    stepped,
    counted,
    wakeTicket,
    Slot (..),
    Mailbox,
    runMailbox,
    post,
    schedule,
  )
where

import Control.Concurrent.STM (atomically, check, orElse, readTVar, registerDelay, retry)
import Control.Exception (bracket, finally)
import Control.Monad ((>=>))
import Data.Bits ((.&.))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.List (partition)
import Foreign.C.Error (eINTR, getErrno, throwErrno, throwErrnoIfMinus1_, throwErrnoIfNull)
import Foreign.C.Types (CInt (..))
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peekElemOff, pokeElemOff)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Directions.Atomic (update)
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
  | -- | It waits for a ticket that another core gives, whose slot holds it; the
    -- giver puts it in the mailbox.
    ParkedRemote
  | -- | It used its steps, and can go on.
    Yielded (IO Step)

-- | A ticket's slot: empty, given its value, or holding the part that waits
-- for it, as what goes on with the value.
data Slot a = Empty | Given a | Waiter (a -> IO Step)

-- | What the parts of a run share.
data Run = Run
  { -- | Parts that tickets woke.
    runTickets :: !(IORef [IO Step]),
    -- | The steps the running part has left before it gives way, the steps
    -- taken, the waits, and the parts that wait for tickets that other cores
    -- give and are not yet back from the mailbox: unboxed, so that counting
    -- allocates nothing. A step is a point to give way at, which a link's
    -- receive or send is, or a wait, or a ticket given or waited for.
    runCounters :: !(ForeignPtr Int),
    runPoller :: !(Ptr Poller),
    -- | Where other threads put parts for this scheduler.
    runMailbox :: !Mailbox,
    -- | Where the poller writes what became ready: descriptors, and kinds.
    runReady :: !(ForeignPtr CInt),
    runKinds :: !(ForeignPtr CInt),
    -- | The parts that wait on each registered descriptor: to read, to write.
    runWaiters :: !(IORef (IntMap.IntMap Waiters))
  }

data Waiters = Waiters [IO Step] [IO Step]

-- | A poller: the C struct that is the kqueue or epoll and its wake handle.
data Poller

-- | The parts that other threads gave the scheduler, newest first, and whether
-- the scheduler sleeps in its poller, which the next one to give a part wakes;
-- or that the scheduler has ended and takes no more.
data Inbox = Open [IO Step] !Bool | Closed

-- | An inbox, and the poller of the scheduler that owns it.
data Mailbox = Mailbox !(IORef Inbox) !(Ptr Poller)

data Posting = Posted | Wake | Refused

-- | How many readinesses one poll gives at most.
batch :: Int
batch = 64

-- | A run with a poller, which 'freeRun' closes: whoever makes it frees it,
-- once nothing that is given parts through its mailbox can run any more.
newRun :: IO Run
newRun = do
  counters <- mallocForeignPtrArray 4
  unsafeWithForeignPtr counters $ \pointer -> mapM_ (\index -> pokeElemOff pointer index 0) [0, 1, 2, 3]
  poller <- throwErrnoIfNull "directions_poller_new" c_pollerNew
  inbox <- newIORef (Open [] False)
  Run <$> newIORef [] <*> pure counters <*> pure poller <*> pure (Mailbox inbox poller) <*> mallocForeignPtrArray batch <*> mallocForeignPtrArray batch <*> newIORef IntMap.empty

freeRun :: Run -> IO ()
freeRun = c_pollerFree . runPoller

withRun :: (Run -> IO a) -> IO a
withRun = bracket newRun freeRun

-- | Gives the scheduler a part to go on with, from any thread, and wakes the
-- scheduler if it sleeps in its poller. It says whether it did: it didn't if
-- the scheduler has ended. The part is not run here.
post :: Mailbox -> IO Step -> IO Bool
post (Mailbox inbox poller) part = do
  posting <- update inbox $ \case
    Open parts sleeping -> (Open (part : parts) False, if sleeping then Wake else Posted)
    Closed -> (Closed, Refused)
  case posting of
    Posted -> pure True
    Wake -> True <$ c_pollerWake poller
    Refused -> pure False

-- | The parts that were given the scheduler since it last looked, in the order
-- they were given. Looking costs a read while there are none.
takeInbox :: Run -> IO [IO Step]
takeInbox run = case runMailbox run of
  Mailbox inbox _ ->
    readIORef inbox >>= \case
      Open [] _ -> pure []
      Closed -> pure []
      Open _ _ -> reverse <$> update inbox (\case Open parts _ -> (Open [] False, parts); Closed -> (Closed, []))

-- | Before the scheduler waits in its poller: the parts that were given it
-- already, in which case it doesn't wait; or none, and the next to give one
-- wakes it.
beforeSleep :: Run -> IO [IO Step]
beforeSleep run = case runMailbox run of
  Mailbox inbox _ ->
    reverse <$> update inbox (\case Open [] _ -> (Open [] True, []); Open parts _ -> (Open [] False, parts); Closed -> (Closed, []))

-- | After the scheduler's wait in its poller: the parts that were given it.
afterSleep :: Run -> IO [IO Step]
afterSleep run = case runMailbox run of
  Mailbox inbox _ ->
    reverse <$> update inbox (\case Open parts _ -> (Open [] False, parts); Closed -> (Closed, []))

-- | Takes the parts that were given the scheduler back from those that wait for
-- them.
arrivals :: Run -> [IO Step] -> IO [IO Step]
arrivals _ [] = pure []
arrivals run parts = parts <$ (counter run 3 >>= setCounter run 3 . subtract (length parts))

-- | The scheduler takes no more parts.
closeInbox :: Run -> IO ()
closeInbox run = case runMailbox run of
  Mailbox inbox _ -> update inbox (const (Closed, ()))

counter :: Run -> Int -> IO Int
counter run index = unsafeWithForeignPtr (runCounters run) (`peekElemOff` index)
{-# INLINE counter #-}

setCounter :: Run -> Int -> Int -> IO ()
setCounter run index value = unsafeWithForeignPtr (runCounters run) (\pointer -> pokeElemOff pointer index value)
{-# INLINE setCounter #-}

-- | Counts a step, and says whether the part has used its steps and yields.
spent :: Run -> IO Bool
spent run = do
  counter run 1 >>= setCounter run 1 . (+ 1)
  remaining <- counter run 0
  if remaining <= 1 then pure True else False <$ setCounter run 0 (remaining - 1)
{-# INLINE spent #-}

-- | Counts a step, and goes on, or yields once the part has used its steps.
stepped :: Run -> IO Step -> IO Step
stepped run continue = spent run >>= \yields -> if yields then pure (Yielded continue) else continue
{-# INLINE stepped #-}

-- | Counts a step that suspends its part.
counted :: Run -> Step -> IO Step
counted run step = step <$ (counter run 1 >>= setCounter run 1 . (+ 1))
{-# INLINE counted #-}

-- | Puts a part that a ticket woke back among those that can go on.
wakeTicket :: Run -> IO Step -> IO ()
wakeTicket run part = modifyIORef' (runTickets run) (part :)

-- | Runs the first part, and the parts it forks, until the result is there:
-- each part that can go on in turn, entered with the given action, for up to
-- 64 steps, which are mostly the receives and sends it makes. At the end it
-- closes the run's mailbox, so that those that give parts to it after that are
-- told they didn't; the run's poller stays until 'freeRun'.
schedule :: Run -> (IO Step -> IO Step) -> IORef (Maybe b) -> IO Step -> IO (b, Report)
schedule run enter result start = go [start] [] [] 0 0 `finally` closeInbox run
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
            ParkedRemote -> (counter run 3 >>= setCounter run 3 . (+ 1)) *> go others later watching descriptors parked
            Yielded part' -> go others (part' : later) watching descriptors parked
            Forked one two -> go others (two : one : later) watching descriptors parked
            Suspended (Readable fd) part' -> waitOn fd True part' *> go others later watching (descriptors + 1) parked
            Suspended (Writable fd) part' -> waitOn fd False part' *> go others later watching (descriptors + 1) parked
            Suspended (Watched register) part' -> register >>= \watch -> go others later ((watch, part') : watching) descriptors parked
      [] -> do
        -- Each round, the parts that tickets woke, that other cores gave, and
        -- that descriptors became ready for join.
        tickets <- readIORef (runTickets run)
        writeIORef (runTickets run) []
        given <- takeInbox run >>= arrivals run
        polled <- if descriptors > (0 :: Int) then poll c_pollerPoll 0 else pure []
        case reverse later <> reverse tickets <> given <> polled of
          [] -> counter run 3 >>= \remote -> idle remote watching descriptors parked
          next -> go next [] watching (descriptors - length polled) (parked - length tickets)
    idle remote watching descriptors parked
      | null watching && descriptors == 0 && remote == 0 =
          ioError . userError $
            if parked > (0 :: Int)
              then "the parts wait for tickets that nothing will give a value"
              else "the program ended without a result"
      | null watching = do
          -- Unless a part was given since the round began: the next to give
          -- one wakes the poller.
          early <- beforeSleep run >>= arrivals run
          if null early
            then do
              countWait
              polled <- poll c_pollerWait (-1)
              given <- afterSleep run >>= arrivals run
              go (given <> polled) [] watching (descriptors - length polled) parked
            else go early [] watching descriptors parked
      | otherwise = do
          countWait
          -- Watches, and descriptors too, and the mailbox: the watches for a
          -- millisecond at most, then the descriptors without waiting, and the
          -- parts that other cores gave in the round that follows.
          delay <- if descriptors > 0 || remote > 0 then Just <$> registerDelay 1000 else pure Nothing
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
  c_pollerNew :: IO (Ptr Poller)

foreign import ccall unsafe "directions_poller_free"
  c_pollerFree :: Ptr Poller -> IO ()

foreign import ccall unsafe "directions_poller_add"
  c_pollerAdd :: Ptr Poller -> CInt -> IO CInt

-- | Ends the poller's wait, or its next one, from any thread.
foreign import ccall unsafe "directions_poller_wake"
  c_pollerWake :: Ptr Poller -> IO CInt

-- | Asks without waiting.
foreign import ccall unsafe "directions_poller_wait"
  c_pollerPoll :: Ptr Poller -> Ptr CInt -> Ptr CInt -> CInt -> CInt -> IO CInt

-- | Waits, and can be interrupted, so that an asynchronous exception reaches
-- the scheduler.
foreign import ccall interruptible "directions_poller_wait"
  c_pollerWait :: Ptr Poller -> Ptr CInt -> Ptr CInt -> CInt -> CInt -> IO CInt
