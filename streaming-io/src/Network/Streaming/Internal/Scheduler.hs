{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE InterruptibleFFI #-}
{-# LANGUAGE LambdaCase #-}

-- | The scheduler of the arrows that run every part of a program on one thread
-- as code: "Network.Streaming.Compiled" and "Network.Streaming.Delimited". A
-- part is an action that runs until it ends, waits, forks or yields, and says
-- which. The scheduler owns a poller (kqueue, or epoll on Linux): a part that
-- blocks on a descriptor's readability or writability has the poller arm that
-- interest, once, for its next poll. When no part can go on, the scheduler waits in the poller,
-- which gives what is ready at once if something is; while others can go on, it asks
-- the poller, without waiting, once every few turns. A wait costs
-- no thread of GHC's IO manager, and on kqueue no system call of its own: the
-- armings go with the poll. A descriptor is armed at each wait, not once for a
-- run, so that a number that a new socket got after a close is waited on like
-- any other, and a readiness that came before the wait is found by it. The
-- scheduler keeps a descriptor's waiting parts only while there are some. A part
-- that waits for a ticket is held by the ticket.
-- A part runs for a turn, a length of time: at a point to give way at, once it
-- has run longer than that, it lets the others go on before it.
--
-- The parts that can go on are in two lanes, a normal one and a fast one, and the scheduler takes a part from each in turn, and
-- from the other if one has none. A part is in the fast lane for as long as it is in the region of 'fastened': the run that it
-- sees says so, and whatever puts it back among the parts that can go on, a yield, a ticket, a timer, a cancellation or a
-- part of another core, puts it in that lane. The parts that it starts are given the run as it is for the normal lane. The scheduler
-- looks at nothing of the lanes until a part has entered the fast lane.
--
-- Each scheduler also has a mailbox that other threads, the schedulers of other
-- cores, put parts in: a part that waited for a ticket that another core fulfils
-- is put there by the promise, which wakes the poller if the scheduler waits in
-- it. The scheduler takes the parts from its mailbox every round, and before
-- it waits.
--
-- The scheduler also keeps what cancels a part: a part that is in a cancel scope
-- says where it waits, so that cancelling the scope takes it off what it waits
-- at and puts it back among those that can go on, with the cancellation to
-- raise where it waits. A part in no scope says nothing, and waits as it always
-- did. A scope holds only the parts it began with, and a part that is suspended
-- at a group of parts or a race says so: cancelling a scope finds the others through those, so that
-- a part enters and leaves a scope without anything but its own frames
-- changing, however many parts there are. The scope of the parts that a group spawns while it runs
-- holds the body and the parts that are spawned into it, each added as it is spawned and taken out as it
-- ends, so that a part that has ended leaves nothing in it. The scheduler runs masked, and an
-- asynchronous exception reaches it where it waits for a descriptor, and once
-- in a while otherwise: it cancels every part that is in a scope, waits until
-- they have ended, and then fails the run with the exception.
module Network.Streaming.Internal.Scheduler
  ( Step (..),
    Turn (..),
    microseconds,
    defaultTurn,
    Run,
    newRun,
    freeRun,
    withRun,
    spent,
    stepped,
    counted,
    wakeTicket,
    Seat (..),
    hereSeat,
    anywhereSeat,
    collectFrom,
    postTo,
    awaitFrom,
    awaitRegisteringFrom,
    handWaitingTo,
    untilParkedFrom,
    commitTo,
    abandonTicket,
    Mailbox,
    runMailbox,
    post,
    schedule,
    fastened,
    plainRun,
    runInFast,
    markFast,
    Part (..),
    PartScope,
    Members,
    Parked (..),
    newPart,
    newScope,
    childPart,
    childParts,
    endPart,
    pushFrame,
    popFrame,
    shieldTop,
    armHeld,
    Floor (..),
    unwindFrames,
    observing,
    checkpoint,
    cancelScope,
    registering,
    nextWait,
    adjust,
    Joined (..),
    groupEnded,
    raceEnded,
    Gang,
    newGang,
    spawnedPart,
    bodyEnded,
    partEnded,
    enqueue,
    Escaped (..),
    guardedBy,
    runFailure,
    runWheel,
    roundNow,
    sleepUntil,
  )
where

import Control.Concurrent (myThreadId, rtsSupportsBoundThreads, yield)
import Control.Concurrent.STM (atomically, orElse, retry)
import Control.Exception (Exception, SomeException, allowInterrupt, bracket, catch, finally, fromException, interruptible, onException, throwIO, toException, try)
import Control.Monad (foldM, forM_, void, when)
import Data.Bits ((.&.), (.|.))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.List (partition)
import qualified Data.List as List (foldl')
import Data.Maybe (fromMaybe, isNothing)
import Data.Word (Word64)
import Foreign.C.Error (eINTR, getErrno, throwErrno, throwErrnoIfNull)
import Foreign.C.Types (CInt (..))
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peekElemOff, pokeElemOff)
import GHC.Conc (BlockReason (..), ThreadStatus (..), listThreads, threadStatus)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Streaming.Classes (Cancelled (..), Event (..), Instant (..), ReleaseFailed (..), cancelledBy, exitOf, monotonicNanoseconds)
import Network.Streaming.Concurrent (Joiner)
import qualified Network.Streaming.Concurrent as Joiner
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.Cancel
import Network.Streaming.Internal.Descriptors (armedBy, newRunNumber)
import Network.Streaming.Internal.Atomic (Change, changeHere)
import Network.Streaming.Internal.Inboxes (Looked (..), Shelf, collectedWrongly, lookOrWait, postNow, postOrQueue, stopWaiting, takeNow, takeWoken, withdraw)
import Network.Streaming.Internal.Tickets
import Network.Streaming.Internal.Timers
import Network.Streaming.Loop (Report (..))
import Network.Streaming.Mode (Instrumented (bump, bumpBy, debugging))
import Network.Streaming.Watch (Watch (..), delayWatch)
import System.Posix.Types (Fd (..))

-- | What a part comes to.
data Step
  = -- | It ended, at a join or with the program's result.
    Ended
  | -- | It waits for the event, and goes on with the action once it fires; the number is what the part's
    -- wait is called by when it is in a scope, and 0 otherwise. The action is what runs where the part
    -- waits: nothing, or the cancellation to raise.
    Suspended !Event !Int (IO () -> IO Step)
  | -- | The same, for a part of the fast lane, which the scheduler makes of a 'Suspended' step that 'markFast' said was one: it goes on in that
    -- lane.
    SuspendedFast !Event !Int (IO () -> IO Step)
  | -- | Parts that go on at once: the halves of a race, or the parts of a group.
    Forked [IO Step]
  | -- | It waits for a ticket, whose slot holds it.
    Parked
  | -- | It waits for a ticket that another core fulfils, whose slot holds it; the
    -- promise puts it in the mailbox.
    ParkedRemote
  | -- | It ran for its turn, and can go on, in the lane that 'markFast' said, if it did.
    Yielded (IO Step)
  | -- | It waits until an instant, and a timer holds it.
    Timed

-- | How long a part runs before it yields at its next point to give way at, in
-- nanoseconds. Zero makes every point one it yields at. A short turn keeps the
-- time that parts wait for their turns short; a part that gives way at
-- cheap points, a link's receives, still passes many of them in one turn.
newtype Turn = Turn Int

microseconds :: Int -> Turn
microseconds = Turn . (* 1000)

-- | The turn of the runs that don't say: about as long as the work between two
-- of a request's points to give way at, so that a part that waits is woken
-- within a few turns, and long enough for a part that receives cheaply to pass
-- many points in one.
defaultTurn :: Turn
defaultTurn = microseconds 10

-- | A part: the scopes it is in and the shields between them, innermost first, where it waits, and whether a
-- cancellation may have reached it, which is all its points look at until it is (see "Network.Streaming.Internal.Cancel").
-- A part that is in a scope is in the run's root scope too, outermost, which a shutdown cancels; a part that is in none
-- has no frames, and says nothing of where it waits. A part that has a release due, or a mark, is in a scope in this sense. The
-- frames of a child begin with those of its parent, which it shares: its own end at its mark.
data Part = Part
  { partIdent :: !Int,
    partFrames :: !(IORef [Frame Members]),
    partWait :: !(IORef Parked),
    partCancelled :: !(IORef Bool)
  }

-- | A scope of a run, with the parts that its body began with: those of a group or the halves of a race, or the part
-- that entered a deadline. The others inside it are reached through those: a part that is suspended at a group says so
-- (see 'InGroup'), and its parts are inside what the part is inside.
type PartScope = CancelScope Members

-- | The parts that a scope holds, by their numbers, so that a part that ends is taken out of it.
type Members = IntMap.IntMap Part

-- | Where a part waits, as what takes it off that.
data Parked
  = NotWaiting
  | -- | A descriptor, the direction and the number of the wait.
    OnFd !Int !Bool !Int
  | -- | A watch, by the number of the wait.
    OnWatch !Int
  | -- | Anything else: takes the part off, and gives it with the cancellation raised where it waits, if it was
    -- still there.
    Other !(Cancelled -> IO (Maybe (IO Step)))
  | -- | A group of parts or the halves of a race, which are the parts to look at next: the part is suspended until
    -- they have ended, and is not taken off anything.
    InGroup !PartScope

-- | What the parts of a run share.
data Run = Run
  { -- | Parts that tickets woke.
    runTickets :: !(IORef [IO Step]),
    -- | When the running part's turn began, on the clock, the steps taken, the
    -- waits, the parts that wait for tickets that other cores fulfil and are not
    -- yet back from the mailbox, the parts that wait on descriptors, those that
    -- wait for tickets, the number of the next wait, the rounds, the time that
    -- the round began at, or 0 before it is read, the parts that are in a
    -- scope, the armings that no poll has taken yet, the run's own number, the time of the last poll, and, on the non-threaded
    -- run time, whether another thread needed the OS thread at the last wait that ended with nothing, and how long the next
    -- wait may last if none did: unboxed, so that
    -- counting allocates nothing. A step is a point to
    -- give way at, which a link's receive or send is, or a wait, or a ticket
    -- fulfilled or awaited.
    runCounters :: !(ForeignPtr Int),
    -- | How long a turn is.
    runTurn :: !Int,
    runPoller :: !(Ptr Poller),
    -- | Where other threads put parts for this scheduler.
    runMailbox :: !Mailbox,
    -- | Where the poller writes what became ready: descriptors, and kinds.
    runReady :: !(ForeignPtr CInt),
    runKinds :: !(ForeignPtr CInt),
    -- | The parts that wait on each descriptor that some wait on: to read, to write.
    runWaiters :: !(IORef (IntMap.IntMap Waiters)),
    -- | The parts that wait on watches.
    runWatching :: !(IORef [Watching]),
    -- | The scope that every part in a scope is in, outermost: it begins with the run's first part.
    runRoot :: !PartScope,
    -- | The asynchronous exception that the run is shutting down for.
    runShutdown :: !(IORef (Maybe SomeException)),
    -- | What goes on with a failure of the part that runs, in a run of "Network.Streaming.Compiled", whose
    -- parts have no handlers of their own on a stack that can be captured: the part sets it when it goes on.
    runFailure :: !(IORef (SomeException -> IO Step)),
    -- | The timers of the parts: deadlines, and those that sleep.
    runWheel :: !Wheel,
    -- | Whether the run is in a build that observes, and asserts what it can (see 'observing').
    runObserved :: !(IORef Bool),
    -- | The parts of the fast lane that can go on, newest first, which the scheduler takes in its turns for that lane.
    runFastReady :: !(IORef [IO Step]),
    -- | Whether the part that has this view of the run is in the fast lane: what it puts back among the parts that can go on goes
    -- to that lane. The view is the run itself with this said, so that everything else is shared.
    runInFast :: !Bool,
    -- | Where the part that has this view of the run puts itself back among those that can go on: 'runTickets' in the normal lane, and
    -- 'runFastReady' in the fast lane.
    runLaneReady :: !(IORef [IO Step])
  }

-- | The parts that wait on a descriptor, as the 'Suspended' steps they gave the scheduler.
data Waiters = Waiters [Step] [Step]

-- | A part that waits on a watch, by the number of the wait, and whether it is in the fast lane.
data Watching = Watching !Int !Bool !Watch (IO () -> IO Step)

-- | A poller: the C struct that is the kqueue or epoll and its wake handle.
data Poller

-- | The parts that other threads gave the scheduler, newest first, those for the normal lane and those for the fast lane, and whether
-- the scheduler sleeps in its poller, which the next one to give a part wakes;
-- or that the scheduler has ended and takes no more.
data Inbox = Open [IO Step] [IO Step] !Bool | Closed

-- | An inbox, and the poller of the scheduler that owns it.
data Mailbox = Mailbox !(IORef Inbox) !(Ptr Poller)

data Posting = Posted | Wake | Refused

-- | How many readinesses one poll gives at most.
batch :: Int
batch = 64

-- | How many turns may pass while other parts can go on before the scheduler polls without waiting: a part that is woken
-- waits at most that long, on top of what it waits for the parts that run before it. A poll costs about a tenth of a turn,
-- so a poll at each turn spends a tenth of the time on polls when parts are short of work, and many rounds are short: with
-- one turn, a 16 KiB request made 2.6 polls that found nothing at 800 requests a second, with four 0.86 and with sixteen
-- none (the VM of the checks, 2 cores).
pollTurns :: Int
pollTurns = 4

-- | How many armings that no poll has taken yet make a poll before the turns are over: kqueue keeps them as changes for the
-- next call, which this keeps from growing without end.
pendingArmings :: Int
pendingArmings = 64

-- | A run with a poller, which 'freeRun' closes: whoever makes it frees it,
-- once nothing that is given parts through its mailbox can run any more. Its
-- parts run for turns of that length.
newRun :: Turn -> IO Run
newRun (Turn turn) = do
  counters <- mallocForeignPtrArray 17
  unsafeWithForeignPtr counters $ \pointer -> mapM_ (\index -> pokeElemOff pointer index 0) [0 .. 16]
  number <- newRunNumber
  unsafeWithForeignPtr counters $ \pointer -> pokeElemOff pointer 11 (fromIntegral number)
  poller <- throwErrnoIfNull "streaming_io_poller_new" c_pollerNew
  inbox <- newIORef (Open [] [] False)
  tickets <- newIORef []
  Run <$> pure tickets <*> pure counters <*> pure turn <*> pure poller <*> pure (Mailbox inbox poller) <*> mallocForeignPtrArray batch <*> mallocForeignPtrArray batch <*> newIORef IntMap.empty <*> newIORef [] <*> newCancelScope 0 IntMap.empty <*> newIORef Nothing <*> newIORef (throwIO . Escaped) <*> (monotonicNanoseconds >>= newWheel) <*> newIORef False <*> newIORef [] <*> pure False <*> pure tickets

freeRun :: Run -> IO ()
freeRun = c_pollerFree . runPoller

withRun :: Turn -> (Run -> IO a) -> IO a
withRun turn = bracket (newRun turn) freeRun

-- | Gives the scheduler a part to go on with, from any thread, and wakes the
-- scheduler if it sleeps in its poller. It says whether it did: it didn't if
-- the scheduler has ended. The part is not run here.
post :: Mailbox -> IO Step -> IO Bool
post (Mailbox inbox poller) part = do
  posting <- update inbox $ \case
    Open parts fast sleeping -> (Open (part : parts) fast False, if sleeping then Wake else Posted)
    Closed -> (Closed, Refused)
  case posting of
    Posted -> pure True
    Wake -> True <$ c_pollerWake poller
    Refused -> pure False

-- | 'post', for a part of the fast lane: the scheduler takes it for its next turn for that lane.
postFast :: Mailbox -> IO Step -> IO Bool
postFast (Mailbox inbox poller) part = do
  posting <- update inbox $ \case
    Open parts fast sleeping -> (Open parts (part : fast) False, if sleeping then Wake else Posted)
    Closed -> (Closed, Refused)
  case posting of
    Posted -> pure True
    Wake -> True <$ c_pollerWake poller
    Refused -> pure False

-- | The inbox with nothing in it, which the scheduler finds without a part to take, and the one of a scheduler that sleeps in its poller.
awake, asleep :: Inbox
awake = Open [] [] False
asleep = Open [] [] True

-- | The parts that were given the scheduler since it last looked, newest first.
-- Looking costs a read while there are none. The parts for the fast lane stay in the mailbox, for the lane to take.
takeInbox :: Run -> IO [IO Step]
takeInbox run = case runMailbox run of
  Mailbox inbox _ ->
    readIORef inbox >>= \case
      Open [] _ _ -> pure []
      Closed -> pure []
      Open _ _ _ -> update inbox (\case Open parts [] _ -> (awake, parts); Open parts fast _ -> (Open [] fast False, parts); Closed -> (Closed, []))

-- | Before the scheduler waits in its poller: the parts that were given it
-- already, newest first, in which case it doesn't wait; or none, and the next to give one
-- wakes it.
beforeSleep :: Run -> IO [IO Step]
beforeSleep run = case runMailbox run of
  Mailbox inbox _ ->
    update inbox (\case Open [] [] _ -> (asleep, []); Open parts [] _ -> (awake, parts); Open parts fast _ -> (Open [] fast False, parts); Closed -> (Closed, []))

-- | After the scheduler's wait in its poller: the parts that were given it, newest first.
afterSleep :: Run -> IO [IO Step]
afterSleep run = case runMailbox run of
  Mailbox inbox _ ->
    update inbox (\case Open parts [] _ -> (awake, parts); Open parts fast _ -> (Open [] fast False, parts); Closed -> (Closed, []))

-- | Whether a part of the fast lane can go on that the scheduler has not taken yet: one that was put back, or one that another core gave.
fastWaiting :: Run -> IO Bool
fastWaiting run =
  readIORef (runFastReady run) >>= \case
    _ : _ -> pure True
    [] -> case runMailbox run of
      Mailbox inbox _ -> readIORef inbox >>= \case Open _ (_ : _) _ -> pure True; _ -> pure False

-- | Takes the parts of the fast lane from the mailbox, newest first.
takeFastInbox :: Run -> IO [IO Step]
takeFastInbox run = case runMailbox run of
  Mailbox inbox _ ->
    readIORef inbox >>= \case
      Open _ [] _ -> pure []
      Closed -> pure []
      Open _ _ _ -> update inbox (\case Open parts fast sleeping -> (Open parts [] sleeping, fast); Closed -> (Closed, []))

-- | Puts the detached mailbox batch in FIFO order ahead of the ready parts,
-- counting the remote arrivals in the same pass.
drainArrivals :: Run -> [IO Step] -> [IO Step] -> IO [IO Step]
drainArrivals run = go 0
  where
    go !count ready [] = ready <$ when (count /= 0) (adjust run 3 (negate count))
    go !count ready (part : rest) = go (count + 1) (part : ready) rest

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

-- | Adds to a counter: 3 the parts that wait for tickets of other cores, 4 those that wait on descriptors, 5
-- those that wait for tickets, 9 the parts that are in a scope, 10 the armings that no poll has taken yet.
adjust :: Run -> Int -> Int -> IO ()
adjust run index delta = counter run index >>= setCounter run index . (+ delta)
{-# INLINE adjust #-}

-- | The part enters the fast lane: the run as the part sees it from now on has the lane. The run keeps whether any part has entered it,
-- which the scheduler looks at to take the parts of the lane in their turns, and costs nothing until one has.
fastened :: Run -> IO Run
fastened run
  | runInFast run = pure run
  | otherwise = run {runInFast = True, runLaneReady = runFastReady run} <$ setCounter run 15 1

-- | The run as a part sees it that is in the normal lane, which the parts that another part starts are given, whichever lane that part is in.
plainRun :: Run -> Run
plainRun run = if runInFast run then run {runInFast = False, runLaneReady = runTickets run} else run
{-# NOINLINE plainRun #-}

-- | Puts a part among those that can go on, in the lane that the run's view says.
readyIn :: Run -> IO Step -> IO ()
readyIn run part = modifyIORef' (runLaneReady run) (part :)
{-# INLINE readyIn #-}

-- | The part is about to give the scheduler a 'Yielded' or a 'Suspended' step: if it is in the fast lane it says so before, in counter 16,
-- which the scheduler reads when it gets the step. The step doesn't say it, so that what a part keeps when it yields or waits is the same
-- in both lanes.
markFast :: Run -> IO ()
markFast run = when (runInFast run) (setCounter run 16 1)
{-# INLINE markFast #-}

-- | What a cancellation gives of a part that it took off what it waited at: the part, to go on in the normal lane. A part of the fast lane is
-- put back in that lane, which has it, and nothing is given.
backIn :: Run -> IO Step -> IO (Maybe (IO Step))
backIn run = inLane run (runInFast run)
{-# INLINE backIn #-}

-- | The time of this round of the scheduler, in nanoseconds: it reads the clock the first time it is asked in a
-- round, and not at all in one where nothing is.
roundNow :: Run -> IO Word64
roundNow run =
  counter run 8 >>= \known ->
    if known /= 0
      then pure (fromIntegral known)
      else monotonicNanoseconds >>= \now -> now <$ setCounter run 8 (fromIntegral now)
{-# INLINE roundNow #-}

-- | The part, which is suspended, waits until the instant: a timer holds it, and puts it back among those that can
-- go on. A part that is in a scope says how it can be taken off the timer.
sleepUntil :: Run -> Part -> Instant -> (IO () -> IO Step) -> IO Step
sleepUntil run part (Instant at) resume = do
  timer <- addTimer (runWheel run) at (modifyIORef' (runLaneReady run) (resume (pure ()) :))
  readIORef (partFrames part) >>= \case
    [] -> pure ()
    _ -> writeIORef (partWait part) (Other (\cancelled -> cancelTimer (runWheel run) timer >>= \wasLive -> if wasLive then backIn run (resume (throwIO cancelled)) else pure Nothing))
  counted run Timed

-- | The number of a part, of a wait of a part that is in a scope, or of a scope: none is 0, and the run's own
-- counter gives each only once.
nextWait :: Run -> IO Int
nextWait run = counter run 6 >>= \n -> n + 1 <$ setCounter run 6 (n + 1)
{-# INLINE nextWait #-}

-- | Counts a step, and says whether the part has run for its turn and yields.
spent :: Run -> IO Bool
spent run = do
  counter run 1 >>= setCounter run 1 . (+ 1)
  began <- counter run 0
  now <- clock
  pure (now - began >= runTurn run)
{-# INLINE spent #-}

-- | The time on the monotonic clock, in nanoseconds, as the counters hold it.
clock :: IO Int
clock = fromIntegral <$> monotonicNanoseconds
{-# INLINE clock #-}

-- | Counts a step, and goes on, or yields once the part has run for its turn.
stepped :: Run -> IO Step -> IO Step
stepped run continue = spent run >>= \yields -> if yields then markFast run *> pure (Yielded continue) else continue
{-# INLINE stepped #-}

-- | Counts a step that suspends its part.
counted :: Run -> Step -> IO Step
counted run step = step <$ (counter run 1 >>= setCounter run 1 . (+ 1))
{-# INLINE counted #-}

-- | Puts a part that a ticket woke back among those that can go on.
wakeTicket :: Run -> IO Step -> IO ()
wakeTicket run part = readyIn run part *> adjust run 5 (-1)

-- | How the parts of a run wait at an inbox or a ticket, and are woken: how the slot is changed, how a part is put back among those
-- that can go on, how a part is given a value, which says whether the part takes it, what a part that waits comes to, and the counter of
-- the parts that wait that way.
data Seat = Seat
  { seatChange :: Change,
    seatWake :: Run -> IO Step -> IO (),
    seatDeliver :: Run -> IO Step -> IO Bool,
    seatParked :: Step,
    seatCounter :: Int
  }

-- | The seat of an inbox or a ticket of this core: plain memory, and the parts that tickets woke.
hereSeat :: Seat
hereSeat = Seat changeHere wakeTicket (\run part -> True <$ wakeTicket run part) Parked 5

-- | The seat of an inbox or a ticket of any core: compare-and-swap, and the mailbox of the part's scheduler, which refuses a part if
-- the scheduler has ended.
anywhereSeat :: Seat
anywhereSeat = Seat update (\run part -> () <$ postIn run part) postIn ParkedRemote 3
  where
    postIn run = if runInFast run then postFast (runMailbox run) else post (runMailbox run)

-- | What a part does that collects from an inbox, for the two arrows that run every part on one thread: the arrow goes on with the
-- value, or, if there is none, captures what follows, and the part waits on the shelf until a post wakes it and it takes the value.
-- A cancellation that reaches the part first is raised before it takes one, and leaves the value in the inbox.
collectFrom :: Seat -> Run -> Part -> IORef (Shelf a) -> (a -> IO r) -> (((IO a -> IO Step) -> IO Step) -> IO r) -> IO r
collectFrom seat run part shelf taken capture = do
  checkpoint part
  takeNow (seatChange seat) shelf >>= \case
    Just value -> taken value
    Nothing -> capture $ \resume -> do
      let again = checkpoint part *> takeWoken (seatChange seat) shelf
      lookOrWait (seatChange seat) shelf (seatWake seat run (resume again)) >>= \case
        Registered -> do
          -- A cancellation takes the part off the shelf, unless a post has woken it already.
          let stop cancelled = stopWaiting (seatChange seat) shelf >>= \stopped -> if stopped then adjust run (seatCounter seat) (-1) *> backIn run (resume (throwIO cancelled)) else pure Nothing
          writeIORef (partWait part) (Other stop)
          pure (seatParked seat)
        -- A value came since the look, from another thread.
        Found -> markFast run *> pure (Yielded (resume again))
        Unusable -> collectedWrongly

-- | What a part does that posts to an inbox, for the two arrows that run every part on one thread: the arrow goes on with whether
-- the inbox took the value, or, if the inbox is full, captures what follows, and the part waits in the queue of the shelf until a
-- collect lets its value in or the inbox closes. A cancellation that reaches the part first is raised before the post, and a
-- cancellation takes the part out of the queue, unless its value went in.
postTo :: Seat -> Run -> Part -> IORef (Shelf a) -> a -> (Bool -> IO r) -> (((IO Bool -> IO Step) -> IO Step) -> IO r) -> IO r
postTo seat run part shelf value posted capture = do
  checkpoint part
  postNow (seatChange seat) shelf value >>= \case
    Just taken -> posted taken
    Nothing -> do
      token <- newIORef ()
      capture $ \resume ->
        postOrQueue (seatChange seat) shelf token value (\outcome -> seatWake seat run (resume (pure outcome))) >>= \case
          Nothing -> do
            let stop cancelled = withdraw (seatChange seat) shelf token >>= \withdrawn -> if withdrawn then adjust run (seatCounter seat) (-1) *> backIn run (resume (throwIO cancelled)) else pure Nothing
            writeIORef (partWait part) (Other stop)
            pure (seatParked seat)
          -- The inbox took the value, or closed, since the look, on another thread.
          Just taken -> markFast run *> pure (Yielded (resume (pure taken)))

-- | The part says where it waits, if it is in a scope, so that a cancellation can take it from there: a part that is in none is not
-- cancelled, and says nothing.
waitsAt :: Part -> Parked -> IO ()
waitsAt part parked =
  readIORef (partFrames part) >>= \case
    [] -> pure ()
    _ -> writeIORef (partWait part) parked
{-# INLINE waitsAt #-}

-- | What a part does that awaits a ticket, for the two arrows that run every part on one thread: the arrow goes on with the value if a
-- sender waits, and else captures what follows, and the part parks in the slot until a sender gives it a value. A cancellation that
-- reaches the part first is raised before it takes a value, which a sender that waits keeps.
awaitFrom :: Seat -> Run -> Part -> IORef (Slot a) -> (IO r -> IO r) -> (a -> IO r) -> (((IO a -> IO Step) -> IO Step) -> IO r) -> IO r
awaitFrom seat run part slot first taken capture = do
  checkpoint part
  valueWaits slot >>= \case
    False -> capture (parkAwaiting seat run part slot)
    -- The part takes the value, after the step that it may give way at, so that a cancellation that it raises leaves the value with its sender.
    True ->
      first $
        takeWaiting (seatChange seat) slot >>= \case
          Took value -> taken value
          Misused -> ioError tookWrongly
          Parks -> capture (parkAwaiting seat run part slot)
{-# INLINE awaitFrom #-}

-- | Parks the captured part in the slot, as the receiver: unless a sender came since the look, and then the part goes on in its turn
-- with the value. The part is given a value by a sender with the value itself: a cancellation reaches it at its next wait.
parkAwaiting :: Seat -> Run -> Part -> IORef (Slot a) -> (IO a -> IO Step) -> IO Step
parkAwaiting seat run part slot resume =
  parkTaker (seatChange seat) slot (\value -> seatDeliver seat run (resume (pure value))) >>= \case
    Parks -> do
      -- A cancellation takes the part off the slot, unless a sender has given it a value already.
      let stop cancelled = stopTaking (seatChange seat) slot >>= \stopped -> if stopped then adjust run (seatCounter seat) (-1) *> backIn run (resume (throwIO cancelled)) else pure Nothing
      waitsAt part (Other stop)
      pure (seatParked seat)
    Took value -> markFast run *> pure (Yielded (resume (pure value)))
    Misused -> ioError tookWrongly

-- | What a part does that awaits and registers: if a sender waits, the arrow goes on with its value, and the registration doesn't run.
-- Else the receiver is committed to take, and the registration runs in the same step, as the part that is about to park: a value that is
-- handed to the receiver meanwhile is its, and the registration may give a value instead, which the arrow goes on with. If neither, the
-- part captures what follows and parks.
awaitRegisteringFrom :: Seat -> Run -> Part -> IORef (Slot a) -> IO (Maybe a) -> (IO r -> IO r) -> (a -> IO r) -> (((IO a -> IO Step) -> IO Step) -> IO r) -> IO r
awaitRegisteringFrom seat run part slot register first taken capture = do
  checkpoint part
  valueWaits slot >>= \there -> if there then first begin else begin
  where
    begin =
      beginRegistering (seatChange seat) slot >>= \case
        Took value -> taken value
        Misused -> ioError tookWrongly
        Parks ->
          (register `onException` giveUpRegistering (seatChange seat) slot) >>= \case
            Just value -> giveUpRegistering (seatChange seat) slot >>= \clean -> if clean then taken value else ioError registeredAndGave
            Nothing ->
              endRegistering (seatChange seat) slot >>= \case
                Took value -> taken value
                Parks -> capture (parkAwaiting seat run part slot)
                Misused -> ioError tookWrongly
{-# INLINE awaitRegisteringFrom #-}

-- | What a part does that hands a value over and waits until it is taken: the arrow goes on with 'Nothing' if the receiver is parked,
-- and takes it, and with the value if the ticket was abandoned. Else it captures what follows, and the part waits in the slot until
-- the receiver takes the value or the ticket is abandoned. A cancellation that reaches the part first is raised before the hand, and a
-- cancellation takes the part out of the slot, unless its value was taken.
handWaitingTo :: Seat -> Run -> Part -> IORef (Slot a) -> a -> (Maybe a -> IO r) -> (((IO (Maybe a) -> IO Step) -> IO Step) -> IO r) -> IO r
handWaitingTo seat run part slot value handed capture = do
  checkpoint part
  handTry (seatChange seat) slot value >>= \case
    Passed -> handed Nothing
    GaveBack -> handed (Just value)
    SecondSender -> ioError twoSenders
    _ -> capture $ \resume -> do
      token <- nextWait run
      parkSender (seatChange seat) slot value token (\taken -> seatWake seat run (resume (pure (if taken then Nothing else Just value)))) >>= \case
        LeftWaiting -> do
          let stop cancelled = withdrawSender (seatChange seat) slot token >>= \withdrawn -> if withdrawn then adjust run (seatCounter seat) (-1) *> backIn run (resume (throwIO cancelled)) else pure Nothing
          waitsAt part (Other stop)
          pure (seatParked seat)
        -- The receiver took the value, or the ticket was abandoned, since the look, on another thread.
        Passed -> markFast run *> pure (Yielded (resume (pure Nothing)))
        GaveBack -> markFast run *> pure (Yielded (resume (pure (Just value))))
        _ -> ioError twoSenders
{-# INLINE handWaitingTo #-}

-- | What a part does that waits until the receiver is parked: the arrow goes on if it is, or the ticket was abandoned. Else it captures
-- what follows, and the part waits in the slot as the watcher, which the receiver's park wakes.
untilParkedFrom :: Seat -> Run -> Part -> IORef (Slot a) -> IO r -> (((IO () -> IO Step) -> IO Step) -> IO r) -> IO r
untilParkedFrom seat run part slot goOn capture = do
  checkpoint part
  lookAtWatch slot >>= \case
    WatchDone -> goOn
    SecondWatcher -> ioError twoWatchers
    WatchWaits -> capture $ \resume -> do
      token <- nextWait run
      parkWatcher (seatChange seat) slot token (seatWake seat run (resume (pure ()))) >>= \case
        WatchWaits -> do
          let stop cancelled = unwatch (seatChange seat) slot token >>= \stopped -> if stopped then adjust run (seatCounter seat) (-1) *> backIn run (resume (throwIO cancelled)) else pure Nothing
          waitsAt part (Other stop)
          pure (seatParked seat)
        WatchDone -> markFast run *> pure (Yielded (resume (pure ())))
        SecondWatcher -> ioError twoWatchers
{-# INLINE untilParkedFrom #-}

-- | The receiver is about to hand its promise out, with steps that may wait, and await the future: a value handed to it from now on is
-- its.
commitTo :: Seat -> IORef (Slot a) -> IO ()
commitTo seat slot = commitRegistering (seatChange seat) slot >>= \committed -> if committed then pure () else ioError tookWrongly

-- | Abandons the ticket, and gives the value that was handed to the receiver while it registered and that it never awaited.
abandonTicket :: Seat -> IORef (Slot a) -> IO (Maybe a)
abandonTicket seat = closeSlot (seatChange seat)

-- | The first part of a run, in no scope: the run's root scope begins with it.
newPart :: Run -> IO Part
newPart run = do
  part <- Part <$> nextWait run <*> newIORef [] <*> newIORef NotWaiting <*> newIORef False
  part <$ modifyIORef' (scopeMembers (runRoot run)) (IntMap.insert (partIdent part) part)

-- | A scope for a group, a race or a deadline of the run.
newScope :: Run -> IO PartScope
newScope run = nextWait run >>= (`newCancelScope` IntMap.empty)

-- | A part for a half of a race, in the scope of the race as well as in those of the part that made it. The
-- parent, which is suspended until the race has ended, says where its halves are. The part begins with the flag of its
-- parent, since a cancellation that came before it existed has not visited it; a parent in no scope is in the root's.
childPart :: Run -> Part -> PartScope -> IO Part
childPart run parent scope' = do
  outer <- readIORef (partFrames parent)
  flagged <- if null outer then isCancelled (runRoot run) else readIORef (partCancelled parent)
  child <- Part <$> nextWait run <*> newIORef (Inside scope' : if null outer then [Inside (runRoot run)] else outer) <*> newIORef NotWaiting <*> newIORef flagged
  adjust run 9 1
  modifyIORef' (scopeMembers scope') (IntMap.insert (partIdent child) child)
  child <$ writeIORef (partWait parent) (InGroup scope')

-- | The parts of a group of this many, in the scope of the group as well as in those of the part that made it. The
-- parent, which is suspended until the group has ended, says where they are, and the scope holds them: it is given
-- them once, not one at a time. Each begins with the flag of the parent, as a half of a race does.
childParts :: Run -> Part -> PartScope -> Int -> IO [Part]
childParts run parent scope' count = do
  outer <- readIORef (partFrames parent)
  flagged <- if null outer then isCancelled (runRoot run) else readIORef (partCancelled parent)
  let frames = Inside scope' : if null outer then [Inside (runRoot run)] else outer
  -- Made in a loop that keeps no stack of its own: a group of thousands of parts starts under a small stack limit.
  children <- reverse <$> foldM (\made _ -> (: made) <$> (Part <$> nextWait run <*> newIORef frames <*> newIORef NotWaiting <*> newIORef flagged)) [] [1 .. count]
  adjust run 9 count
  writeIORef (scopeMembers scope') (IntMap.fromDistinctAscList [(partIdent child, child) | child <- children])
  children <$ writeIORef (partWait parent) (InGroup scope')

-- | A part that has ended is in no scope any more.
endPart :: Run -> Part -> IO ()
endPart run part = do
  readIORef (partFrames part) >>= \case
    [] -> pure ()
    _ -> adjust run 9 (-1)
  writeIORef (partFrames part) []

-- | The part goes in a scope, or behind a shield; a part that was in none goes in the run's root scope too. A
-- scope that a part goes in this way begins with it. Not inlined, so that the frames of a part that was in none
-- are built only then, and not at every scope.
pushFrame :: Run -> Part -> Frame Members -> IO ()
pushFrame run part frame = do
  frames <- readIORef (partFrames part)
  case frames of
    [] -> do
      adjust run 9 1
      writeIORef (partFrames part) [frame, Inside (runRoot run)]
      -- The root may have been cancelled while the part was in no scope.
      isCancelled (runRoot run) >>= \cancelled -> when cancelled (writeIORef (partCancelled part) True)
    _ -> writeIORef (partFrames part) (frame : frames)
  case frame of Inside s -> writeIORef (scopeMembers s) (IntMap.singleton (partIdent part) part); _ -> pure ()
{-# NOINLINE pushFrame #-}

-- | The part leaves the innermost scope or shield, which it went in with 'pushFrame'; the scope holds no part
-- then, which a timer of a deadline would keep alive.
popFrame :: Run -> Part -> IO ()
popFrame run part = do
  frames <- readIORef (partFrames part)
  case frames of
    [frame, Inside root] | scopeIdent root == scopeIdent (runRoot run) -> adjust run 9 (-1) *> writeIORef (partFrames part) [] *> forget frame
    frame : outer -> writeIORef (partFrames part) outer *> forget frame
    [] -> pure ()
  where
    forget = \case Inside s -> writeIORef (scopeMembers s) IntMap.empty; _ -> pure ()

-- | The innermost frame of the part is a shield, whatever it was: a release that is due runs behind one.
shieldTop :: Part -> IO ()
shieldTop part =
  readIORef (partFrames part) >>= \case
    _ : outer -> writeIORef (partFrames part) (Shielded : outer)
    [] -> pure ()

-- | Where the unwinding of a failure stops: at the mark that a catcher pushed, or at the bottom, for the base of a part that
-- is not a child, which has no catcher of its own to push one.
data Floor = AtMark | AtBottom

-- | The run is in a build that observes: the arrows that run it assert what they can, which a build that observes nothing leaves
-- to the code that it was built from.
observing :: forall m. (Instrumented m) => Run -> IO ()
observing run = debugging @m (writeIORef (runObserved run) True)

-- | The failure leaves the frames of the part, down to the floor, innermost first: a release that is due is run behind a shield,
-- with how the body ended, and its failure replaces the failure, with how the body ended, for the releases that follow; the
-- other frames are dropped. It gives the failure that is left. A release may wait, and runs on the part's own stack. In a run that
-- observes, a part whose frames run out before its mark is found has no mark at its base, and the releases of the parts that its
-- frames are shared with have been run.
unwindFrames :: Run -> Part -> Floor -> SomeException -> IO SomeException
unwindFrames run part floor' failure =
  readIORef (partFrames part) >>= \case
    Owes owed : _ -> do
      let exit = exitOf (Left failure)
      shieldTop part
      released <- try (owed exit)
      popFrame run part
      unwindFrames run part floor' (either (toException . ReleaseFailed exit) (const failure) released)
    Marked : _ | AtMark <- floor' -> pure failure
    [] -> do
      case floor' of
        AtMark -> readIORef (runObserved run) >>= \observed -> when observed (ioError (userError "a failure unwound the frames of a part to the bottom, and found no mark at its base"))
        AtBottom -> pure ()
      pure failure
    _ : _ -> popFrame run part *> unwindFrames run part floor' failure

-- | Raises the cancellation that reaches the part, if there is one: it looks at the part's scopes only if its flag says
-- that one may have.
checkpoint :: Part -> IO ()
checkpoint part =
  readIORef (partCancelled part) >>= \flagged ->
    when flagged (readIORef (partFrames part) >>= reachedBy (partCancelled part) >>= maybe (pure ()) (throwIO . Cancelled))

-- | Cancels the scope: its flag is set, and so is that of every part inside it, whether or not it waits and a shield
-- holds it. The parts that wait, and whose scope lies above their innermost shield, with no grace between that has a period still to
-- pass, are taken off what they wait at, and put back among those that can go on, with the cancellation to raise where they wait.
-- The others see it at their next wait, and for each period of a grace that holds it from a part, a timer of the run's wheel walks
-- the scope again when the period has passed since now. The parts inside it are those it began with, and the parts of the groups and
-- the halves of the races that those are suspended at, and so on.
cancelScope :: Run -> PartScope -> IO ()
cancelScope run scope' = setCancelled scope' *> reach run scope' True

-- | Tells the parts inside the scope of its cancellation, as 'cancelScope' does, or, if it is not arriving, as the timer of a
-- period does, which finds that a part is held no longer when the period has passed since the scope was cancelled.
reach :: Run -> PartScope -> Bool -> IO ()
reach run scope' arriving = readIORef (scopeMembers scope') >>= inside [] . IntMap.elems
  where
    ident = scopeIdent scope'
    -- The periods that graces hold the cancellation from some part for, each once.
    inside periods = \case
      [] -> when arriving (forM_ periods (laterIn run scope'))
      part : others -> do
        frames <- readIORef (partFrames part)
        waiting <- readIORef (partWait part)
        writeIORef (partCancelled part) True
        let Hold shielded period = holdOf ident frames
        reached <- if shielded then pure False else if period == 0 then pure True else if arriving then pure False else passed period
        when reached (deliver waiting)
        -- A grace holds the cancellation from the part, whether a shield does as well or not: a timer lets it in.
        let periods' = if arriving && period > 0 && period `notElem` periods then period : periods else periods
        case waiting of
          InGroup members -> readIORef (scopeMembers members) >>= \more -> inside periods' (IntMap.elems more <> others)
          _ -> inside periods' others
    deliver waiting =
      stopWait run (Cancelled ident) waiting >>= \case
        Nothing -> pure ()
        Just resume -> modifyIORef' (runTickets run) (resume :)
    passed period = do
      now <- roundNow run
      arrival <- arrivalOf scope' now
      pure (now >= heldUntil arrival period)

-- | A timer that walks the scope again when the period has passed since it was cancelled, unless one is armed for the period already,
-- or the period ends at the end of time.
laterIn :: Run -> PartScope -> Word64 -> IO ()
laterIn run scope' period = do
  now <- roundNow run
  armOnce scope' now period >>= \case
    Nothing -> pure ()
    Just end -> void (addTimer (runWheel run) end (reach run scope' False))

-- | The part has entered a grace of this period: if a cancellation is pending, the timers that lets in the cancelled scopes that the
-- period of this grace holds from it. Not inlined, as it is for the part that enters a region with a cancellation pending only.
armHeld :: Run -> Part -> Word64 -> IO ()
armHeld run part period = do
  held <- readIORef (partFrames part) >>= heldIn
  forM_ [s | (s, longest) <- held, longest == period] (\s -> laterIn run s period)
{-# NOINLINE armHeld #-}

-- | Takes the part off what it waits at, if it still does: what goes on with the part, if it is in the normal lane. A part of the fast
-- lane is put back in that lane, which has it, with the cancellation to raise where it waited.
stopWait :: Run -> Cancelled -> Parked -> IO (Maybe (IO Step))
stopWait run cancelled = \case
  NotWaiting -> pure Nothing
  OnFd fd reading ident -> do
    waiters <- readIORef (runWaiters run)
    case IntMap.lookup fd waiters of
      Nothing -> pure Nothing
      Just (Waiters readers writers) ->
        let (found, rest) = takeEntry ident (if reading then readers else writers)
         in case found of
              Nothing -> pure Nothing
              Just entry -> do
                writeIORef (runWaiters run) (settle fd (if reading then Waiters rest writers else Waiters readers rest) waiters)
                adjust run 4 (-1)
                inLane run (isSuspendedFast entry) (resumeOf entry (throwIO cancelled))
  OnWatch ident -> do
    watching <- readIORef (runWatching run)
    case partition (\(Watching other _ _ _) -> other == ident) watching of
      ([Watching _ fast watch resume], rest) -> writeIORef (runWatching run) rest *> watchCancel watch *> inLane run fast (resume (throwIO cancelled))
      _ -> pure Nothing
  Other stop -> stop cancelled
  InGroup _ -> pure Nothing

-- | What goes on with a part that a cancellation took off what it waited at: the part, in the normal lane, and nothing in the fast lane,
-- where it is put back.
inLane :: Run -> Bool -> IO Step -> IO (Maybe (IO Step))
inLane run fast part = if fast then Nothing <$ modifyIORef' (runFastReady run) (part :) else pure (Just part)

-- | Whether the step is of a part in the fast lane.
isSuspendedFast :: Step -> Bool
isSuspendedFast = \case SuspendedFast {} -> True; _ -> False

-- | A failure that nothing in the part handles: it leaves the run.
newtype Escaped = Escaped SomeException
  deriving (Show)

instance Exception Escaped

-- | Runs a part of "Network.Streaming.Compiled": a failure that leaves it goes to the handler that the part
-- last set, which goes on with the part. A failure of that handler goes to the handler it set.
guardedBy :: Run -> IO Step -> IO Step
guardedBy run = (`catch` unwind)
  where
    unwind :: SomeException -> IO Step
    unwind failure = case fromException failure of
      Just (Escaped _) -> throwIO failure
      Nothing -> (readIORef (runFailure run) >>= \handler -> handler failure) `catch` unwind

-- | A wait of a part that is in a scope: it raises the cancellation that is there, and says where it waits.
-- A part that is in none says nothing.
registering :: Part -> Parked -> IO ()
registering part parked =
  readIORef (partFrames part) >>= \case
    [] -> pure ()
    frames -> cancelledFlagged (partCancelled part) frames >>= maybe (writeIORef (partWait part) parked) (throwIO . Cancelled)
{-# INLINE registering #-}

-- | What a group has of its first failure, and how many of its parts have ended.
data Joined = Joined !(Maybe SomeException) !Int

-- | What a group does when one of its parts, of this many, has ended: the part leaves its scopes, and the group
-- keeps its failure, if it is the first, which cancels the others; the part that ends last goes on with the parent:
-- with the first failure, if there was one.
groupEnded :: Run -> PartScope -> Int -> IORef Joined -> (SomeException -> IO Step) -> (() -> IO Step) -> Part -> Either SomeException () -> IO Step
groupEnded run scope' size joined failure success part outcome = do
  endPart run part
  Joined failed count <- readIORef joined
  failed' <- case outcome of
    Left thrown
      | isNothing failed -> Just thrown <$ cancelScope run scope'
      | otherwise -> pure failed
    Right () -> pure failed
  writeIORef joined (Joined failed' (count + 1))
  if count + 1 < size
    then pure Ended
    else writeIORef (scopeMembers scope') IntMap.empty *> maybe (success ()) failure failed'

-- | What a race does when one of its halves has ended: the first to end is the winner, which cancels the other;
-- the loser, once it has ended, lets the race go on with the parent, with the winner's failure, or the loser's
-- if it isn't its cancellation, or else the winner's result.
raceEnded :: Run -> PartScope -> IORef (Maybe (Either SomeException (Either b c))) -> (SomeException -> IO Step) -> (Either b c -> IO Step) -> Part -> Either SomeException (Either b c) -> IO Step
raceEnded run scope' winner failure success half outcome = do
  endPart run half
  readIORef winner >>= \case
    Nothing -> do
      writeIORef winner (Just outcome)
      cancelScope run scope'
      pure Ended
    Just won -> do
      writeIORef (scopeMembers scope') IntMap.empty
      case (won, outcome) of
        (Left thrown, _) -> failure thrown
        (Right _, Left thrown) | isNothing (cancelledBy thrown) -> failure thrown
        (Right result, _) -> success result

-- | A group that parts are spawned into, for the arrows that run every part on one thread: the cancel scope of its parts, the part
-- that made it, the scopes that its parts begin in, and what it has of its parts. Its body is a part of its own, which is in
-- the scope of the part that made the group and not in that of the parts, so that the joiner's cancellation doesn't reach it.
data Gang j = Gang
  { gangScope :: !PartScope,
    gangParent :: !Part,
    gangFrames :: [Frame Members],
    gangStanding :: !(IORef (Standing j))
  }

-- | What a group has of its parts: how many were spawned, how many members are yet to end (the body is one), the joiner's tally,
-- and what goes on with the part that made the group, which the body gives once it has ended.
data Standing j = Standing !Int !Int !(Joiner.Tally j) (Maybe (Joiner.Tally j -> IO Step))

-- | A new group of the part, and the part that is its body. The part that made it is suspended until the group has ended, and
-- says where its members are, so that a cancellation finds them.
newGang :: (Joiner j) => Run -> Part -> j -> IO (Gang j, Part)
newGang run parent joiner = do
  scope' <- newScope run
  outer <- readIORef (partFrames parent)
  flagged <- if null outer then isCancelled (runRoot run) else readIORef (partCancelled parent)
  let below = if null outer then [Inside (runRoot run)] else outer
  body <- Part <$> nextWait run <*> newIORef below <*> newIORef NotWaiting <*> newIORef flagged
  adjust run 9 1
  modifyIORef' (scopeMembers scope') (IntMap.insert (partIdent body) body)
  standing <- newIORef $! Standing 0 1 (Joiner.begin joiner) Nothing
  writeIORef (partWait parent) (InGroup scope')
  pure (Gang scope' parent (Inside scope' : below) standing, body)

-- | The part for the next spawn, with its number, which is in the scope of the group and in those outside it, and is a member of
-- the group. If the parts of the group were cancelled, there is none: the spawn counts as a part that ended cancelled.
spawnedPart :: forall j. (Joiner j) => Run -> Gang j -> IO (Maybe (Int, Part))
spawnedPart run gang = do
  Standing number live tally finish <- readIORef (gangStanding gang)
  cancelled <- isCancelled (gangScope gang)
  if cancelled
    then do
      let refused = Left (toException (Cancelled (scopeIdent (gangScope gang))))
      Nothing <$ (writeIORef (gangStanding gang) $! Standing (number + 1) live (fst (Joiner.ended @j number refused tally)) finish)
    else do
      outer <- readIORef (partFrames (gangParent gang))
      flagged <- if null outer then isCancelled (runRoot run) else readIORef (partCancelled (gangParent gang))
      child <- Part <$> nextWait run <*> newIORef (gangFrames gang) <*> newIORef NotWaiting <*> newIORef flagged
      adjust run 9 1
      modifyIORef' (scopeMembers (gangScope gang)) (IntMap.insert (partIdent child) child)
      writeIORef (gangStanding gang) $! Standing (number + 1) (live + 1) tally finish
      pure (Just (number, child))
{-# INLINE spawnedPart #-}

-- | The body of the group ended: it leaves its frames and the members, and cancels the parts if it failed, or was cancelled. If no part
-- is left, the part that made the group goes on, with the body's failure, or its result and what the joiner made of the parts, or
-- the failure that it gives; if some are, the one that ends last does.
bodyEnded :: forall j b. (Joiner j) => Run -> Gang j -> Part -> (SomeException -> IO Step) -> ((b, Joiner.Joined j) -> IO Step) -> Either SomeException b -> IO Step
bodyEnded run gang part failure success outcome = do
  endPart run part
  modifyIORef' (scopeMembers (gangScope gang)) (IntMap.delete (partIdent part))
  either (\_ -> cancelScope run (gangScope gang)) (\_ -> pure ()) outcome
  Standing spawned live tally _ <- readIORef (gangStanding gang)
  let finish tally' = case outcome of
        Left thrown -> failure thrown
        Right result -> either failure (success . (,) result) (Joiner.joined @j tally')
  writeIORef (gangStanding gang) $! Standing spawned (live - 1) tally (Just finish)
  groupDone gang (live - 1) tally finish
{-# INLINE bodyEnded #-}

-- | A part of the group, spawned as this number, ended in this way: it leaves its frames and the members, the joiner is told, and
-- the parts are cancelled if it says so. The part that ends last goes on with the part that made the group.
partEnded :: forall j. (Joiner j) => Run -> Gang j -> Part -> Int -> Either SomeException (Joiner.Part j) -> IO Step
partEnded run gang part number outcome = do
  endPart run part
  modifyIORef' (scopeMembers (gangScope gang)) (IntMap.delete (partIdent part))
  Standing spawned live tally finish <- readIORef (gangStanding gang)
  let (tally', cancelling) = Joiner.ended @j number outcome tally
  when cancelling (cancelScope run (gangScope gang))
  writeIORef (gangStanding gang) $! Standing spawned (live - 1) tally' finish
  maybe (pure Ended) (groupDone gang (live - 1) tally') finish
{-# INLINE partEnded #-}

-- | The group has ended if no member is left: its scope holds no part, and the part that made it goes on.
groupDone :: Gang j -> Int -> Joiner.Tally j -> (Joiner.Tally j -> IO Step) -> IO Step
groupDone gang live tally finish
  | live > 0 = pure Ended
  | otherwise = writeIORef (scopeMembers (gangScope gang)) IntMap.empty *> finish tally

-- | Gives the scheduler a part to go on with, in its next round: the part that a spawn made.
enqueue :: Run -> IO Step -> IO ()
enqueue run part = modifyIORef' (runTickets run) (part :)

-- | What goes on with a part that waits, from its step.
resumeOf :: Step -> IO () -> IO Step
resumeOf (Suspended _ _ resume) = resume
resumeOf (SuspendedFast _ _ resume) = resume
resumeOf _ = const (pure Ended)

-- | The step of the wait, and the others.
takeEntry :: Int -> [Step] -> (Maybe Step, [Step])
takeEntry ident = go []
  where
    go before = \case
      [] -> (Nothing, reverse before)
      entry@(Suspended _ other _) : after
        | other == ident -> (Just entry, reverse before <> after)
        | otherwise -> go (entry : before) after
      entry@(SuspendedFast _ other _) : after
        | other == ident -> (Just entry, reverse before <> after)
        | otherwise -> go (entry : before) after
      entry : after -> go (entry : before) after

-- | Runs the first part, and the parts it forks, until the result is there:
-- each part that can go on in turn, entered with the given action, until it
-- has run for the run's turn and reaches a point to give way at, which are
-- mostly the receives and sends it makes. At the end it closes the run's
-- mailbox, so that those that give parts to it after that are told they
-- didn't; the run's poller stays until 'freeRun'.
--
-- An asynchronous exception reaches the scheduler where it waits for a
-- descriptor or a watch, and at the start of each round: the parts that are in
-- a scope are cancelled, and the run goes on until they have ended, and then
-- fails with the exception. The exception of a part that no scope holds fails
-- the run at once.
schedule :: forall m b. (Instrumented m) => Run -> (IO Step -> IO Step) -> IORef (Maybe b) -> IO Step -> IO (b, Report)
schedule run enterPart result start = (observing @m run *> go [start] [] [] True `catch` failed) `finally` closeInbox run
  where
    -- A shutdown fails the run with its exception, whatever the parts failed with, which is their cancellation.
    failed :: SomeException -> IO a
    failed failure = readIORef (runShutdown run) >>= throwIO . fromMaybe (unescaped failure)
    unescaped failure = maybe failure (\(Escaped inner) -> inner) (fromException failure)
    -- The parts that can go on in this round, in the next and in the fast lane, and whether it is the fast lane's turn. The lanes
    -- take turns, one part of each, and a lane that has none lets the other go on. The fast lane is looked at only when a part has
    -- entered it, and the parts that other cores gave it are taken at its turns, so that a part that is woken is served after at most
    -- one part of the normal lane, not at the end of the round.
    go !ready later !fast !fastTurn = do
      lanes <- counter run 15
      fast' <- if lanes /= 0 && fastTurn then fastReady fast else pure fast
      case fast' of
        part : others | fastTurn -> turn True part ready later others
        _ -> case ready of
          part : others -> turn False part others later fast'
          [] -> boundary later fast' fastTurn
    -- A turn of the part, after which it is the other lane's turn.
    turn !fromFast part ready later fast = do
      clock >>= setCounter run 0
      step <- enterPart part
      readIORef result >>= \case
        Just value -> (\steps waits -> (value, Report steps waits)) <$> counter run 1 <*> counter run 2
        Nothing -> case step of
          Ended -> go ready later fast (not fromFast)
          Timed -> go ready later fast (not fromFast)
          Parked -> adjust run 5 1 *> go ready later fast (not fromFast)
          ParkedRemote -> adjust run 3 1 *> go ready later fast (not fromFast)
          Yielded part' ->
            counter run 16 >>= \inFast ->
              if inFast == 0
                then go ready (part' : later) fast (not fromFast)
                else setCounter run 16 0 *> modifyIORef' (runFastReady run) (part' :) *> go ready later fast (not fromFast)
          Forked parts -> go ready (foldl (flip (:)) later parts) fast (not fromFast)
          Suspended event ident resume ->
            counter run 16 >>= \inFast ->
              if inFast == 0
                then suspended False fromFast step event ident resume ready later fast
                else setCounter run 16 0 *> suspended True fromFast (SuspendedFast event ident resume) event ident resume ready later fast
          SuspendedFast event ident resume -> suspended True fromFast step event ident resume ready later fast
    -- A part that waits for the event, and the turn after it.
    suspended !inFast !fromFast step event !ident resume ready later fast = case event of
      Readable fd -> waitOn fd True step *> adjust run 4 1 *> go ready later fast (not fromFast)
      Writable fd -> waitOn fd False step *> adjust run 4 1 *> go ready later fast (not fromFast)
      Until (Instant at) -> addTimer (runWheel run) at (modifyIORef' (if inFast then runFastReady run else runTickets run) (resume (pure ()) :)) *> go ready later fast (not fromFast)
      Watched register -> register >>= \watch -> modifyIORef' (runWatching run) (Watching ident inFast watch resume :) *> go ready later fast (not fromFast)
    -- Take another batch only after the staged parts have run. In each batch,
    -- the parts from the mailbox go before the ones put back locally.
    fastReady = \case
      fast@(_ : _) -> pure fast
      [] -> do
        remote <- takeFastInbox run
        local <- readIORef (runFastReady run)
        when (not (null local)) (writeIORef (runFastReady run) [])
        drainArrivals run (reverse local) remote
    -- The normal lane has none left in this round.
    boundary later fast !fastTurn = do
        -- Each round, the parts that descriptors became ready for, that other
        -- cores gave, and that tickets or timers woke, or a cancellation took
        -- off what they waited at, go before those that yielded: a part that
        -- waited for something is woken at the start of the next round, not at
        -- its end. Those that yielded run every round, so they are not starved.
        rounds <- counter run 7
        setCounter run 7 (rounds + 1)
        setCounter run 8 0
        when (rounds .&. 63 == 0) (interruptedBy allowInterrupt)
        -- The timers that are due: deadlines cancel their scopes, and sleepers wake.
        timers <- pending (runWheel run)
        when (timers > 0) (roundNow run >>= expire (runWheel run))
        tickets <- readIORef (runTickets run)
        writeIORef (runTickets run) []
        remote <- takeInbox run
        let local = List.foldl' (flip (:)) (reverse later) tickets
        -- Consume the local list directly on empty rounds so it need not be thunked for the drain.
        ready <- case remote of
          [] -> pure local
          _ -> drainArrivals run local remote
        case ready of
          -- No part of the normal lane can go on: the fast lane goes on, if it has a part, and else the blocking poll in 'idle' returns
          -- at once if a descriptor is ready.
          [] -> fastReady fast >>= \case
            [] -> idle
            fast' -> go [] [] fast' True
          others -> looking >>= \polled -> go (polled <> others) [] fast fastTurn
    -- Polls without waiting, if 'pollTurns' turns have passed since the last poll or enough armings are waiting for one, and
    -- gives the parts it woke. The armings are those of waits that were cancelled: they stay armed until a poll, and on kqueue
    -- they are changes that the poll submits.
    looking = do
      descriptors <- counter run 4
      armed <- counter run 10
      if descriptors + armed == 0
        then pure []
        else do
          -- Not the time of the round, which the clock arrow gives to the parts and which is read when it is first asked for.
          began <- clock
          polledAt <- counter run 12
          if armed >= pendingArmings || began - polledAt >= pollTurns * runTurn run then pollNow else pure []
    idle = do
      remote <- counter run 3
      descriptors <- counter run 4
      parked <- counter run 5
      watching <- readIORef (runWatching run)
      shutdown <- readIORef (runShutdown run)
      inScopes <- counter run 9
      timers <- pending (runWheel run)
      case () of
        _
          | Just failure <- shutdown, inScopes == 0 -> throwIO failure
          | null watching && descriptors == 0 && remote == 0 && timers == 0 ->
              ioError . userError $
                if parked > (0 :: Int)
                  then "the parts wait for tickets that nothing will fulfil"
                  else "the program ended without a result"
          | null watching -> do
              -- Unless a part was given since the round began: the next to give
              -- one wakes the poller.
              early <- beforeSleep run >>= drainArrivals run []
              -- Or a part of the fast lane, which the mailbox gave.
              fastGiven <- fastWaiting run
              if null early && not fastGiven
                then do
                  countWait
                  until' <- untilNextTimer
                  polled <- pollWait (maybe (-1) (fromIntegral . min 2000000000 . (`div` 1000000) . (+ 999999)) until')
                  given <- afterSleep run >>= drainArrivals run []
                  -- The time of the round was read before the wait, if a timer made the round expire its timers: the parts that the wait ended
                  -- read it again, as a round's first part does.
                  setCounter run 8 0
                  go (polled <> given) [] [] True
                else go early [] [] True
          | otherwise -> do
              countWait
              -- Watches, and descriptors too, and the mailbox: the watches for a
              -- millisecond at most, then the descriptors without waiting, and the
              -- parts that other cores gave in the round that follows.
              until' <- untilNextTimer
              let micros = case (descriptors > 0 || remote > 0, until') of
                    (True, Just nanos) -> Just (min 1000 (fromIntegral (nanos `div` 1000)))
                    (True, Nothing) -> Just 1000
                    (False, Just nanos) -> Just (fromIntegral (nanos `div` 1000))
                    (False, Nothing) -> Nothing
              delay <- maybe (pure Nothing) (fmap Just . delayWatch) micros
              interruptedBy . atomically $ maybe retry watchReady delay `orElse` foldr (orElse . (\(Watching _ _ watch _) -> watchReady watch)) retry watching
              mapM_ watchCancel delay
              watched <- readIORef (runWatching run)
              checked <- traverse (\entry@(Watching _ _ watch _) -> (,) entry <$> hasFired watch) watched
              let (fired, still) = partition snd checked
              mapM_ (\(Watching _ _ watch _, _) -> watchCancel watch) fired
              writeIORef (runWatching run) (map fst still)
              armed <- counter run 10
              polled <- if descriptors + armed > 0 then pollNow else pure []
              setCounter run 8 0
              let (firedFast, firedNormal) = partition (\(Watching _ inFast _ _, _) -> inFast) fired
              modifyIORef' (runFastReady run) (\queued -> foldl (\newest (Watching _ _ _ resume, _) -> resume (pure ()) : newest) queued firedFast)
              go (polled <> map (\(Watching _ _ _ resume, _) -> resume (pure ())) firedNormal) [] [] True
    countWait = counter run 2 >>= setCounter run 2 . (+ 1)
    -- How long until the next timer is due, in nanoseconds.
    untilNextTimer =
      nextDeadline (runWheel run) >>= \case
        Nothing -> pure Nothing
        Just deadline -> (\now -> Just (if deadline > now then deadline - now else 0)) <$> monotonicNanoseconds
    -- An asynchronous exception that arrives at a wait begins a shutdown: every part that is in a scope is
    -- cancelled. One that arrives again is the same.
    shuttingDown failure =
      readIORef (runShutdown run) >>= \case
        Nothing -> writeIORef (runShutdown run) (Just failure) *> cancelScope run (runRoot run)
        Just _ -> pure ()
    interruptedBy wait = try wait >>= either shuttingDown pure
    -- Adds the part to the waiters of the descriptor, and has the poller arm the descriptor for the interest, unless
    -- parts that wait for it have already: it is armed until it reports.
    waitOn (Fd fd) reading entry = do
      waiters <- readIORef (runWaiters run)
      let key = fromIntegral fd
          Waiters readers writers = IntMap.findWithDefault (Waiters [] []) key waiters
          waiting = if reading then Waiters (entry : readers) writers else Waiters readers (entry : writers)
      bump @m DescriptorWaits
      when (null (if reading then readers else writers)) $ do
        debugging @m $ do
          number <- counter run 11
          moved <- armedBy key (fromIntegral number)
          when moved (bump @m DescriptorMoves)
        bump @m Armings
        arm fd (if reading then kindReadable else kindWritable) waiting *> adjust run 10 1
      writeIORef (runWaiters run) (IntMap.insert key waiting waiters)
    -- The poller arms the descriptor for the kinds that the waiters wait for, of which added are new.
    arm fd added waiting =
      c_pollerArm (runPoller run) fd added (kindsOf waiting) >>= \calls ->
        if calls < 0 then throwErrno "streaming_io_poller_arm" else bumpBy @m ArmCalls (fromIntegral calls)
    -- Asks the poller what became ready, without waiting.
    pollNow = bump @m PollsImmediate *> rawPoll c_pollerPoll 0 >>= afterPoll
    -- Waits in the poller, which an asynchronous exception can interrupt: it begins a shutdown. No part runs
    -- while the scheduler waits, so it waits unmasked: a masked thread in an interruptible foreign call returns
    -- from it with the exception still to come.
    pollWait timeout =
      bump @m PollsBlocking *> capped timeout >>= \limit -> try (interruptible (rawPoll c_pollerWait limit)) >>= \case
        Left failure -> [] <$ shuttingDown failure
        Right count -> afterPoll count >>= \ready -> ready <$ afterWait ready
    -- How long the wait may last: as told on the threaded run time. On the non-threaded one, a millisecond while other
    -- threads need the OS thread, and else the cap that 'afterWait' has settled on.
    capped timeout
      | rtsSupportsBoundThreads = pure timeout
      | otherwise = do
          shared <- counter run 13
          settled <- counter run 14
          let limit = if shared /= 0 then sharedCap else if settled == 0 then firstCap else settled
          pure (if timeout < 0 || fromIntegral timeout > limit then fromIntegral limit else timeout)
    -- What a wait that ended means for the next, on the non-threaded run time. A wait that gave nothing, which a signal or
    -- its cap ended, lets the other threads run, and the next one is cut short if any of them needs the OS thread, and
    -- else may last twice as long, up to 'lastCap': an idle scheduler settles at that. A wait that gave something starts
    -- the caps over.
    afterWait ready
      | rtsSupportsBoundThreads = pure ()
      | null ready = do
          shared <- yieldToOthers
          setCounter run 13 (fromEnum shared)
          settled <- counter run 14
          setCounter run 14 (min lastCap (max firstCap settled * 2))
      | otherwise = setCounter run 14 0
    rawPoll call timeout =
      unsafeWithForeignPtr (runReady run) $ \fds ->
        unsafeWithForeignPtr (runKinds run) $ \kinds ->
          call (runPoller run) fds kinds (fromIntegral batch) timeout
    -- The parts that wait on what became ready.
    afterPoll count
      | count < 0 = getErrno >>= \errno -> if errno == eINTR then pure [] else throwErrno "streaming_io_poller_wait"
      | otherwise = do
          setCounter run 10 0
          clock >>= setCounter run 12
          if count == 0
            then pure []
            else do
              waiters <- readIORef (runWaiters run)
              unsafeWithForeignPtr (runReady run) $ \fds ->
                unsafeWithForeignPtr (runKinds run) $ \kinds -> do
                  (waiters', normal, fast, woken) <- collectReady fds kinds (fromIntegral count) 0 waiters
                  writeIORef (runWaiters run) waiters'
                  when (woken /= 0) (adjust run 4 (negate woken))
                  queueFastReady fast
                  pure normal
    queueFastReady [] = pure ()
    queueFastReady fast = modifyIORef' (runFastReady run) (\queued -> List.foldl' (flip (:)) queued fast)
    -- Read and rearm in poll order, using the updated waiters when the poll reports the same descriptor again.
    -- On the way back, prepend each report's waiters, newest first within each direction, to get FIFO ready lists.
    collectReady fds kinds count !index !waiters
      | index == count = pure (waiters, [], [], 0)
      | otherwise = do
          fd <- peekElemOff fds index
          kind <- peekElemOff kinds index
          let key = fromIntegral fd
              Waiters readers writers = IntMap.findWithDefault (Waiters [] []) key waiters
              (fromReaders, readers') = if kind .&. kindReadable /= 0 then (readers, []) else ([], readers)
              (fromWriters, writers') = if kind .&. kindWritable /= 0 then (writers, []) else ([], writers)
              waiting = Waiters readers' writers'
          -- On epoll, a report may have used up the arming of both directions.
          when (kindsOf waiting /= 0) (arm fd 0 waiting)
          (waiters', normal, fast, woken) <- collectReady fds kinds count (index + 1) (settle key waiting waiters)
          routeReady waiters' fromWriters fromReaders normal fast woken
    routeReady waiters [] [] normal fast !woken = pure (waiters, normal, fast, woken)
    routeReady waiters [] readers normal fast woken = routeReady waiters readers [] normal fast woken
    routeReady waiters (entry : entries) readers normal fast !woken
      | isSuspendedFast entry = routeReady waiters entries readers normal (resume : fast) (woken + 1)
      | otherwise = routeReady waiters entries readers (resume : normal) fast (woken + 1)
      where
        resume = resumeOf entry (pure ())

-- | How long the scheduler waits in its poller on the non-threaded run time, in milliseconds: the first wait that ends with
-- nothing, the longest, and while other threads need the OS thread. That run time has one OS thread, which a blocking
-- foreign call holds: no other thread of the program runs while the scheduler waits. A signal ends the wait, and the
-- handler's thread, which the run time starts only once the scheduler thread has gone back to it, gets its turn then
-- ('yieldToOthers'). But a signal that arrived before the wait, while a part ran, ends nothing: its handler's thread waits
-- behind the scheduler thread, which would wait for ever. So a wait ends by itself, and an idle scheduler wakes up twice a
-- second, after the first few waits, which are shorter. The threaded run time runs the handler's thread on another OS
-- thread, and waits as long as it is told.
firstCap, lastCap, sharedCap :: Int
firstCap = 10
lastCap = 500
sharedCap = 1

-- | Lets the other threads of the program run until each has ended or waits, which a signal's handler needs to stop the
-- run: its thread throws to the thread that waits for the run, which stops the scheduler's thread in its turn. The
-- scheduler thread yields as long as another thread can run, at most four times. It says whether another thread
-- needs the OS thread: one that can still run, or that waits for the run time's own wait for a descriptor or for a delay,
-- which the run time serves only when the scheduler thread gives the OS thread back. A thread that waits for an 'MVar'
-- or a transaction is woken by another thread, and needs nothing from the scheduler.
yieldToOthers :: IO Bool
yieldToOthers = myThreadId >>= \me -> go me (4 :: Int)
  where
    go me turns = do
      yield
      others <- filter (/= me) <$> listThreads
      statuses <- traverse threadStatus others
      let running = any (== ThreadRunning) statuses
      if running && turns > 1 then go me (turns - 1) else pure (any needsThread statuses)
    needsThread = \case
      ThreadRunning -> True
      ThreadBlocked BlockedOnOther -> True
      _ -> False

-- | The kinds of readiness that the poller reports and is armed for: those of C, which are bits.
kindReadable, kindWritable :: CInt
kindReadable = 1
kindWritable = 2

-- | The kinds that the waiters wait for.
kindsOf :: Waiters -> CInt
kindsOf (Waiters readers writers) = (if null readers then 0 else kindReadable) .|. (if null writers then 0 else kindWritable)

-- | The waiters of a descriptor in the map, or the map without the descriptor when none are left.
settle :: Int -> Waiters -> IntMap.IntMap Waiters -> IntMap.IntMap Waiters
settle key waiting@(Waiters readers writers) waiters
  | null readers && null writers = IntMap.delete key waiters
  | otherwise = IntMap.insert key waiting waiters

-- | Whether the watch has fired, without waiting.
hasFired :: Watch -> IO Bool
hasFired watch = atomically ((True <$ watchReady watch) `orElse` pure False)

foreign import ccall unsafe "streaming_io_poller_new"
  c_pollerNew :: IO (Ptr Poller)

foreign import ccall unsafe "streaming_io_poller_free"
  c_pollerFree :: Ptr Poller -> IO ()

-- | Arms the descriptor for the next poll: for the kinds that are new, and for all that parts wait for. Gives the
-- number of system calls it made, or -1 and the error.
foreign import ccall unsafe "streaming_io_poller_arm"
  c_pollerArm :: Ptr Poller -> CInt -> CInt -> CInt -> IO CInt

-- | Ends the poller's wait, or its next one, from any thread.
foreign import ccall unsafe "streaming_io_poller_wake"
  c_pollerWake :: Ptr Poller -> IO CInt

-- | Asks without waiting.
foreign import ccall unsafe "streaming_io_poller_wait"
  c_pollerPoll :: Ptr Poller -> Ptr CInt -> Ptr CInt -> CInt -> CInt -> IO CInt

-- | Waits, and can be interrupted, so that an asynchronous exception reaches
-- the scheduler.
foreign import ccall interruptible "streaming_io_poller_wait"
  c_pollerWait :: Ptr Poller -> Ptr CInt -> Ptr CInt -> CInt -> CInt -> IO CInt
