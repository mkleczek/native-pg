{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE InterruptibleFFI #-}
{-# LANGUAGE LambdaCase #-}

-- | The scheduler of the arrows that run every part of a program on one thread
-- as code: "Network.Streaming.Compiled" and "Network.Streaming.Delimited". A
-- part is an action that runs until it ends, waits, forks or yields, and says
-- which. The scheduler owns a poller (kqueue, or epoll on Linux): a part that
-- blocks on a descriptor's readability or writability has the poller arm that
-- interest, once, for its next poll, and the scheduler asks the poller what
-- became ready when no part can go on, or once a round otherwise. A wait costs
-- no thread of GHC's IO manager, and on kqueue no system call of its own: the
-- armings go with the poll. A descriptor is armed at each wait, not once for a
-- run, so that a number that a new socket got after a close is waited on like
-- any other, and a readiness that came before the wait is found by it. The
-- scheduler keeps a descriptor's waiting parts only while there are some. A part
-- that waits for a ticket is held by the ticket.
-- A part runs for a turn, a length of time: at a point to give way at, once it
-- has run longer than that, it lets the others go on before it.
--
-- Each scheduler also has a mailbox that other threads, the schedulers of other
-- cores, put parts in: a part that waited for a ticket that another core gives
-- is put there by the giver, which wakes the poller if the scheduler waits in
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
-- changing, however many parts there are. The scheduler runs masked, and an
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
    RemoteSlot (..),
    awaitRemote,
    giveRemote,
    abandonRemote,
    Mailbox,
    runMailbox,
    post,
    schedule,
    Part (..),
    PartScope,
    Parked (..),
    newPart,
    newScope,
    childPart,
    childParts,
    endPart,
    pushFrame,
    popFrame,
    checkpoint,
    cancelScope,
    registering,
    nextWait,
    adjust,
    Joined (..),
    groupEnded,
    raceEnded,
    Escaped (..),
    guardedBy,
    runFailure,
    runWheel,
    roundNow,
    sleepUntil,
  )
where

import Control.Concurrent.STM (atomically, check, orElse, readTVar, registerDelay, retry)
import Control.Exception (Exception, SomeException, allowInterrupt, bracket, catch, finally, fromException, interruptible, throwIO, try)
import Control.Monad (foldM, replicateM, when, (>=>))
import Data.Bits ((.&.), (.|.))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.List (partition)
import Data.Maybe (fromMaybe, isNothing)
import Data.Word (Word64)
import Foreign.C.Error (eINTR, getErrno, throwErrno, throwErrnoIfNull)
import Foreign.C.Types (CInt (..))
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peekElemOff, pokeElemOff)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Streaming.Classes (Cancelled (..), Event (..), Instant (..), cancelledBy, monotonicNanoseconds)
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.Cancel
import Network.Streaming.Internal.Descriptors (armedBy, newRunNumber)
import Network.Streaming.Internal.Tickets (Slot (..))
import Network.Streaming.Internal.Timers
import Network.Streaming.Loop (Report (..))
import Network.Streaming.Mode (Instrumented (bump, bumpBy, debugging))
import Network.Streaming.Watch (Watch (..))
import System.Posix.Types (Fd (..))

-- | What a part comes to.
data Step
  = -- | It ended, at a join or with the program's result.
    Ended
  | -- | It waits for the event, and goes on with the action once it fires; the number is what the part's
    -- wait is called by when it is in a scope, and 0 otherwise. The action is what runs where the part
    -- waits: nothing, or the cancellation to raise.
    Suspended !Event !Int (IO () -> IO Step)
  | -- | Parts that go on at once: the halves of a race, or the parts of a group.
    Forked [IO Step]
  | -- | It waits for a ticket, whose slot holds it.
    Parked
  | -- | It waits for a ticket that another core gives, whose slot holds it; the
    -- giver puts it in the mailbox.
    ParkedRemote
  | -- | It ran for its turn, and can go on.
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
-- has no frames, and says nothing of where it waits.
data Part = Part
  { partIdent :: !Int,
    partFrames :: !(IORef [Frame [Part]]),
    partWait :: !(IORef Parked),
    partCancelled :: !(IORef Bool)
  }

-- | A scope of a run, with the parts that its body began with: those of a group or the halves of a race, or the part
-- that entered a deadline. The others inside it are reached through those: a part that is suspended at a group says so
-- (see 'InGroup'), and its parts are inside what the part is inside.
type PartScope = CancelScope [Part]

-- | Where a part waits, as what takes it off that.
data Parked
  = NotWaiting
  | forall a. OnTicket !(IORef (Slot a))
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
    -- waits, the parts that wait for tickets that other cores give and are not
    -- yet back from the mailbox, the parts that wait on descriptors, those that
    -- wait for tickets, the number of the next wait, the rounds, the time that
    -- the round began at, or 0 before it is read, the parts that are in a
    -- scope, the armings that no poll has taken yet, and the run's own number: unboxed, so that counting allocates nothing. A step is a point to
    -- give way at, which a link's receive or send is, or a wait, or a ticket
    -- given or waited for.
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
    runWheel :: !Wheel
  }

-- | The parts that wait on a descriptor, as the 'Suspended' steps they gave the scheduler.
data Waiters = Waiters [Step] [Step]

-- | A part that waits on a watch.
data Watching = Watching !Int !Watch (IO () -> IO Step)

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
-- once nothing that is given parts through its mailbox can run any more. Its
-- parts run for turns of that length.
newRun :: Turn -> IO Run
newRun (Turn turn) = do
  counters <- mallocForeignPtrArray 12
  unsafeWithForeignPtr counters $ \pointer -> mapM_ (\index -> pokeElemOff pointer index 0) [0 .. 11]
  number <- newRunNumber
  unsafeWithForeignPtr counters $ \pointer -> pokeElemOff pointer 11 (fromIntegral number)
  poller <- throwErrnoIfNull "streaming_io_poller_new" c_pollerNew
  inbox <- newIORef (Open [] False)
  Run <$> newIORef [] <*> pure counters <*> pure turn <*> pure poller <*> pure (Mailbox inbox poller) <*> mallocForeignPtrArray batch <*> mallocForeignPtrArray batch <*> newIORef IntMap.empty <*> newIORef [] <*> newCancelScope 0 [] <*> newIORef Nothing <*> newIORef (throwIO . Escaped) <*> (monotonicNanoseconds >>= newWheel)

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
arrivals run parts = parts <$ adjust run 3 (negate (length parts))

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
  timer <- addTimer (runWheel run) at (modifyIORef' (runTickets run) (resume (pure ()) :))
  readIORef (partFrames part) >>= \case
    [] -> pure ()
    _ -> writeIORef (partWait part) (Other (\cancelled -> cancelTimer (runWheel run) timer >>= \wasLive -> pure (if wasLive then Just (resume (throwIO cancelled)) else Nothing)))
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
stepped run continue = spent run >>= \yields -> if yields then pure (Yielded continue) else continue
{-# INLINE stepped #-}

-- | Counts a step that suspends its part.
counted :: Run -> Step -> IO Step
counted run step = step <$ (counter run 1 >>= setCounter run 1 . (+ 1))
{-# INLINE counted #-}

-- | Puts a part that a ticket woke back among those that can go on.
wakeTicket :: Run -> IO Step -> IO ()
wakeTicket run part = modifyIORef' (runTickets run) (part :) *> adjust run 5 (-1)

-- | What the slot of a ticket of reach @'Anywhere@ holds, and what a thread that gives it a value finds: nothing
-- yet, the value, the part that waits for it, or that the waiter has gone. Several threads read and change it,
-- atomically. The value is evaluated, as a local slot's is. The waiter is what puts the part back among those that
-- can go on, with the action that runs where it waited: the value, or a cancellation to raise.
data RemoteSlot a = RemoteEmpty | RemoteGiven !a | RemoteWaiter (IO a -> IO Step) | RemoteAbandoned

-- | What a give did to the slot.
data Giving a = Stored | Resume (IO a -> IO Step) | Gone | Twice

-- | What a waiter's parking did to the slot.
data Parking a = Parking | Ready a | Misused

-- | What a part does that waits for a ticket of reach @'Anywhere@, for the two arrows that run parts on one
-- thread. If the value is there, the arrow goes on with it. If it isn't, the part says where it waits, so that a
-- cancellation takes it off the slot, and the arrow captures what follows the wait, which it hands to the slot
-- as the waiter. It is the arrow's own to say how it goes on and how it captures.
awaitRemote :: Run -> Part -> IORef (RemoteSlot a) -> (a -> IO r) -> (((IO a -> IO Step) -> IO Step) -> IO r) -> IO r
awaitRemote run part slot given capture =
  readIORef slot >>= \case
    RemoteGiven value -> checkpoint part *> given value
    RemoteEmpty -> do
      registering part (Other (stopRemote run slot))
      capture (parkRemote slot)
    _ -> ioError (userError "a ticket was waited for twice, or after it was abandoned")
{-# INLINE awaitRemote #-}

-- | Parks the captured part in the slot, which was empty at the look: unless a value was given since, and then
-- the part goes on in its turn.
parkRemote :: IORef (RemoteSlot a) -> (IO a -> IO Step) -> IO Step
parkRemote slot resume = do
  parking <- update slot $ \case
    RemoteEmpty -> (RemoteWaiter resume, Parking)
    RemoteGiven value -> (RemoteGiven value, Ready value)
    other -> (other, Misused)
  case parking of
    Parking -> pure ParkedRemote
    -- Given since the look: goes on in its turn.
    Ready value -> pure (Yielded (resume (pure value)))
    Misused -> ioError (userError "a ticket was waited for twice, or after it was abandoned")

-- | Takes the part off the slot, if it still waits there: not if a value was given since, which has put the
-- part in the mailbox, and which the part finds at its next wait.
stopRemote :: Run -> IORef (RemoteSlot a) -> Cancelled -> IO (Maybe (IO Step))
stopRemote run slot cancelled =
  update slot (\case RemoteWaiter resume -> (RemoteEmpty, Just resume); other -> (other, Nothing)) >>= \case
    Nothing -> pure Nothing
    Just resume -> Just (resume (throwIO cancelled)) <$ adjust run 3 (-1)

-- | Gives the slot its value, and if a part waits for it, puts the part in the
-- mailbox of the scheduler that owns it. It says whether the waiter takes it. The value is evaluated before
-- the compare-and-swap, not between its read and its swap.
giveRemote :: Mailbox -> IORef (RemoteSlot a) -> a -> IO Bool
giveRemote mailbox slot !value = do
  giving <- update slot $ \case
    RemoteEmpty -> (RemoteGiven value, Stored)
    RemoteWaiter resume -> (RemoteGiven value, Resume resume)
    RemoteAbandoned -> (RemoteAbandoned, Gone)
    given -> (given, Twice)
  case giving of
    Stored -> pure True
    Resume resume -> post mailbox (resume (pure value))
    Gone -> pure False
    Twice -> ioError (userError "a ticket was given a value twice")

-- | The waiter stops wanting the value, before it waits for it: a later give says it isn't taken. A value given
-- already is the waiter's, and this gives it back.
abandonRemote :: IORef (RemoteSlot a) -> IO (Maybe a)
abandonRemote slot =
  update slot $ \case
    RemoteGiven value -> (RemoteAbandoned, Just value)
    _ -> (RemoteAbandoned, Nothing)

-- | The first part of a run, in no scope: the run's root scope begins with it.
newPart :: Run -> IO Part
newPart run = do
  part <- Part <$> nextWait run <*> newIORef [] <*> newIORef NotWaiting <*> newIORef False
  part <$ modifyIORef' (scopeMembers (runRoot run)) (part :)

-- | A scope for a group, a race or a deadline of the run.
newScope :: Run -> IO PartScope
newScope run = nextWait run >>= (`newCancelScope` [])

-- | A part for a half of a race, in the scope of the race as well as in those of the part that made it. The
-- parent, which is suspended until the race has ended, says where its halves are. The part begins with the flag of its
-- parent, since a cancellation that came before it existed has not visited it; a parent in no scope is in the root's.
childPart :: Run -> Part -> PartScope -> IO Part
childPart run parent scope' = do
  outer <- readIORef (partFrames parent)
  flagged <- if null outer then readIORef (scopeCancelled (runRoot run)) else readIORef (partCancelled parent)
  child <- Part <$> nextWait run <*> newIORef (Inside scope' : if null outer then [Inside (runRoot run)] else outer) <*> newIORef NotWaiting <*> newIORef flagged
  adjust run 9 1
  modifyIORef' (scopeMembers scope') (<> [child])
  child <$ writeIORef (partWait parent) (InGroup scope')

-- | The parts of a group of this many, in the scope of the group as well as in those of the part that made it. The
-- parent, which is suspended until the group has ended, says where they are, and the scope holds them: it is given
-- them once, not one at a time. Each begins with the flag of the parent, as a half of a race does.
childParts :: Run -> Part -> PartScope -> Int -> IO [Part]
childParts run parent scope' count = do
  outer <- readIORef (partFrames parent)
  flagged <- if null outer then readIORef (scopeCancelled (runRoot run)) else readIORef (partCancelled parent)
  let frames = Inside scope' : if null outer then [Inside (runRoot run)] else outer
  children <- replicateM count (Part <$> nextWait run <*> newIORef frames <*> newIORef NotWaiting <*> newIORef flagged)
  adjust run 9 count
  writeIORef (scopeMembers scope') children
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
pushFrame :: Run -> Part -> Frame [Part] -> IO ()
pushFrame run part frame = do
  frames <- readIORef (partFrames part)
  case frames of
    [] -> do
      adjust run 9 1
      writeIORef (partFrames part) [frame, Inside (runRoot run)]
      -- The root may have been cancelled while the part was in no scope.
      readIORef (scopeCancelled (runRoot run)) >>= \cancelled -> when cancelled (writeIORef (partCancelled part) True)
    _ -> writeIORef (partFrames part) (frame : frames)
  case frame of Inside s -> writeIORef (scopeMembers s) [part]; _ -> pure ()
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
    forget = \case Inside s -> writeIORef (scopeMembers s) []; _ -> pure ()

-- | Raises the cancellation that reaches the part, if there is one: it looks at the part's scopes only if its flag says
-- that one may have.
checkpoint :: Part -> IO ()
checkpoint part =
  readIORef (partCancelled part) >>= \flagged ->
    when flagged (readIORef (partFrames part) >>= reachedBy (partCancelled part) >>= maybe (pure ()) (throwIO . Cancelled))

-- | Cancels the scope: its flag is set, and so is that of every part inside it, whether or not it waits and a shield
-- holds it. The parts that wait, and whose scope lies above their innermost shield, are taken off what they wait at,
-- and put back among those that can go on, with the cancellation to raise where they wait. The others see it at their
-- next wait. The parts inside it are those it began with, and the parts of the groups and the halves of the races that
-- those are suspended at, and so on.
cancelScope :: Run -> PartScope -> IO ()
cancelScope run scope' = do
  writeIORef (scopeCancelled scope') True
  readIORef (scopeMembers scope') >>= inside
  where
    inside = \case
      [] -> pure ()
      part : others -> do
        frames <- readIORef (partFrames part)
        waiting <- readIORef (partWait part)
        writeIORef (partCancelled part) True
        when (deliverable (scopeIdent scope') frames) $
          stopWait run (Cancelled (scopeIdent scope')) waiting >>= \case
            Nothing -> pure ()
            Just resume -> modifyIORef' (runTickets run) (resume :)
        case waiting of
          InGroup members -> readIORef (scopeMembers members) >>= \more -> inside (more <> others)
          _ -> inside others

-- | Takes the part off what it waits at, if it still does: what goes on with the part.
stopWait :: Run -> Cancelled -> Parked -> IO (Maybe (IO Step))
stopWait run cancelled = \case
  NotWaiting -> pure Nothing
  -- The waiter in the slot puts the part back among those that can go on itself, as a give does, with the
  -- cancellation to raise where it waits.
  OnTicket slot ->
    readIORef slot >>= \case
      Waiter wake -> Nothing <$ (writeIORef slot Empty *> wake (throwIO cancelled))
      _ -> pure Nothing
  OnFd fd reading ident -> do
    waiters <- readIORef (runWaiters run)
    case IntMap.lookup fd waiters of
      Nothing -> pure Nothing
      Just (Waiters readers writers) ->
        let (found, rest) = takeEntry ident (if reading then readers else writers)
         in case found of
              Nothing -> pure Nothing
              Just resume -> do
                writeIORef (runWaiters run) (settle fd (if reading then Waiters rest writers else Waiters readers rest) waiters)
                adjust run 4 (-1)
                pure (Just (resume (throwIO cancelled)))
  OnWatch ident -> do
    watching <- readIORef (runWatching run)
    case partition (\(Watching other _ _) -> other == ident) watching of
      ([Watching _ watch resume], rest) -> Just (resume (throwIO cancelled)) <$ (writeIORef (runWatching run) rest *> watchCancel watch)
      _ -> pure Nothing
  Other stop -> stop cancelled
  InGroup _ -> pure Nothing

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
    else writeIORef (scopeMembers scope') [] *> maybe (success ()) failure failed'

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
      writeIORef (scopeMembers scope') []
      case (won, outcome) of
        (Left thrown, _) -> failure thrown
        (Right _, Left thrown) | isNothing (cancelledBy thrown) -> failure thrown
        (Right result, _) -> success result

-- | What goes on with a part that waits, from its step.
resumeOf :: Step -> IO () -> IO Step
resumeOf (Suspended _ _ resume) = resume
resumeOf _ = const (pure Ended)

-- | The step of the wait, and the others.
takeEntry :: Int -> [Step] -> (Maybe (IO () -> IO Step), [Step])
takeEntry ident = go []
  where
    go before = \case
      [] -> (Nothing, reverse before)
      entry@(Suspended _ other resume) : after
        | other == ident -> (Just resume, reverse before <> after)
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
schedule run enterPart result start = (go [start] [] `catch` failed) `finally` closeInbox run
  where
    -- A shutdown fails the run with its exception, whatever the parts failed with, which is their cancellation.
    failed :: SomeException -> IO a
    failed failure = readIORef (runShutdown run) >>= throwIO . fromMaybe (unescaped failure)
    unescaped failure = maybe failure (\(Escaped inner) -> inner) (fromException failure)
    -- The parts that can go on in this round, and in the next.
    go ready later = case ready of
      part : others -> do
        clock >>= setCounter run 0
        step <- enterPart part
        readIORef result >>= \case
          Just value -> (\steps waits -> (value, Report steps waits)) <$> counter run 1 <*> counter run 2
          Nothing -> case step of
            Ended -> go others later
            Timed -> go others later
            Parked -> adjust run 5 1 *> go others later
            ParkedRemote -> adjust run 3 1 *> go others later
            Yielded part' -> go others (part' : later)
            Forked parts -> go others (foldl (flip (:)) later parts)
            Suspended (Readable fd) _ _ -> waitOn fd True step *> adjust run 4 1 *> go others later
            Suspended (Writable fd) _ _ -> waitOn fd False step *> adjust run 4 1 *> go others later
            Suspended (Until (Instant at)) _ resume -> addTimer (runWheel run) at (modifyIORef' (runTickets run) (resume (pure ()) :)) *> go others later
            Suspended (Watched register) ident resume -> register >>= \watch -> modifyIORef' (runWatching run) (Watching ident watch resume :) *> go others later
      [] -> do
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
        given <- takeInbox run >>= arrivals run
        descriptors <- counter run 4
        armed <- counter run 10
        -- A poll in each round that has an arming, so that the armings of parts that were cancelled by then are not left.
        polled <- if descriptors + armed > 0 then pollNow else pure []
        case polled <> given <> reverse tickets <> reverse later of
          [] -> idle
          next -> go next []
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
                  then "the parts wait for tickets that nothing will give a value"
                  else "the program ended without a result"
          | null watching -> do
              -- Unless a part was given since the round began: the next to give
              -- one wakes the poller.
              early <- beforeSleep run >>= arrivals run
              if null early
                then do
                  countWait
                  until' <- untilNextTimer
                  polled <- pollWait (maybe (-1) (fromIntegral . min 2000000000 . (`div` 1000000) . (+ 999999)) until')
                  given <- afterSleep run >>= arrivals run
                  go (polled <> given) []
                else go early []
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
              delay <- maybe (pure Nothing) (fmap Just . registerDelay) micros
              interruptedBy . atomically $ maybe retry (readTVar >=> check) delay `orElse` foldr (orElse . (\(Watching _ watch _) -> watchReady watch)) retry watching
              watched <- readIORef (runWatching run)
              checked <- traverse (\entry@(Watching _ watch _) -> (,) entry <$> hasFired watch) watched
              let (fired, still) = partition snd checked
              mapM_ (\(Watching _ watch _, _) -> watchCancel watch) fired
              writeIORef (runWatching run) (map fst still)
              armed <- counter run 10
              polled <- if descriptors + armed > 0 then pollNow else pure []
              go (polled <> map (\(Watching _ _ resume, _) -> resume (pure ())) fired) []
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
      bump @m PollsBlocking *> try (interruptible (rawPoll c_pollerWait timeout)) >>= \case
        Left failure -> [] <$ shuttingDown failure
        Right count -> afterPoll count
    rawPoll call timeout =
      unsafeWithForeignPtr (runReady run) $ \fds ->
        unsafeWithForeignPtr (runKinds run) $ \kinds ->
          call (runPoller run) fds kinds (fromIntegral batch) timeout
    -- The parts that wait on what became ready.
    afterPoll count
      | count < 0 = getErrno >>= \errno -> if errno == eINTR then pure [] else throwErrno "streaming_io_poller_wait"
      | otherwise = do
          setCounter run 10 0
          readied <-
            unsafeWithForeignPtr (runReady run) $ \fds ->
              unsafeWithForeignPtr (runKinds run) $ \kinds ->
                traverse (\index -> (,) <$> peekElemOff fds index <*> peekElemOff kinds index) [0 .. fromIntegral count - 1]
          waiters <- readIORef (runWaiters run)
          (woken, waiters') <- foldM wake ([], waiters) readied
          writeIORef (runWaiters run) waiters'
          adjust run 4 (negate (length woken))
          pure (map (\entry -> resumeOf entry (pure ())) woken)
    -- Takes the parts that wait for what was reported off the descriptor, and arms it again for the others: a report
    -- may have used up the arming of a kind that nothing waits for any more, or, on epoll, the arming of both.
    wake (woken, waiters) (fd, kind) = do
      let key = fromIntegral fd
          Waiters readers writers = IntMap.findWithDefault (Waiters [] []) key waiters
          (fromReaders, readers') = if kind .&. kindReadable /= 0 then (readers, []) else ([], readers)
          (fromWriters, writers') = if kind .&. kindWritable /= 0 then (writers, []) else ([], writers)
          waiting = Waiters readers' writers'
      when (kindsOf waiting /= 0) (arm fd 0 waiting)
      pure (woken <> reverse fromReaders <> reverse fromWriters, settle key waiting waiters)

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
