{-# LANGUAGE ExistentialQuantification #-}
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
--
-- The scheduler also keeps what cancels a part: a part that is in a cancel scope
-- says where it waits, so that cancelling the scope takes it off what it waits
-- at and puts it back among those that can go on, with the cancellation to
-- raise where it waits. A part in no scope says nothing, and waits as it always
-- did. The scheduler runs masked, and an asynchronous exception reaches it where
-- it waits for a descriptor, and once in a while otherwise: it cancels every
-- part that is in a scope, waits until they have ended, and then fails the run
-- with the exception.
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
    Part (..),
    Parked (..),
    newPart,
    childPart,
    endPart,
    pushFrame,
    popFrame,
    checkpoint,
    cancelScope,
    registering,
    nextWait,
    adjust,
    Joined (..),
    pairEnded,
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
import Control.Monad (forM_, when, (>=>))
import Data.Bits ((.&.))
import Data.Functor (($>))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.List (partition)
import Data.Maybe (fromMaybe, isNothing)
import Data.Word (Word64)
import Foreign.C.Error (eINTR, getErrno, throwErrno, throwErrnoIfMinus1_, throwErrnoIfNull)
import Foreign.C.Types (CInt (..))
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peekElemOff, pokeElemOff)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Directions.Atomic (update)
import Network.Directions.Cancel
import Network.Directions.Classes (Cancelled (..), Event (..), Instant (..), cancelledBy)
import Network.Directions.Loop (Report (..))
import Network.Directions.Timers
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
  | -- | Two parts that go on at once.
    Forked (IO Step) (IO Step)
  | -- | It waits for a ticket, whose slot holds it.
    Parked
  | -- | It waits for a ticket that another core gives, whose slot holds it; the
    -- giver puts it in the mailbox.
    ParkedRemote
  | -- | It used its steps, and can go on.
    Yielded (IO Step)
  | -- | It waits until an instant, and a timer holds it.
    Timed

-- | A ticket's slot: empty, given its value, or holding the part that waits
-- for it, as what puts the part back with the action that runs where it waits:
-- the value, or a cancellation.
data Slot a = Empty | Given a | Waiter (IO a -> IO Step)

-- | A part: the scopes it is in and the shields between them, innermost first, and where it waits. A part
-- that is in a scope is in the run's root scope too, outermost, which a shutdown cancels; a part that is in
-- none has no frames, and says nothing of where it waits.
data Part = Part
  { partIdent :: !Int,
    partFrames :: !(IORef [Frame Part]),
    partWait :: !(IORef Parked)
  }

instance Identified Part where
  identOf = partIdent

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

-- | What the parts of a run share.
data Run = Run
  { -- | Parts that tickets woke.
    runTickets :: !(IORef [IO Step]),
    -- | The steps the running part has left before it gives way, the steps
    -- taken, the waits, the parts that wait for tickets that other cores
    -- give and are not yet back from the mailbox, the parts that wait on
    -- descriptors, those that wait for tickets, the number of the next wait,
    -- the rounds, and the time that the round began at, or 0 before it is read:
    -- unboxed, so that counting allocates nothing. A step is a point
    -- to give way at, which a link's receive or send is, or a wait, or a
    -- ticket given or waited for.
    runCounters :: !(ForeignPtr Int),
    runPoller :: !(Ptr Poller),
    -- | Where other threads put parts for this scheduler.
    runMailbox :: !Mailbox,
    -- | Where the poller writes what became ready: descriptors, and kinds.
    runReady :: !(ForeignPtr CInt),
    runKinds :: !(ForeignPtr CInt),
    -- | The parts that wait on each registered descriptor: to read, to write.
    runWaiters :: !(IORef (IntMap.IntMap Waiters)),
    -- | The parts that wait on watches.
    runWatching :: !(IORef [Watching]),
    -- | The scope that every part in a scope is in, outermost.
    runRoot :: !(CancelScope Part),
    -- | The asynchronous exception that the run is shutting down for.
    runShutdown :: !(IORef (Maybe SomeException)),
    -- | What goes on with a failure of the part that runs, in a run of "Network.Directions.Compiled", whose
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
-- once nothing that is given parts through its mailbox can run any more.
newRun :: IO Run
newRun = do
  counters <- mallocForeignPtrArray 9
  unsafeWithForeignPtr counters $ \pointer -> mapM_ (\index -> pokeElemOff pointer index 0) [0 .. 8]
  poller <- throwErrnoIfNull "directions_poller_new" c_pollerNew
  inbox <- newIORef (Open [] False)
  Run <$> newIORef [] <*> pure counters <*> pure poller <*> pure (Mailbox inbox poller) <*> mallocForeignPtrArray batch <*> mallocForeignPtrArray batch <*> newIORef IntMap.empty <*> newIORef [] <*> newCancelScope <*> newIORef Nothing <*> newIORef (throwIO . Escaped) <*> (getMonotonicTimeNSec >>= newWheel)

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
-- those that wait for tickets.
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
      else getMonotonicTimeNSec >>= \now -> now <$ setCounter run 8 (fromIntegral now)
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

-- | The number of a part or of a wait of a part that is in a scope, which is not 0.
nextWait :: Run -> IO Int
nextWait run = counter run 6 >>= \n -> n + 1 <$ setCounter run 6 (n + 1)
{-# INLINE nextWait #-}

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
wakeTicket run part = modifyIORef' (runTickets run) (part :) *> adjust run 5 (-1)

-- | A part in no scope, to run as the first part of a run, or one that only holds a resource.
newPart :: Run -> IO Part
newPart run = Part <$> nextWait run <*> newIORef [] <*> newIORef NotWaiting

-- | A part for a half of a pair, in the scope of the pair as well as in those of the part that made it.
childPart :: Run -> Part -> CancelScope Part -> IO Part
childPart run parent scope' = do
  outer <- readIORef (partFrames parent)
  let frames = Inside scope' : if null outer then [Inside (runRoot run)] else outer
  child <- Part <$> nextWait run <*> newIORef frames <*> newIORef NotWaiting
  forM_ frames $ \case Inside s -> enter s child; _ -> pure ()
  pure child

-- | A part that has ended is in no scope any more.
endPart :: Part -> IO ()
endPart part = do
  frames <- readIORef (partFrames part)
  forM_ frames $ \case Inside s -> leave s part; _ -> pure ()
  writeIORef (partFrames part) []

-- | The part goes in a scope, or behind a shield; a part that was in none goes in the run's root scope too.
pushFrame :: Run -> Part -> Frame Part -> IO ()
pushFrame run part frame = do
  frames <- readIORef (partFrames part)
  case frames of
    [] -> enter (runRoot run) part *> writeIORef (partFrames part) [frame, Inside (runRoot run)]
    _ -> writeIORef (partFrames part) (frame : frames)
  case frame of Inside s -> enter s part; _ -> pure ()

-- | The part leaves the innermost scope or shield, which it went in with 'pushFrame'.
popFrame :: Run -> Part -> IO ()
popFrame run part = do
  frames <- readIORef (partFrames part)
  case frames of
    [frame, Inside root] | scopeIdent root == scopeIdent (runRoot run) -> do
      leaveFrame frame
      leave root part
      writeIORef (partFrames part) []
    frame : outer -> leaveFrame frame *> writeIORef (partFrames part) outer
    [] -> pure ()
  where
    leaveFrame = \case Inside s -> leave s part; _ -> pure ()

-- | Raises the cancellation that reaches the part, if there is one.
checkpoint :: Part -> IO ()
checkpoint part =
  readIORef (partFrames part) >>= \case
    [] -> pure ()
    frames -> cancelledIn frames >>= maybe (pure ()) (throwIO . Cancelled)

-- | Cancels the scope: its flag is set, and the parts inside it that wait, and whose scope lies above their
-- innermost shield, are taken off what they wait at, and put back among those that can go on, with the
-- cancellation to raise where they wait. The others see the flag at their next wait.
cancelScope :: Run -> CancelScope Part -> IO ()
cancelScope run scope' = do
  parts <- markCancelled scope'
  forM_ parts $ \part -> do
    frames <- readIORef (partFrames part)
    when (deliverable (scopeIdent scope') frames) $
      (readIORef (partWait part) >>= stopWait run (Cancelled (scopeIdent scope'))) >>= \case
        Nothing -> pure ()
        Just resume -> modifyIORef' (runTickets run) (resume :)

-- | Takes the part off what it waits at, if it still does: what goes on with the part.
stopWait :: Run -> Cancelled -> Parked -> IO (Maybe (IO Step))
stopWait run cancelled = \case
  NotWaiting -> pure Nothing
  OnTicket slot ->
    readIORef slot >>= \case
      Waiter resume -> Just (resume (throwIO cancelled)) <$ (writeIORef slot Empty *> adjust run 5 (-1))
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
                writeIORef (runWaiters run) (IntMap.insert fd (if reading then Waiters rest writers else Waiters readers rest) waiters)
                adjust run 4 (-1)
                pure (Just (resume (throwIO cancelled)))
  OnWatch ident -> do
    watching <- readIORef (runWatching run)
    case partition (\(Watching other _ _) -> other == ident) watching of
      ([Watching _ watch resume], rest) -> Just (resume (throwIO cancelled)) <$ (writeIORef (runWatching run) rest *> watchCancel watch)
      _ -> pure Nothing
  Other stop -> stop cancelled

-- | A failure that nothing in the part handles: it leaves the run.
newtype Escaped = Escaped SomeException
  deriving (Show)

instance Exception Escaped

-- | Runs a part of "Network.Directions.Compiled": a failure that leaves it goes to the handler that the part
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
    frames -> cancelledIn frames >>= maybe (writeIORef (partWait part) parked) (throwIO . Cancelled)
{-# INLINE registering #-}

-- | What a pair of halves has of its halves' results, and its first failure, and how many have ended.
data Joined b c = Joined !(Maybe b) !(Maybe c) !(Maybe SomeException) !Int

-- | What a pair does when one of its halves has ended: it leaves its scopes, and keeps its result, or its failure
-- if it is the first, which cancels the other half; the half that ends last goes on with the parent: with the
-- first failure, if there was one.
pairEnded :: Run -> CancelScope Part -> IORef (Joined b c) -> (SomeException -> IO Step) -> ((b, c) -> IO Step) -> Part -> (x -> Joined b c -> Joined b c) -> Either SomeException x -> IO Step
pairEnded run scope' joined failure success half record outcome = do
  endPart half
  Joined b c failed count <- readIORef joined
  joined' <- case outcome of
    Left thrown
      | isNothing failed -> cancelScope run scope' $> Joined b c (Just thrown) (count + 1)
      | otherwise -> pure (Joined b c failed (count + 1))
    Right result -> pure (record result (Joined b c failed (count + 1)))
  writeIORef joined joined'
  case joined' of
    Joined _ _ (Just thrown) 2 -> failure thrown
    Joined (Just b') (Just c') _ 2 -> success (b', c')
    Joined{} -> pure Ended

-- | What a race does when one of its halves has ended: the first to end is the winner, which cancels the other;
-- the loser, once it has ended, lets the race go on with the parent, with the winner's failure, or the loser's
-- if it isn't its cancellation, or else the winner's result.
raceEnded :: Run -> CancelScope Part -> IORef (Maybe (Either SomeException (Either b c))) -> (SomeException -> IO Step) -> (Either b c -> IO Step) -> Part -> Either SomeException (Either b c) -> IO Step
raceEnded run scope' winner failure success half outcome = do
  endPart half
  readIORef winner >>= \case
    Nothing -> do
      writeIORef winner (Just outcome)
      cancelScope run scope'
      pure Ended
    Just won -> case (won, outcome) of
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
-- each part that can go on in turn, entered with the given action, for up to
-- 64 steps, which are mostly the receives and sends it makes. At the end it
-- closes the run's mailbox, so that those that give parts to it after that are
-- told they didn't; the run's poller stays until 'freeRun'.
--
-- An asynchronous exception reaches the scheduler where it waits for a
-- descriptor or a watch, and at the start of each round: the parts that are in
-- a scope are cancelled, and the run goes on until they have ended, and then
-- fails with the exception. The exception of a part that no scope holds fails
-- the run at once.
schedule :: Run -> (IO Step -> IO Step) -> IORef (Maybe b) -> IO Step -> IO (b, Report)
schedule run enterPart result start = (go [start] [] `catch` failed) `finally` closeInbox run
  where
    -- A shutdown fails the run with its exception, whatever the parts failed with, which is their cancellation.
    failed :: SomeException -> IO a
    failed failure = readIORef (runShutdown run) >>= throwIO . fromMaybe (unescaped failure)
    unescaped failure = maybe failure (\(Escaped inner) -> inner) (fromException failure)
    -- The parts that can go on in this round, and in the next.
    go ready later = case ready of
      part : others -> do
        setCounter run 0 64
        step <- enterPart part
        readIORef result >>= \case
          Just value -> (\steps waits -> (value, Report steps waits)) <$> counter run 1 <*> counter run 2
          Nothing -> case step of
            Ended -> go others later
            Timed -> go others later
            Parked -> adjust run 5 1 *> go others later
            ParkedRemote -> adjust run 3 1 *> go others later
            Yielded part' -> go others (part' : later)
            Forked one two -> go others (two : one : later)
            Suspended (Readable fd) _ _ -> waitOn fd True step *> adjust run 4 1 *> go others later
            Suspended (Writable fd) _ _ -> waitOn fd False step *> adjust run 4 1 *> go others later
            Suspended (Until (Instant at)) _ resume -> addTimer (runWheel run) at (modifyIORef' (runTickets run) (resume (pure ()) :)) *> go others later
            Suspended (Watched register) ident resume -> register >>= \watch -> modifyIORef' (runWatching run) (Watching ident watch resume :) *> go others later
      [] -> do
        -- Each round, the parts that tickets woke, that other cores gave, and
        -- that descriptors became ready for join.
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
        polled <- if descriptors > 0 then pollNow else pure []
        case reverse later <> reverse tickets <> given <> polled of
          [] -> idle
          next -> go next []
    idle = do
      remote <- counter run 3
      descriptors <- counter run 4
      parked <- counter run 5
      watching <- readIORef (runWatching run)
      shutdown <- readIORef (runShutdown run)
      members <- readIORef (scopeMembers (runRoot run))
      timers <- pending (runWheel run)
      case () of
        _
          | Just failure <- shutdown, null members -> throwIO failure
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
                  go (given <> polled) []
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
              polled <- if descriptors > 0 then pollNow else pure []
              go (map (\(Watching _ _ resume, _) -> resume (pure ())) fired <> polled) []
    countWait = counter run 2 >>= setCounter run 2 . (+ 1)
    -- How long until the next timer is due, in nanoseconds.
    untilNextTimer =
      nextDeadline (runWheel run) >>= \case
        Nothing -> pure Nothing
        Just deadline -> (\now -> Just (if deadline > now then deadline - now else 0)) <$> getMonotonicTimeNSec
    -- An asynchronous exception that arrives at a wait begins a shutdown: every part that is in a scope is
    -- cancelled. One that arrives again is the same.
    shuttingDown failure =
      readIORef (runShutdown run) >>= \case
        Nothing -> writeIORef (runShutdown run) (Just failure) *> cancelScope run (runRoot run)
        Just _ -> pure ()
    interruptedBy wait = try wait >>= either shuttingDown pure
    -- Registers the descriptor with the poller the first time a part waits on
    -- it, and adds the part to its waiters.
    waitOn (Fd fd) reading entry = do
      waiters <- readIORef (runWaiters run)
      let key = fromIntegral fd
          Waiters readers writers = IntMap.findWithDefault (Waiters [] []) key waiters
      if IntMap.member key waiters then pure () else throwErrnoIfMinus1_ "directions_poller_add" (c_pollerAdd (runPoller run) fd)
      writeIORef (runWaiters run) $
        IntMap.insert key (if reading then Waiters (entry : readers) writers else Waiters readers (entry : writers)) waiters
    -- Asks the poller what became ready, without waiting.
    pollNow = rawPoll c_pollerPoll 0 >>= afterPoll
    -- Waits in the poller, which an asynchronous exception can interrupt: it begins a shutdown. No part runs
    -- while the scheduler waits, so it waits unmasked: a masked thread in an interruptible foreign call returns
    -- from it with the exception still to come.
    pollWait timeout =
      try (interruptible (rawPoll c_pollerWait timeout)) >>= \case
        Left failure -> [] <$ shuttingDown failure
        Right count -> afterPoll count
    rawPoll call timeout =
      unsafeWithForeignPtr (runReady run) $ \fds ->
        unsafeWithForeignPtr (runKinds run) $ \kinds ->
          call (runPoller run) fds kinds (fromIntegral batch) timeout
    -- The parts that wait on what became ready.
    afterPoll count
      | count < 0 = getErrno >>= \errno -> if errno == eINTR then pure [] else throwErrno "directions_poller_wait"
      | otherwise = do
          readied <-
            unsafeWithForeignPtr (runReady run) $ \fds ->
              unsafeWithForeignPtr (runKinds run) $ \kinds ->
                traverse (\index -> (,) <$> peekElemOff fds index <*> peekElemOff kinds index) [0 .. fromIntegral count - 1]
          waiters <- readIORef (runWaiters run)
          let (woken, waiters') = foldl wake ([], waiters) readied
          writeIORef (runWaiters run) waiters'
          adjust run 4 (negate (length woken))
          pure (map (\entry -> resumeOf entry (pure ())) woken)
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
