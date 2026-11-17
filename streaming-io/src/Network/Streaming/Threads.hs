{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnboxedTuples #-}

-- | A thread for each concurrent part: an arrow is a function to IO, and a
-- link blocks its thread.
--
-- Every part runs masked, so that a cancellation, which is an exception thrown
-- to the part's thread, reaches it only where it waits. A part that is in a
-- cancel scope keeps its state, which the thread that cancels reads: the
-- scopes it is in and the shields between them, and whether it waits. A
-- cancellation throws once, and only to a part that waits and whose scope lies
-- above its innermost shield; a part that doesn't wait looks at the scopes'
-- flags when it does, so that a cancellation is neither lost nor early.
module Network.Streaming.Threads
  ( Threads,
    ThreadsM,
    runThreads,
    runThreadsM,
  )
where

import Control.Applicative ((<|>))
import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Concurrent (ThreadId, forkIO, killThread, myThreadId, rtsSupportsBoundThreads, threadDelay, threadWaitRead, threadWaitWrite, throwTo, yield)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, takeMVar, tryPutMVar)
import Control.Concurrent.STM (TVar, atomically, check, modifyTVar', newTVarIO, readTVar)
import Control.Exception (SomeException, allowInterrupt, finally, mask_, onException, throwIO, toException, try, uninterruptibleMask_)
import Control.Monad (foldM, forM_, unless, void, when)
import Data.Bits ((.&.))
import Data.Foldable (traverse_)
import Data.Functor (($>))
import Data.IORef (atomicWriteIORef, newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.Maybe (isNothing)
import Data.Word (Word64)
import Foreign.Marshal.Alloc (malloc)
import Foreign.Ptr (Ptr)
import Foreign.Storable (peek, poke)
import GHC.Event (getSystemTimerManager, registerTimeout, unregisterTimeout)
import GHC.Exts (casMutVar#)
import GHC.IO (IO (..))
import GHC.IORef (IORef (..), atomicSwapIORef)
import GHC.STRef (STRef (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done)
import Network.Streaming.Concurrent (Concurrent (Group, group, spawn), Joiner)
import qualified Network.Streaming.Concurrent as Joiner
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Mode (Instrumented (..), Mode (..), ModeOf)
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.Cancel (CancelScope (..), Frame (..), Hold (..), arrivalOf, armOnce, cancelledAnywhere, cancelledFlagged, gracedFrame, heldIn, heldUntil, holdOf, isCancelled, newCancelScope, reachedBy, setCancelledAtomic)
import Network.Streaming.Internal.Inboxes (Address (..), Looked (..), Shelf, closeShelf, collectedWrongly, hasValue, lookOrWait, newShelf, postNow, postOrQueue, stopWaiting, takeWoken, withdraw)
import Network.Streaming.Internal.Tickets
import Network.Streaming.Watch (awaitWatch)
import System.IO.Unsafe (unsafePerformIO)
import Prelude hiding (id, (.))

-- | An arrow whose concurrent parts run on threads of their own: a function of the part that runs it. Built in the mode @m@.
newtype ThreadsM (m :: Mode) a b = Threads (Part -> a -> IO b)

-- | The arrow in 'Release'.
type Threads = ThreadsM 'Release

-- | A part: its number, its thread, what it is in, innermost first, whether it waits, and whether a cancellation may
-- have reached it, which its points look at until it is. Only the thread of the part changes its frames; the thread
-- that cancels it changes its phase, from waiting to thrown, and the part changes it back; every thread that
-- cancels a scope the part is in sets its flag, and the part clears it.
data Part = Part !Int !ThreadId !(IORef [Frame Members]) !(IORef Phase) !(IORef Bool)

-- | The parts inside a scope, by their numbers, which threads change by compare-and-swap: a scope is entered and
-- left from every thread that has a part in it.
type Members = IntMap.IntMap Part

counter :: IORef Int
counter = unsafePerformIO (newIORef 0)
{-# NOINLINE counter #-}

-- | How many points to give way at the threads have passed, on the non-threaded run time, whose one OS thread runs them one at a
-- time: not exact. Unboxed, so that counting allocates nothing.
gaveWay :: Ptr Int
gaveWay = unsafePerformIO (malloc >>= \cell -> cell <$ poke cell 0)
{-# NOINLINE gaveWay #-}

-- | A number that no other part or scope has, from any thread.
nextIdent :: IO Int
nextIdent = update counter (\n -> (n + 1, n))

newScope :: IO (CancelScope Members)
newScope = nextIdent >>= \ident -> newCancelScope ident IntMap.empty

-- | The part is inside the scope, or is not any more.
enter, leave :: CancelScope Members -> Part -> IO ()
enter s part@(Part ident _ _ _ _) = update (scopeMembers s) (\parts -> (IntMap.insert ident part parts, ()))
leave s (Part ident _ _ _ _) = update (scopeMembers s) (\parts -> (IntMap.delete ident parts, ()))

data Phase
  = Running
  | -- | At a wait, which a cancellation can interrupt, in the scopes and behind the shields that it was in when it
    -- began to wait. The object is what a cancellation changes: a part that waits again has another.
    Waiting ![Frame Members]
  | -- | A cancellation was thrown to the part, and reaches it at its wait or just after it.
    Thrown

-- | Changes the phase, if it is still the one that was read.
casPhase :: IORef Phase -> Phase -> Phase -> IO Bool
casPhase (IORef (STRef var)) old new = IO $ \s -> case casMutVar# var old new s of
  (# s', 0#, _ #) -> (# s', True #)
  (# s', _, _ #) -> (# s', False #)

-- | Runs the program, masked: the thread that calls it is its first part.
runThreads :: Threads a b -> a -> IO b
runThreads = runThreadsM
{-# INLINE runThreads #-}

-- | 'runThreads' for an arrow of any mode.
runThreadsM :: ThreadsM m a b -> a -> IO b
runThreadsM (Threads run) input = mask_ $ do
  thread <- myThreadId
  ident <- nextIdent
  part <- Part ident thread <$> newIORef [] <*> newIORef Running <*> newIORef False
  run part input

instance Category (ThreadsM m) where
  id = Threads (const pure)
  Threads after . Threads before = Threads (\part value -> before part value >>= after part)

instance Arrow (ThreadsM m) where
  arr function = Threads (\_ value -> pure (function value))
  first (Threads inner) = Threads (\part (a, c) -> (,c) <$> inner part a)

instance Functor (ThreadsM m a) where
  fmap function (Threads inner) = Threads (\part value -> function <$> inner part value)

instance Applicative (ThreadsM m a) where
  pure result = Threads (\_ _ -> pure result)
  Threads function <*> Threads argument = Threads (\part value -> function part value <*> argument part value)

instance ArrowApplicative (ThreadsM m)

instance ArrowChoice (ThreadsM m) where
  left (Threads inner) = Threads $ \part -> \case
    Left a -> Left <$> inner part a
    Right c -> pure (Right c)
  Threads one ||| Threads two = Threads (\part -> either (one part) (two part))

instance Effect (ThreadsM m) where
  now action = Threads (const action)

-- | A point where the part can be cancelled: a part in a scope raises a cancellation that is there, and lets the
-- exceptions that wait for it in.
instance GiveWay (ThreadsM m) where
  giveWay = Threads (\part value -> value <$ givingWay part)
  {-# INLINE giveWay #-}

-- | Nothing: GHC's scheduler has no lanes.
instance FastLane (ThreadsM m) where
  fastLane = id
  {-# INLINE fastLane #-}

-- | The point itself, which what the part has in hand doesn't go through: the call is not given it, so that a caller
-- doesn't have to build it, as a link's input is a pair of the link and where it receives, for the call to give it back.
givingWay :: Part -> IO ()
givingWay (Part _ _ frames _ flag) = do
  -- The non-threaded run time has one capability, and a thread that doesn't allocate is not preempted: a part that polls
  -- for what another thread does has to let it run. One point in 64 yields, since a yield costs what the stack is deep.
  unless rtsSupportsBoundThreads $ do
    passed <- peek gaveWay
    poke gaveWay (passed + 1)
    when (passed .&. 63 == 0) yield
  readIORef flag >>= \flagged ->
    if flagged
      then readIORef frames >>= reachedBy flag >>= maybe allowInterrupt (throwIO . Cancelled)
      else allowInterrupt
{-# NOINLINE givingWay #-}

-- | A wait of a part that is in scopes: it says it waits, looks for a cancellation that came before, and waits;
-- a cancellation thrown to it ends the wait. Whatever ended it, the part says it doesn't wait.
armed :: Part -> [Frame Members] -> IO a -> IO a
armed (Part _ _ _ phase flag) frames wait = do
  -- Evaluated, so that what the phase holds is the object that a cancellation compares with.
  let !waits = Waiting frames
  _ <- atomicSwapIORef phase waits
  cancelledFlagged flag frames >>= \case
    Just ident ->
      disarm >>= \case
        Thrown -> reached
        _ -> throwIO (Cancelled ident)
    Nothing -> do
      result <- wait `onException` disarm
      disarm >>= \case
        Thrown -> reached
        _ -> pure result
  where
    disarm = atomicSwapIORef phase Running
    -- The thread that cancelled the part throws once it has said so: the part lets the exception in.
    reached = allowInterrupt *> yield *> reached

-- | A wait of the part.
waiting :: Part -> IO a -> IO a
waiting part@(Part _ _ frames _ _) wait =
  readIORef frames >>= \case
    [] -> wait
    scopes -> armed part scopes wait
{-# INLINE waiting #-}

-- | Cancels the scope: every part inside it has its flag set, whether or not it waits and a shield holds it, and
-- those that wait, and whose scope lies above their innermost shield, with no grace between that has a period still to pass, are
-- thrown to, once; the others see it at their next wait. For each period of a grace that holds it from a part, a timer walks the
-- scope again when the period has passed since the scope was cancelled. The flag is set, with a barrier, before the phase is read: a
-- part that begins to wait meanwhile swaps its phase, and then looks at its flag.
cancelScope :: CancelScope Members -> IO ()
cancelScope scope' = mask_ (setCancelledAtomic scope' *> reaching scope' True)

-- | Tells the parts inside the scope of its cancellation, as 'cancelScope' does, or, if it is not arriving, as the timer of a period
-- does, which finds that a part is held no longer when the period has passed since the scope was cancelled.
reaching :: CancelScope Members -> Bool -> IO ()
reaching scope' arriving = do
  parts <- IntMap.elems <$> readIORef (scopeMembers scope')
  periods <- foldM visit [] parts
  when arriving (forM_ periods (laterIn scope'))
  where
    ident = scopeIdent scope'
    -- The periods that graces hold the cancellation from some part for, each once.
    visit periods (Part _ thread frames phase flag) = do
      atomicWriteIORef flag True
      -- What the part's frames say now, whether it waits or runs: a part that enters a grace meanwhile does the same itself.
      Hold _ held <- holdOf ident <$> readIORef frames
      -- A part that began to wait again meanwhile has another phase to look at.
      let attempt' = do
            current <- readIORef phase
            case current of
              Waiting asleep -> do
                reached <- case holdOf ident asleep of
                  Hold True _ -> pure False
                  Hold False 0 -> pure True
                  Hold False period -> if arriving then pure False else passed period
                when reached $ do
                  thrown <- casPhase phase current Thrown
                  if thrown then uninterruptibleMask_ (throwTo thread (Cancelled ident)) else attempt'
              _ -> pure ()
      attempt'
      pure (if arriving && held > 0 && held `notElem` periods then held : periods else periods)
    passed period = do
      instant <- monotonicNanoseconds
      arrival <- arrivalOf scope' instant
      pure (instant >= heldUntil arrival period)

-- | A timer that walks the scope again when the period has passed since it was cancelled, unless one is armed for the period already,
-- or the period ends at the end of time: on a thread of GHC's timer manager, or a thread of its own where the run time has none.
laterIn :: CancelScope Members -> Word64 -> IO ()
laterIn scope' period = do
  instant <- monotonicNanoseconds
  armOnce scope' instant period >>= \case
    Nothing -> pure ()
    Just end -> do
      let wait = fromIntegral (min 1000000000000000 ((end - min end instant + 999) `div` 1000))
          again = mask_ (reaching scope' False)
      if rtsSupportsBoundThreads
        then getSystemTimerManager >>= \manager -> void (registerTimeout manager wait again)
        else void (mask_ (forkIO (threadDelay wait *> again)))

-- | The part has entered a grace of this period: if a cancellation is pending, the timers that let in the cancelled scopes that the
-- period of this grace holds from it.
armHeld :: Word64 -> Part -> IO ()
armHeld period (Part _ _ frames _ _) = do
  held <- readIORef frames >>= heldIn
  forM_ [s | (s, longest) <- held, longest == period] (`laterIn` period)

-- | Pushes a frame on the part's stack, runs the action, and pops it.
framed :: Frame Members -> Part -> IO a -> IO a
framed frame (Part _ _ frames _ _) action = do
  readIORef frames >>= writeIORef frames . (frame :)
  outcome <- try action
  readIORef frames >>= writeIORef frames . drop 1
  either (\failure -> throwIO (failure :: SomeException)) pure outcome

-- | Runs the action behind a shield.
shielded :: Part -> IO a -> IO a
shielded = framed Shielded

-- | Runs the action behind a grace of this period, whose frame is given. The frame is put on the stack with a barrier, and then the
-- part looks at its flag, as a thread that cancels a scope sets the flag and then looks at the stack: one of them sees the other, and
-- a cancellation that is pending when the part enters has its timer armed.
graced :: Word64 -> Frame Members -> Part -> IO a -> IO a
graced period frame part@(Part _ _ frames _ flag) action = do
  readIORef frames >>= atomicWriteIORef frames . (frame :)
  readIORef flag >>= \flagged -> when flagged (armHeld period part)
  outcome <- try action
  readIORef frames >>= writeIORef frames . drop 1
  either (\failure -> throwIO (failure :: SomeException)) pure outcome

-- | The part goes behind a shield, and leaves it.
pushed, popped :: Part -> IO ()
pushed (Part _ _ frames _ _) = readIORef frames >>= writeIORef frames . (Shielded :)
popped (Part _ _ frames _ _) = readIORef frames >>= writeIORef frames . drop 1

instance Scope (ThreadsM m) where
  scope (Threads acquire) (Threads release) (Threads body) = Threads $ \part value -> do
    acquired <- acquire part value
    outcome <- try (body part (acquired, value))
    let exit = exitOf outcome
    -- The release runs behind a shield.
    pushed part
    released <- try (release part (acquired, exit))
    popped part
    case released of
      Left failure -> throwIO (ReleaseFailed exit failure)
      Right () -> either (\failure -> throwIO (failure :: SomeException)) pure outcome

  tryA (Threads inner) = Threads $ \part value ->
    try (inner part value) >>= \case
      Left failure | Just _ <- cancelledBy failure -> throwIO failure
      other -> pure other

  shield (Threads inner) = Threads (\part value -> shielded part (inner part value))

  -- The frame is made when the arrow is built, so that entering the region allocates what entering a shield does.
  grace (Duration limit) (Threads inner) =
    let !frame = gracedFrame limit
     in Threads (\part value -> graced limit frame part (inner part value))

-- | A part that runs in the scope as well as in those of the part that made it, on a thread of its own, and what
-- it came to once it ended: the scopes are left before the pair's policy is told.
forkPart :: Part -> CancelScope Members -> (Part -> IO a) -> (Either SomeException a -> IO ()) -> IO (MVar (Either SomeException a))
forkPart (Part _ _ parent _ parentFlag) scope' run ended = do
  finished <- newEmptyMVar
  startPart parentFlag ((Inside scope' :) <$> readIORef parent) run (\outcome -> ended outcome *> putMVar finished outcome)
  pure finished

-- | A part that runs on a thread of its own, in the scopes that the action gives, which it enters and leaves, and tells how it
-- ended once it has left them.
startPart :: IORef Bool -> IO [Frame Members] -> (Part -> IO a) -> (Either SomeException a -> IO ()) -> IO ()
startPart parentFlag framesOf run ended = do
  -- Forked masked, as its part is.
  _ <- forkIO $ do
    thread <- myThreadId
    ident <- nextIdent
    frames <- framesOf
    state <- newIORef frames
    phase <- newIORef Running
    -- The part begins with the flag of its parent, which a cancellation that came before it existed has set.
    flag <- readIORef parentFlag >>= newIORef
    let part = Part ident thread state phase flag
    forM_ frames (inside part)
    -- A cancellation that was made while it entered its scopes, and didn't find it in them, has set their flags: the part
    -- sees them, as it entered them with a barrier after.
    cancelledAnywhere frames >>= \cancelled -> when cancelled (atomicWriteIORef flag True)
    outcome <- try (run part)
    forM_ frames (outside part)
    ended outcome
  pure ()
  where
    inside part = \case Inside s -> enter s part; _ -> pure ()
    outside part = \case Inside s -> leave s part; _ -> pure ()

-- | Waits for both, and if it is interrupted, cancels them and waits for them still: no part outlives the race.
awaitBoth :: CancelScope Members -> MVar a -> MVar b -> IO (a, b)
awaitBoth scope' one other =
  ((,) <$> readMVar one <*> readMVar other)
    `onException` (cancelScope scope' *> uninterruptibleMask_ (readMVar one *> readMVar other $> ()))

-- | Waits for all, and if it is interrupted, cancels them and waits for them still: no part outlives the group.
awaitAll :: CancelScope Members -> [MVar a] -> IO ()
awaitAll scope' dones =
  traverse_ readMVar dones
    `onException` (cancelScope scope' *> uninterruptibleMask_ (traverse_ readMVar dones))

-- | What a group does when one of its parts has ended: the first failure is kept, and cancels the scope.
failing :: IORef (Maybe SomeException) -> CancelScope Members -> Either SomeException x -> IO ()
failing failure scope' = \case
  Left failed -> do
    first' <- update failure (\known -> (known <|> Just failed, isNothing known))
    when first' (cancelScope scope')
  Right _ -> pure ()

-- | Annotations are for diagrams: the arrow is run as it is.
instance Annotate (ThreadsM m)

-- | Each part on a thread of its own, in a cancel scope that the first failure cancels: the others are cancelled at
-- their waits, and waited for, and the first failure is rethrown. When several fail, the later are dropped. A part is
-- entered into the scope of the group and the scopes that enclose it: a group of many parts doesn't make them deeper.
instance Concurrent (ThreadsM m) where
  newtype Group (ThreadsM m) s j = ThreadsGroup (Gang j)

  group joiner (Threads body) = Threads $ \part input -> do
    outer <- framesIn part
    scope' <- newScope
    gang <- Gang scope' (Inside scope' : outer) <$> (newIORef $! Joiner.begin joiner) <*> newIORef 0 <*> newTVarIO 0
    -- The body runs as the first member of the group, on the thread that runs the group.
    outcome <- try (body part (ThreadsGroup gang, input))
    -- A body that failed, or was cancelled, cancels the parts at once.
    either (\_ -> cancelScope scope') (\_ -> pure ()) outcome
    awaitParts gang
    grouped gang outcome
  {-# INLINE group #-}

  spawn (Threads body) = Threads (\part (ThreadsGroup gang, input) -> spawned gang part (`body` input))
  {-# INLINE spawn #-}

  together = \case
    [] -> done
    [one] -> one
    parts -> Threads $ \part input -> do
      scope' <- newScope
      failure <- newIORef Nothing
      -- Started and awaited in loops that keep no stack of their own: a group of thousands of parts starts under a small stack limit.
      dones <- reverse <$> foldM (\started (Threads body) -> (: started) <$> forkPart part scope' (`body` input) (failing failure scope')) [] parts
      awaitAll scope' dones
      readIORef failure >>= maybe (pure ()) throwIO

-- | The scopes that the part is in, innermost first.
framesIn :: Part -> IO [Frame Members]
framesIn (Part _ _ frames _ _) = readIORef frames

-- | A group that parts are spawned into: the cancel scope of its parts, the scopes that its parts begin in, the joiner's tally, how
-- many parts were spawned, and how many of them are running. Threads change them all atomically.
data Gang j = Gang
  { gangScope :: !(CancelScope Members),
    gangFrames :: [Frame Members],
    gangTally :: !(IORef (Joiner.Tally j)),
    gangSpawned :: !(IORef Int),
    gangRunning :: !(TVar Int)
  }

-- | Starts the arrow as a part of the group, on a thread of its own, in the scopes of the group and the scopes outside it, and
-- not in those that the spawning part is in. If the parts of the group were cancelled, it doesn't run, and counts as
-- cancelled.
spawned :: forall j. (Joiner j) => Gang j -> Part -> (Part -> IO (Joiner.Part j)) -> IO ()
spawned gang (Part _ _ _ _ flag) run = do
  number <- update (gangSpawned gang) (\n -> (n + 1, n))
  isCancelled (gangScope gang) >>= \cancelled ->
    if cancelled
      then partEnded gang number (Left (toException (Cancelled (scopeIdent (gangScope gang)))))
      else do
        atomically (modifyTVar' (gangRunning gang) (+ 1))
        startPart flag (pure (gangFrames gang)) run (\outcome -> partEnded gang number outcome `finally` atomically (modifyTVar' (gangRunning gang) (subtract 1)))
{-# INLINE spawned #-}

-- | The joiner is told that the part of this number ended in this way, and the parts are cancelled if it says so.
partEnded :: forall j. (Joiner j) => Gang j -> Int -> Either SomeException (Joiner.Part j) -> IO ()
partEnded gang number outcome = do
  cancelling <- update (gangTally gang) (Joiner.ended @j number outcome)
  when cancelling (cancelScope (gangScope gang))
{-# INLINE partEnded #-}

-- | Waits until no part of the group is running, and if it is interrupted, cancels them and waits for them still: no part
-- outlives the group.
awaitParts :: Gang j -> IO ()
awaitParts gang = none `onException` (cancelScope (gangScope gang) *> uninterruptibleMask_ none)
  where
    none = atomically (readTVar (gangRunning gang) >>= check . (== 0))

-- | What the group gives once all of it ended: the failure of the body, or the result of the body with what the joiner made of the
-- parts, or the failure that it gives.
grouped :: forall j b. (Joiner j) => Gang j -> Either SomeException b -> IO (b, Joiner.Joined j)
grouped gang = \case
  Left failure -> throwIO failure
  Right result -> readIORef (gangTally gang) >>= either throwIO (pure . (,) result) . Joiner.joined @j

-- | The first half to end is the result, and the other is cancelled and waited for. The loser's failure is
-- rethrown too, unless it is a cancellation.
instance Race (ThreadsM m) where
  race (Threads one) (Threads two) = Threads $ \part input -> do
    scope' <- newScope
    winner <- newIORef Nothing
    let ended outcome = do
          first' <- update winner (\known -> (known <|> Just outcome, isNothing known))
          when first' (cancelScope scope')
    first' <- forkPart part scope' (\child -> Left <$> one child input) ended
    second' <- forkPart part scope' (\child -> Right <$> two child input) ended
    (b, d) <- awaitBoth scope' first' second'
    readIORef winner >>= \case
      Just (Left failed) -> throwIO failed
      Just (Right result) -> do
        -- The half that did not win: a failure of it that isn't its cancellation is the part's.
        case if either (const True) (const False) result then d else b of
          Left failed | isNothing (cancelledBy failed) -> throwIO failed
          _ -> pure ()
        pure result
      Nothing -> ioError (userError "race: neither half ended")

-- | Blocks the thread: on a descriptor directly, as 'threadWaitRead' does, and
-- on any other event by its watch.
instance (Instrumented m) => Block (ThreadsM m) where
  block = Threads $ \part event -> waiting part $ case event of
    Readable fd -> waited @m *> threadWaitRead fd
    Writable fd -> waited @m *> threadWaitWrite fd
    Watched register -> awaitWatch register
    Until (Instant at) -> monotonicNanoseconds >>= \current -> when (at > current) (threadDelay (fromIntegral ((at - current + 999) `div` 1000)))

-- | A wait on a descriptor is counted as a wait and as an arming: GHC's IO manager registers the descriptor at each.
waited :: forall m. (Instrumented m) => IO ()
waited = bump @m DescriptorWaits *> bump @m Armings

instance Time (ThreadsM m) where
  clock = Threads (\_ () -> Instant <$> monotonicNanoseconds)

-- | The deadline is a timeout of GHC's timer manager, or a thread that sleeps where the run time has none, which cancels
-- the scope when it fires, and a cancel scope that the part is in while the body runs: the cancellation that ends it is
-- recognised by it, and another passes through.
instance Within (ThreadsM m) where
  within (Duration limit) (Threads body) = Threads (\part value -> deadlined limit part (body part value))
  {-# INLINE within #-}
  timeoutA (Threads body) = Threads (\part (Duration limit, value) -> deadlined limit part (body part value))
  {-# INLINE timeoutA #-}

-- | The action with a deadline of this many nanoseconds: nothing if the deadline passed first.
deadlined :: Word64 -> Part -> IO a -> IO (Maybe a)
deadlined limit part action = do
  scope' <- newScope
  let wait = fromIntegral (limit `div` 1000)
  outcome <-
    if rtsSupportsBoundThreads
      then do
        manager <- getSystemTimerManager
        key <- registerTimeout manager wait (cancelScope scope')
        result <- inScope part scope' action
        result <$ unregisterTimeout manager key
      else do
        -- The non-threaded run time has no timer manager: a thread of its own sleeps, and is stopped with the deadline.
        timer <- mask_ (forkIO (threadDelay wait *> cancelScope scope'))
        result <- inScope part scope' action
        result <$ uninterruptibleMask_ (killThread timer)
  case outcome of
    Right result -> pure (Just result)
    Left failure
      | cancelledBy failure == Just (scopeIdent scope') -> pure Nothing
      | otherwise -> throwIO failure
{-# INLINE deadlined #-}

-- | Runs the action as a part that is in the cancel scope, and how it ended.
inScope :: Part -> CancelScope Members -> IO a -> IO (Either SomeException a)
inScope part@(Part _ _ frames _ flag) scope' action = do
  readIORef frames >>= writeIORef frames . (Inside scope' :)
  enter scope' part
  -- The deadline may have passed before the part was among the scope's members.
  isCancelled scope' >>= \cancelled -> when cancelled (atomicWriteIORef flag True)
  outcome <- try action
  leave scope' part
  readIORef frames >>= writeIORef frames . drop 1
  pure outcome

-- | The threads have no cores: every ticket can be fulfilled by every thread, so there is one reach.
type instance Local (ThreadsM m) = 'Anywhere

type instance ModeOf (ThreadsM m) = m

-- | A ticket is a slot that every thread changes atomically, and a part that waits at it blocks its thread on an 'MVar' that the hand
-- or the abandon that it waits for fills. The threads have no cores, so there is one reach. A ticket keeps no value for a part that is
-- not parked.
instance Exchange (ThreadsM m) 'Anywhere where
  newtype Future (ThreadsM m) 'Anywhere a = ThreadFuture (IORef (Slot a))
  ticket = Threads (\_ () -> newSlot >>= \slot -> pure (ThreadFuture slot, AnywherePromise slot))
  await = Threads (\part (ThreadFuture slot) -> awaitThread part slot)
  fulfil = Threads (\_ (promise, value) -> fulfilNow promise value)
  abandon = Threads (\_ (ThreadFuture slot) -> closeSlot update slot)
  awaitRegistering = Threads (\part (ThreadFuture slot, register) -> awaitRegisteringThread part slot (register (AnywherePromise slot)))
  willAwait = Threads (\_ future@(ThreadFuture slot) -> commitRegistering update slot >>= \committed -> if committed then pure future else ioError tookWrongly)
  handWaiting = Threads (\part (promise, value) -> anywhereSlot promise >>= \slot -> handThread part slot value)
  untilParked = Threads (\part promise -> anywhereSlot promise >>= \slot -> untilParkedThread part slot)

-- | An await from a thread: it takes the value of a sender that waits, or parks on an 'MVar' that a hand fills. The value stays in the
-- 'MVar' until the part has it: a cancellation that comes after the hand reaches the part after the wait, with the value still there.
awaitThread :: Part -> IORef (Slot a) -> IO a
awaitThread part slot = do
  givingWay part
  valueWaits slot >>= \case
    False -> parkedThread part slot
    True ->
      takeWaiting update slot >>= \case
        Took value -> pure value
        Misused -> ioError tookWrongly
        Parks -> parkedThread part slot

-- | The thread parks as the receiver, unless a sender came since the look. A cancellation takes it off the slot; if a hand has given it a
-- value already, the value is the receiver's, and the cancellation reaches the part at its next wait.
parkedThread :: Part -> IORef (Slot a) -> IO a
parkedThread part slot = do
  gate <- newEmptyMVar
  parkTaker update slot (\value -> True <$ putMVar gate value) >>= \case
    Took value -> pure value
    Misused -> ioError tookWrongly
    Parks ->
      try (waiting part (readMVar gate)) >>= \case
        Right value -> pure value
        Left failure -> stopTaking update slot >>= \stopped -> if stopped || isNothing (cancelledBy failure) then throwIO (failure :: SomeException) else readMVar gate

-- | An await that registers, from a thread, as the arrows that run every part on one thread do it.
awaitRegisteringThread :: Part -> IORef (Slot a) -> IO (Maybe a) -> IO a
awaitRegisteringThread part slot register = do
  givingWay part
  beginRegistering update slot >>= \case
    Took value -> pure value
    Misused -> ioError tookWrongly
    Parks ->
      (register `onException` giveUpRegistering update slot) >>= \case
        Just value -> giveUpRegistering update slot >>= \clean -> if clean then pure value else ioError registeredAndGave
        Nothing ->
          endRegistering update slot >>= \case
            Took value -> pure value
            Misused -> ioError tookWrongly
            Parks -> parkedThread part slot

-- | A hand from a thread that waits until the value is taken: if the receiver isn't parked, the thread waits on an 'MVar' that the
-- receiver's take, or the abandon, fills with whether the value was taken. A cancellation takes the hand out of the slot; if the value was
-- taken already, the hand is done, and the cancellation reaches the part at its next wait.
handThread :: Part -> IORef (Slot a) -> a -> IO (Maybe a)
handThread part slot value = do
  givingWay part
  handTry update slot value >>= \case
    Passed -> pure Nothing
    GaveBack -> pure (Just value)
    SecondSender -> ioError twoSenders
    _ -> do
      gate <- newEmptyMVar
      token <- nextIdent
      parkSender update slot value token (putMVar gate) >>= \case
        Passed -> pure Nothing
        GaveBack -> pure (Just value)
        LeftWaiting ->
          try (waiting part (readMVar gate)) >>= \case
            Right taken -> pure (outcome taken)
            Left failure -> withdrawSender update slot token >>= \withdrawn -> if withdrawn || isNothing (cancelledBy failure) then throwIO (failure :: SomeException) else outcome <$> readMVar gate
        _ -> ioError twoSenders
  where
    outcome taken = if taken then Nothing else Just value

-- | A watch from a thread: it waits on an 'MVar' that the receiver's park, or the abandon, fills.
untilParkedThread :: Part -> IORef (Slot a) -> IO ()
untilParkedThread part slot = do
  givingWay part
  lookAtWatch slot >>= \case
    WatchDone -> pure ()
    SecondWatcher -> ioError twoWatchers
    WatchWaits -> do
      gate <- newEmptyMVar
      token <- nextIdent
      parkWatcher update slot token (() <$ tryPutMVar gate ()) >>= \case
        WatchDone -> pure ()
        SecondWatcher -> ioError twoWatchers
        WatchWaits ->
          try (waiting part (readMVar gate)) >>= \case
            Right () -> pure ()
            Left failure -> unwatch update slot token >>= \stopped -> if stopped || isNothing (cancelledBy failure) then throwIO (failure :: SomeException) else readMVar gate

-- | An inbox is a shelf that every thread changes atomically, and a part that waits at it blocks its thread on an 'MVar' that
-- the post, the collect or the closing that it waits for fills. The threads have no cores, so there is one reach.
instance Inboxes (ThreadsM m) 'Anywhere where
  newtype Inbox (ThreadsM m) name 'Anywhere a = ThreadsInbox (IORef (Shelf a))
  inbox = Threads (\_ () -> newShelf >>= \shelf -> pure (ThreadsInbox shelf, AnywhereAddress shelf))
  collect = Threads (\part (ThreadsInbox shelf) -> waiting part (valueThere shelf) *> takeWoken update shelf)
  post = Threads (\part (AnywhereAddress shelf, value) -> postThread part shelf value)
  closeInbox = Threads (\_ (ThreadsInbox shelf) -> closeShelf update shelf)

-- | Blocks until the inbox has a value, and leaves it there: the collect takes it after the wait, so that a cancellation that
-- reaches the part first leaves it for the next collect.
valueThere :: IORef (Shelf a) -> IO ()
valueThere shelf =
  hasValue shelf >>= \there ->
    if there
      then pure ()
      else do
        gate <- newEmptyMVar
        lookOrWait update shelf (() <$ tryPutMVar gate ()) >>= \case
          Registered -> takeMVar gate `onException` stopWaiting update shelf
          Found -> pure ()
          Unusable -> collectedWrongly

-- | A post from a thread: if the inbox is full, the thread waits on an 'MVar' that a collect, which lets its value in, or the
-- closing fills with whether it went in. A cancellation takes the post out of the queue; if the value went in already, the
-- post is done, and the cancellation reaches the part at its next wait.
postThread :: Part -> IORef (Shelf a) -> a -> IO Bool
postThread part shelf value =
  postNow update shelf value >>= \case
    Just taken -> pure taken
    Nothing -> do
      token <- newIORef ()
      outcome <- newEmptyMVar
      postOrQueue update shelf token value (putMVar outcome) >>= \case
        Just taken -> pure taken
        Nothing ->
          try (waiting part (readMVar outcome)) >>= \case
            Right taken -> pure taken
            Left failure -> withdraw update shelf token >>= \withdrawn -> if withdrawn || isNothing (cancelledBy failure) then throwIO failure else readMVar outcome
