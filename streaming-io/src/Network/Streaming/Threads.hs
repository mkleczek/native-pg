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
import Control.Concurrent.STM (TMVar, TVar, atomically, newEmptyTMVarIO, newTVarIO, readTMVar, readTVar, throwSTM, tryPutTMVar, tryTakeTMVar, writeTVar)
import Control.Exception (SomeException, allowInterrupt, mask_, onException, throwIO, try, uninterruptibleMask_)
import Control.Monad (forM_, when)
import Data.Foldable (traverse_)
import Data.Functor (($>))
import Data.IORef (atomicWriteIORef, newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.Maybe (isNothing)
import Data.Word (Word64)
import GHC.Event (getSystemTimerManager, registerTimeout, unregisterTimeout)
import GHC.Exts (casMutVar#)
import GHC.IO (IO (..))
import GHC.IORef (IORef (..), atomicSwapIORef)
import GHC.STRef (STRef (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done)
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Mode (Instrumented (..), Mode (..), ModeOf)
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.Cancel (CancelScope (..), Frame (..), cancelledAnywhere, cancelledFlagged, deliverable, newCancelScope, reachedBy)
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

-- | A number that no other part or scope has, from any thread.
nextIdent :: IO Int
nextIdent = update counter (\n -> (n + 1, n))

newScope :: IO (CancelScope Members)
newScope = nextIdent >>= \ident -> newCancelScope ident IntMap.empty

-- | The part is inside the scope, or is not any more.
enter, leave :: CancelScope Members -> Part -> IO ()
enter s part@(Part ident _ _ _ _) = update (scopeMembers s) (\parts -> (IntMap.insert ident part parts, ()))
leave s (Part ident _ _ _ _) = update (scopeMembers s) (\parts -> (IntMap.delete ident parts, ()))

-- | Sets the scope's flag, and gives the parts that were inside it. The flag is set with a barrier, so that a
-- part that enters the scope after the parts are read sees it.
markCancelled :: CancelScope Members -> IO [Part]
markCancelled s = atomicWriteIORef (scopeCancelled s) True *> (IntMap.elems <$> readIORef (scopeMembers s))

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

-- | The point itself, which what the part has in hand doesn't go through: the call is not given it, so that a caller
-- doesn't have to build it, as a link's input is a pair of the link and where it receives, for the call to give it back.
givingWay :: Part -> IO ()
givingWay (Part _ _ frames _ flag) =
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
-- those that wait, and whose scope lies above their innermost shield, are thrown to, once; the others see it at
-- their next wait. The flag is set, with a barrier, before the phase is read: a part that begins to wait meanwhile
-- swaps its phase, and then looks at its flag.
cancelScope :: CancelScope Members -> IO ()
cancelScope scope' = mask_ $ do
  parts <- markCancelled scope'
  forM_ parts $ \(Part _ thread _ phase flag) -> do
    atomicWriteIORef flag True
    -- A part that began to wait again meanwhile has another phase to look at.
    let attempt' = do
          current <- readIORef phase
          case current of
            Waiting frames | deliverable (scopeIdent scope') frames -> do
              thrown <- casPhase phase current Thrown
              if thrown then uninterruptibleMask_ (throwTo thread (Cancelled (scopeIdent scope'))) else attempt'
            _ -> pure ()
    attempt'

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

-- | A part that runs in the scope as well as in those of the part that made it, on a thread of its own, and what
-- it came to once it ended: the scopes are left before the pair's policy is told.
spawn :: Part -> CancelScope Members -> (Part -> IO a) -> (Either SomeException a -> IO ()) -> IO (MVar (Either SomeException a))
spawn (Part _ _ parent _ parentFlag) scope' run ended = do
  finished <- newEmptyMVar
  -- Forked masked, as its part is.
  _ <- forkIO $ do
    thread <- myThreadId
    ident <- nextIdent
    frames <- (Inside scope' :) <$> readIORef parent
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
    putMVar finished outcome
  pure finished
  where
    inside part = \case Inside s -> enter s part; _ -> pure ()
    outside part = \case Inside s -> leave s part; _ -> pure ()

-- | Waits for both, and if it is interrupted, cancels them and waits for them still: no part outlives the race.
awaitBoth :: CancelScope Members -> MVar a -> MVar b -> IO (a, b)
awaitBoth scope' one other =
  ((,) <$> readMVar one <*> readMVar other)
    `onException` (cancelScope scope' *> uninterruptibleMask_ (readMVar one *> readMVar other $> ()))

-- | Waits for all, and if it is interrupted, cancels them and waits for them still: no part outlives the group.
awaitAll :: CancelScope Members -> [MVar a] -> IO [a]
awaitAll scope' dones =
  traverse readMVar dones
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
  together = \case
    [] -> done
    [one] -> one
    parts -> Threads $ \part input -> do
      scope' <- newScope
      failure <- newIORef Nothing
      dones <- traverse (\(Threads body) -> spawn part scope' (`body` input) (failing failure scope')) parts
      _ <- awaitAll scope' dones
      readIORef failure >>= maybe (pure ()) throwIO

-- | The first half to end is the result, and the other is cancelled and waited for. The loser's failure is
-- rethrown too, unless it is a cancellation.
instance Race (ThreadsM m) where
  race (Threads one) (Threads two) = Threads $ \part input -> do
    scope' <- newScope
    winner <- newIORef Nothing
    let ended outcome = do
          first' <- update winner (\known -> (known <|> Just outcome, isNothing known))
          when first' (cancelScope scope')
    first' <- spawn part scope' (\child -> Left <$> one child input) ended
    second' <- spawn part scope' (\child -> Right <$> two child input) ended
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
  let micros = fromIntegral (limit `div` 1000)
  outcome <-
    if rtsSupportsBoundThreads
      then do
        manager <- getSystemTimerManager
        key <- registerTimeout manager micros (cancelScope scope')
        result <- inScope part scope' action
        result <$ unregisterTimeout manager key
      else do
        -- The non-threaded run time has no timer manager: a thread of its own sleeps, and is stopped with the deadline.
        timer <- mask_ (forkIO (threadDelay micros *> cancelScope scope'))
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
  readIORef (scopeCancelled scope') >>= \cancelled -> when cancelled (atomicWriteIORef flag True)
  outcome <- try action
  leave scope' part
  readIORef frames >>= writeIORef frames . drop 1
  pure outcome

-- | The threads have no cores: every ticket can be fulfilled by every thread, so there is one reach.
type instance Local (ThreadsM m) = 'Anywhere

type instance ModeOf (ThreadsM m) = m

-- | A ticket is a 'TMVar', which blocks the thread that waits for it, and whether the waiter has gone. The future
-- and the promise are the same 'TMVar'. Every thread can reach both: there is no core to belong to.
instance Exchange (ThreadsM m) 'Anywhere where
  data Future (ThreadsM m) 'Anywhere a = ThreadFuture (TMVar a) (TVar Bool)
  ticket = Threads $ \_ () -> do
    var <- newEmptyTMVarIO
    abandoned <- newTVarIO False
    pure (ThreadFuture var abandoned, AnywherePromise (fulfilThread var abandoned))
  await = Threads (\part (ThreadFuture var _) -> waiting part (atomically (readTMVar var)))
  fulfil = Threads (\_ (AnywherePromise promise, value) -> promise value)
  abandon = Threads $ \_ (ThreadFuture var abandoned) -> atomically (writeTVar abandoned True *> tryTakeTMVar var)

-- | The promise evaluates the value, before the transaction, as the other arrows' promises do.
fulfilThread :: TMVar a -> TVar Bool -> a -> IO Bool
fulfilThread var abandoned !value = atomically $ do
  gone <- readTVar abandoned
  if gone
    then pure False
    else do
      fulfilled <- tryPutTMVar var value
      if fulfilled then pure True else throwSTM (userError "a promise was fulfilled twice")

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
