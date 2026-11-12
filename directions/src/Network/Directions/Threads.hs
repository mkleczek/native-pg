{-# LANGUAGE BangPatterns #-}
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
module Network.Directions.Threads
  ( Threads,
    runThreads,
  )
where

import Control.Applicative ((<|>))
import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Concurrent (ThreadId, forkIO, myThreadId, threadDelay, threadWaitRead, threadWaitWrite, throwTo, yield)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import Control.Concurrent.STM (TMVar, TVar, atomically, newEmptyTMVarIO, newTVarIO, readTMVar, readTVar, throwSTM, tryPutTMVar, tryTakeTMVar, writeTVar)
import Control.Exception (SomeException, allowInterrupt, mask_, onException, throwIO, try, uninterruptibleMask_)
import Control.Monad (forM_, unless, when)
import Data.Functor (($>))
import Data.IORef (atomicWriteIORef, newIORef, readIORef, writeIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.Maybe (isNothing)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Event (getSystemTimerManager, registerTimeout, unregisterTimeout)
import GHC.Exts (casMutVar#)
import GHC.IO (IO (..))
import GHC.IORef (IORef (..), atomicSwapIORef)
import GHC.STRef (STRef (..))
import Network.Directions.Atomic (update)
import Network.Directions.Cancel (CancelScope (..), Frame (..), cancelledIn, deliverable, newCancelScope)
import Network.Directions.Classes
import Network.Streaming.Watch (awaitWatch)
import System.IO.Unsafe (unsafePerformIO)
import Prelude hiding (id, (.))

-- | An arrow whose concurrent parts run on threads of their own: a function of the part that runs it.
newtype Threads a b = Threads (Part -> a -> IO b)

-- | A part: its number, its thread, what it is in, innermost first, and whether it waits. Only the thread of the
-- part changes its frames; the thread that cancels it changes its phase, from waiting to thrown, and the part
-- changes it back.
data Part = Part !Int !ThreadId !(IORef [Frame Members]) !(IORef Phase)

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
enter s part@(Part ident _ _ _) = update (scopeMembers s) (\parts -> (IntMap.insert ident part parts, ()))
leave s (Part ident _ _ _) = update (scopeMembers s) (\parts -> (IntMap.delete ident parts, ()))

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
runThreads (Threads run) input = mask_ $ do
  thread <- myThreadId
  ident <- nextIdent
  part <- Part ident thread <$> newIORef [] <*> newIORef Running
  run part input

instance Category Threads where
  id = Threads (const pure)
  Threads after . Threads before = Threads (\part value -> before part value >>= after part)

instance Arrow Threads where
  arr function = Threads (\_ value -> pure (function value))
  first (Threads inner) = Threads (\part (a, c) -> (,c) <$> inner part a)

instance ArrowChoice Threads where
  left (Threads inner) = Threads $ \part -> \case
    Left a -> Left <$> inner part a
    Right c -> pure (Right c)
  Threads one ||| Threads two = Threads (\part -> either (one part) (two part))

instance Effect Threads where
  now action = Threads (const action)

-- | A point where the part can be cancelled: a part in a scope raises a cancellation that is there, and lets the
-- exceptions that wait for it in.
instance GiveWay Threads where
  giveWay = Threads $ \(Part _ _ frames _) value ->
    readIORef frames >>= \case
      [] -> value <$ allowInterrupt
      scopes -> cancelledIn scopes >>= maybe (value <$ allowInterrupt) (throwIO . Cancelled)

-- | A wait of a part that is in scopes: it says it waits, looks for a cancellation that came before, and waits;
-- a cancellation thrown to it ends the wait. Whatever ended it, the part says it doesn't wait.
armed :: Part -> [Frame Members] -> IO a -> IO a
armed (Part _ _ _ phase) frames wait = do
  -- Evaluated, so that what the phase holds is the object that a cancellation compares with.
  let !waits = Waiting frames
  _ <- atomicSwapIORef phase waits
  cancelledIn frames >>= \case
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
waiting part@(Part _ _ frames _) wait =
  readIORef frames >>= \case
    [] -> wait
    scopes -> armed part scopes wait
{-# INLINE waiting #-}

-- | Cancels the scope: the parts inside it that wait, and whose scope lies above their innermost shield, are
-- thrown to, once; the others see its flag at their next wait.
cancelScope :: CancelScope Members -> IO ()
cancelScope scope' = mask_ $ do
  parts <- markCancelled scope'
  forM_ parts $ \(Part _ thread _ phase) -> do
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
framed frame (Part _ _ frames _) action = do
  readIORef frames >>= writeIORef frames . (frame :)
  outcome <- try action
  readIORef frames >>= writeIORef frames . drop 1
  either (\failure -> throwIO (failure :: SomeException)) pure outcome

-- | Runs the action behind a shield.
shielded :: Part -> IO a -> IO a
shielded = framed Shielded

-- | The part goes behind a shield, and leaves it.
pushed, popped :: Part -> IO ()
pushed (Part _ _ frames _) = readIORef frames >>= writeIORef frames . (Shielded :)
popped (Part _ _ frames _) = readIORef frames >>= writeIORef frames . drop 1

instance Scope Threads where
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
spawn (Part _ _ parent _) scope' run ended = do
  done <- newEmptyMVar
  -- Forked masked, as its part is.
  _ <- forkIO $ do
    thread <- myThreadId
    ident <- nextIdent
    frames <- (Inside scope' :) <$> readIORef parent
    state <- newIORef frames
    phase <- newIORef Running
    let part = Part ident thread state phase
    forM_ frames (inside part)
    outcome <- try (run part)
    forM_ frames (outside part)
    ended outcome
    putMVar done outcome
  pure done
  where
    inside part = \case Inside s -> enter s part; _ -> pure ()
    outside part = \case Inside s -> leave s part; _ -> pure ()

-- | Waits for both, and if it is interrupted, cancels them and waits for them still: no part outlives the pair.
awaitBoth :: CancelScope Members -> MVar a -> MVar b -> IO (a, b)
awaitBoth scope' one other =
  ((,) <$> readMVar one <*> readMVar other)
    `onException` (cancelScope scope' *> uninterruptibleMask_ (readMVar one *> readMVar other $> ()))

-- | What a pair of halves does when one has ended: the first failure is kept, and cancels the scope.
failing :: IORef (Maybe SomeException) -> CancelScope Members -> Either SomeException x -> IO ()
failing failure scope' = \case
  Left failed -> do
    first' <- update failure (\known -> (known <|> Just failed, isNothing known))
    when first' (cancelScope scope')
  Right _ -> pure ()

-- | Each on a thread of its own, in a cancel scope that the first failure cancels: the other half is cancelled
-- at its wait, and waited for, and the first failure is rethrown. When both fail, the second is dropped.
instance Concurrent Threads where
  concurrently (Threads one) (Threads two) = Threads $ \part input -> do
    scope' <- newScope
    failure <- newIORef Nothing
    first' <- spawn part scope' (`one` input) (failing failure scope')
    second' <- spawn part scope' (`two` input) (failing failure scope')
    (b, d) <- awaitBoth scope' first' second'
    readIORef failure >>= \case
      Just failed -> throwIO failed
      Nothing -> case (b, d) of
        (Right b', Right d') -> pure (b', d')
        (Left failed, _) -> throwIO failed
        (_, Left failed) -> throwIO failed

-- | The first half to end is the result, and the other is cancelled and waited for. The loser's failure is
-- rethrown too, unless it is a cancellation.
instance Race Threads where
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
instance Block Threads where
  block = Threads $ \part event -> waiting part $ case event of
    Readable fd -> threadWaitRead fd
    Writable fd -> threadWaitWrite fd
    Watched register -> awaitWatch register
    Until (Instant at) -> getMonotonicTimeNSec >>= \current -> when (at > current) (threadDelay (fromIntegral ((at - current + 999) `div` 1000)))

instance Time Threads where
  clock = Threads (\_ () -> Instant <$> getMonotonicTimeNSec)

-- | The deadline is a timeout of GHC's timer manager, which cancels the scope when it fires, and a cancel scope
-- that the part is in while the body runs: the cancellation that ends it is recognised by it, and another
-- passes through.
instance Within Threads where
  within (Duration limit) (Threads body) = Threads $ \part value -> do
    scope' <- newScope
    manager <- getSystemTimerManager
    key <- registerTimeout manager (fromIntegral (limit `div` 1000)) (cancelScope scope')
    outcome <- inScope part scope' (body part value)
    unregisterTimeout manager key
    case outcome of
      Right result -> pure (Just result)
      Left failure
        | cancelledBy failure == Just (scopeIdent scope') -> pure Nothing
        | otherwise -> throwIO failure

-- | Runs the action as a part that is in the cancel scope, and how it ended.
inScope :: Part -> CancelScope Members -> IO a -> IO (Either SomeException a)
inScope part@(Part _ _ frames _) scope' action = do
  readIORef frames >>= writeIORef frames . (Inside scope' :)
  enter scope' part
  outcome <- try action
  leave scope' part
  readIORef frames >>= writeIORef frames . drop 1
  pure outcome

-- | A ticket is a 'TMVar', which blocks the thread that waits for it.
instance Exchange Threads where
  newtype Ticket Threads a = ThreadTicket (TMVar a)
  newTicket = Threads (\_ () -> ThreadTicket <$> newEmptyTMVarIO)
  fulfil = Threads $ \_ (ThreadTicket var, value) -> do
    given <- atomically (tryPutTMVar var value)
    unless given $ ioError (userError "a ticket was given a value twice")
  await = Threads (\part (ThreadTicket var) -> waiting part (atomically (readTMVar var)))

-- | The two ends of a remote ticket are the same 'TMVar', and whether the
-- waiter has gone. Every thread can reach both: there is no core to belong to.
instance Remote Threads where
  data Awaiting Threads a = ThreadAwaiting (TMVar a) (TVar Bool)
  newRemote = Threads $ \_ () -> do
    var <- newEmptyTMVarIO
    abandoned <- newTVarIO False
    pure (ThreadAwaiting var abandoned, Giver (giveThread var abandoned))
  awaitRemote = Threads (\part (ThreadAwaiting var _) -> waiting part (atomically (readTMVar var)))
  give = Threads (\_ (Giver giving, value) -> giving value)
  abandon = Threads $ \_ (ThreadAwaiting var abandoned) -> atomically (writeTVar abandoned True *> tryTakeTMVar var)

giveThread :: TMVar a -> TVar Bool -> a -> IO Bool
giveThread var abandoned value = atomically $ do
  gone <- readTVar abandoned
  if gone
    then pure False
    else do
      given <- tryPutTMVar var value
      if given then pure True else throwSTM (userError "a remote ticket was given a value twice")
