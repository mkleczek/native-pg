{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | Every concurrent part on one thread, as 'Network.Streaming.Loop' runs
-- them, but compiled: an arrow is a function that takes what follows it as a
-- continuation, so a program's composition is function composition, which GHC
-- inlines. A part that waits is a closure, which can't be looked into, and is
-- built only where the part waits.
--
-- A part's handlers are not on a stack that a wait captures, as they are in
-- "Network.Streaming.Delimited": each scope gives what runs inside it its own
-- failure continuation, and the part says which is the one it runs in when it
-- goes on. A failure that leaves the code the scheduler runs is handed, by the
-- scheduler's catch, to the one the part set last, which runs the scope's
-- release as an arrow, and then goes on to the scope outside.
module Network.Streaming.Compiled
  ( Compiled,
    CompiledM,
    runCompiled,
    runCompiledTurn,
    runCompiledM,
    runCompiledTurnM,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Exception (SomeException, mask_, throwIO, toException)
import Control.Monad (when)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Word (Word64)
import Network.Streaming.Classes hiding (Stream (..))
import Network.Streaming.Combinators (done)
import Network.Streaming.Internal.Cancel
import Network.Streaming.Internal.Inboxes (Address (..), Shelf, closeShelf, newShelf)
import Network.Streaming.Internal.Scheduler hiding (post)
import Network.Streaming.Internal.Tickets
import Network.Streaming.Internal.Timers (addTimer, cancelTimer)
import Network.Streaming.Loop (Report)
import Network.Streaming.Mode (Instrumented, Mode (..), ModeOf)
import System.Posix.Types (Fd (..))
import Prelude hiding (id, (.))

-- | The run, the part, and what goes on with a failure of the code that runs in it.
data Context = Context !Run !Part !(SomeException -> IO Step)

-- | An arrow as a function of its context, its input, and of what follows it. Built in the mode @m@.
newtype CompiledM (m :: Mode) a b = Compiled (Context -> a -> (b -> IO Step) -> IO Step)

-- | The arrow in 'Release'.
type Compiled = CompiledM 'Release

instance Category (CompiledM m) where
  id = Compiled (\_ value next -> next value)
  {-# INLINE id #-}
  Compiled after . Compiled before = Compiled (\context value next -> before context value (\middle -> after context middle next))
  {-# INLINE (.) #-}

instance Arrow (CompiledM m) where
  arr function = Compiled (\_ value next -> next (function value))
  {-# INLINE arr #-}
  first (Compiled inner) = Compiled (\context (a, c) next -> inner context a (\b -> next (b, c)))
  {-# INLINE first #-}

instance Functor (CompiledM m a) where
  fmap function (Compiled inner) = Compiled (\context value next -> inner context value (next . function))
  {-# INLINE fmap #-}

instance Applicative (CompiledM m a) where
  pure result = Compiled (\_ _ next -> next result)
  {-# INLINE pure #-}
  Compiled function <*> Compiled argument =
    Compiled (\context value next -> function context value (\applied -> argument context value (next . applied)))
  {-# INLINE (<*>) #-}

instance ArrowApplicative (CompiledM m)

-- | A choice goes on with the branch's continuation itself, so that it wraps
-- nothing.
instance ArrowChoice (CompiledM m) where
  left (Compiled inner) = Compiled $ \context choice next -> case choice of
    Left a -> inner context a (next . Left)
    Right c -> next (Right c)
  {-# INLINE left #-}
  Compiled one ||| Compiled two = Compiled $ \context choice next -> case choice of
    Left a -> one context a next
    Right c -> two context c next
  {-# INLINE (|||) #-}

instance Effect (CompiledM m) where
  now action = Compiled (\_ value next -> action value >>= next)
  {-# INLINE now #-}

-- | The part goes on, in its context: its failures are the context's.
resumed :: Context -> IO a -> IO a
resumed (Context run _ failure) action = writeIORef (runFailure run) failure *> action
{-# INLINE resumed #-}

-- | Counts a step, and goes on, or once the part has run for its turn lets the others go on before it.
steppedIn :: Context -> IO Step -> IO Step
steppedIn context@(Context run _ _) continue = spent run >>= \yields -> if yields then yielded context continue else continue
{-# INLINE steppedIn #-}

-- | The part lets the others go on before it, and raises the cancellation that came meanwhile, if it is in a
-- scope, when it goes on. Not inlined, so that what it builds is built only when the part yields, and not at
-- every step.
yielded :: Context -> IO Step -> IO Step
yielded context@(Context _ part _) continue = pure (Yielded (resumed context (checkpoint part *> continue)))
{-# NOINLINE yielded #-}

-- | Counts a step, and once the part has run for its turn, lets the others go
-- on before it.
instance GiveWay (CompiledM m) where
  giveWay = Compiled (\context value next -> steppedIn context (next value))
  {-# INLINE giveWay #-}

instance Block (CompiledM m) where
  block = Compiled $ \context@(Context run part _) event next ->
    readIORef (partFrames part) >>= \case
      [] -> waitFor context 0 event next
      frames ->
        cancelledFlagged (partCancelled part) frames >>= \case
          Just ident -> throwIO (Cancelled ident)
          Nothing -> do
            ident <- nextWait run
            writeIORef (partWait part) $ case event of
              Readable (Fd fd) -> OnFd (fromIntegral fd) True ident
              Writable (Fd fd) -> OnFd (fromIntegral fd) False ident
              Watched _ -> OnWatch ident
              Until _ -> NotWaiting
            waitFor context ident event next
  {-# INLINE block #-}

-- | Suspends the part until the event: a timer holds it if the event is an instant.
waitFor :: Context -> Int -> Event -> (() -> IO Step) -> IO Step
waitFor context@(Context run part _) ident event next = case event of
  Until instant -> sleepUntil run part instant (\action -> resumed context (action *> next ()))
  _ -> counted run (Suspended event ident (\action -> resumed context (action *> next ())))
{-# INLINE waitFor #-}

-- | The time of the round of the scheduler.
instance Time (CompiledM m) where
  clock = Compiled (\(Context run _ _) () next -> roundNow run >>= next . Instant)

-- | The deadline is a timer of the scheduler's wheel, which cancels the scope when it fires, and a cancel scope
-- that the part is in while the body runs: the cancellation that ends it is recognised by it, and another
-- passes through.
instance Within (CompiledM m) where
  within (Duration limit) (Compiled body) = Compiled (deadlined limit body)
  {-# INLINE within #-}
  withinIn (Compiled body) = Compiled (\context (Duration limit, value) next -> deadlined limit body context value next)
  {-# INLINE withinIn #-}

-- | The body with a deadline of this many nanoseconds, which goes on with nothing if the deadline passed first.
deadlined :: Word64 -> (Context -> a -> (b -> IO Step) -> IO Step) -> Context -> a -> (Maybe b -> IO Step) -> IO Step
deadlined limit body (Context run part outer) value next = do
  scope' <- newScope run
  started <- roundNow run
  timer <- addTimer (runWheel run) (started + limit) (cancelScope run scope')
  pushFrame run part (Inside scope')
  let leaves = popFrame run part *> cancelTimer (runWheel run) timer *> writeIORef (runFailure run) outer
      onFailure failure = do
        leaves
        if cancelledBy failure == Just (scopeIdent scope') then next Nothing else outer failure
  writeIORef (runFailure run) onFailure
  body (Context run part onFailure) value (\result -> leaves *> next (Just result))
{-# INLINE deadlined #-}

instance Scope (CompiledM m) where
  scope (Compiled acquire) (Compiled release) (Compiled body) = Compiled $ \context@(Context run part outer) value next ->
    acquire context value $ \acquired -> do
      -- A part that is in no scope is in the root scope while it holds what it acquired, so that a shutdown
      -- reaches it, and its release runs.
      held <- readIORef (partFrames part) >>= \case [] -> True <$ pushFrame run part Held; _ -> pure False
      let leaves = when held (popFrame run part) *> writeIORef (runFailure run) outer
          -- The release runs shielded: a failure of it is the part's, with how the body ended.
          releasing exit continue = do
            pushFrame run part Shielded
            let unshield = popFrame run part
                failed failure = unshield *> leaves *> outer (toException (ReleaseFailed exit failure))
                inRelease = Context run part failed
            writeIORef (runFailure run) failed
            release inRelease (acquired, exit) (\() -> unshield *> continue)
          -- The body failed: its release runs, and then the failure goes on.
          onFailure failure = releasing (exitOf (Left failure)) (leaves *> throwIO failure)
      writeIORef (runFailure run) onFailure
      body (Context run part onFailure) (acquired, value) $ \result ->
        releasing Done (leaves *> next result)

  tryA (Compiled inner) = Compiled $ \(Context run part outer) value next -> do
    let onFailure failure = do
          writeIORef (runFailure run) outer
          case cancelledBy failure of
            Just _ -> outer failure
            Nothing -> next (Left failure)
    writeIORef (runFailure run) onFailure
    inner (Context run part onFailure) value $ \result -> writeIORef (runFailure run) outer *> next (Right result)

  -- The part leaves the shield as the arrow ends, or fails.
  shield (Compiled inner) = Compiled $ \(Context run part outer) value next -> do
    pushFrame run part Shielded
    let leaves = popFrame run part *> writeIORef (runFailure run) outer
        failed failure = leaves *> outer failure
    writeIORef (runFailure run) failed
    inner (Context run part failed) value (\result -> leaves *> next result)

-- | Annotations are for diagrams: the arrow is run as it is.
instance Annotate (CompiledM m)

-- | The part that starts the group hands what follows it to the group, and the part that ends last goes on with it.
-- The parts are in a cancel scope of their own, which the first failure cancels: the others are cancelled where they
-- wait, and their scopes are released before the group ends, which fails with the first failure. A later failure is
-- dropped.
instance Concurrent (CompiledM m) where
  together = \case
    [] -> done
    [one] -> one
    parts -> Compiled $ \(Context run part outer) value next -> do
      let size = length parts
      scope' <- newScope run
      children <- childParts run part scope' size
      joined <- newIORef (Joined Nothing 0)
      let failure thrown = writeIORef (runFailure run) outer *> outer thrown
          success () = writeIORef (runFailure run) outer *> next ()
          started (Compiled body) child =
            let ended = groupEnded run scope' size joined failure success child
                failing = Context run child (ended . Left)
             in resumed failing (body failing value (ended . Right))
      pure (Forked (zipWith started parts children))

-- | The first half to end is the result: the other is cancelled where it waits, and the race ends once its
-- scopes are released. If the first to end failed, its failure is the race's then. A failure of the loser that
-- isn't its cancellation is too.
instance Race (CompiledM m) where
  race (Compiled one) (Compiled two) = Compiled $ \(Context run part outer) value next -> do
    scope' <- newScope run
    first' <- childPart run part scope'
    second' <- childPart run part scope'
    winner <- newIORef Nothing
    let failure thrown = writeIORef (runFailure run) outer *> outer thrown
        success result = writeIORef (runFailure run) outer *> next result
        ended1 = raceEnded run scope' winner failure success first'
        ended2 = raceEnded run scope' winner failure success second'
        failing1 = Context run first' (ended1 . Left)
        failing2 = Context run second' (ended2 . Left)
    pure (Forked [resumed failing1 (one failing1 value (ended1 . Right . Left)), resumed failing2 (two failing2 value (ended2 . Right . Right))])

-- | The region of the tickets of this arrow: there is one core, the thread that runs it.
data Core

-- | A ticket is a slot on this thread: a part that waits for it is parked in
-- it, as a closure that puts its continuation back among those that can go on,
-- and goes back when the ticket's promise is fulfilled. Fulfilling and awaiting count a
-- step, and can give way: a ticket is where parts coordinate, and none is on a
-- parser's path.
type instance Local (CompiledM m) = 'Here Core

type instance ModeOf (CompiledM m) = m

instance Exchange (CompiledM m) ('Here Core) where
  newtype Future (CompiledM m) ('Here Core) a = CompiledFuture (IORef (Slot a))
  ticket = Compiled (\_ () next -> newIORef Empty >>= \slot -> next (CompiledFuture slot, HerePromise slot))
  await = Compiled $ \context@(Context run part _) (CompiledFuture slot) next ->
    readIORef slot >>= \case
      Fulfilled value -> checkpoint part *> steppedIn context (next value)
      Empty -> registering part (OnTicket slot) *> (Parked <$ writeIORef slot (Waiter (\action -> wakeTicket run (resumed context (action >>= next)))))
      Waiter _ -> ioError (userError "two parts wait for one ticket")
      Abandoned -> ioError (userError "a ticket was waited for after it was abandoned")
  fulfil = Compiled (\context (HerePromise slot, value) next -> fulfilHere slot value >>= \taken -> steppedIn context (next taken))
  abandon = Compiled (\_ (CompiledFuture slot) next -> abandonHere slot >>= next)

-- | The waiter of a ticket that any thread can fulfil is parked in the slot as a local ticket's is, but the
-- promise, on another thread, doesn't run it: it puts it in the mailbox of this scheduler, which goes on with it in
-- its next round, and wakes the scheduler if it sleeps in its poller. There is one core: a program of this arrow
-- makes the ticket and waits at it on this thread, and what fulfils its promise is any thread, or the same.
instance Exchange (CompiledM m) 'Anywhere where
  newtype Future (CompiledM m) 'Anywhere a = CompiledRemoteFuture (IORef (RemoteSlot a))
  ticket = Compiled $ \(Context run _ _) () next -> do
    slot <- newIORef RemoteEmpty
    next (CompiledRemoteFuture slot, AnywherePromise (fulfilRemote (runMailbox run) slot))
  await = Compiled $ \context@(Context run part _) (CompiledRemoteFuture slot) next ->
    awaitRemote run part slot (steppedIn context . next) (\park -> park (\action -> resumed context (action >>= next)))
  fulfil = Compiled $ \context (AnywherePromise promise, value) next -> promise value >>= \taken -> steppedIn context (next taken)
  abandon = Compiled $ \_ (CompiledRemoteFuture slot) next -> abandonRemote slot >>= next

-- | An inbox of this core is a shelf of plain memory, and a part that waits at it is parked there, as at a ticket, until a post
-- or a collect lets it go on. Posting and collecting count a step and can give way, as a ticket does, before they act: a
-- cancellation that the yield raises leaves them undone.
instance Inboxes (CompiledM m) ('Here Core) where
  newtype Inbox (CompiledM m) name ('Here Core) a = CompiledInbox (IORef (Shelf a))
  inbox = Compiled (\_ () next -> newShelf >>= \shelf -> next (CompiledInbox shelf, HereAddress shelf))
  collect = Compiled $ \context@(Context run part _) (CompiledInbox shelf) next ->
    steppedIn context (collectFrom hereSeat run part shelf next (\park -> park (\action -> resumed context (action >>= next))))
  post = Compiled $ \context@(Context run part _) (HereAddress shelf, value) next ->
    steppedIn context (postTo hereSeat run part shelf value next (\park -> park (\action -> resumed context (action >>= next))))
  closeInbox = Compiled (\_ (CompiledInbox shelf) next -> closeShelf (seatChange hereSeat) shelf >>= next)

-- | The shelf of an inbox of any core is changed by compare-and-swap, and the part that lets a part that waits at it go on puts
-- it in the mailbox of its scheduler, as at a ticket of any core.
instance Inboxes (CompiledM m) 'Anywhere where
  newtype Inbox (CompiledM m) name 'Anywhere a = CompiledRemoteInbox (IORef (Shelf a))
  inbox = Compiled (\_ () next -> newShelf >>= \shelf -> next (CompiledRemoteInbox shelf, AnywhereAddress shelf))
  collect = Compiled $ \context@(Context run part _) (CompiledRemoteInbox shelf) next ->
    steppedIn context (collectFrom anywhereSeat run part shelf next (\park -> park (\action -> resumed context (action >>= next))))
  post = Compiled $ \context@(Context run part _) (AnywhereAddress shelf, value) next ->
    steppedIn context (postTo anywhereSeat run part shelf value next (\park -> park (\action -> resumed context (action >>= next))))
  closeInbox = Compiled (\_ (CompiledRemoteInbox shelf) next -> closeShelf (seatChange anywhereSeat) shelf >>= next)

-- | Runs the program and all of its concurrent parts on this thread: each part
-- that can go on in turn, until it has run for a turn and reaches a point to
-- give way at (a link's receives and sends), and when none can, a wait until a
-- descriptor wakes one, or a watch fires.
runCompiled :: Compiled a b -> a -> IO (b, Report)
runCompiled = runCompiledTurn defaultTurn

-- | 'runCompiled' for an arrow of any mode.
runCompiledM :: (Instrumented m) => CompiledM m a b -> a -> IO (b, Report)
runCompiledM = runCompiledTurnM defaultTurn

-- | 'runCompiled' with a turn of the given length.
runCompiledTurn :: Turn -> Compiled a b -> a -> IO (b, Report)
runCompiledTurn = runCompiledTurnM

-- | 'runCompiledTurn' for an arrow of any mode.
runCompiledTurnM :: forall m a b. (Instrumented m) => Turn -> CompiledM m a b -> a -> IO (b, Report)
runCompiledTurnM turn (Compiled program) input = do
  result <- newIORef Nothing
  withRun turn $ \run -> do
    root <- newPart run
    let failure = throwIO . Escaped
        context = Context run root failure
    writeIORef (runFailure run) failure
    mask_ $ schedule @m run (guardedBy run) result (program context input (\value -> Ended <$ writeIORef result (Just value)))
