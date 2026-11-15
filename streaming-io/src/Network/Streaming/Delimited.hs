{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RoleAnnotations #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Every concurrent part on one thread, as 'Network.Streaming.Compiled' runs
-- them, but as plain IO: an arrow is a function to IO, so a program is the
-- code GHC compiles it to, and a part that has to wait captures what follows
-- it, up to the scheduler's prompt, with GHC's delimited continuations
-- ('control0#'). Only a part that waits pays for its continuation: the stack
-- between the prompt and the wait, copied to the heap.
--
-- The scheduler's core is in the arrow's type, as the state thread is in
-- @ST@'s: a program is @'Delimited' s a b@ for any @s@ that its run chooses, and
-- what it makes with a ticket of reach @'Here s@ belongs to that @s@, so that
-- it can't leave the run it was made in, or be used by the program of another
-- core. What crosses cores is a 'Promise' of reach @'Anywhere@, which has no @s@.
--
-- A scope is plain IO code in the part, and keeps nothing of its own on the stack that a wait captures but
-- one frame, which runs its release when its body ends. What it owes for a failure is a record in the part's
-- frames, and the catcher that the failure comes to runs it: 'tryA', a deadline, the base of a part of a group or a race,
-- and the base of the program, each of which has a handler on the stack and a mark in the frames, down to which it runs the
-- releases that are due, so that a release runs once, after the resume, however the part ended. A part is cancelled by
-- resuming it, where it waits, with the cancellation to raise there. The scheduler runs and resumes every part
-- masked, so that the part sees one masking state everywhere, as if it never suspended.
module Network.Streaming.Delimited
  ( Delimited,
    DelimitedM,
    runDelimited,
    runDelimitedTurn,
    runDelimitedWith,
    runDelimitedWithTurn,
    runCores,
    runCoresTurn,
    runCoresReport,
    runCoresReportTurn,
    runDelimitedM,
    runDelimitedTurnM,
    runCoresM,
    runCoresTurnM,
    runCoresReportTurnM,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Concurrent (forkOnWithUnmask, killThread)
import Control.Concurrent.Chan (newChan, readChan, writeChan)
import Control.Exception (SomeException, catch, mask, mask_, onException, throwIO, try)
import Control.Monad (replicateM_, void, when)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Kind (Type)
import Data.List (sortOn)
import Data.Word (Word64)
import GHC.Exts (PromptTag#, control0#, newPromptTag#, prompt#)
import GHC.IO (IO (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done)
import Network.Streaming.Internal.Cancel
import Network.Streaming.Internal.Inboxes (Address (..), Shelf, closeShelf, newShelf)
import Network.Streaming.Internal.Scheduler hiding (post)
import Network.Streaming.Internal.Tickets
import Network.Streaming.Internal.Timers (Timer, addTimer, cancelTimer)
import Network.Streaming.Loop (Report)
import Network.Streaming.Counters (Counter (Captures))
import Network.Streaming.Mode (Instrumented (bump), Mode (..), ModeOf)
import Network.Streaming.NotImplemented (notImplementedIO)
import System.Posix.Types (Fd (..))
import Prelude hiding (id, (.))

-- | An arrow as a function to IO, which runs under the scheduler's prompt, on
-- the core that @s@ stands for, built in the mode @m@. Nominal in @s@, so that it can't be coerced
-- to another's, and in @m@, since what is observed of a run is the mode's.
newtype DelimitedM (m :: Mode) (s :: Type) a b = Delimited (Context -> a -> IO b)

type role DelimitedM nominal nominal representational representational

-- | The arrow in 'Release'.
type Delimited = DelimitedM 'Release

-- | The run, the prompt that a part's continuation is captured up to, and the part.
data Context = Context !Run !(PromptTag Step) !Part

data PromptTag a = PromptTag (PromptTag# a)

newPromptTag :: IO (PromptTag a)
newPromptTag = IO (\state -> case newPromptTag# state of (# state', tag #) -> (# state', PromptTag tag #))

-- | Runs the action as far as it goes before something in it captures what
-- follows up to here.
prompt :: PromptTag a -> IO a -> IO a
prompt (PromptTag tag) (IO action) = IO (prompt# tag action)

-- | Captures what follows, up to the nearest prompt of the tag, and gives the
-- handler how to go on with it; what the handler gives is what the prompt
-- gives.
control0 :: PromptTag a -> ((IO b -> IO a) -> IO a) -> IO b
control0 (PromptTag tag) handler = IO (control0# tag (\resume -> case handler (\(IO rest) -> IO (resume rest)) of IO captured -> captured))

instance Category (DelimitedM m s) where
  id = Delimited (const pure)
  {-# INLINE id #-}
  Delimited after . Delimited before = Delimited (\context value -> before context value >>= after context)
  {-# INLINE (.) #-}

instance Arrow (DelimitedM m s) where
  arr function = Delimited (\_ value -> pure (function value))
  {-# INLINE arr #-}
  first (Delimited inner) = Delimited (\context (a, c) -> (,c) <$> inner context a)
  {-# INLINE first #-}

instance Functor (DelimitedM m s a) where
  fmap function (Delimited inner) = Delimited (\context value -> function <$> inner context value)
  {-# INLINE fmap #-}

instance Applicative (DelimitedM m s a) where
  pure result = Delimited (\_ _ -> pure result)
  {-# INLINE pure #-}
  Delimited function <*> Delimited argument = Delimited (\context value -> function context value <*> argument context value)
  {-# INLINE (<*>) #-}

instance ArrowApplicative (DelimitedM m s)

-- | A choice runs its branch in its place, so that a loop through a branch is
-- a tail call.
instance ArrowChoice (DelimitedM m s) where
  left (Delimited inner) = Delimited $ \context -> \case
    Left a -> Left <$> inner context a
    Right c -> pure (Right c)
  {-# INLINE left #-}
  Delimited one ||| Delimited two = Delimited $ \context -> \case
    Left a -> one context a
    Right c -> two context c
  {-# INLINE (|||) #-}

instance Effect (DelimitedM m s) where
  now action = Delimited (\_ value -> action value)
  {-# INLINE now #-}

-- | Counts a step, and once the part has run for its turn, lets the others go
-- on before it.
instance GiveWay (DelimitedM m s) where
  giveWay = Delimited (\context value -> value <$ step context)
  {-# INLINE giveWay #-}

instance Block (DelimitedM m s) where
  block = Delimited $ \(Context run tag part) event ->
    readIORef (partFrames part) >>= \case
      [] -> waitFor run tag part 0 event
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
            waitFor run tag part ident event
  {-# INLINE block #-}

-- | Suspends the part until the event: a timer holds it if the event is an instant.
waitFor :: Run -> PromptTag Step -> Part -> Int -> Event -> IO ()
waitFor run tag part ident event = case event of
  Until instant -> control0 tag (sleepUntil run part instant)
  _ -> control0 tag (counted run . Suspended event ident)
{-# INLINE waitFor #-}

-- | The time of the round of the scheduler.
instance Time (DelimitedM m s) where
  clock = Delimited (\(Context run _ _) () -> Instant <$> roundNow run)

-- | The deadline is a timer of the scheduler's wheel, which cancels the scope when it fires, and a cancel scope
-- that the part is in while the body runs, with a mark above it for the failure of the body. The scope is the body's own: the
-- cancellation that ends it is recognised by it, and another passes through.
instance Within (DelimitedM m s) where
  within (Duration limit) (Delimited body) = Delimited (deadlined limit body)
  {-# INLINE within #-}
  timeoutA (Delimited body) = Delimited (\context (Duration limit, value) -> deadlined limit body context value)
  {-# INLINE timeoutA #-}

-- | The body with a deadline of this many nanoseconds: nothing if the deadline passed first.
deadlined :: Word64 -> (Context -> a -> IO b) -> Context -> a -> IO (Maybe b)
deadlined limit body context@(Context run _ part) value = do
  scope' <- newScope run
  started <- roundNow run
  timer <- addTimer (runWheel run) (started + limit) (cancelScope run scope')
  pushFrame run part (Inside scope')
  marking context
  let deadline = Deadline context scope' timer
  catch (body context value >>= deadlineDone deadline) (deadlineFailed deadline)
{-# INLINE deadlined #-}

-- | A deadline that the part is in: the part, its scope and its timer, which is all that the frame that the deadline leaves on
-- the stack holds, the one pointer.
data Deadline = Deadline !Context !PartScope !Timer

-- | The part leaves the deadline, and its timer is cancelled.
leavesDeadline :: Deadline -> IO ()
leavesDeadline (Deadline (Context run _ part) _ timer) = popFrame run part *> popFrame run part *> void (cancelTimer (runWheel run) timer)

-- | The body of a deadline ended with its result before the deadline passed.
deadlineDone :: Deadline -> a -> IO (Maybe a)
deadlineDone deadline result = Just result <$ leavesDeadline deadline
{-# OPAQUE deadlineDone #-}

-- | The body of a deadline failed: the releases that its failure finds are run, and the part leaves the deadline. The body gave
-- nothing if the deadline passed, and any other failure is the part's.
deadlineFailed :: Deadline -> SomeException -> IO (Maybe a)
deadlineFailed deadline@(Deadline (Context run _ part) scope' _) failure = do
  unwound <- unwindFrames run part AtMark failure
  leavesDeadline deadline
  if cancelledBy unwound == Just (scopeIdent scope') then pure Nothing else throwIO unwound
{-# OPAQUE deadlineFailed #-}

-- | The failure of the body of a part of a group or a race, for the part that pushed a mark at its base: the releases that are
-- due above the mark are run, and what is left of the failure is the part's.
unwinding :: Run -> Part -> SomeException -> IO (Either SomeException a)
unwinding run part failure = Left <$> unwindFrames run part AtMark failure

-- | A scope that was entered: the part, and the release that is due, which is all that the frame that the scope leaves on the
-- stack holds, the one pointer.
data Entered = Entered !Context !(Exit -> IO ())

-- | The part enters the scope: the release is due from now on. Not inlined, so that the frame that follows the body holds
-- only what 'leaving' is given.
entering :: Context -> (Exit -> IO ()) -> IO Entered
entering context@(Context run _ part) owed = Entered context owed <$ pushFrame run part (Owes owed)
{-# OPAQUE entering #-}

-- | The body of the scope ended with the result: its release runs behind a shield, and the scope is left. A part that is in no
-- other scope was in the root scope while it held what it acquired, so that a shutdown reaches it, and its release runs.
leaving :: Entered -> b -> IO b
leaving (Entered (Context run _ part) owed) result = do
  shieldTop part
  released <- try (owed Done)
  popFrame run part
  case released of
    Left failure -> throwIO (ReleaseFailed Done failure)
    Right () -> pure result
{-# OPAQUE leaving #-}

-- | The part goes behind a shield. Not inlined, so that the frame that follows the body holds only the context.
shielding :: Context -> IO ()
shielding (Context run _ part) = pushFrame run part Shielded
{-# OPAQUE shielding #-}

-- | The shield ended with the result: the part leaves it. Not inlined, so that the frame that follows the arrow holds only the
-- context.
unshielded :: Context -> b -> IO b
unshielded (Context run _ part) result = result <$ popFrame run part
{-# OPAQUE unshielded #-}

-- | The part pushes the mark of a catcher, above which a failure runs the releases that are due. Not inlined, so that the frame
-- that follows the body holds only what the catcher's helpers are given.
marking :: Context -> IO ()
marking (Context run _ part) = pushFrame run part Marked
{-# OPAQUE marking #-}

-- | The body of 'tryA' ended with its result: the part leaves the catcher.
unmarked :: Context -> b -> IO (Either SomeException b)
unmarked (Context run _ part) result = Right result <$ popFrame run part
{-# OPAQUE unmarked #-}

-- | The body of 'tryA' failed: the releases that its failure finds are run, the part leaves the catcher, and a failure that is
-- not a cancellation is the result.
caught :: Context -> SomeException -> IO (Either SomeException b)
caught (Context run _ part) failure = do
  unwound <- unwindFrames run part AtMark failure
  popFrame run part
  case cancelledBy unwound of
    Just _ -> throwIO unwound
    Nothing -> pure (Left unwound)
{-# OPAQUE caught #-}

instance Scope (DelimitedM m s) where
  scope (Delimited acquire) (Delimited release) (Delimited body) = Delimited $ \context value -> do
    acquired <- acquire context value
    entered <- entering context (\exit -> release context (acquired, exit))
    body context (acquired, value) >>= leaving entered

  tryA (Delimited inner) = Delimited $ \context value -> do
    marking context
    catch (inner context value >>= unmarked context) (caught context)

  shield (Delimited inner) = Delimited $ \context value -> do
    shielding context
    inner context value >>= unshielded context

  grace _ _ = Delimited (\_ _ -> notImplementedIO "grace")

-- | Annotations are for diagrams: the arrow is run as it is.
instance Annotate (DelimitedM m s)

-- | The part that starts the group hands its continuation to the group, and the part that ends last goes on
-- with it. The parts are in a cancel scope of their own, which the first failure cancels: the others are
-- cancelled where they wait, and their scopes are released before the group ends, which rethrows the first
-- failure. A later failure is dropped.
instance Concurrent (DelimitedM m s) where
  together = \case
    [] -> done
    [one] -> one
    parts -> Delimited $ \(Context run tag part) value ->
      control0 tag $ \resume -> do
        let size = length parts
        scope' <- newScope run
        children <- childParts run part scope' size
        joined <- newIORef (Joined Nothing 0)
        pure $
          Forked
            [ pushFrame run child Marked
                *> catch (Right <$> body (Context run tag child) value) (unwinding run child)
                >>= groupEnded run scope' size joined (resume . throwIO) (resume . pure) child
              | (Delimited body, child) <- zip parts children
            ]

-- | The first half to end is the result: the other is cancelled where it waits, and the race ends once its
-- scopes are released. If the first to end failed, its failure is rethrown then. A failure of the loser that
-- isn't its cancellation is rethrown too.
instance Race (DelimitedM m s) where
  race (Delimited one) (Delimited two) = Delimited $ \(Context run tag part) value ->
    control0 tag $ \resume -> do
      scope' <- newScope run
      first' <- childPart run part scope'
      second' <- childPart run part scope'
      winner <- newIORef Nothing
      pure $
        Forked
          [ pushFrame run first' Marked
              *> catch (Right . Left <$> one (Context run tag first') value) (unwinding run first')
              >>= raceEnded run scope' winner (resume . throwIO) (resume . pure) first',
            pushFrame run second' Marked
              *> catch (Right . Right <$> two (Context run tag second') value) (unwinding run second')
              >>= raceEnded run scope' winner (resume . throwIO) (resume . pure) second'
          ]

-- | The tickets that belong to this core: a slot, which the parts of this core read
-- and write as plain memory. A part that waits for the ticket is parked in the
-- slot, as a closure that puts its captured continuation back among those that
-- can go on, and goes back when the ticket's promise is fulfilled. Fulfilling and
-- awaiting count a step, and can give way: a ticket is where parts coordinate,
-- and none is on a parser's path.
type instance Local (DelimitedM m s) = 'Here s

type instance ModeOf (DelimitedM m s) = m

instance Exchange (DelimitedM m s) ('Here s) where
  newtype Future (DelimitedM m s) ('Here s) a = HereFuture (IORef (Slot a))
  ticket = Delimited (\_ () -> newIORef Empty >>= \slot -> pure (HereFuture slot, HerePromise slot))
  await = Delimited $ \context@(Context run tag part) (HereFuture slot) ->
    readIORef slot >>= \case
      Fulfilled value -> checkpoint part *> (value <$ step context)
      Empty -> registering part (OnTicket slot) *> control0 tag (\resume -> Parked <$ writeIORef slot (Waiter (wakeTicket run . resume)))
      Waiter _ -> ioError (userError "two parts wait for one ticket")
      Abandoned -> ioError (userError "a ticket was waited for after it was abandoned")
  fulfil = Delimited (\context (HerePromise slot, value) -> fulfilHere slot value <* step context)
  abandon = Delimited (\_ (HereFuture slot) -> abandonHere slot)

-- | The waiter of a ticket that any core can fulfil is parked in the slot as a local ticket's is, but the
-- promise, on another core, doesn't run it: it puts it in the mailbox of the
-- scheduler of the waiter's core, which goes on with it in its next round, and
-- wakes that scheduler if it sleeps in its poller.
instance Exchange (DelimitedM m s) 'Anywhere where
  newtype Future (DelimitedM m s) 'Anywhere a = AnywhereFuture (IORef (RemoteSlot a))
  ticket = Delimited $ \(Context run _ _) () -> do
    slot <- newIORef RemoteEmpty
    pure (AnywhereFuture slot, AnywherePromise (fulfilRemote (runMailbox run) slot))
  await = Delimited $ \context@(Context run tag part) (AnywhereFuture slot) ->
    awaitRemote run part slot (\value -> value <$ step context) (control0 tag)
  fulfil = Delimited $ \context (AnywherePromise promise, value) -> promise value <* step context
  abandon = Delimited $ \_ (AnywhereFuture slot) -> abandonRemote slot

-- | An inbox of this core is a shelf of plain memory, and a part that waits at it is parked there, as at a ticket, until a post
-- or a collect lets it go on. Posting and collecting count a step and can give way, as a ticket does, before they act: a
-- cancellation that the yield raises leaves them undone.
instance Inboxes (DelimitedM m s) ('Here s) where
  newtype Inbox (DelimitedM m s) name ('Here s) a = HereInbox (IORef (Shelf a))
  inbox = Delimited (\_ () -> newShelf >>= \shelf -> pure (HereInbox shelf, HereAddress shelf))
  collect = Delimited $ \context@(Context run tag part) (HereInbox shelf) -> step context *> collectFrom hereSeat run part shelf pure (control0 tag)
  post = Delimited $ \context@(Context run tag part) (HereAddress shelf, value) -> step context *> postTo hereSeat run part shelf value pure (control0 tag)
  closeInbox = Delimited (\_ (HereInbox shelf) -> closeShelf (seatChange hereSeat) shelf)

-- | The shelf of an inbox of any core is changed by compare-and-swap, and the part that lets a part that waits at it go on puts
-- it in the mailbox of its scheduler, as at a ticket of any core.
instance Inboxes (DelimitedM m s) 'Anywhere where
  newtype Inbox (DelimitedM m s) name 'Anywhere a = AnywhereInbox (IORef (Shelf a))
  inbox = Delimited (\_ () -> newShelf >>= \shelf -> pure (AnywhereInbox shelf, AnywhereAddress shelf))
  collect = Delimited $ \context@(Context run tag part) (AnywhereInbox shelf) -> step context *> collectFrom anywhereSeat run part shelf pure (control0 tag)
  post = Delimited $ \context@(Context run tag part) (AnywhereAddress shelf, value) -> step context *> postTo anywhereSeat run part shelf value pure (control0 tag)
  closeInbox = Delimited (\_ (AnywhereInbox shelf) -> closeShelf (seatChange anywhereSeat) shelf)

-- | Counts a step, and once the part has run for its turn, yields: captures what
-- follows and gives it back to the scheduler. When the part goes on, it
-- raises the cancellation that came meanwhile, if it is in a scope.
step :: Context -> IO ()
step (Context run tag part) = spent run >>= \yields -> when yields (control0 tag (\resume -> pure (Yielded (resume (checkpoint part)))))
{-# INLINE step #-}

-- | Runs the program and all of its concurrent parts on this thread: each part
-- that can go on in turn, under the prompt, until it has run for a turn and
-- reaches a point to give way at (a link's receives and sends), and when none
-- can, a wait until a descriptor wakes one, or a watch fires. The program is
-- for any core: the tickets it makes can't be fulfilled by the program of another
-- run.
runDelimited :: (forall s. Delimited s a b) -> a -> IO (b, Report)
runDelimited = runDelimitedTurn defaultTurn
-- The entry points are INLINE so that GHC specialises the program they run to the Delimited arrow. The program has type
-- @forall s. Delimited s a b@, and GHC does not specialise a call at a type that contains a lambda-bound type variable
-- such as @s@. Without inlining, the functions the program calls (the server, the transactions) take the arrow's class
-- dictionaries at run time. Inlined into its caller, the entry point instantiates @s@ and the calls are specialised.
{-# INLINE runDelimited #-}

-- | 'runDelimited' for an arrow of any mode, which the type application names.
runDelimitedM :: forall m a b. (Instrumented m) => (forall s. DelimitedM m s a b) -> a -> IO (b, Report)
runDelimitedM = runDelimitedTurnM @m defaultTurn
{-# INLINE runDelimitedM #-}

-- | 'runDelimited' with a turn of the given length.
runDelimitedTurn :: Turn -> (forall s. Delimited s a b) -> a -> IO (b, Report)
runDelimitedTurn = runDelimitedTurnM @'Release
{-# INLINE runDelimitedTurn #-}

-- | 'runDelimitedTurn' for an arrow of any mode.
runDelimitedTurnM :: forall m a b. (Instrumented m) => Turn -> (forall s. DelimitedM m s a b) -> a -> IO (b, Report)
runDelimitedTurnM turn program input = withRun turn (\run -> runIn run program input)
{-# INLINE runDelimitedTurnM #-}

-- | 'runDelimited' for a program that is made in IO, by an action that is run
-- once, for the core that runs the program.
runDelimitedWith :: (forall s. IO (Delimited s a b)) -> a -> IO (b, Report)
runDelimitedWith = runDelimitedWithTurn defaultTurn
{-# INLINE runDelimitedWith #-}

-- | 'runDelimitedWith' with a turn of the given length.
runDelimitedWithTurn :: Turn -> (forall s. IO (Delimited s a b)) -> a -> IO (b, Report)
runDelimitedWithTurn turn setup input = setup >>= \program -> withRun turn (\run -> runIn run program input)
{-# INLINE runDelimitedWithTurn #-}

runIn :: forall m s a b. (Instrumented m) => Run -> DelimitedM m s a b -> a -> IO (b, Report)
runIn run (Delimited program) input = do
  tag <- newPromptTag
  result <- newIORef Nothing
  root <- newPart run
  let rooted = Rooted run root result
  mask_ $ schedule @m run (\part -> prompt tag part >>= capturesCounted @m) result (catch (program (Context run tag root) input >>= finished rooted) (failed rooted))

-- | Counts a capture, where a build observes: each step that a part gives the scheduler, but its end, is one.
capturesCounted :: forall m. (Instrumented m) => Step -> IO Step
capturesCounted = \case
  Ended -> pure Ended
  suspended -> suspended <$ bump @m Captures

-- | The first part of a run, and where its result goes, which is all that the frame that follows the program holds, the one pointer.
data Rooted b = Rooted !Run !Part !(IORef (Maybe b))

-- | The program ended with its result.
finished :: Rooted b -> b -> IO Step
finished (Rooted _ _ result) value = Ended <$ writeIORef result (Just value)
{-# OPAQUE finished #-}

-- | The program failed: the releases that are due in the frames of the first part are run, all of them, since it has no mark,
-- and the run fails.
failed :: Rooted b -> SomeException -> IO Step
failed (Rooted run root _) failure = unwindFrames run root AtBottom failure >>= throwIO
{-# OPAQUE failed #-}

-- | Runs the program of each of the cores, each on a thread of its own, pinned
-- to a capability (@forkOn@: core @n@ to capability @n@, modulo their number,
-- so the run needs @+RTS -N@ with at least as many as it has cores to have a
-- core for each) with a scheduler of its own: its own poller, prompt and
-- mailbox. The setup makes what the cores share; each program is given it, and
-- its own core's number. What the programs have in common are what the shared
-- structure holds, and the 'Promise's they make and put in it: the tickets of one
-- core don't have the type of another's. It gives what each program gave, in
-- the order of the cores, once all have ended. If any core fails, the others
-- are stopped, even if they wait, and its exception is rethrown.
runCores :: Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [r]
runCores = runCoresTurn defaultTurn
{-# INLINE runCores #-}

-- | 'runCores' for an arrow of any mode.
runCoresM :: forall m shared r. (Instrumented m) => Int -> IO shared -> (forall s. shared -> Int -> DelimitedM m s () r) -> IO [r]
runCoresM = runCoresTurnM @m defaultTurn
{-# INLINE runCoresM #-}

-- | 'runCores' with turns of the given length.
runCoresTurn :: Turn -> Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [r]
runCoresTurn = runCoresTurnM @'Release
{-# INLINE runCoresTurn #-}

-- | 'runCoresTurn' for an arrow of any mode.
runCoresTurnM :: forall m shared r. (Instrumented m) => Turn -> Int -> IO shared -> (forall s. shared -> Int -> DelimitedM m s () r) -> IO [r]
runCoresTurnM turn cores setup program = map fst <$> runCoresReportTurnM @m turn cores setup program
{-# INLINE runCoresTurnM #-}

-- | 'runCores', with each core's report.
runCoresReport :: Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [(r, Report)]
runCoresReport = runCoresReportTurn defaultTurn
{-# INLINE runCoresReport #-}

-- | 'runCoresReport' with turns of the given length.
runCoresReportTurn :: Turn -> Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [(r, Report)]
runCoresReportTurn = runCoresReportTurnM @'Release
{-# INLINE runCoresReportTurn #-}

-- | 'runCoresReportTurn' for an arrow of any mode.
runCoresReportTurnM :: forall m shared r. (Instrumented m) => Turn -> Int -> IO shared -> (forall s. shared -> Int -> DelimitedM m s () r) -> IO [(r, Report)]
runCoresReportTurnM turn cores setup program = do
  shared <- setup
  withRuns turn cores $ \runs -> do
    outcomes <- newChan
    pending <- newIORef cores
    mask $ \restore -> do
      -- Masked, so that a core always says how it ended: a stop reaches it where its scheduler waits.
      threads <-
        traverse
          (\(core, run) -> forkOnWithUnmask core (\unmask -> try (unmask (runIn run (program shared core) ())) >>= writeChan outcomes . (,) core))
          (zip [0 ..] runs)
      let gather results
            | length results == cores = pure (map snd (sortOn fst results))
            | otherwise = do
                (core, outcome) <- readChan outcomes
                modifyIORef' pending (subtract 1)
                case outcome of
                  Left failure -> throwIO (failure :: SomeException)
                  Right result -> gather ((core :: Int, result) : results)
          -- Stops the cores that still run, and waits until each has released what its parts hold.
          stopAll = do
            mapM_ killThread threads
            readIORef pending >>= \remaining -> replicateM_ remaining (readChan outcomes)
      restore (gather []) `onException` stopAll
{-# INLINE runCoresReportTurnM #-}

-- | The runs of that many cores, all freed once the action ends: after the
-- threads that use them have.
withRuns :: Turn -> Int -> ([Run] -> IO a) -> IO a
withRuns turn cores continue
  | cores <= 0 = continue []
  | otherwise = withRun turn (\run -> withRuns turn (cores - 1) (continue . (run :)))
