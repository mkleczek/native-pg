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
-- core. What crosses cores is a 'Giver' of reach @'Anywhere@, which has no @s@.
--
-- A scope is plain IO code in the part: the continuation that a wait captures
-- holds the part's handlers, so a release runs once, after the resume, however
-- the part ended. A part is cancelled by resuming it, where it waits, with the
-- cancellation to raise there. The scheduler runs and resumes every part
-- masked, so that the part sees one masking state everywhere, as if it never
-- suspended.
module Network.Streaming.Delimited
  ( Delimited,
    runDelimited,
    runDelimitedTurn,
    runDelimitedWith,
    runDelimitedWithTurn,
    runCores,
    runCoresTurn,
    runCoresReport,
    runCoresReportTurn,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Concurrent (forkOnWithUnmask, killThread)
import Control.Concurrent.Chan (newChan, readChan, writeChan)
import Control.Exception (SomeException, mask, mask_, onException, throwIO, try)
import Control.Monad (replicateM_, when)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Kind (Type)
import Data.List (sortOn)
import GHC.Exts (PromptTag#, control0#, newPromptTag#, prompt#)
import GHC.IO (IO (..))
import Network.Streaming.Classes
import Network.Streaming.Internal.Cancel
import Network.Streaming.Internal.Scheduler
import Network.Streaming.Internal.Tickets
import Network.Streaming.Internal.Timers (addTimer, cancelTimer)
import Network.Streaming.Loop (Report)
import System.Posix.Types (Fd (..))
import Prelude hiding (id, (.))

-- | An arrow as a function to IO, which runs under the scheduler's prompt, on
-- the core that @s@ stands for. Nominal in @s@, so that it can't be coerced
-- to another's.
newtype Delimited (s :: Type) a b = Delimited (Context -> a -> IO b)

type role Delimited nominal representational representational

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

instance Category (Delimited s) where
  id = Delimited (const pure)
  {-# INLINE id #-}
  Delimited after . Delimited before = Delimited (\context value -> before context value >>= after context)
  {-# INLINE (.) #-}

instance Arrow (Delimited s) where
  arr function = Delimited (\_ value -> pure (function value))
  {-# INLINE arr #-}
  first (Delimited inner) = Delimited (\context (a, c) -> (,c) <$> inner context a)
  {-# INLINE first #-}

-- | A choice runs its branch in its place, so that a loop through a branch is
-- a tail call.
instance ArrowChoice (Delimited s) where
  left (Delimited inner) = Delimited $ \context -> \case
    Left a -> Left <$> inner context a
    Right c -> pure (Right c)
  {-# INLINE left #-}
  Delimited one ||| Delimited two = Delimited $ \context -> \case
    Left a -> one context a
    Right c -> two context c
  {-# INLINE (|||) #-}

instance Effect (Delimited s) where
  now action = Delimited (\_ value -> action value)
  {-# INLINE now #-}

-- | Counts a step, and once the part has run for its turn, lets the others go
-- on before it.
instance GiveWay (Delimited s) where
  giveWay = Delimited (\context value -> value <$ step context)
  {-# INLINE giveWay #-}

instance Block (Delimited s) where
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
instance Time (Delimited s) where
  clock = Delimited (\(Context run _ _) () -> Instant <$> roundNow run)

-- | The deadline is a timer of the scheduler's wheel, which cancels the scope when it fires, and a cancel scope
-- that the part is in while the body runs. The scope is the body's own: the cancellation that ends it is
-- recognised by it, and another passes through.
instance Within (Delimited s) where
  within (Duration limit) (Delimited body) = Delimited $ \context@(Context run _ part) value -> do
    scope' <- newScope run
    started <- roundNow run
    timer <- addTimer (runWheel run) (started + limit) (cancelScope run scope')
    pushFrame run part (Inside scope')
    outcome <- try (body context value)
    popFrame run part
    _ <- cancelTimer (runWheel run) timer
    case outcome of
      Right result -> pure (Just result)
      Left failure
        | cancelledBy failure == Just (scopeIdent scope') -> pure Nothing
        | otherwise -> throwIO failure

-- | Runs the action behind a shield.
shielded :: Context -> IO a -> IO a
shielded (Context run _ part) action = do
  pushFrame run part Shielded
  outcome <- try action
  popFrame run part
  either (\failure -> throwIO (failure :: SomeException)) pure outcome

instance Scope (Delimited s) where
  scope (Delimited acquire) (Delimited release) (Delimited body) = Delimited $ \context@(Context run _ part) value -> do
    acquired <- acquire context value
    -- A part that is in no scope is in the root scope while it holds what it acquired, so that a shutdown
    -- reaches it, and its release runs.
    held <- readIORef (partFrames part) >>= \case [] -> True <$ pushFrame run part Held; _ -> pure False
    outcome <- try (body context (acquired, value))
    let exit = exitOf outcome
    -- The release runs behind a shield.
    pushFrame run part Shielded
    released <- try (release context (acquired, exit))
    popFrame run part
    when held (popFrame run part)
    case released of
      Left failure -> throwIO (ReleaseFailed exit failure)
      Right () -> either (\failure -> throwIO (failure :: SomeException)) pure outcome

  tryA (Delimited inner) = Delimited $ \context value ->
    try (inner context value) >>= \case
      Left failure | Just _ <- cancelledBy failure -> throwIO failure
      other -> pure other

  shield (Delimited inner) = Delimited (\context value -> shielded context (inner context value))

-- | The part that starts the group hands its continuation to the group, and the part that ends last goes on
-- with it. The parts are in a cancel scope of their own, which the first failure cancels: the others are
-- cancelled where they wait, and their scopes are released before the group ends, which rethrows the first
-- failure. A later failure is dropped.
instance Concurrent (Delimited s) where
  together = \case
    [] -> arr (const ())
    [one] -> one
    parts -> Delimited $ \(Context run tag part) value ->
      control0 tag $ \resume -> do
        let size = length parts
        scope' <- newScope run
        children <- childParts run part scope' size
        joined <- newIORef (Joined Nothing 0)
        pure $
          Forked
            [ try (body (Context run tag child) value) >>= groupEnded run scope' size joined (resume . throwIO) (resume . pure) child
              | (Delimited body, child) <- zip parts children
            ]

-- | The first half to end is the result: the other is cancelled where it waits, and the race ends once its
-- scopes are released. If the first to end failed, its failure is rethrown then. A failure of the loser that
-- isn't its cancellation is rethrown too.
instance Race (Delimited s) where
  race (Delimited one) (Delimited two) = Delimited $ \(Context run tag part) value ->
    control0 tag $ \resume -> do
      scope' <- newScope run
      first' <- childPart run part scope'
      second' <- childPart run part scope'
      winner <- newIORef Nothing
      pure $
        Forked
          [ try (Left <$> one (Context run tag first') value) >>= raceEnded run scope' winner (resume . throwIO) (resume . pure) first',
            try (Right <$> two (Context run tag second') value) >>= raceEnded run scope' winner (resume . throwIO) (resume . pure) second'
          ]

-- | The tickets that belong to this core: a slot, which the parts of this core read
-- and write as plain memory. A part that waits for the ticket is parked in the
-- slot, as a closure that puts its captured continuation back among those that
-- can go on, and goes back when the ticket is given its value. Giving and
-- waiting count a step, and can give way: a ticket is where parts coordinate,
-- and none is on a parser's path.
type instance Local (Delimited s) = 'Here s

instance Exchange (Delimited s) ('Here s) where
  newtype Waiting (Delimited s) ('Here s) a = HereWaiting (IORef (Slot a))
  ticket = Delimited (\_ () -> newIORef Empty >>= \slot -> pure (HereWaiting slot, HereGiver slot))
  await = Delimited $ \context@(Context run tag part) (HereWaiting slot) ->
    readIORef slot >>= \case
      Given value -> checkpoint part *> (value <$ step context)
      Empty -> registering part (OnTicket slot) *> control0 tag (\resume -> Parked <$ writeIORef slot (Waiter (wakeTicket run . resume)))
      Waiter _ -> ioError (userError "two parts wait for one ticket")
      Abandoned -> ioError (userError "a ticket was waited for after it was abandoned")
  give = Delimited (\context (HereGiver slot, value) -> giveHere slot value <* step context)
  abandon = Delimited (\_ (HereWaiting slot) -> abandonHere slot)

-- | The waiter of a ticket that any core can give is parked in the slot as a local ticket's is, but the
-- giver, on another core, doesn't run it: it puts it in the mailbox of the
-- scheduler of the waiter's core, which goes on with it in its next round, and
-- wakes that scheduler if it sleeps in its poller.
instance Exchange (Delimited s) 'Anywhere where
  newtype Waiting (Delimited s) 'Anywhere a = AnywhereWaiting (IORef (RemoteSlot a))
  ticket = Delimited $ \(Context run _ _) () -> do
    slot <- newIORef RemoteEmpty
    pure (AnywhereWaiting slot, AnywhereGiver (giveRemote (runMailbox run) slot))
  await = Delimited $ \context@(Context run tag part) (AnywhereWaiting slot) ->
    awaitRemote run part slot (\value -> value <$ step context) (control0 tag)
  give = Delimited $ \context (AnywhereGiver giving, value) -> giving value <* step context
  abandon = Delimited $ \_ (AnywhereWaiting slot) -> abandonRemote slot

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
-- for any core: the tickets it makes can't be given to the program of another
-- run.
runDelimited :: (forall s. Delimited s a b) -> a -> IO (b, Report)
runDelimited = runDelimitedTurn defaultTurn

-- | 'runDelimited' with a turn of the given length.
runDelimitedTurn :: Turn -> (forall s. Delimited s a b) -> a -> IO (b, Report)
runDelimitedTurn turn program input = withRun turn (\run -> runIn run program input)

-- | 'runDelimited' for a program that is made in IO, by an action that is run
-- once, for the core that runs the program.
runDelimitedWith :: (forall s. IO (Delimited s a b)) -> a -> IO (b, Report)
runDelimitedWith = runDelimitedWithTurn defaultTurn

-- | 'runDelimitedWith' with a turn of the given length.
runDelimitedWithTurn :: Turn -> (forall s. IO (Delimited s a b)) -> a -> IO (b, Report)
runDelimitedWithTurn turn setup input = setup >>= \program -> withRun turn (\run -> runIn run program input)

runIn :: Run -> Delimited s a b -> a -> IO (b, Report)
runIn run (Delimited program) input = do
  tag <- newPromptTag
  result <- newIORef Nothing
  root <- newPart run
  mask_ $ schedule run (prompt tag) result (program (Context run tag root) input >>= \value -> Ended <$ writeIORef result (Just value))

-- | Runs the program of each of the cores, each on a thread of its own, pinned
-- to a capability (@forkOn@: core @n@ to capability @n@, modulo their number,
-- so the run needs @+RTS -N@ with at least as many as it has cores to have a
-- core for each) with a scheduler of its own: its own poller, prompt and
-- mailbox. The setup makes what the cores share; each program is given it, and
-- its own core's number. What the programs have in common are what the shared
-- structure holds, and the 'Giver's they make and put in it: the tickets of one
-- core don't have the type of another's. It gives what each program gave, in
-- the order of the cores, once all have ended. If any core fails, the others
-- are stopped, even if they wait, and its exception is rethrown.
runCores :: Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [r]
runCores = runCoresTurn defaultTurn

-- | 'runCores' with turns of the given length.
runCoresTurn :: Turn -> Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [r]
runCoresTurn turn cores setup program = map fst <$> runCoresReportTurn turn cores setup program

-- | 'runCores', with each core's report.
runCoresReport :: Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [(r, Report)]
runCoresReport = runCoresReportTurn defaultTurn

-- | 'runCoresReport' with turns of the given length.
runCoresReportTurn :: Turn -> Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [(r, Report)]
runCoresReportTurn turn cores setup program = do
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
      let collect results
            | length results == cores = pure (map snd (sortOn fst results))
            | otherwise = do
                (core, outcome) <- readChan outcomes
                modifyIORef' pending (subtract 1)
                case outcome of
                  Left failure -> throwIO (failure :: SomeException)
                  Right result -> collect ((core :: Int, result) : results)
          -- Stops the cores that still run, and waits until each has released what its parts hold.
          stopAll = do
            mapM_ killThread threads
            readIORef pending >>= \remaining -> replicateM_ remaining (readChan outcomes)
      restore (collect []) `onException` stopAll

-- | The runs of that many cores, all freed once the action ends: after the
-- threads that use them have.
withRuns :: Turn -> Int -> ([Run] -> IO a) -> IO a
withRuns turn cores continue
  | cores <= 0 = continue []
  | otherwise = withRun turn (\run -> withRuns turn (cores - 1) (continue . (run :)))
