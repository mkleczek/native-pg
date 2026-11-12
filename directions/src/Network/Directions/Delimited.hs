{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RoleAnnotations #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Every concurrent part on one thread, as 'Network.Directions.Compiled' runs
-- them, but as plain IO: an arrow is a function to IO, so a program is the
-- code GHC compiles it to, and a part that has to wait captures what follows
-- it, up to the scheduler's prompt, with GHC's delimited continuations
-- ('control0#'). Only a part that waits pays for its continuation: the stack
-- between the prompt and the wait, copied to the heap.
--
-- The scheduler's core is in the arrow's type, as the state thread is in
-- @ST@'s: a program is @'Delimited' s a b@ for any @s@ that its run chooses, and
-- what it makes with 'newTicket' or 'newRemote' belongs to that @s@, so that
-- it can't leave the run it was made in, or be used by the program of another
-- core. What crosses cores is a 'Giver', which has no @s@.
module Network.Directions.Delimited
  ( Delimited,
    runDelimited,
    runDelimitedWith,
    runCores,
    runCoresReport,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Concurrent (forkOn, killThread)
import Control.Concurrent.Chan (newChan, readChan, writeChan)
import Control.Exception (SomeException, finally, mask, throwIO, try)
import Control.Monad (when)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Kind (Type)
import Data.List (sortOn)
import GHC.Exts (PromptTag#, control0#, newPromptTag#, prompt#)
import GHC.IO (IO (..))
import Network.Directions.Atomic (update)
import Network.Directions.Classes
import Network.Directions.Loop (Report)
import Network.Directions.Scheduler
import Prelude hiding (id, (.))

-- | An arrow as a function to IO, which runs under the scheduler's prompt, on
-- the core that @s@ stands for. Nominal in @s@, so that it can't be coerced
-- to another's.
newtype Delimited (s :: Type) a b = Delimited (Context -> a -> IO b)

type role Delimited nominal representational representational

-- | The run, and the prompt that a part's continuation is captured up to.
data Context = Context !Run !(PromptTag Step)

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

-- | Counts a step, and once the part has used its steps, lets the others go
-- on before it.
instance GiveWay (Delimited s) where
  giveWay = Delimited (\context value -> value <$ step context)
  {-# INLINE giveWay #-}

instance Block (Delimited s) where
  block = Delimited (\(Context run tag) event -> control0 tag (\resume -> counted run (Suspended event (resume (pure ())))))
  {-# INLINE block #-}

-- | The part that starts the pair hands its continuation to the pair, and the
-- half that ends last goes on with it.
instance Concurrent (Delimited s) where
  concurrently (Delimited one) (Delimited two) = Delimited $ \context@(Context _ tag) value ->
    control0 tag $ \resume -> do
      joined <- newIORef Neither
      let finish b c = resume (pure (b, c))
      pure $
        Forked
          (one context value >>= \b -> readIORef joined >>= \case SecondDone c -> finish b c; _ -> Ended <$ writeIORef joined (FirstDone b))
          (two context value >>= \c -> readIORef joined >>= \case FirstDone b -> finish b c; _ -> Ended <$ writeIORef joined (SecondDone c))

-- | What a concurrent pair has of its halves' results.
data Half b c = Neither | FirstDone b | SecondDone c

-- | A ticket is a slot on this core: a part that waits for it is parked in it,
-- as its captured continuation, and goes back among those that can go on when
-- the ticket is given its value. Giving and waiting count a step, and can give
-- way: a ticket is where parts coordinate, and none is on a parser's path.
instance Exchange (Delimited s) where
  newtype Ticket (Delimited s) a = DelimitedTicket (IORef (Slot a))
  newTicket = Delimited (\_ () -> DelimitedTicket <$> newIORef Empty)
  fulfil = Delimited $ \context@(Context run _) (DelimitedTicket slot, value) ->
    readIORef slot >>= \case
      Empty -> writeIORef slot (Given value) *> step context
      Waiter resume -> writeIORef slot (Given value) *> wakeTicket run (resume value) *> step context
      Given _ -> ioError (userError "a ticket was given a value twice")
  await = Delimited $ \context@(Context _ tag) (DelimitedTicket slot) ->
    readIORef slot >>= \case
      Given value -> value <$ step context
      Empty -> control0 tag (\resume -> Parked <$ writeIORef slot (Waiter (resume . pure)))
      Waiter _ -> ioError (userError "two parts wait for one ticket")

-- | What a remote ticket's slot holds, and what a core that gives it a value
-- finds: nothing yet, the value, the part that waits for it, or that the waiter
-- has gone. Several cores read and change it, atomically.
data RemoteSlot a = RemoteEmpty | RemoteGiven a | RemoteWaiter (a -> IO Step) | RemoteAbandoned

-- | What a give did to the slot.
data Giving a = Stored | Resume (a -> IO Step) | Gone | Twice

-- | What a waiter's parking did to the slot.
data Parking a = Parking | Ready a | Misused

-- | A remote ticket's waiter is parked in the slot as a ticket's is, but the
-- giver, on another core, doesn't run it: it puts it in the mailbox of the
-- scheduler of the waiter's core, which goes on with it in its next round, and
-- wakes that scheduler if it sleeps in its poller.
instance Remote (Delimited s) where
  newtype Awaiting (Delimited s) a = DelimitedAwaiting (IORef (RemoteSlot a))
  newRemote = Delimited $ \(Context run _) () -> do
    slot <- newIORef RemoteEmpty
    pure (DelimitedAwaiting slot, Giver (giveRemote (runMailbox run) slot))
  awaitRemote = Delimited $ \context@(Context _ tag) (DelimitedAwaiting slot) ->
    readIORef slot >>= \case
      RemoteGiven value -> value <$ step context
      RemoteEmpty ->
        control0 tag $ \resume -> do
          parking <- update slot $ \case
            RemoteEmpty -> (RemoteWaiter (resume . pure), Parking)
            RemoteGiven value -> (RemoteGiven value, Ready value)
            other -> (other, Misused)
          case parking of
            Parking -> pure ParkedRemote
            -- Given since the look: goes on in its turn.
            Ready value -> pure (Yielded (resume (pure value)))
            Misused -> ioError (userError "a remote ticket was waited for twice, or after it was abandoned")
      _ -> ioError (userError "a remote ticket was waited for twice, or after it was abandoned")
  give = Delimited $ \context (Giver giving, value) -> giving value <* step context
  abandon = Delimited $ \_ (DelimitedAwaiting slot) ->
    update slot $ \case
      RemoteGiven value -> (RemoteAbandoned, Just value)
      _ -> (RemoteAbandoned, Nothing)

-- | Gives the slot its value, and if a part waits for it, puts the part in the
-- mailbox of the scheduler that owns it. It says whether the waiter takes it.
giveRemote :: Mailbox -> IORef (RemoteSlot a) -> a -> IO Bool
giveRemote mailbox slot value = do
  giving <- update slot $ \case
    RemoteEmpty -> (RemoteGiven value, Stored)
    RemoteWaiter resume -> (RemoteGiven value, Resume resume)
    RemoteAbandoned -> (RemoteAbandoned, Gone)
    given -> (given, Twice)
  case giving of
    Stored -> pure True
    Resume resume -> post mailbox (resume value)
    Gone -> pure False
    Twice -> ioError (userError "a remote ticket was given a value twice")

-- | Counts a step, and once the part has used its steps, yields: captures what
-- follows and gives it back to the scheduler.
step :: Context -> IO ()
step (Context run tag) = spent run >>= \yields -> when yields (control0 tag (\resume -> pure (Yielded (resume (pure ())))))
{-# INLINE step #-}

-- | Runs the program and all of its concurrent parts on this thread: each part
-- that can go on in turn, under the prompt, until it has passed 64 points to
-- give way at (a link's receives and sends), and when none can, a wait until a
-- descriptor wakes one, or a watch fires. The program is for any core: the
-- tickets it makes can't be given to the program of another run.
runDelimited :: (forall s. Delimited s a b) -> a -> IO (b, Report)
runDelimited program input = withRun (\run -> runIn run program input)

-- | 'runDelimited' for a program that is made in IO, by an action that is run
-- once, for the core that runs the program.
runDelimitedWith :: (forall s. IO (Delimited s a b)) -> a -> IO (b, Report)
runDelimitedWith setup input = setup >>= \program -> withRun (\run -> runIn run program input)

runIn :: Run -> Delimited s a b -> a -> IO (b, Report)
runIn run (Delimited program) input = do
  tag <- newPromptTag
  result <- newIORef Nothing
  schedule run (prompt tag) result (program (Context run tag) input >>= \value -> Ended <$ writeIORef result (Just value))

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
runCores cores setup program = map fst <$> runCoresReport cores setup program

-- | 'runCores', with each core's report.
runCoresReport :: Int -> IO shared -> (forall s. shared -> Int -> Delimited s () r) -> IO [(r, Report)]
runCoresReport cores setup program = do
  shared <- setup
  withRuns cores $ \runs -> do
    outcomes <- newChan
    mask $ \restore -> do
      threads <-
        traverse
          (\(core, run) -> forkOn core (restore (try (runIn run (program shared core) ())) >>= writeChan outcomes . (,) core))
          (zip [0 ..] runs)
      let collect results remaining
            | remaining == (0 :: Int) = pure (map snd (sortOn fst results))
            | otherwise =
                readChan outcomes >>= \case
                  (_, Left failure) -> throwIO (failure :: SomeException)
                  (core, Right result) -> collect ((core :: Int, result) : results) (remaining - 1)
      restore (collect [] cores) `finally` mapM_ killThread threads

-- | The runs of that many cores, all freed once the action ends: after the
-- threads that use them have.
withRuns :: Int -> ([Run] -> IO a) -> IO a
withRuns cores continue
  | cores <= 0 = continue []
  | otherwise = withRun (\run -> withRuns (cores - 1) (continue . (run :)))
