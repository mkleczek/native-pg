{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | Every concurrent part on one thread, as 'Network.Directions.Loop' runs
-- them, but compiled: an arrow is a function that takes what follows it as a
-- continuation, so a program's composition is function composition, which GHC
-- inlines. A part that waits is a closure, which can't be looked into, and is
-- built only where the part waits.
--
-- A part's handlers are not on a stack that a wait captures, as they are in
-- "Network.Directions.Delimited": each scope gives what runs inside it its own
-- failure continuation, and the part says which is the one it runs in when it
-- goes on. A failure that leaves the code the scheduler runs is handed, by the
-- scheduler's catch, to the one the part set last, which runs the scope's
-- release as an arrow, and then goes on to the scope outside.
module Network.Directions.Compiled
  ( Compiled,
    runCompiled,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Exception (SomeException, mask_, throwIO, toException)
import Control.Monad (when)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Network.Directions.Cancel
import Network.Directions.Classes
import Network.Directions.Loop (Report)
import Network.Directions.Scheduler
import System.Posix.Types (Fd (..))
import Prelude hiding (id, (.))

-- | The run, the part, and what goes on with a failure of the code that runs in it.
data Context = Context !Run !Part !(SomeException -> IO Step)

-- | An arrow as a function of its context, its input, and of what follows it.
newtype Compiled a b = Compiled (Context -> a -> (b -> IO Step) -> IO Step)

instance Category Compiled where
  id = Compiled (\_ value next -> next value)
  {-# INLINE id #-}
  Compiled after . Compiled before = Compiled (\context value next -> before context value (\middle -> after context middle next))
  {-# INLINE (.) #-}

instance Arrow Compiled where
  arr function = Compiled (\_ value next -> next (function value))
  {-# INLINE arr #-}
  first (Compiled inner) = Compiled (\context (a, c) next -> inner context a (\b -> next (b, c)))
  {-# INLINE first #-}

-- | A choice goes on with the branch's continuation itself, so that it wraps
-- nothing.
instance ArrowChoice Compiled where
  left (Compiled inner) = Compiled $ \context choice next -> case choice of
    Left a -> inner context a (next . Left)
    Right c -> next (Right c)
  {-# INLINE left #-}
  Compiled one ||| Compiled two = Compiled $ \context choice next -> case choice of
    Left a -> one context a next
    Right c -> two context c next
  {-# INLINE (|||) #-}

instance Effect Compiled where
  now action = Compiled (\_ value next -> action value >>= next)
  {-# INLINE now #-}

-- | The part goes on, in its context: its failures are the context's.
resumed :: Context -> IO a -> IO a
resumed (Context run _ failure) action = writeIORef (runFailure run) failure *> action
{-# INLINE resumed #-}

-- | Counts a step, and goes on, or once the part has used its steps lets the others go on before it: it raises
-- the cancellation that came meanwhile, if it is in a scope, when it does.
steppedIn :: Context -> IO Step -> IO Step
steppedIn context@(Context run part _) continue =
  spent run >>= \yields -> if yields then pure (Yielded (resumed context (checkpoint part *> continue))) else continue
{-# INLINE steppedIn #-}

-- | Counts a step, and once the part has used its steps, lets the others go
-- on before it.
instance GiveWay Compiled where
  giveWay = Compiled (\context value next -> steppedIn context (next value))
  {-# INLINE giveWay #-}

instance Block Compiled where
  block = Compiled $ \context@(Context run part _) event next ->
    readIORef (partFrames part) >>= \case
      [] -> counted run (Suspended event 0 (\action -> resumed context (action *> next ())))
      frames ->
        cancelledIn frames >>= \case
          Just ident -> throwIO (Cancelled ident)
          Nothing -> do
            ident <- nextWait run
            writeIORef (partWait part) $ case event of
              Readable (Fd fd) -> OnFd (fromIntegral fd) True ident
              Writable (Fd fd) -> OnFd (fromIntegral fd) False ident
              Watched _ -> OnWatch ident
            counted run (Suspended event ident (\action -> resumed context (action *> next ())))
  {-# INLINE block #-}

instance Scope Compiled where
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

-- | The part that starts the pair hands what follows it to the pair, and the half that ends last goes on with it.
-- The halves are parts in a cancel scope of their own, which the first failure cancels: the other half is
-- cancelled where it waits, and its scopes are released before the pair ends, which fails with the first
-- failure. A failure of the second half is dropped.
instance Concurrent Compiled where
  concurrently (Compiled one) (Compiled two) = Compiled $ \(Context run part outer) value next -> do
    scope' <- newCancelScope
    first' <- childPart run part scope'
    second' <- childPart run part scope'
    joined <- newIORef (Joined Nothing Nothing Nothing 0)
    let failure thrown = writeIORef (runFailure run) outer *> outer thrown
        success result = writeIORef (runFailure run) outer *> next result
        ended1 = pairEnded run scope' joined failure success first' (\b (Joined _ c thrown count) -> Joined (Just b) c thrown count)
        ended2 = pairEnded run scope' joined failure success second' (\c (Joined b _ thrown count) -> Joined b (Just c) thrown count)
        failing1 = Context run first' (ended1 . Left)
        failing2 = Context run second' (ended2 . Left)
    pure $
      Forked
        (resumed failing1 (one failing1 value (ended1 . Right)))
        (resumed failing2 (two failing2 value (ended2 . Right)))

-- | The first half to end is the result: the other is cancelled where it waits, and the pair ends once its
-- scopes are released. If the first to end failed, its failure is the pair's then. A failure of the loser that
-- isn't its cancellation is too.
instance Race Compiled where
  race (Compiled one) (Compiled two) = Compiled $ \(Context run part outer) value next -> do
    scope' <- newCancelScope
    first' <- childPart run part scope'
    second' <- childPart run part scope'
    winner <- newIORef Nothing
    let failure thrown = writeIORef (runFailure run) outer *> outer thrown
        success result = writeIORef (runFailure run) outer *> next result
        ended1 = raceEnded run scope' winner failure success first'
        ended2 = raceEnded run scope' winner failure success second'
        failing1 = Context run first' (ended1 . Left)
        failing2 = Context run second' (ended2 . Left)
    pure $
      Forked
        (resumed failing1 (one failing1 value (ended1 . Right . Left)))
        (resumed failing2 (two failing2 value (ended2 . Right . Right)))

-- | A ticket is a slot on this thread: a part that waits for it is parked in
-- it, as its continuation, and goes back among those that can go on when the
-- ticket is given its value. Giving and waiting count a step, and can give
-- way: a ticket is where parts coordinate, and none is on a parser's path.
instance Exchange Compiled where
  newtype Ticket Compiled a = CompiledTicket (IORef (Slot a))
  newTicket = Compiled (\_ () next -> newIORef Empty >>= next . CompiledTicket)
  fulfil = Compiled $ \context@(Context run _ _) (CompiledTicket slot, value) next ->
    readIORef slot >>= \case
      Empty -> writeIORef slot (Given value) *> steppedIn context (next ())
      Waiter resume -> do
        writeIORef slot (Given value)
        wakeTicket run (resume (pure value))
        steppedIn context (next ())
      Given _ -> ioError (userError "a ticket was given a value twice")
  await = Compiled $ \context@(Context _ part _) (CompiledTicket slot) next ->
    readIORef slot >>= \case
      Given value -> checkpoint part *> steppedIn context (next value)
      Empty -> registering part (OnTicket slot) *> (Parked <$ writeIORef slot (Waiter (\action -> resumed context (action >>= next))))
      Waiter _ -> ioError (userError "two parts wait for one ticket")

-- | Runs the program and all of its concurrent parts on this thread: each part
-- that can go on in turn, until it has passed 64 points to give way at (a
-- link's receives and sends), and when none can, a wait until a descriptor
-- wakes one, or a watch fires.
runCompiled :: Compiled a b -> a -> IO (b, Report)
runCompiled (Compiled program) input = do
  result <- newIORef Nothing
  withRun $ \run -> do
    root <- newPart run
    let failure = throwIO . Escaped
        context = Context run root failure
    writeIORef (runFailure run) failure
    mask_ $ schedule run (guardedBy run) result (program context input (\value -> Ended <$ writeIORef result (Just value)))
