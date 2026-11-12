{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Every concurrent part on one thread, as 'Network.Directions.Compiled' runs
-- them, but as plain IO: an arrow is a function to IO, so a program is the
-- code GHC compiles it to, and a part that has to wait captures what follows
-- it, up to the scheduler's prompt, with GHC's delimited continuations
-- ('control0#'). Only a part that waits pays for its continuation: the stack
-- between the prompt and the wait, copied to the heap.
module Network.Directions.Delimited
  ( Delimited,
    runDelimited,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Control.Monad (when)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import GHC.Exts (PromptTag#, control0#, newPromptTag#, prompt#)
import GHC.IO (IO (..))
import Network.Directions.Classes
import Network.Directions.Loop (Report)
import Network.Directions.Scheduler
import Prelude hiding (id, (.))

-- | An arrow as a function to IO, which runs under the scheduler's prompt.
newtype Delimited a b = Delimited (Context -> a -> IO b)

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

instance Category Delimited where
  id = Delimited (const pure)
  {-# INLINE id #-}
  Delimited after . Delimited before = Delimited (\context value -> before context value >>= after context)
  {-# INLINE (.) #-}

instance Arrow Delimited where
  arr function = Delimited (\_ value -> pure (function value))
  {-# INLINE arr #-}
  first (Delimited inner) = Delimited (\context (a, c) -> (,c) <$> inner context a)
  {-# INLINE first #-}

-- | A choice runs its branch in its place, so that a loop through a branch is
-- a tail call.
instance ArrowChoice Delimited where
  left (Delimited inner) = Delimited $ \context -> \case
    Left a -> Left <$> inner context a
    Right c -> pure (Right c)
  {-# INLINE left #-}
  Delimited one ||| Delimited two = Delimited $ \context -> \case
    Left a -> one context a
    Right c -> two context c
  {-# INLINE (|||) #-}

instance Effect Delimited where
  now action = Delimited (\_ value -> action value)
  {-# INLINE now #-}

-- | Counts a step, and once the part has used its steps, lets the others go
-- on before it.
instance GiveWay Delimited where
  giveWay = Delimited (\context value -> value <$ step context)
  {-# INLINE giveWay #-}

instance Block Delimited where
  block = Delimited (\(Context run tag) event -> control0 tag (\resume -> counted run (Suspended event (resume (pure ())))))
  {-# INLINE block #-}

-- | The part that starts the pair hands its continuation to the pair, and the
-- half that ends last goes on with it.
instance Concurrent Delimited where
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

-- | A ticket is a slot on this thread: a part that waits for it is parked in
-- it, as its captured continuation, and goes back among those that can go on
-- when the ticket is given its value. Giving and waiting count a step, and can
-- give way: a ticket is where parts coordinate, and none is on a parser's
-- path.
instance Exchange Delimited where
  newtype Ticket Delimited a = DelimitedTicket (IORef (Slot a))
  newTicket = DelimitedTicket <$> newIORef Empty
  fulfil (DelimitedTicket slot) = Delimited $ \context@(Context run _) value ->
    readIORef slot >>= \case
      Empty -> writeIORef slot (Given value) *> step context
      Waiter resume -> writeIORef slot (Given value) *> wakeTicket run (resume value) *> step context
      Given _ -> ioError (userError "a ticket was given a value twice")
  await (DelimitedTicket slot) = Delimited $ \context@(Context _ tag) () ->
    readIORef slot >>= \case
      Given value -> value <$ step context
      Empty -> control0 tag (\resume -> Parked <$ writeIORef slot (Waiter (resume . pure)))
      Waiter _ -> ioError (userError "two parts wait for one ticket")

-- | Counts a step, and once the part has used its steps, yields: captures what
-- follows and gives it back to the scheduler.
step :: Context -> IO ()
step (Context run tag) = spent run >>= \yields -> when yields (control0 tag (\resume -> pure (Yielded (resume (pure ())))))
{-# INLINE step #-}

-- | Runs the program and all of its concurrent parts on this thread: each part
-- that can go on in turn, under the prompt, until it has passed 64 points to
-- give way at (a link's receives and sends), and when none can, a wait until a
-- descriptor wakes one, or a watch fires.
runDelimited :: Delimited a b -> a -> IO (b, Report)
runDelimited (Delimited program) input = do
  run <- newRun
  tag <- newPromptTag
  result <- newIORef Nothing
  schedule run (prompt tag) result (program (Context run tag) input >>= \value -> Ended <$ writeIORef result (Just value))
