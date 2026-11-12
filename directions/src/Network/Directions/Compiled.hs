{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | Every concurrent part on one thread, as 'Network.Directions.Loop' runs
-- them, but compiled: an arrow is a function that takes what follows it as a
-- continuation, so a program's composition is function composition, which GHC
-- inlines. A part that waits is a closure, which can't be looked into, and is
-- built only where the part waits.
module Network.Directions.Compiled
  ( Compiled,
    runCompiled,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import qualified Data.Function as Function
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Network.Directions.Classes
import Network.Directions.Loop (Report)
import Network.Directions.Scheduler
import Prelude hiding (id, (.))

-- | An arrow as a function of its input and of what follows it.
newtype Compiled a b = Compiled (Run -> a -> (b -> IO Step) -> IO Step)

instance Category Compiled where
  id = Compiled (\_ value next -> next value)
  {-# INLINE id #-}
  Compiled after . Compiled before = Compiled (\run value next -> before run value (\middle -> after run middle next))
  {-# INLINE (.) #-}

instance Arrow Compiled where
  arr function = Compiled (\_ value next -> next (function value))
  {-# INLINE arr #-}
  first (Compiled inner) = Compiled (\run (a, c) next -> inner run a (\b -> next (b, c)))
  {-# INLINE first #-}

-- | A choice goes on with the branch's continuation itself, so that it wraps
-- nothing.
instance ArrowChoice Compiled where
  left (Compiled inner) = Compiled $ \run choice next -> case choice of
    Left a -> inner run a (next . Left)
    Right c -> next (Right c)
  {-# INLINE left #-}
  Compiled one ||| Compiled two = Compiled $ \run choice next -> case choice of
    Left a -> one run a next
    Right c -> two run c next
  {-# INLINE (|||) #-}

instance Effect Compiled where
  now action = Compiled (\_ value next -> action value >>= next)
  {-# INLINE now #-}

-- | Counts a step, and once the part has used its steps, lets the others go
-- on before it.
instance GiveWay Compiled where
  giveWay = Compiled (\run value next -> stepped run (next value))
  {-# INLINE giveWay #-}

instance Block Compiled where
  block = Compiled (\run event next -> counted run (Suspended event (next ())))
  {-# INLINE block #-}

instance Concurrent Compiled where
  concurrently (Compiled one) (Compiled two) = Compiled $ \run value next -> do
    joined <- newIORef Neither
    pure $
      Forked
        (one run value (\b -> readIORef joined >>= \case SecondDone c -> next (b, c); _ -> Ended <$ writeIORef joined (FirstDone b)))
        (two run value (\c -> readIORef joined >>= \case FirstDone b -> next (b, c); _ -> Ended <$ writeIORef joined (SecondDone c)))

-- | What a concurrent pair has of its halves' results.
data Half b c = Neither | FirstDone b | SecondDone c

-- | A ticket is a slot on this thread: a part that waits for it is parked in
-- it, as its continuation, and goes back among those that can go on when the
-- ticket is given its value. Giving and waiting count a step, and can give
-- way: a ticket is where parts coordinate, and none is on a parser's path.
instance Exchange Compiled where
  newtype Ticket Compiled a = CompiledTicket (IORef (Slot a))
  newTicket = CompiledTicket <$> newIORef Empty
  fulfil (CompiledTicket slot) = Compiled $ \run value next ->
    readIORef slot >>= \case
      Empty -> writeIORef slot (Given value) *> stepped run (next ())
      Waiter resume -> do
        writeIORef slot (Given value)
        wakeTicket run (resume value)
        stepped run (next ())
      Given _ -> ioError (userError "a ticket was given a value twice")
  await (CompiledTicket slot) = Compiled $ \run () next ->
    readIORef slot >>= \case
      Given value -> stepped run (next value)
      Empty -> Parked <$ writeIORef slot (Waiter next)
      Waiter _ -> ioError (userError "two parts wait for one ticket")

-- | Runs the program and all of its concurrent parts on this thread: each part
-- that can go on in turn, until it has passed 64 points to give way at (a
-- link's receives and sends), and when none can, a wait until a descriptor
-- wakes one, or a watch fires.
runCompiled :: Compiled a b -> a -> IO (b, Report)
runCompiled (Compiled program) input = do
  run <- newRun
  result <- newIORef Nothing
  schedule run Function.id result (program run input (\value -> Ended <$ writeIORef result (Just value)))
