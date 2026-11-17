{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | An arrow with one run-time value in its environment, so that the program's text doesn't thread it: a link, a
-- reader, a cursor, a connection. @'With' r arr a b@ is an arrow of @arr@ that is given an @r@ besides its input,
-- and takes it out of the environment where the program 'ask's for it. It is the arrow of a reader monad: what a
-- program in it needs from the arrow it runs in, it has the same capabilities for ('Effect', 'Block', 'Scope', and
-- the others), by lifting the arrow's own, which don't look at the environment; @'with'@ makes it an arrow of @arr@
-- that takes the environment as the first part of its input.
--
-- There is no 'ArrowLoop' and no 'ArrowApply' for it, as there are for none of the arrows of the runtime: a
-- program is built once, and its run-time values are its inputs. Every method is INLINE, so that for an arrow that
-- is known where the program is built, the environment is a part of the input that no step has to rebuild.
module Network.Streaming.With
  ( With (..),
    ask,
    asks,
    lift,
    with,
    withOnly,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..), (>>>), (>>^), (^>>))
import Control.Category (Category (..))
import Data.Tuple (swap)
import Network.Streaming.Annotate (Annotate (exchange, flowCase, flowScope, flowStep))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done)
import Network.Streaming.Concurrent (Concurrent (Group, group, spawn))
import Network.Streaming.Mode (ModeOf)
import Prelude hiding (id, (.))

-- | An arrow of @arr@ that is given an @r@ besides its input.
newtype With r arr a b = With (arr (r, a) b)

-- | The environment, which the arrow gives as its output.
ask :: (Arrow arr) => With r arr a r
ask = With (arr fst)
{-# INLINE ask #-}

-- | The arrow on the environment: what it gives, whatever the input.
asks :: (Arrow arr) => arr r b -> With r arr a b
asks step = With (fst ^>> step)
{-# INLINE asks #-}

-- | An arrow of @arr@, which doesn't look at the environment.
lift :: (Arrow arr) => arr a b -> With r arr a b
lift step = With (snd ^>> step)
{-# INLINE lift #-}

-- | The arrow of @arr@ that takes the environment before its input.
with :: With r arr a b -> arr (r, a) b
with (With step) = step
{-# INLINE with #-}

-- | The arrow of @arr@ that takes only the environment, for a program that is given no other input.
withOnly :: (Arrow arr) => With r arr () b -> arr r b
withOnly (With step) = (,()) ^>> step
{-# INLINE withOnly #-}

instance (Arrow arr) => Category (With r arr) where
  id = With (arr snd)
  {-# INLINE id #-}
  With after . With before = With ((\(r, a) -> ((r, a), r)) ^>> first before >>> swap ^>> after)
  {-# INLINE (.) #-}

instance (Arrow arr) => Arrow (With r arr) where
  arr function = With (arr (function . snd))
  {-# INLINE arr #-}
  first (With inner) = With ((\(r, (a, c)) -> ((r, a), c)) ^>> first inner)
  {-# INLINE first #-}
  second (With inner) = With ((\(r, (c, a)) -> (c, (r, a))) ^>> second inner)
  {-# INLINE second #-}
  With one *** With two = With ((\(r, (a, c)) -> ((r, a), (r, c))) ^>> one *** two)
  {-# INLINE (***) #-}
  With one &&& With two = With ((\(r, a) -> ((r, a), (r, a))) ^>> one *** two)
  {-# INLINE (&&&) #-}

instance (ArrowChoice arr) => ArrowChoice (With r arr) where
  left (With inner) = With ((\(r, choice) -> either (Left . (r,)) Right choice) ^>> left inner)
  {-# INLINE left #-}
  right (With inner) = With ((\(r, choice) -> fmap (r,) choice) ^>> right inner)
  {-# INLINE right #-}
  With one +++ With two = With ((\(r, choice) -> either (Left . (r,)) (Right . (r,)) choice) ^>> one +++ two)
  {-# INLINE (+++) #-}
  With one ||| With two = With ((\(r, choice) -> either (Left . (r,)) (Right . (r,)) choice) ^>> (one ||| two))
  {-# INLINE (|||) #-}

instance (Effect arr) => Effect (With r arr) where
  now action = With (now (action . snd))
  {-# INLINE now #-}

instance (Functor (arr (r, a))) => Functor (With r arr a) where
  fmap function (With inner) = With (fmap function inner)
  {-# INLINE fmap #-}

instance (Applicative (arr (r, a))) => Applicative (With r arr a) where
  pure result = With (pure result)
  {-# INLINE pure #-}
  With function <*> With argument = With (function <*> argument)
  {-# INLINE (<*>) #-}

instance (ArrowApplicative arr) => ArrowApplicative (With r arr)

instance (Arrow arr, GiveWay arr) => GiveWay (With r arr) where
  giveWay = With (snd ^>> giveWay)
  {-# INLINE giveWay #-}

-- | The lane is the arrow's: the body is given the environment as it is.
instance (FastLane arr) => FastLane (With r arr) where
  fastLane (With inner) = With (fastLane inner)
  {-# INLINE fastLane #-}

instance (Arrow arr, Block arr) => Block (With r arr) where
  block = With (snd ^>> block)
  {-# INLINE block #-}

instance (Arrow arr, Time arr) => Time (With r arr) where
  clock = With (done >>> clock)
  {-# INLINE clock #-}

-- | The deadline is the arrow's: the body is given the environment as it is.
instance (Arrow arr, Within arr) => Within (With r arr) where
  within duration (With body) = With (within duration body)
  {-# INLINE within #-}
  timeoutA (With body) = With ((\(r, (duration, a)) -> (duration, (r, a))) ^>> timeoutA body)
  {-# INLINE timeoutA #-}

-- | What the scope acquires is acquired in the environment, and the release and the body are given both.
instance (Arrow arr, Scope arr) => Scope (With r arr) where
  scope (With acquire) (With release) (With body) =
    With $
      scope
        (arr fst &&& acquire)
        ((\((r, acquired), exit) -> (r, (acquired, exit))) ^>> release)
        ((\((r, acquired), (_, a)) -> (r, (acquired, a))) ^>> body)
  {-# INLINE scope #-}
  tryA (With inner) = With (tryA inner)
  {-# INLINE tryA #-}
  shield (With inner) = With (shield inner)
  {-# INLINE shield #-}
  grace duration (With inner) = With (grace duration inner)
  {-# INLINE grace #-}

-- | Every part is given the environment.
instance (Arrow arr, Concurrent arr) => Concurrent (With r arr) where
  newtype Group (With r arr) s j = WithGroup (Group arr s j)
  together parts = With (together [part | With part <- parts])
  {-# INLINE together #-}
  group joiner body = With (group joiner ((\(handle, (r, a)) -> (r, (WithGroup handle, a))) ^>> with body))
  {-# INLINE group #-}
  spawn (With part) = With ((\(r, (WithGroup handle, x)) -> (handle, (r, x))) ^>> spawn part)
  {-# INLINE spawn #-}

instance (Race arr) => Race (With r arr) where
  race (With one) (With two) = With (race one two)
  {-# INLINE race #-}

-- | The box, the names of the wires, the ends of exchanges, the flows and their steps and the allowed checks are the arrow's:
-- the body is given the environment as it is.
instance (Annotate arr) => Annotate (With r arr) where
  named label (With inner) = With (named label inner)
  {-# INLINE named #-}
  wires input output (With inner) = With (wires input output inner)
  {-# INLINE wires #-}
  exchange side name (With inner) = With (exchange side name inner)
  {-# INLINE exchange #-}
  flowStep names bindings (With inner) = With (flowStep names bindings inner)
  {-# INLINE flowStep #-}
  flowCase names (With inner) = With (flowCase names inner)
  {-# INLINE flowCase #-}
  flowScope name (With inner) = With (flowScope name inner)
  {-# INLINE flowScope #-}
  allowing check reason (With inner) = With (allowing check reason inner)
  {-# INLINE allowing #-}

-- | The tickets are the arrow's own, of the same reach.
type instance Local (With r arr) = Local arr

-- | An arrow with an environment is built in the mode of its inner arrow.
type instance ModeOf (With r arr) = ModeOf arr

-- | The future of the inner arrow's ticket, from the input that has it beside the environment.
innerFuture :: (r, Future (With r arr) reach a) -> Future arr reach a
innerFuture (_, WithFuture future) = future
{-# INLINE innerFuture #-}

instance (Arrow arr, Exchange arr reach) => Exchange (With r arr) reach where
  newtype Future (With r arr) reach a = WithFuture (Future arr reach a)
  ticket = With (done >>> ticket >>^ first WithFuture)
  {-# INLINE ticket #-}
  await = With (innerFuture ^>> await)
  {-# INLINE await #-}
  fulfil = With (snd ^>> fulfil)
  {-# INLINE fulfil #-}
  abandon = With (innerFuture ^>> abandon)
  {-# INLINE abandon #-}
  awaitRegistering = With ((\(_, (WithFuture future, register)) -> (future, register)) ^>> awaitRegistering)
  {-# INLINE awaitRegistering #-}
  willAwait = With (innerFuture ^>> willAwait >>^ WithFuture)
  {-# INLINE willAwait #-}
  handWaiting = With (snd ^>> handWaiting)
  {-# INLINE handWaiting #-}
  untilParked = With (snd ^>> untilParked)
  {-# INLINE untilParked #-}

-- | The inbox of the inner arrow, from the input that has it beside the environment.
innerInbox :: (r, Inbox (With r arr) name reach a) -> Inbox arr name reach a
innerInbox (_, WithInbox box) = box
{-# INLINE innerInbox #-}

instance (Arrow arr, Inboxes arr reach) => Inboxes (With r arr) reach where
  newtype Inbox (With r arr) name reach a = WithInbox (Inbox arr name reach a)
  inbox = With (done >>> inbox >>^ first WithInbox)
  {-# INLINE inbox #-}
  collect = With (innerInbox ^>> collect)
  {-# INLINE collect #-}
  post = With (snd ^>> post)
  {-# INLINE post #-}
  closeInbox = With (innerInbox ^>> closeInbox)
  {-# INLINE closeInbox #-}
