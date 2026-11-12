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

import Control.Arrow (Arrow (..), ArrowChoice (..), (>>>))
import Control.Category (Category (..))
import Network.Streaming.Classes
import Prelude hiding (id, (.))

-- | An arrow of @arr@ that is given an @r@ besides its input.
newtype With r arr a b = With (arr (r, a) b)

-- | The environment, which the arrow gives as its output.
ask :: (Arrow arr) => With r arr a r
ask = With (arr fst)
{-# INLINE ask #-}

-- | The arrow on the environment: what it gives, whatever the input.
asks :: (Arrow arr) => arr r b -> With r arr a b
asks step = With (arr fst >>> step)
{-# INLINE asks #-}

-- | An arrow of @arr@, which doesn't look at the environment.
lift :: (Arrow arr) => arr a b -> With r arr a b
lift step = With (arr snd >>> step)
{-# INLINE lift #-}

-- | The arrow of @arr@ that takes the environment before its input.
with :: With r arr a b -> arr (r, a) b
with (With step) = step
{-# INLINE with #-}

-- | The arrow of @arr@ that takes only the environment, for a program that is given no other input.
withOnly :: (Arrow arr) => With r arr () b -> arr r b
withOnly (With step) = arr (,()) >>> step
{-# INLINE withOnly #-}

instance (Arrow arr) => Category (With r arr) where
  id = With (arr snd)
  {-# INLINE id #-}
  With after . With before = With (arr (\(r, a) -> ((r, a), r)) >>> first before >>> arr (\(b, r) -> (r, b)) >>> after)
  {-# INLINE (.) #-}

instance (Arrow arr) => Arrow (With r arr) where
  arr function = With (arr (function . snd))
  {-# INLINE arr #-}
  first (With inner) = With (arr (\(r, (a, c)) -> ((r, a), c)) >>> first inner)
  {-# INLINE first #-}
  second (With inner) = With (arr (\(r, (c, a)) -> (c, (r, a))) >>> second inner)
  {-# INLINE second #-}
  With one *** With two = With (arr (\(r, (a, c)) -> ((r, a), (r, c))) >>> one *** two)
  {-# INLINE (***) #-}
  With one &&& With two = With (arr (\(r, a) -> ((r, a), (r, a))) >>> one *** two)
  {-# INLINE (&&&) #-}

instance (ArrowChoice arr) => ArrowChoice (With r arr) where
  left (With inner) = With (arr (\(r, choice) -> either (Left . (r,)) Right choice) >>> left inner)
  {-# INLINE left #-}
  right (With inner) = With (arr (\(r, choice) -> fmap (r,) choice) >>> right inner)
  {-# INLINE right #-}
  With one +++ With two = With (arr (\(r, choice) -> either (Left . (r,)) (Right . (r,)) choice) >>> one +++ two)
  {-# INLINE (+++) #-}
  With one ||| With two = With (arr (\(r, choice) -> either (Left . (r,)) (Right . (r,)) choice) >>> (one ||| two))
  {-# INLINE (|||) #-}

instance (Effect arr) => Effect (With r arr) where
  now action = With (now (action . snd))
  {-# INLINE now #-}

instance (Arrow arr, GiveWay arr) => GiveWay (With r arr) where
  giveWay = With (arr snd >>> giveWay)
  {-# INLINE giveWay #-}

instance (Arrow arr, Block arr) => Block (With r arr) where
  block = With (arr snd >>> block)
  {-# INLINE block #-}

instance (Arrow arr, Time arr) => Time (With r arr) where
  clock = With (arr (const ()) >>> clock)
  {-# INLINE clock #-}

-- | The deadline is the arrow's: the body is given the environment as it is.
instance (Within arr) => Within (With r arr) where
  within duration (With body) = With (within duration body)
  {-# INLINE within #-}

-- | What the scope acquires is acquired in the environment, and the release and the body are given both.
instance (Arrow arr, Scope arr) => Scope (With r arr) where
  scope (With acquire) (With release) (With body) =
    With $
      scope
        (arr fst &&& acquire)
        (arr (\((r, acquired), exit) -> (r, (acquired, exit))) >>> release)
        (arr (\((r, acquired), (_, a)) -> (r, (acquired, a))) >>> body)
  {-# INLINE scope #-}
  tryA (With inner) = With (tryA inner)
  {-# INLINE tryA #-}
  shield (With inner) = With (shield inner)
  {-# INLINE shield #-}

-- | Both parts are given the environment.
instance (Concurrent arr) => Concurrent (With r arr) where
  concurrently (With one) (With two) = With (concurrently one two)
  {-# INLINE concurrently #-}

instance (Race arr) => Race (With r arr) where
  race (With one) (With two) = With (race one two)
  {-# INLINE race #-}

-- | The tickets are the arrow's own, of the same reach.
type instance Local (With r arr) = Local arr

instance (Arrow arr, Exchange arr reach) => Exchange (With r arr) reach where
  newtype Waiting (With r arr) reach a = WithWaiting (Waiting arr reach a)
  ticket = With (arr (const ()) >>> ticket >>> arr (first WithWaiting))
  {-# INLINE ticket #-}
  await = With (arr (\(_, WithWaiting waiting) -> waiting) >>> await)
  {-# INLINE await #-}
  give = With (arr snd >>> give)
  {-# INLINE give #-}
  abandon = With (arr (\(_, WithWaiting waiting) -> waiting) >>> abandon)
  {-# INLINE abandon #-}
