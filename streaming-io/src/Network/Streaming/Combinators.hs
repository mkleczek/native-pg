-- | Helpers for writing programs over the classes, so that a value is made where it is used and not carried
-- through the arrows in between: loops with a state, a retry, and a request that carries the end its answer is
-- waited at. Each is written once for any arrow that has what it needs. All are INLINE, not INLINABLE: in an
-- arrow of continuations, such as "Network.Streaming.Compiled", one that isn't inlined is passed its step and
-- continuation as unknown functions, and allocates for them on every turn.
module Network.Streaming.Combinators
  ( timesWith,
    times,
    concurrently,
    untilJust,
    keeping,
    expect,
    call,
    sleep,
    releaseWithin,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, (&&&), (***), (>>>), (>>^), (^>>), (|||))
import Network.Streaming.Classes

-- | The step this many times, each on what the one before gave.
timesWith :: (ArrowChoice arr) => Int -> arr s s -> arr s s
timesWith count step = (,count) ^>> loop
  where
    loop = (\(s, n) -> if n <= 0 then Left s else Right (s, n - 1)) ^>> (returnA ||| (first step >>> loop))
{-# INLINE timesWith #-}

-- | Runs the arrow this many times.
times :: (ArrowChoice arr) => Int -> arr () () -> arr () ()
times = timesWith
{-# INLINE times #-}

-- | Two arrows at once on the same input: both outputs, once both have ended. It is a group of two parts, each of which
-- gives its result to a ticket of the arrow's own reach that was made before the group began; when the group has
-- ended, the results are taken from the tickets, which have them already, so that taking them doesn't wait. The first
-- failure cancels the other half where it waits, and is rethrown once it has ended.
concurrently :: forall arr a b c. (Arrow arr, Concurrent arr, Exchange arr (Local arr)) => arr a b -> arr a c -> arr a (b, c)
concurrently one two =
  (returnA &&& tickets)
    >>> (together [toFirst, toSecond] &&& arr (\(_, (first', second')) -> (fst first', fst second')))
    >>> arr snd
    >>> (await *** await)
  where
    tickets :: arr a ((Waiting arr (Local arr) b, Giver (Local arr) b), (Waiting arr (Local arr) c, Giver (Local arr) c))
    tickets = arr (const ()) >>> (ticket &&& ticket)
    toFirst = (\(input, (first', _)) -> (input, snd first')) ^>> first one >>> arr (\(result, giver) -> (giver, result)) >>> give >>> arr (const ())
    toSecond = (\(input, (_, second')) -> (input, snd second')) ^>> first two >>> arr (\(result, giver) -> (giver, result)) >>> give >>> arr (const ())
{-# INLINE concurrently #-}

-- | The action until it gives something, giving way between tries.
untilJust :: (ArrowChoice arr, Effect arr, GiveWay arr) => IO (Maybe a) -> arr () a
untilJust action = loop
  where
    loop = now (\() -> maybe (Left ()) Right <$> action) >>> ((giveWay >>> loop) ||| returnA)
{-# INLINE untilJust #-}

-- | The arrow, which needs no input, with what it gave and the input that came, which it leaves as it was.
keeping :: (Arrow arr) => arr () b -> arr c (b, c)
keeping act = ((),) ^>> first act
{-# INLINE keeping #-}

-- | Makes a ticket, gives the message that carries its giver, and keeps the end to wait at. It doesn't say whether
-- the giver took the message: a caller that needs that gives it itself. For any reach: a ticket of the arrow's own
-- core, whose message goes to a part of that core, or a ticket of any core, whose message crosses cores.
expect :: (Arrow arr, Exchange arr r) => (Giver r a -> b) -> arr (Giver r b) (Waiting arr r a)
expect message =
  ((),)
    ^>> first ticket
    >>> (\((end, giver), to) -> ((to, message giver), end))
    ^>> first give
    >>^ snd
{-# INLINE expect #-}

-- | Asks: gives the message that carries the giver of the answer, and waits for the answer.
call :: (Arrow arr, Exchange arr r) => (Giver r a -> b) -> arr (Giver r b) a
call message = expect message >>> await
{-# INLINE call #-}

-- | Waits for this long. It is a wait, so a cancellation reaches it.
sleep :: (Arrow arr, Time arr, Block arr) => Duration -> arr () ()
sleep duration = clock >>> (\now' -> Until (now' `plus` duration)) ^>> block
{-# INLINE sleep #-}

-- | A release that has a deadline of its own: shielded from the cancellation that makes it run, but not from its
-- deadline. 'Nothing' if it missed it, and the part that releases what it holds should discard it.
releaseWithin :: (Scope arr, Within arr) => Duration -> arr a () -> arr a (Maybe ())
releaseWithin duration release = shield (within duration release)
{-# INLINE releaseWithin #-}
