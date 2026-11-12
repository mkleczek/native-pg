-- | Helpers for writing programs over the classes, so that a value is made where it is used and not carried
-- through the arrows in between: loops with a state, a retry, and a request that carries the end its answer is
-- waited at. Each is written once for any arrow that has what it needs. All are INLINE, not INLINABLE: in an
-- arrow of continuations, such as "Network.Directions.Compiled", one that isn't inlined is passed its step and
-- continuation as unknown functions, and allocates for them on every turn.
module Network.Directions.Combinators
  ( timesWith,
    untilJust,
    keeping,
    expect,
    call,
    expectLocal,
    callLocal,
    sleep,
    releaseWithin,
  )
where

import Control.Arrow (Arrow, ArrowChoice, first, returnA, (>>>), (>>^), (^>>), (|||))
import Network.Directions.Classes

-- | The step this many times, each on what the one before gave.
timesWith :: (ArrowChoice arr) => Int -> arr s s -> arr s s
timesWith count step = (,count) ^>> loop
  where
    loop = (\(s, n) -> if n <= 0 then Left s else Right (s, n - 1)) ^>> (returnA ||| (first step >>> loop))
{-# INLINE timesWith #-}

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

-- | Makes an end to wait at, gives the message that carries its giver, and keeps the end. It doesn't say whether
-- the giver took the message: a caller that needs that gives it itself.
expect :: (Arrow arr, Remote arr) => (Giver a -> b) -> arr (Giver b) (Awaiting arr a)
expect message =
  ((),)
    ^>> first newRemote
    >>> (\((end, giver), to) -> ((to, message giver), end))
    ^>> first give
    >>^ snd
{-# INLINE expect #-}

-- | Asks: gives the message that carries the giver of the answer, and waits for the answer.
call :: (Arrow arr, Remote arr) => (Giver a -> b) -> arr (Giver b) a
call message = expect message >>> awaitRemote
{-# INLINE call #-}

-- | 'expect' for a ticket of this core, which is both ends: makes the ticket, gives the message that carries it to
-- the ticket, and keeps it to wait at.
expectLocal :: (Arrow arr, Exchange arr) => (Ticket arr a -> b) -> arr (Ticket arr b) (Ticket arr a)
expectLocal message =
  ((),)
    ^>> first newTicket
    >>> (\(ticket, to) -> ((to, message ticket), ticket))
    ^>> first fulfil
    >>^ snd
{-# INLINE expectLocal #-}

-- | Waits for this long. It is a wait, so a cancellation reaches it.
sleep :: (Arrow arr, Time arr, Block arr) => Duration -> arr () ()
sleep duration = clock >>> (\now' -> Until (now' `plus` duration)) ^>> block
{-# INLINE sleep #-}

-- | A release that has a deadline of its own: shielded from the cancellation that makes it run, but not from its
-- deadline. 'Nothing' if it missed it, and the part that releases what it holds should discard it.
releaseWithin :: (Scope arr, Within arr) => Duration -> arr a () -> arr a (Maybe ())
releaseWithin duration release = shield (within duration release)
{-# INLINE releaseWithin #-}

-- | 'call' for a ticket of this core.
callLocal :: (Arrow arr, Exchange arr) => (Ticket arr a -> b) -> arr (Ticket arr b) a
callLocal message = expectLocal message >>> await
{-# INLINE callLocal #-}
