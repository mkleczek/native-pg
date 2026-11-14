{-# LANGUAGE PatternSynonyms #-}

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
    done,
    dup,
    whether,
    alongside,
    keeping,
    tap,
    consume,
    boolA,
    Step,
    pattern Again,
    pattern Finish,
    iterateA,
    iterateWith,
    foreverA,
    localTicket,
    deliver,
    expect,
    call,
    sleep,
    releaseWithin,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, second, (&&&), (***), (>>>), (^>>))
import Network.Streaming.Classes
import Network.Streaming.Plumbing

-- | The step this many times, each on what the one before gave.
timesWith :: (ArrowChoice arr) => Int -> arr s s -> arr s s
timesWith count step = (,count) ^>> iterateWith (arr countDown) (first step)
  where
    countDown (s, n) = if n <= 0 then Finish s else Again (s, n - 1)
{-# INLINE timesWith #-}

-- | Runs the arrow this many times.
times :: (ArrowChoice arr) => Int -> arr () () -> arr () ()
times = timesWith
{-# INLINE times #-}

-- | Runs both arrows at once on the same input and returns both results. The first failure cancels the other arrow
-- and is rethrown once both have stopped.
concurrently :: (Arrow arr, Concurrent arr, Exchange arr (Local arr)) => arr a b -> arr a c -> arr a (b, c)
concurrently one two =
  keeping (localTicket &&& localTicket)
    >>> tap (together [firstGiver ^>> deliver one, secondGiver ^>> deliver two])
    >>> ends ^>> (await *** await)
  where
    -- The group's input is the two tickets and the input. Each arrow gives its result to its own ticket, made before
    -- the group starts, so the awaits after the group never wait.
    firstGiver = first (snd . fst)
    secondGiver = first (snd . snd)
    ends = (fst *** fst) . fst
{-# INLINE concurrently #-}

-- | The action until it gives something, giving way between tries.
untilJust :: (ArrowChoice arr, Effect arr, GiveWay arr) => IO (Maybe a) -> arr () a
untilJust action = iterateWith (now (const (maybe (Again ()) Finish <$> action))) giveWay
{-# INLINE untilJust #-}

-- | A ticket of the arrow's own reach: one that only parts of this core give and await.
localTicket :: (Exchange arr (Local arr)) => arr () (Waiting arr (Local arr) a, Giver (Local arr) a)
localTicket = ticket
{-# INLINE localTicket #-}

-- | Runs the arrow on the input and gives its result to the giver, without saying whether the waiter took it.
deliver :: (Arrow arr, Exchange arr r) => arr a b -> arr (Giver r b, a) ()
deliver step = second step >>> give >>> done
{-# INLINE deliver #-}

-- | Makes a ticket, gives the message that carries its giver, and keeps the end to wait at. It doesn't say whether
-- the giver took the message: a caller that needs that gives it itself. For any reach: a ticket of the arrow's own
-- core, whose message goes to a part of that core, or a ticket of any core, whose message crosses cores.
expect :: (Arrow arr, Exchange arr r) => (Giver r a -> b) -> arr (Giver r b) (Waiting arr r a)
expect message =
  keeping ticket
    >>> addressed
    ^>> consume (give >>> done)
  where
    -- The message to send, with the giver of the answer in it, and the end where the answer is awaited.
    addressed ((end, giver), to) = ((to, message giver), end)
{-# INLINE expect #-}

-- | Asks: gives the message that carries the giver of the answer, and waits for the answer.
call :: (Arrow arr, Exchange arr r) => (Giver r a -> b) -> arr (Giver r b) a
call message = expect message >>> await
{-# INLINE call #-}

-- | Waits for this long. It is a wait, so a cancellation reaches it.
sleep :: (Arrow arr, Time arr, Block arr) => Duration -> arr () ()
sleep duration = clock >>> Until . (`plus` duration) ^>> block
{-# INLINE sleep #-}

-- | A release that has a deadline of its own: shielded from the cancellation that makes it run, but not from its
-- deadline. 'Nothing' if it missed it, and the part that releases what it holds should discard it.
releaseWithin :: (Scope arr, Within arr) => Duration -> arr a () -> arr a (Maybe ())
releaseWithin duration release = shield (within duration release)
{-# INLINE releaseWithin #-}
