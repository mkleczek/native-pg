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
    withTicket,
    tryTicket,
    expect,
    call,
    sleep,
    releaseWithin,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, right, second, (&&&), (***), (>>>), (>>^), (^>>), (|||))
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

-- | Makes a ticket, runs the arrow on its giver and the input, and gives what it gave, and the end to wait at.
handingOut :: (Arrow arr, Exchange arr r) => arr (Giver r a, b) c -> arr b (c, Waiting arr r a)
handingOut handOut = keeping ticket >>> handing ^>> first handOut
  where
    handing ((waiting, giver), b) = ((giver, b), waiting)
{-# INLINE handingOut #-}

-- | Makes a ticket, hands out its giver, and waits for the value: gives what the hand-out gave, and the value. If the
-- wait ends any other way, the ticket is abandoned, and a value given before that goes to the disposal, with the input.
-- The hand-out must not wait.
withTicket :: (ArrowChoice arr, Scope arr, Exchange arr r) => arr (Giver r a, b) c -> arr (a, b) () -> arr b (c, a)
withTicket handOut dispose = scope (alongside (handingOut handOut)) (unawaited ^>> abandoning dispose) (fst . fst ^>> second await)
  where
    -- Only a wait that didn't end in 'Done' is abandoned: after an 'await' the slot stays given, so an 'abandon' would
    -- hand the value out again. The release therefore trusts 'Done' to mean that the end was awaited.
    unawaited (((_, waiting), b), exit) = case exit of
      Done -> Left ()
      _ -> Right (waiting, b)
{-# INLINE withTicket #-}

-- | Makes a ticket and tries to hand out its giver. Left: the giver wasn't handed out, so the ticket is abandoned at
-- once, and the result is Left. Right: it waits for the value as 'withTicket' does. The hand-out must not wait.
tryTicket :: (ArrowChoice arr, Scope arr, Exchange arr r) => arr (Giver r a, b) (Either c d) -> arr (a, b) () -> arr b (Either c (d, a))
tryTicket handOut dispose = scope (alongside (handingOut handOut)) (unawaited ^>> abandoning dispose) (waited ^>> right (second await))
  where
    -- A ticket whose giver wasn't handed out is abandoned too, with whatever was somehow given to it. As in 'withTicket',
    -- 'Done' after a hand-out means that the end was awaited.
    unawaited (((handedOut, waiting), b), exit) = case (exit, handedOut) of
      (Done, Right _) -> Left ()
      _ -> Right (waiting, b)
    waited (((handedOut, waiting), _), _) = (,waiting) <$> handedOut
{-# INLINE tryTicket #-}

-- | The release of a ticket's scope, for what is left of the wait: it does nothing if there is nothing, and else it
-- abandons the end, and gives a value that was given before to the disposal, with the input.
abandoning :: (ArrowChoice arr, Exchange arr r) => arr (a, b) () -> arr (Either () (Waiting arr r a, b)) ()
abandoning dispose = returnA ||| (first abandon >>> late ^>> (returnA ||| dispose))
  where
    late (given, b) = maybe (Left ()) (Right . (,b)) given
{-# INLINE abandoning #-}

-- | Makes a ticket, gives the message that carries its giver, and keeps the end to wait at. It doesn't say whether
-- the giver took the message: a caller that needs that gives it itself. For any reach: a ticket of the arrow's own
-- core, whose message goes to a part of that core, or a ticket of any core, whose message crosses cores.
expect :: (Arrow arr, Exchange arr r) => (Giver r a -> b) -> arr (Giver r b) (Waiting arr r a)
expect message = handingOut (addressed ^>> give >>> done) >>^ snd
  where
    -- The giver to give the message to, and the message, which carries the giver of the answer.
    addressed (giver, to) = (to, message giver)
{-# INLINE expect #-}

-- | Asks: gives the message that carries the giver of the answer, and waits for the answer, unless the giver the message
-- was given to has gone: then it is 'Nothing', at once. If the wait ends any other way, the ticket is abandoned, and an
-- answer that came before goes to the disposal, with the giver the message was given to.
call :: (ArrowChoice arr, Scope arr, Exchange arr r) => (Giver r a -> b) -> arr (a, Giver r b) () -> arr (Giver r b) (Maybe a)
call message dispose = tryTicket (addressed ^>> give >>^ delivered) dispose >>^ either (const Nothing) (Just . snd)
  where
    -- The giver to give the message to, and the message, which carries the giver of the answer.
    addressed (giver, to) = (to, message giver)
    delivered taken = if taken then Right () else Left ()
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
