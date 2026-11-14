{-# LANGUAGE AllowAmbiguousTypes #-}
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
    decide,
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
    box,
    asking,
    answering,
    step,
    taking,
    giving,
    typed,
    field,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, second, (&&&), (***), (>>>), (>>^), (^>>), (|||))
import Data.Proxy (Proxy (..))
import Data.Typeable (Typeable, typeRep)
import GHC.Records (HasField (..))
import GHC.Stack (HasCallStack)
import GHC.TypeLits (KnownSymbol, symbolVal)
import Network.Streaming.Annotate (answering, asking, box)
import Network.Streaming.Classes
import Network.Streaming.Plumbing

-- | The step this many times, each on what the one before gave.
timesWith :: (ArrowChoice arr, Annotate arr) => Int -> arr s s -> arr s s
timesWith count body = (,count) ^>> iterateWith (arr countDown) (first body)
  where
    countDown (s, n) = if n <= 0 then Finish s else Again (s, n - 1)
{-# INLINE timesWith #-}

-- | Runs the arrow this many times.
times :: (ArrowChoice arr, Annotate arr) => Int -> arr () () -> arr () ()
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
untilJust :: (ArrowChoice arr, Effect arr, GiveWay arr, Annotate arr) => IO (Maybe a) -> arr () a
untilJust action = iterateWith (now (const (maybe (Again ()) Finish <$> action))) giveWay
{-# INLINE untilJust #-}

-- | A ticket of the arrow's own reach: one that only parts of this core give and await.
localTicket :: (Exchange arr (Local arr)) => arr () (Waiting arr (Local arr) a, Giver (Local arr) a)
localTicket = ticket
{-# INLINE localTicket #-}

-- | Runs the arrow on the input and gives its result to the giver, without saying whether the waiter took it.
deliver :: (Arrow arr, Exchange arr r) => arr a b -> arr (Giver r b, a) ()
deliver body = second body >>> give >>> done
{-# INLINE deliver #-}

-- | Runs the arrow on the giver of a ticket and the input, and gives what it gave, and the end to wait at.
handingOut ::
  (Arrow arr, Exchange arr r) =>
  arr (Giver r a, b) c ->
  arr ((Waiting arr r a, Giver r a), b) (c, Waiting arr r a)
handingOut handOut = handing ^>> first handOut
  where
    handing ((waiting, giver), b) = ((giver, b), waiting)
{-# INLINE handingOut #-}

-- | Makes a ticket, hands out its giver, and waits for the value: gives what the hand-out gave, and the value. If the
-- hand-out or the wait ends any other way, the ticket is abandoned, and a value given before that goes to the disposal,
-- with the input. The hand-out may wait, and is cancelled where it does.
withTicket :: (ArrowChoice arr, Annotate arr, Scope arr, Exchange arr r) => arr (Giver r a, b) c -> arr (a, b) () -> arr b (c, a)
withTicket handOut dispose =
  scope
    (keeping ticket)
    (decide "awaited?" unawaited >>> abandoning dispose)
    (fst ^>> handingOut handOut >>> second await)
  where
    -- Only a wait that didn't end in 'Done' is abandoned: after an 'await' the slot stays given, so an 'abandon' would
    -- hand the value out again. The release therefore trusts 'Done' to mean that the end was awaited.
    unawaited (((waiting, _), b), exit) = case exit of
      Done -> Left ()
      _ -> Right (waiting, b)
{-# INLINE withTicket #-}

-- | Makes a ticket and tries to hand out its giver. Left: the giver wasn't handed out, so the ticket is abandoned at
-- once, a value given before that goes to the disposal, with the input, and the result is Left. Right: it waits for the
-- value as 'withTicket' does. The hand-out may wait, and is cancelled where it does.
tryTicket :: (ArrowChoice arr, Annotate arr, Scope arr, Exchange arr r) => arr (Giver r a, b) (Either c d) -> arr (a, b) () -> arr b (Either c (d, a))
tryTicket handOut dispose =
  scope
    (keeping ticket)
    (decide "awaited?" unawaited >>> abandoning dispose)
    ( handing . fst
        ^>> first handOut
        >>> decide "handed out?" routed
        >>> (taking "no" declined ||| taking "yes" accepted)
    )
  where
    -- The input stays with the end, for the disposal of a value that the hand-out gave before it declined.
    handing ((waiting, giver), b) = ((giver, b), (waiting, b))
    routed (handedOut, (waiting, b)) = either (\c -> Left (Right (waiting, b), c)) (\d -> Right (d, waiting)) handedOut
    declined = consume (abandoning dispose) >>^ Left
    accepted = second await >>^ Right
    -- As in 'withTicket'. A hand-out that declined has abandoned the end by then, and 'Done' says so too.
    unawaited (((waiting, _), b), exit) = case exit of
      Done -> Left ()
      _ -> Right (waiting, b)
{-# INLINE tryTicket #-}

-- | The release of a ticket's scope, for what is left of the wait: it does nothing if there is nothing, and else it
-- abandons the end, and gives a value that was given before to the disposal, with the input.
abandoning :: (ArrowChoice arr, Annotate arr, Exchange arr r) => arr (a, b) () -> arr (Either () (Waiting arr r a, b)) ()
abandoning dispose =
  taking "yes" returnA
    ||| taking "no" (first abandon >>> decide "value given already?" late >>> (taking "no" returnA ||| taking "yes" dispose))
  where
    late (given, b) = maybe (Left ()) (Right . (,b)) given
{-# INLINE abandoning #-}

-- | Makes a ticket, gives the message that carries its giver, and keeps the end to wait at. It doesn't say whether
-- the giver took the message: a caller that needs that gives it itself. For any reach: a ticket of the arrow's own
-- core, whose message goes to a part of that core, or a ticket of any core, whose message crosses cores.
expect :: (Arrow arr, Exchange arr r) => (Giver r a -> b) -> arr (Giver r b) (Waiting arr r a)
expect message = keeping ticket >>> handingOut (addressed ^>> give >>> done) >>^ snd
  where
    -- The giver to give the message to, and the message, which carries the giver of the answer.
    addressed (giver, to) = (to, message giver)
{-# INLINE expect #-}

-- | Asks: gives the message that carries the giver of the answer, and waits for the answer, unless the giver the message
-- was given to has gone: then it is 'Nothing', at once. If the give or the wait ends any other way, the ticket is
-- abandoned, and an answer that came before goes to the disposal, with the giver the message was given to.
call :: (ArrowChoice arr, Annotate arr, Scope arr, Exchange arr r) => (Giver r a -> b) -> arr (a, Giver r b) () -> arr (Giver r b) (Maybe a)
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

-- | An effect with a name, drawn as a box with that name.
step :: (HasCallStack, Effect arr, Annotate arr) => String -> (a -> IO b) -> arr a b
step name = box name . now
{-# INLINE step #-}

-- | The arrow, with its input and output wires named by their types in diagrams. The type @()@ names nothing.
typed :: forall a b arr. (Typeable a, Typeable b, Annotate arr) => arr a b -> arr a b
typed = wires (typeName (Proxy @a)) (typeName (Proxy @b))
  where
    typeName proxy = case show (typeRep proxy) of
      "()" -> Nothing
      name -> Just name
{-# INLINE typed #-}

-- | The field of a record, whose output wire is named after the field in diagrams.
field :: forall name r a arr. (HasField name r a, KnownSymbol name, Arrow arr, Annotate arr) => arr r a
field = giving (symbolVal (Proxy @name)) (arr (getField @name))
{-# INLINE field #-}
