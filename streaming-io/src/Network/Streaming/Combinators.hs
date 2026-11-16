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
    through,
    _1,
    _2,
    _Left,
    _Right,
    _Just,
    on,
    boolA,
    Step,
    pattern Again,
    pattern Finish,
    iterateA,
    iterateWith,
    foreverA,
    undoing,
    localTicket,
    deliver,
    withTicket,
    tryTicket,
    withInbox,
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
    (-|),
    (|-),
    typed,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, second, (&&&), (***), (>>>), (>>^), (^>>), (|||))
import Control.Exception (throwIO)
import Data.Proxy (Proxy (..))
import Data.Typeable (Typeable, typeRep)
import GHC.Stack (HasCallStack)
import GHC.TypeLits (KnownSymbol)
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
    >>> tap (together [firstPromise ^>> deliver one, secondPromise ^>> deliver two])
    >>> ends ^>> (await *** await)
  where
    -- The group's input is the two tickets and the input. Each arrow fulfils the promise of its own ticket, made before
    -- the group starts, so the awaits after the group never wait.
    firstPromise = first (snd . fst)
    secondPromise = first (snd . snd)
    ends = (fst *** fst) . fst
{-# INLINE concurrently #-}

-- | The action until it gives something, giving way between tries.
untilJust :: (ArrowChoice arr, Effect arr, GiveWay arr, Annotate arr) => IO (Maybe a) -> arr () a
untilJust action = iterateWith (now (const (maybe (Again ()) Finish <$> action))) giveWay
{-# INLINE untilJust #-}

-- | Runs the step; if it fails, runs the cleanup on the same input, and fails the same way. Neither of them can wait, so a
-- cancellation can't stop them half way, and catching the failure is enough. If the cleanup fails too, its failure goes on
-- instead.
undoing :: (Steps arr) => (forall p. (Steps p) => p a b) -> (forall p. (Steps p) => p a ()) -> arr a b
undoing body cleanup =
  alongside (tryA body)
    >>> decide "failed?" (\(result, input) -> either (Left . (input,)) Right result)
    >>> ("failed" |- (consume cleanup >>> now throwIO) ||| "done" |- returnA)
{-# INLINE undoing #-}

-- | A ticket of the arrow's own reach: one that only parts of this core fulfil and await.
localTicket :: (Exchange arr (Local arr)) => arr () (Future arr (Local arr) a, Promise (Local arr) a)
localTicket = ticket
{-# INLINE localTicket #-}

-- | Runs the arrow on the input and fulfils the promise with its result, without saying whether the waiter took it.
deliver :: (Arrow arr, Exchange arr r) => arr a b -> arr (Promise r b, a) ()
deliver body = second body >>> fulfil >>> done
{-# INLINE deliver #-}

-- | Runs the arrow on the promise of a ticket and the input, and returns what it returned, and the future to await.
handingOut ::
  (Arrow arr, Exchange arr r) =>
  arr (Promise r a, b) c ->
  arr ((Future arr r a, Promise r a), b) (c, Future arr r a)
handingOut handOut = handing ^>> first handOut
  where
    handing ((future, promise), b) = ((promise, b), future)
{-# INLINE handingOut #-}

-- | Makes a ticket, hands out its promise, and waits for the value: returns what the hand-out returned, and the value. If the
-- hand-out or the wait ends any other way, the ticket is abandoned, and a value fulfilled before that goes to the disposal,
-- with the input. The hand-out may wait, and is cancelled where it does.
withTicket :: (ArrowChoice arr, Annotate arr, Scope arr, Exchange arr r) => arr (Promise r a, b) c -> arr (a, b) () -> arr b (c, a)
withTicket handOut dispose =
  scope
    (keeping ticket)
    (decide "awaited?" unawaited >>> abandoning dispose)
    (fst ^>> handingOut handOut >>> second await)
  where
    -- Only a wait that didn't end in 'Done' is abandoned: after an 'await' the slot stays fulfilled, so an 'abandon' would
    -- hand the value out again. The release therefore trusts 'Done' to mean that the future was awaited.
    unawaited (((future, _), b), exit) = case exit of
      Done -> Left ()
      _ -> Right (future, b)
{-# INLINE withTicket #-}

-- | Opens an inbox for the body, and closes it when the body ends however it ends: a value left in it goes to the
-- disposal, with the input.
withInbox :: forall name r arr a b c. (ArrowChoice arr, Annotate arr, Scope arr, Inboxes arr r, KnownSymbol name) => arr (a, b) () -> arr ((Inbox arr name r a, Address name r a), b) c -> arr b c
withInbox dispose body = scope (keeping (inbox @arr @r @name)) (closing @name @r dispose) (fst ^>> body)
{-# INLINE withInbox #-}

-- | The release of an inbox's scope: it closes the inbox, and gives the value that was left in it to the disposal, with
-- the input.
closing :: forall name r arr a b. (ArrowChoice arr, Annotate arr, Inboxes arr r, KnownSymbol name) => arr (a, b) () -> arr (((Inbox arr name r a, Address name r a), b), Exit) ()
closing dispose =
  (\(((opened, _), b), _) -> (opened, b)) ^>> first (closeInbox @arr @r @name) >>> decide "value left?" left >>> ("no" |- returnA ||| "yes" |- dispose)
  where
    left (leftover, b) = maybe (Left ()) (Right . (,b)) leftover
{-# INLINE closing #-}

-- | Makes a ticket and tries to hand out its promise. Left: the promise wasn't handed out, so the ticket is abandoned at
-- once, a value fulfilled before that goes to the disposal, with the input, and the result is Left. Right: it waits for the
-- value as 'withTicket' does. The hand-out may wait, and is cancelled where it does.
tryTicket :: (ArrowChoice arr, Annotate arr, Scope arr, Exchange arr r) => arr (Promise r a, b) (Either c d) -> arr (a, b) () -> arr b (Either c (d, a))
tryTicket handOut dispose =
  scope
    (keeping ticket)
    (decide "awaited?" unawaited >>> abandoning dispose)
    ( handing . fst
        ^>> first handOut
        >>> decide "handed out?" routed
        >>> ("no" |- declined ||| "yes" |- accepted)
    )
  where
    -- The input stays with the future, for the disposal of a value that the hand-out fulfilled before it declined.
    handing ((future, promise), b) = ((promise, b), (future, b))
    routed (handedOut, (future, b)) = either (\c -> Left (Right (future, b), c)) (\d -> Right (d, future)) handedOut
    declined = consume (abandoning dispose) >>^ Left
    accepted = second await >>^ Right
    -- As in 'withTicket'. A hand-out that declined has abandoned the future by then, and 'Done' says so too.
    unawaited (((future, _), b), exit) = case exit of
      Done -> Left ()
      _ -> Right (future, b)
{-# INLINE tryTicket #-}

-- | The release of a ticket's scope, for what is left of the wait: it does nothing if there is nothing, and else it
-- abandons the future, and hands a value that was fulfilled before to the disposal, with the input.
abandoning :: (ArrowChoice arr, Annotate arr, Exchange arr r) => arr (a, b) () -> arr (Either () (Future arr r a, b)) ()
abandoning dispose =
  "yes" |- returnA
    ||| "no" |- (first abandon >>> decide "value fulfilled already?" late >>> ("no" |- returnA ||| "yes" |- dispose))
  where
    late (fulfilled, b) = maybe (Left ()) (Right . (,b)) fulfilled
{-# INLINE abandoning #-}

-- | Makes a ticket, hands out the message that carries its promise, and keeps the future to await. It doesn't say whether
-- the promise took the message: a caller that needs that fulfils it itself. For any reach: a ticket of the arrow's own
-- core, whose message goes to a part of that core, or a ticket of any core, whose message crosses cores.
expect :: (Arrow arr, Exchange arr r) => (Promise r a -> b) -> arr (Promise r b) (Future arr r a)
expect message = keeping ticket >>> handingOut (addressed ^>> fulfil >>> done) >>^ snd
  where
    -- The promise to fulfil with the message, and the message, which carries the promise of the answer.
    addressed (promise, to) = (to, message promise)
{-# INLINE expect #-}

-- | Asks: fulfils a promise with the message that carries the promise of the answer, and waits for the answer, unless the
-- promise the message went to has gone: then it is 'Nothing', at once. If the fulfil or the wait ends any other way, the
-- ticket is abandoned, and an answer that came before goes to the disposal, with the promise the message went to.
call :: (ArrowChoice arr, Annotate arr, Scope arr, Exchange arr r) => (Promise r a -> b) -> arr (a, Promise r b) () -> arr (Promise r b) (Maybe a)
call message dispose = tryTicket (addressed ^>> fulfil >>^ delivered) dispose >>^ either (const Nothing) (Just . snd)
  where
    -- The promise to fulfil with the message, and the message, which carries the promise of the answer.
    addressed (promise, to) = (to, message promise)
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
