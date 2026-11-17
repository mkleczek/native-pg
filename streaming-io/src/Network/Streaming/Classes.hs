{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | The classes of the design: a link whose calls block in the arrow that runs
-- them, what an arrow can do for a link (run an action that never waits, give
-- way to the other parts, and block until an event fires), and how directions
-- exchange values. Two arrows provide them, "Network.Streaming.Threads", a
-- thread for each concurrent part, and "Network.Streaming.Loop", every part on
-- one thread; a link's instance is written once for any arrow that does, over
-- the link's own calls that never wait.
--
-- == Build time and run time
--
-- A program is built once and then runs many times. Every expression whose type is an arrow, @arr a b@, is evaluated
-- when the program is built: the arguments of the functions that make arrows, the @if@ and @case@ expressions and
-- lists that decide which arrows exist, and where-bound arrows such as a list of workers. Only the functions passed
-- into an arrow run at run time: those given to @arr@, @^>>@, @>>^@ and 'now', and the decisions they return.
--
-- So there are two kinds of choice:
--
-- * which arrows exist is chosen at build time, with ordinary Haskell: @if@, @case@, and building lists and functions;
--
-- * which of the existing arrows runs is chosen at run time, by a value: @|||@, @boolA@, @left@, and the @Again@ or
--   @Finish@ that a step of @iterateA@ or @iterateWith@ returns.
--
-- A run-time value can't build an arrow or pick one that doesn't exist: the arrows have no @app@ and are not monads,
-- so the types keep the two apart. What is known when the program is built is a function's parameter; what differs
-- from run to run is the arrow's input.
module Network.Streaming.Classes
  ( Link (..),
    Stream (..),
    Steps,
    Runtime,
    ArrowApplicative,
    -- The methods 'group' and 'spawn' are left out, as their names are common ones: programs import them, and the joiners,
    -- from "Network.Streaming.Concurrent".
    Concurrent (together),
    Exit (..),
    ScopeId (..),
    Cancelled (..),
    ReleaseFailed (..),
    cancelledBy,
    exitOf,
    Scope (..),
    Race (..),
    Instant (..),
    monotonicNanoseconds,
    Duration (..),
    nanos,
    micros,
    millis,
    secs,
    plus,
    Time (..),
    Within (..),
    Label (..),
    -- The method 'exchange' is left out, as its name is a common one: programs use 'asking' and 'answering'.
    Check (..),
    Annotate (named, wires, allowing),
    taking,
    giving,
    (-|),
    (|-),
    Effect (..),
    GiveWay (..),
    FastLane (..),
    Block (..),
    Event (..),
    Reach (..),
    Promise,
    Fulfilling (..),
    Exchange (..),
    Address,
    Inboxes (..),
    Local,
    receiveWith,
    sendAllWith,
    -- A socket's own calls that never wait, for a link that is a socket among other things.
    tryReceive,
    trySend,
    receiveFirst,
    Room (..),
    roomOf,
    view,
  )
where

import Control.Arrow (Arrow, ArrowChoice, first, second, (>>>), (^>>))
import Control.Exception (Exception, SomeException, fromException)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Kind (Type)
import Data.Word (Word64, Word8)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (Ptr, plusPtr)
import GHC.TypeLits (KnownSymbol, Symbol)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Annotate (Annotate (..), Check (..), Label (..), box, giving, taking, (-|), (|-))
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Concurrent (Concurrent (together))
import Network.Streaming.Internal.Inboxes (Address)
import Network.Streaming.Internal.Socket (receiveFirst, tryReceive, trySend)
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.Plumbing (pattern Again, pattern Finish, consume, iterateWith)
import Network.Streaming.Internal.Tickets (Fulfilling (..), Promise, Reach (..))
import Network.Streaming.Watch (Watch)
import System.Posix.Types (Fd (..))

-- | A byte stream whose calls block in the arrow that runs them. How they
-- block is the arrow's. The link is a run-time value, so it is an input of each
-- call, and not an argument.
class Link arr link where
  -- | Receives into the room: how many bytes, 0 at the end of the stream.
  receive :: arr (link, Room) Int

  -- | Sends all of the pieces, and returns once they may be reused: once the
  -- kernel has copied them, or, for a link that sends without copying, once
  -- the kernel no longer reads them.
  sendAll :: arr (link, [ByteString]) ()

  -- | Waits until the link has input, and takes none of it: a part that
  -- waits with it holds no buffer to receive into while it does. The input
  -- may be the end of the stream.
  awaitInput :: arr link ()

  -- | Ends the stream's output, once what was sent is written.
  shutdownSend :: arr link ()

  close :: arr link ()

-- | A stream of byte views: a field's value, a request body. A piece is a view,
-- valid until the next step on the stream; the end is 'Nothing'.
class Stream arr s where
  next :: arr s (Maybe ByteString)

-- | Arrows whose programs from one input are applicative functors, so that steps that don't depend on each other's
-- results are combined with '<$>' and '<*>'. The effects run from left to right, as with '&&&'.
class (forall x. Applicative (arr x)) => ArrowApplicative arr

-- | What a step that can't wait needs of the arrow it runs in: the classes of 'Runtime' that have no waiting method. A step
-- that is polymorphic over them ends by returning or by throwing, so that catching its failure is enough to clean up after
-- it, and a combinator that only catches takes such a step and refuses one that can wait.
type Steps arr =
  ( ArrowChoice arr,
    ArrowApplicative arr,
    Effect arr,
    Scope arr,
    Time arr,
    Annotate arr,
    Instrumented (ModeOf arr)
  )

-- | What a program needs of the arrow it runs in, so that it can be written
-- once and run by any of them: the capabilities of its classes, and tickets
-- and inboxes at both reaches. It is 'Steps' and what can wait.
type Runtime arr =
  ( Steps arr,
    Block arr,
    GiveWay arr,
    FastLane arr,
    Race arr,
    Within arr,
    Concurrent arr,
    Exchange arr (Local arr),
    Exchange arr 'Anywhere,
    Inboxes arr (Local arr),
    Inboxes arr 'Anywhere
  )

-- | A ticket hands values from one part to another, over and over, and keeps none: a value is its sender's until a receiver takes it, and
-- then the receiver's, and is never anywhere else. It is a rendezvous. The ticket is the way directions exchange what they know.
--
-- * __Ends.__ A ticket has a receiving end, a 'Future' that the part which takes awaits, on the core that made the ticket, and a handing
--   end, a 'Promise', a plain value that can be passed to any part and stored. A ticket is made when the program runs, and its ends
--   are passed through arrows like any value, so a part can make one for each thing it waits for.
-- * __Reach.__ The ticket's reach @r@ says in its type where the promise can be used. A promise of reach @'Here s@ belongs to the core
--   of the arrow @s@ that made the ticket, as the arrow's own region does: only the parts of that core can hand a value over, and the
--   arrow can keep its slot as plain memory, with no atomic operation, and its parts on its own run list. A promise of reach
--   @'Anywhere@ belongs to no core, so that shared structures can hold it, and the parts of any core can hand a value over: it costs
--   what a value that crosses cores costs. An arrow has the instances for the reaches it can keep.
-- * __Take__ ('await'): waits until a sender hands a value over, and returns it.
-- * __Take, registering__ ('awaitRegistering'): the same, but it runs an action at the moment it parks, in the same atomic step. The
--   action is given the promise, to put in a structure that senders share, such as the queue of waiters of a pool. It may give a value
--   at once instead, and then the receiver does not park. This closes the window between registering a promise and waiting.
--   'willAwait' does the same for registration steps that are arrows, which may wait: the receiver is committed to take from the moment
--   it says so until its 'await', so that a value handed to it meanwhile is not lost, and 'abandon' gives such a value back.
-- * __Hand now__ ('fulfil', and 'fulfilNow' as an action): never waits. It gives the value to a receiver that is parked right now, or
--   committed to take, and says so with 'True'; otherwise it says 'False', and the value is the sender's still. It is for code inside a
--   step, which must not wait.
-- * __Hand__ ('handWaiting'): waits until the receiver takes the value. If the ticket is abandoned first, the value comes back to the
--   sender: the result is 'Nothing' if it was taken, and @'Just' value@ if it is still the sender's. A sender that is cancelled while it
--   waits keeps its value. A second sender that waits while one waits fails.
-- * __Watch__ ('untilParked'): waits until the receiver is parked, and takes nothing. One part watches at a time. A watcher is let go
--   when the ticket is abandoned too, since no receiver will park then: a hand gives its value back.
-- * __Close__ ('abandon'): done by the receiving side as its scope ends. A sender that waits gets its value back, and a later hand
--   gives it back at once, as a later 'fulfil' says 'False'. The receiver is given back only the value that was handed to it while it
--   was committed to take and that it never awaited.
-- * __Delivery wins over cancellation.__ Once a value has been handed to a parked receiver, the 'await' returns it: the receiver owns it,
--   and a cancellation reaches it at its next wait, not at this one. A receiver that is cancelled while parked, before any hand, is
--   unparked atomically, so that a hand that races with it either delivers, and the receiver has the value, or gives it back to the
--   sender: never both, and never neither.
-- * __Reusable.__ After each exchange a ticket is as new, and can be awaited again: a hundred thousand exchanges on one leave the live
--   heap as it was.
--
-- Only the arrows wait, as the check that no module but the arrows' implementations waits in IO holds: 'await', 'handWaiting' and
-- 'untilParked' are arrows, and 'fulfilNow' is the form of 'fulfil' in 'IO'. The ends can be named for diagrams and the lint by 'asking'
-- and 'answering', which the lint pairs.
class (Fulfilling r) => Exchange arr (r :: Reach) where
  data Future arr r a

  -- | The ends of a new ticket, made on this core.
  ticket :: arr () (Future arr r a, Promise r a)

  -- | A new ticket whose receiver is already committed to await. A value fulfilled before the await is held for it.
  ticketAwaited :: Arrow arr => arr () (Future arr r a, Promise r a)
  ticketAwaited = ticket >>> first willAwait
  {-# INLINE ticketAwaited #-}

  -- | Waits until a sender hands a value over, and returns it. Only on the core that made the ticket, which the arrow's type says.
  await :: arr (Future arr r a) a

  -- | Hands the value to the receiver, and says whether it took it: it did if it is parked, or committed to take, and it didn't if
  -- none is, or the ticket was abandoned, and then the value is the sender's. It never waits.
  fulfil :: arr (Promise r a, a) Bool

  -- | The receiver closes the ticket, which it does as its scope ends: a later 'fulfil' says it didn't take the value, a sender that
  -- waits gets its value back, and a part that watches is let go. It returns the value that was handed to the receiver while it was
  -- committed to take, and that it never awaited, which is the receiver's.
  abandon :: arr (Future arr r a) (Maybe a)

  -- | Waits as 'await' does, and runs the action at the moment the receiver parks, in the same atomic step, if no sender waits
  -- already. The action is given the promise, to put in a structure that senders share, such as the queue of waiters of a pool. It
  -- may give a value instead, which the receiver takes at once without parking.
  awaitRegistering :: arr (Future arr r a, Promise r a -> IO (Maybe a)) a

  -- | The receiver is about to hand its promise out, with steps that may wait, and then to await the future, which it gives back: a
  -- 'fulfil' in between is held for it, as one at its await is. It does not wait.
  willAwait :: arr (Future arr r a) (Future arr r a)

  -- | Hands the value over, and waits until the receiver takes it. It returns 'Nothing' if the receiver took it, and @'Just' value@ if
  -- the ticket was abandoned first: the value is the sender's still, as it is when the sender is cancelled. A second sender that
  -- waits while one waits fails.
  handWaiting :: arr (Promise r a, a) (Maybe a)

  -- | Waits until the receiver is parked, and takes nothing. One part watches at a time. A watcher is let go when the ticket is
  -- abandoned too, since no receiver will park: a hand then gives its value back.
  untilParked :: arr (Promise r a) ()

-- | The cheapest reach that an arrow has: that of the tickets whose parts all
-- run on one core, which is @'Here s@ for an arrow with a region @s@, and
-- @'Anywhere@ for one that has no cores to belong to.
type family Local (arr :: Type -> Type -> Type) :: Reach

-- | Values that one part hands another over and over: where a ticket is for one value, an inbox lasts as long as the part
-- that owns it, and a value that comes late is good for its next collect. An inbox holds one value. The part that owns it
-- collects from its 'Inbox', on the core that made it, and closes it as its scope ends; the parts that post to it hold its
-- 'Address', which passes through arrows like any value. The reach @r@ is a ticket's: an address of reach @'Here s@
-- belongs to the core of the arrow @s@ that made the inbox, and one of reach @'Anywhere@ belongs to no core, so that the
-- parts of any core can post to it. An inbox has a name, a type-level string that no runtime looks at: diagrams draw it,
-- and the lint pairs the posts and collects by it.
class Inboxes arr (r :: Reach) where
  data Inbox arr (name :: Symbol) r a

  -- | A new inbox, made on this core, and its address.
  inbox :: (KnownSymbol name) => arr () (Inbox arr name r a, Address name r a)

  -- | Waits until the inbox has a value, and takes it. A collect that is cancelled before it has taken the value leaves
  -- it in the inbox.
  collect :: (KnownSymbol name) => arr (Inbox arr name r a) a

  -- | Posts the value, and waits while the inbox is full; posters that wait are served in the order they came. It
  -- returns 'False' if the inbox has closed, before the post or while it waited, and the value is the poster's still.
  post :: (KnownSymbol name) => arr (Address name r a, a) Bool

  -- | Closes the inbox, which the part that owns it does as its scope ends: it returns the value left in it, and every
  -- post that waits, or comes later, returns 'False'.
  closeInbox :: (KnownSymbol name) => arr (Inbox arr name r a) (Maybe a)

-- | How the body of a 'scope' ended.
data Exit = Done | Failed SomeException | Cancelled'
  deriving (Show)

-- | Which cancel scope a cancellation is of: the scope of a deadline, or of a group of concurrent parts or the halves of a race. The
-- parts inside a cancelled scope are cancelled at their next wait.
newtype ScopeId = ScopeId Int
  deriving (Eq, Ord, Show)

-- | The exception that cancels a part, thrown at its wait. It carries the scope that was cancelled, so that a
-- deadline or a race can tell its own cancellation from one that comes from outside. It is not a failure: 'tryA'
-- lets it pass, and a 'scope' reports it to its release as 'Cancelled''.
newtype Cancelled = Cancelled ScopeId
  deriving (Show)

instance Exception Cancelled

-- | A release failed. It carries how the scope's body had ended, and the release's failure: the part fails with
-- it, in place of the body's own exit, and the part that acquired the resource should discard it.
data ReleaseFailed = ReleaseFailed Exit SomeException
  deriving (Show)

instance Exception ReleaseFailed

-- | The scope that a failure cancels, if it is a cancellation.
cancelledBy :: SomeException -> Maybe ScopeId
cancelledBy failure = (\(Cancelled ident) -> ident) <$> fromException failure

-- | How a body that ended as the result says ended.
exitOf :: Either SomeException a -> Exit
exitOf = either (\failure -> maybe (Failed failure) (const Cancelled') (cancelledBy failure)) (const Done)

-- | Resources and failures, and what cancellation reaches.
--
-- A part is cancelled at its waits only ('block', 'await', and 'giveWay'): a part that waits gets it
-- at once, and one that runs, at its next wait, so that no 'now' is ever interrupted. A cancellation is of a
-- scope, and is delivered to the parts inside it, unless a 'shield' lies between: then it is kept until the part
-- has left the shield, and one that a 'grace' lies between is kept until the part has left it or its period has passed.
class Scope arr where
  -- | Acquires, runs the body with what it acquired, and releases it on every exit, however the body ended: the
  -- release is shielded, so no cancellation reaches it, and it may wait. A failure of the release is reported
  -- as 'ReleaseFailed'.
  scope :: arr a r -> arr (r, Exit) () -> arr (r, a) b -> arr a b

  -- | The failure of the body as a value. A cancellation is not caught: it passes on.
  tryA :: arr a b -> arr a (Either SomeException b)

  -- | Runs the arrow with the cancellations of scopes outside it kept until it ends. A deadline inside it still
  -- fires.
  shield :: arr a b -> arr a b

  -- | Runs the arrow with the cancellations of scopes outside it kept, as 'shield' keeps them, but for at most the duration,
  -- counted from when the cancellation arrives: then it is delivered to the parts inside, at their next wait, and not
  -- during an action that never waits. A body that ends first leaves with the cancellation still pending, so the scope that
  -- was cancelled finds it at its next wait; one that arrives after the body has ended is not kept at all. The period of each of
  -- nested regions is its own, a deadline inside still fires, and a 'shield' inside keeps the cancellation until the part has left it.
  grace :: Duration -> arr a b -> arr a b

-- | Two arrows that run at once, until the first ends.
class Race arr where
  -- | The first half to end is the result, and the other is cancelled and waited for: 'race' returns once its
  -- scopes are released. If the first to end failed, its failure is rethrown then.
  race :: arr a b -> arr a c -> arr a (Either b c)

-- | A point on the monotonic clock, in nanoseconds.
newtype Instant = Instant Word64
  deriving (Eq, Ord, Show)

-- | The time on that clock, which every instant and every turn of a scheduler is read from, so that they agree:
-- GHC's own is another clock on macOS, with another base, and ticks in microseconds.
foreign import ccall unsafe "streaming_io_clock_ns"
  monotonicNanoseconds :: IO Word64

-- | A length of time, in nanoseconds.
newtype Duration = Duration Word64
  deriving (Eq, Ord, Show)

-- | A duration of this many nanoseconds, microseconds, milliseconds or seconds.
nanos, micros, millis, secs :: Word64 -> Duration
nanos = Duration
micros = Duration . (* 1000)
millis = Duration . (* 1000000)
secs = Duration . (* 1000000000)

-- | The instant that is the duration after the instant.
plus :: Instant -> Duration -> Instant
plus (Instant instant) (Duration duration) = Instant (instant + duration)

-- | Time, only through the arrow: a program never reads the clock in an action that never waits, which would
-- make a simulation of it differ from the run.
class Time arr where
  -- | The time now, which the arrows that run every part on one thread read once for each round of their
  -- scheduler: reading it costs next to nothing.
  clock :: arr () Instant

-- | A deadline for what runs inside it: a scope, inherited by every part inside, and nested deadlines take the
-- nearest. When it passes, the parts inside are cancelled at their next wait, or at once if they wait: it is
-- soft, and no action that never waits is interrupted. It is observed at a wait or a 'giveWay'.
class Within arr where
  -- | The body with a deadline: 'Nothing' when the deadline passed first, and the body was cancelled and its
  -- scopes released. A cancellation from outside passes through.
  within :: Duration -> arr a b -> arr a (Maybe b)

  -- | The same for a deadline that the input gives, in front of the body's own input, so that a program that is built once
  -- takes its deadline from the run: a connect timeout that is a setting, or a checkout deadline that is a pool's.
  timeoutA :: arr a b -> arr (Duration, a) (Maybe b)

-- | An action that never waits, as an arrow.
class Effect arr where
  now :: (a -> IO b) -> arr a b

-- | A point where a part gives way to the others, if it has used its turn:
-- the arrows that run every part on one thread time a part's turn, and once
-- it has run for a turn's length, it lets the others go on before it. Only
-- where a part does I/O, in a link's receive and send, is there one: an action
-- that never waits doesn't give way. An arrow that doesn't need it, because
-- something else preempts its parts, as GHC's scheduler does threads, makes it
-- the arrow that does nothing.
class GiveWay arr where
  giveWay :: arr a a

-- | Parts that the scheduler serves ahead of the others. Each core has two lanes of parts, a normal one and a fast one, and its scheduler
-- takes a part from each in turn, skipping a lane that has none ready: so a part in the fast lane, whenever it is ready, runs after at most
-- one part of the normal lane, and the normal lane is never starved, as it gets every other turn at least.
--
-- * __The lane is a region of a part.__ The part is in the fast lane from when it enters the arrow until the arrow ends, however it ends:
--   by its result, by a failure, or by a cancellation.
-- * __Parts it starts begin in the normal lane.__ Those that 'together', 'race' and @spawn@ start inside the arrow are not in the fast
--   lane, whatever the part that starts them is in.
-- * __Under the Threads arrow the arrow runs as it is,__ because GHC's scheduler has no lanes.
--
-- A part is ready when its scheduler knows that it can go on: a part that waits for a descriptor or a deadline is ready when the
-- scheduler looks at them, at the start of a round of the normal lane, as it does for every part.
--
-- An arrow that has no lanes makes it the arrow that does nothing.
class FastLane arr where
  -- | Runs the arrow with its part in the fast lane, so that each time the part is ready it runs after at most one part of the normal lane.
  fastLane :: arr a b -> arr a b

-- | Waiting until an event fires.
class Block arr where
  block :: arr Event ()

-- | What a part can wait for: a descriptor that becomes readable or writable,
-- which an arrow can wait for directly, or any other event, by the
-- registration of its watch, which the wait releases, or an instant that the monotonic clock reaches. It is
-- @Until@, and not @At@, which the cursors of native-pg have.
data Event = Readable !Fd | Writable !Fd | Watched (IO Watch) | Until !Instant

-- | A socket, in any arrow that can run its tries, block, and give way: the
-- tries are the socket's own calls that never wait, and while one finds
-- nothing ready, the part blocks until the descriptor is readable or writable.
instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => Link arr Socket where
  receive =
    named (Label "receive" "Receives into the room, and blocks while the socket has no input.") $
      receiveWith (tryReceive @(ModeOf arr)) readable
  {-# INLINE receive #-}
  sendAll =
    named (Label "send" "Sends all of the pieces, and blocks while the socket takes no more.") $
      sendAllWith (trySend @(ModeOf arr)) (\_ -> pure True) writable
  {-# INLINE sendAll #-}
  -- What the wait finds is for the receive after it to read: it doesn't wait first because of a short read before the wait.
  awaitInput =
    named (Label "await input" "Blocks until the socket has input, and takes none of it.") $
      box "watch for input" (now (\sock -> receiveFirst sock *> readable sock)) >>> block
  {-# INLINE awaitInput #-}
  shutdownSend = box "shut down output" (now (`Socket.shutdown` Socket.ShutdownSend))
  {-# INLINE shutdownSend #-}
  close = box "close socket" (now Socket.close)
  {-# INLINE close #-}

-- | The event of a socket's input being ready, and of its output being.
readable, writable :: Socket -> IO Event
readable sock = Readable . Fd <$> Socket.unsafeFdSocket sock
writable sock = Writable . Fd <$> Socket.unsafeFdSocket sock

-- | A receive over a link's try, which an instance supplies: a point to give
-- way at, the try, and while it finds nothing ready, a block until the event
-- fires, and the try again. The try receives from the link into the memory at
-- the address, up to the size, and gives how many bytes, or 'Nothing' if none
-- are ready. The part gives way before it tries, so that one whose link always
-- has bytes ready still does, once for each receive.
receiveWith :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr) => (link -> Ptr Word8 -> Int -> IO (Maybe Int)) -> (link -> IO Event) -> arr (link, Room) Int
receiveWith try readyToReceive =
  giveWay
    >>> iterateWith (box "try receive" (now attempt) -| "received?") ("nothing ready" |- consume block)
  where
    attempt (link, room@(Room (Buffer memory _) offset size)) =
      withForeignPtr memory (\pointer -> try link (pointer `plusPtr` offset) size) >>= \case
        Just count -> pure (Finish count)
        Nothing -> (\event -> Again (event, (link, room))) <$> readyToReceive link
{-# INLINE receiveWith #-}

-- | A send over a link's tries, which an instance supplies: a point to give
-- way at, then it sends what the link takes of the pieces, at most 16 at a
-- time, and writes it, and while some is left, blocks until the event fires,
-- and goes on. The send gives how many bytes the link took, without waiting;
-- the flush writes what it took and hasn't written yet, without waiting, and
-- says whether nothing is left.
sendAllWith :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr) => (link -> [ByteString] -> IO Int) -> (link -> IO Bool) -> (link -> IO Event) -> arr (link, [ByteString]) ()
sendAllWith send flush readyToSend =
  giveWay
    >>> second (filter (not . BS.null))
    ^>> iterateWith (box "try send" (now attempt) -| "all sent?") ("socket full" |- consume block)
  where
    attempt (link, pieces) = case pieces of
      [] -> flush link >>= \flushed -> if flushed then pure (Finish ()) else blocked link []
      _ -> do
        sent <- send link (take 16 pieces)
        case dropSent sent pieces of
          [] -> attempt (link, [])
          rest -> blocked link rest
    blocked link rest = (\event -> Again (event, (link, rest))) <$> readyToSend link
{-# INLINE sendAllWith #-}

-- | Memory to receive into: a view of a buffer, from an offset and up to a
-- length. The room owns nothing: the buffer is the memory.
data Room = Room !Buffer !Int !Int

-- | All of the buffer, to receive into.
roomOf :: Buffer -> Room
roomOf buffer = Room buffer 0 (bufferCapacity buffer)

-- | The buffer's first bytes, without a copy: valid until it receives again.
view :: Buffer -> Int -> ByteString
view (Buffer memory _) = BSI.BS memory

-- | What a send of the pieces left, as the pieces that remain.
dropSent :: Int -> [ByteString] -> [ByteString]
dropSent _ [] = []
dropSent sent (piece : others)
  | sent >= BS.length piece = dropSent (sent - BS.length piece) others
  | otherwise = BS.drop sent piece : others
