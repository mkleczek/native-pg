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
    Runtime,
    ArrowApplicative,
    Concurrent (..),
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
    Annotate (named, wires),
    taking,
    giving,
    Effect (..),
    GiveWay (..),
    Block (..),
    Event (..),
    Reach (..),
    Giver,
    Exchange (..),
    Local,
    receiveWith,
    sendAllWith,
    Room (..),
    roomOf,
    view,
  )
where

import Control.Arrow (ArrowChoice, second, (>>>), (^>>))
import Control.Exception (Exception, SomeException, fromException)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Kind (Type)
import Data.Word (Word64, Word8)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (Ptr, plusPtr)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Annotate (Annotate (..), Label (..), box, giving, taking)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Internal.Socket (receiveFirst, tryReceive, trySend)
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.Plumbing (pattern Again, pattern Finish, consume, iterateWith)
import Network.Streaming.Internal.Tickets (Giver, Reach (..))
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

-- | What a program needs of the arrow it runs in, so that it can be written
-- once and run by any of them: the capabilities of its classes, and tickets
-- at both reaches.
type Runtime arr =
  ( ArrowChoice arr,
    ArrowApplicative arr,
    Effect arr,
    Block arr,
    GiveWay arr,
    Scope arr,
    Race arr,
    Within arr,
    Time arr,
    Concurrent arr,
    Annotate arr,
    Exchange arr (Local arr),
    Exchange arr 'Anywhere,
    Instrumented (ModeOf arr)
  )

-- | Values that one direction gives another, once each: the way directions
-- exchange what they know. A ticket has two ends, a 'Waiting' end that the
-- part which needs the value waits at, on the core that made the ticket, and
-- a 'Giver' that gives it, which the part that has the value keeps. A ticket
-- is made when the program runs, and its ends are passed through arrows like
-- any value, so a part can make one for each thing it waits for.
--
-- The ticket's reach @r@ says in its type where the giver can be used. A giver
-- of reach @'Here s@ belongs to the core of the arrow @s@ that made the ticket,
-- as the arrow's own region does: only the parts of that core can give it a
-- value, and the arrow can keep its slot as plain memory, with no atomic
-- operation, and its parts on its own run list. A giver of reach @'Anywhere@
-- belongs to no core, so that shared structures can hold it, and the parts of
-- any core can give it a value: it costs what a value that crosses cores costs.
-- An arrow has the instances for the reaches it can keep.
class Exchange arr (r :: Reach) where
  data Waiting arr r a

  -- | The ends of a new ticket, made on this core.
  ticket :: arr () (Waiting arr r a, Giver r a)

  -- | Waits until the ticket is given its value, and gives it. Only on the
  -- core that made it, which the arrow's type says.
  await :: arr (Waiting arr r a) a

  -- | Gives the value to the part that waits at the other end, and says whether
  -- it did: it didn't if the waiter has gone, and the value is the giver's
  -- again. A ticket is given one value once.
  give :: arr (Giver r a, a) Bool

  -- | The waiter stops wanting the value, before it waits for it: a later
  -- 'give' says it didn't. If the value was given already, it is the waiter's,
  -- and this gives it back.
  abandon :: arr (Waiting arr r a) (Maybe a)

-- | The cheapest reach that an arrow has: that of the tickets whose parts all
-- run on one core, which is @'Here s@ for an arrow with a region @s@, and
-- @'Anywhere@ for one that has no cores to belong to.
type family Local (arr :: Type -> Type -> Type) :: Reach

-- | Arrows that run at once on the same input, as '&&&' runs two of them one after the other.
class Concurrent arr where
  -- | Runs the parts, each on the input, as the parts of one cancel scope of their own, and ends when all have ended.
  -- The first failure cancels the others where they wait, and is rethrown once they have ended; a later failure is
  -- dropped. No part is made while it runs: the parts are known when the program is built. @together []@ ends at
  -- once, and one part may run as it is.
  together :: [arr a ()] -> arr a ()

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
-- has left the shield.
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
    >>> iterateWith (giving "received?" (box "try receive" (now attempt))) (taking "nothing ready" (consume block))
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
    ^>> iterateWith (giving "all sent?" (box "try send" (now attempt))) (taking "socket full" (consume block))
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
