# I/O by data direction

Draft for review. It proposes replacing the hand-written wait loops of
native-pg, streaming-io and the Warp fork with code written once per data
direction, in direct style. `Link` is parametrized by the monad that runs the
directions, and blocks in that monad's way. Each link's instance states what it
needs of the monad through small capability classes, and each monad provides
them: IO with a thread per direction, `Loop`, which runs both directions on one
thread and suspends with GHC's delimited continuations, and a deterministic
simulator for tests. It adds no dependency. It builds on `STREAMING-IO.md`: it
keeps the layers below the waits (output, gathering, loans, scopes) and replaces
the drivers.

References are to native-pg d9d4ed6c11b3 and the Warp fork b89c12ac0037.

## Why

**There is no event loop.** Every place that can't go on waits on its own, for
what it knows of:

| loop | reads | sends | waits for |
|---|---|---|---|
| `receiveWaiting`, `Channel.hs:119` | yes | pending output | input, room, an upload's source |
| `pollStep`, `Channel.hs:140` | yes | pending output, without waiting | gives a watch |
| `channelPush`, `Channel.hs:83` | no | pending output, without waiting | gives a watch |
| `sendOutput`, `Channel.hs:167` | no | one output | room, an upload's source |
| `borrowInputWaiting`, `Codec/Input.hs:275` | yes, through `Receiver` | through `receivePush` | the caller's wait, given a `Stall` |
| Warp `receiveInto`, `LinkConnection.hs:130` | yes | nothing | input, shutdown |
| Warp `answering`, `LinkAnswer.hs:101` | a one-byte probe | the answer | the kept readable watch, room, the body's source |
| TLS handshake, `Tls/Internal.hs:252` | yes | yes | input or room |

- **Ten types say what a wait is for:** `Stalls`, `Resume`, `Pushed`,
  `Interest`, `Blocked`, `Stall`, `Poll`, `Receiver`, and Warp's `Waiting` and
  `Client`. Each covers the part that its loop sees.
- **Rules are restated per loop.** "After the rest of the output is sent, read
  before waiting" is at `Channel.hs:132`, and again at `Channel.hs:149`.
- **The loops differ where they shouldn't.** `sendOutput` waits with
  `waitingStalls` (`Sender.hs:180`), which read nothing. If the server's
  answers fill the socket while a long send waits for room, each side waits for
  the other: the deadlock that `receiveWaiting` exists to avoid. It sends for
  `sendContextOutput` and the startup and session messages; whether any of them
  sends that much hasn't been checked.
- **The sender hands control back through callbacks.** `Stalls` answer `Again`
  or `Stop` because the push can't return to a loop that decides what comes
  next: there is none.

**The cause: one stack, two concerns.** PostgREST is a transform with two
inputs, the client's bytes and the server's, and two outputs. Written in direct
style on one stack, it waits for one input at a time.
- **Enough for half duplex:** a request, then its answer.
- **Not enough where they overlap:** an upload overlaps the server's answers to
  earlier statements, or its early failure. The stack is in the upload, and the
  answers need attention too.
- **What it takes today:** `receiveWaiting`, `Stalls` and the waits in place
  (`WaitingSource`, `Field`'s wait, `endingWaiting`) hop between the two
  concerns on one stack.

**"No helper threads" was about other threads.** `STREAMING-IO.md` set it as a
goal against a producer thread that sat inside a callback-scoped source, and two
waiter threads forked for every wait for "the source or the socket". Those
threads shared a link and handed data across. A thread per direction does
neither.

## A proxy first

A proxy copies a client's bytes to a server and the server's back, streaming,
with bounded memory. With a thread per direction:

```haskell
proxy :: (Link a, Link b) => a -> b -> Int -> IO ()
proxy client server size =
  withAsync (pump server client) (\answers -> pump client server *> wait answers)
    `finally` (closeLink client *> closeLink server)
  where
    pump source sink = do
      memory <- mallocPlainForeignPtrBytes size
      let loop =
            receive source (Room memory size) >>= \case
              0 -> shutdownSend sink
              n -> sendAll sink [BS memory n] *> loop
      loop
```

- **The buffer is the bound, and waiting is the backpressure.** A direction
  whose sink is full stops reading its source, and TCP passes that on.
- **The directions never wait for each other.** A full sink stops only its own
  direction.
- **One coordination point:** both pumps end, and a failure of either cancels
  the other.
- **The state is each thread's stack.** The same proxy on one thread needs a
  record per direction, a pass that moves what it can without waiting, and a
  loop that waits on the union of what the passes found blocked: the event loop
  this code base lacks, written out.

PostgREST is the same proxy with a transform in each direction, coupled by
control: the answer depends on the plan, and the request depends on answers (a
failure stops the upload, and the ending depends on the result).

## Ownership

**Each half of each socket has exactly one owner at a time. Data never crosses
directions; only control does.**

- **The request direction** owns the client's input and the server's output. It
  reads the request and its body, and writes statements, parameters and the
  upload, framed, to the server.
- **The answer direction** owns the server's input and the client's output. It
  reads the answers, decodes and renders them, and writes the response, status
  and headers included.
- **No chunk is handed over,** so the data path has no queue, and each
  direction's loans are its own.
- **Ownership moves only at the coordination points,** for example the
  client's input from the request's header to its body to the next request.

This replaces `STREAMING-IO.md`'s invariant "One owner per link": a link's
halves may be used by two threads at once, one each. A raw socket allows it,
since the kernel and GHC's IO manager keep a reader and a writer of one
descriptor apart. A TLS session doesn't (see "Costs and risks").

## Coordination points

All of them:

1. **Start.** The request direction checks a connection out, then starts the
   answer direction with the connection's read half and the plan of the answer.
2. **Expected answers, request to answer.** Each message the request direction
   sends puts its expected answer on the connection's queue, in order: discard
   it (ParseComplete, BindComplete), decode it into a result, or stream it to
   the client. Sync's entry is ReadyForQuery. The answer direction takes the
   entries in order. This is the protocol's own FIFO, which pipelining drivers
   keep as tickets.
3. **Results the request depends on, answer to request.** Where the next
   statement depends on an answer (commit or roll back after `max-affected`,
   the pre-request function's outcome), the request direction waits for that
   entry's ticket. These are its only waits on the answer direction.
4. **Abort, either way:** a flag that each direction checks where it can stop.
   - **The server failed:** the request direction stops at the next message
     boundary and sends Sync. In the middle of a message it finishes the message
     or breaks the connection, as an aborted upload does today.
   - **The client is gone, or the request timed out:** the answer direction
     stops writing, and the request direction cancels the statement.
5. **End.** The request direction waits for the answer direction to end. The
   connection goes back to the pool with the ending both saw, `Completed` or
   `Aborted`, as `checkoutIn`'s scope decides today.
6. **Handover of the client's output,** for pipelining only: response N's
   answer direction passes it to N+1's when it ends.

## Cases

Overlap is the general case. The others are instances of it, with no code of
their own.

- **A read.** The request direction writes a short batch, queues its entries and
  is done; the answer direction streams the answers. The same code as an
  upload.
- **An upload with early answers.** The server's answers to earlier statements,
  or its failure, are read while the body is still going in, because the other
  direction reads them.
- **An early response.** The answer direction writes a failure before the body
  has been read; the request direction stops at the next boundary, and the body
  is drained or the connection closed.
- **A client that goes away.** The request direction sees the end of its input,
  sets the abort and cancels the statement. Today the one-byte probe of
  `answering` does this.
- **An ending that depends on a result.** A wait at point 3, then COMMIT or
  ROLLBACK. Write mode's rule that the final chunk waits for COMMIT becomes: the
  answer direction holds the final chunk until it has read the ending's entry.
- **HTTP request pipelining.**
  - The request direction reads request N+1 once N's body is consumed.
  - N+1's statements go on the same connection's queue behind N's Sync, or on a
    connection of their own. On the same connection, answers arrive in the
    order responses are written: HTTP pipelining maps one to one onto
    PostgreSQL's.
  - The client's output passes from N's answer direction to N+1's (point 6).
  - A limit on requests read ahead is the request direction's backpressure.
- **HTTP/2 bends the rule.** Concurrent streams share the client's output, and
  their frames interleave, so one writer per socket multiplexes them, and the
  data of every stream crosses into it. The rule becomes one owner per half per
  stream, and one writer per socket.

## Links and capabilities

### Link, blocking in the monad

```haskell
-- | A byte stream, in the monad that runs its directions. Calls block in the
-- monad's way: a thread waits, and a direction of a scheduler suspends.
class Monad m => Link m link where
  -- | Reads into the room: how many bytes, 0 at the end of the stream.
  receive :: link -> Room -> m Int
  -- | Sends all of the pieces, and writes them.
  sendAll :: link -> [ByteString] -> m ()
  -- | Ends this direction of the stream, once what was sent is written.
  shutdownSend :: link -> m ()
  close :: link -> m ()

-- | Memory to receive into: its start and size. Made only where the size is
-- known, from a buffer's free part or by 'withRoom'.
data Room = Room {-# UNPACK #-} !(Ptr Word8) {-# UNPACK #-} !Int

-- | The halves of a link. A direction is given one of each, of different
-- links, so that each half has one owner; direction code receives and sends
-- through them only.
newtype Reader link = Reader link
newtype Writer link = Writer link
```

### Capabilities

```haskell
-- | Waiting, and running what never waits.
class Monad m => MonadWait m where
  -- | Runs an action that never waits: a try of a link, decoding,
  -- rendering, fulfilling a ticket.
  now :: IO a -> m a
  -- | Waits until the descriptor may be tried again for the interest.
  waitFd :: Fd -> Interest -> m ()
  -- | Waits until the watch fires, and releases it: loans, tickets, timers.
  waitWatch :: IO Watch -> m ()

-- | Exclusion between the directions of one connection. The section is IO,
-- so nothing can wait while it holds the lock.
class MonadWait m => MonadLock m where
  type Lock m
  newLock :: m (Lock m)
  withLock :: Lock m -> IO a -> m a

-- | The other direction, and resources that are released however a
-- direction ends.
class MonadWait m => MonadSpawn m where
  -- | Starts the other direction, and gives the wait for its end, which
  -- rethrows its failure. The other direction ends if this one does first.
  spawn :: m a -> m (m a)
  -- | Acquires, uses and releases, also when the direction is cancelled.
  -- The release learns how the use ended.
  bracketIn :: m a -> (Ending -> a -> m ()) -> (a -> m b) -> m b
```

### Instances of links

A link's instance states what it needs, and nothing about threads:

```haskell
-- | What a try that couldn't go on waits for before it is tried again.
data Interest = Readable | Writable

-- | What a try did: something, or nothing until the interest fires.
data Try a = Done !a | Blocked !Interest

instance MonadWait m => Link m Socket where
  receive sock room = go
    where
      go = now (tryRecv sock room) >>= \case
        Done n -> pure n
        Blocked interest -> waitFd (socketFd sock) interest *> go
  ...

-- | A TLS connection belongs to the monad it was made for, whose lock guards
-- the session that the connection's two halves share.
data TlsConnection m = TlsConnection !Socket !Session !(Lock m)

instance MonadLock m => Link m (TlsConnection m) where
  receive (TlsConnection sock session lock) room = go
    where
      go = withLock lock (tryTlsReceive sock session room) >>= \case
        Done n -> pure n
        Blocked interest -> waitFd (socketFd sock) interest *> go
  ...
```

- **The tries stay non-blocking,** inside each instance: a socket's `recv` and
  `writev`, a TLS session's calls. A try says what it is blocked on: over TLS a
  receive may have to write first, and so wait to be writable, which today's
  `receiveReady` can't say.
- **One instance per link,** constrained by what it needs, not one per link and
  monad.
- **Nothing waits under the lock.** `withLock` takes IO, not `m`, so the types
  keep the waits out of the section.
- **`now` is a review rule.** Its action must not wait, which the types can't
  check, like "valid until the next pull".
- **A connection belongs to one monad.** `TlsConnection m`, and so a pool of
  them, is made for the monad that runs its directions: a server runs in one
  mode, chosen when it is built or started.

### Tickets and sources

```haskell
-- | The result of an entry of the queue, which the answer direction fulfils.
newtype Ticket a = Ticket (TMVar (Either SomeException a))

-- | A try, then a wait for the watch of the 'TMVar'.
await :: MonadWait m => Ticket a -> m a

-- | The next chunk of a source, waiting for it.
pull :: (MonadWait m, Source source) => source -> m Pulled
pull source = now (pullNow source) >>= either (\watch -> waitWatch watch *> pull source) pure
```

- **One source class.** A source has a pull that doesn't wait and gives a watch
  when nothing is ready, as `Source` has, without the stall callback; waiting is
  `pull`. `WaitingSource` exists for consumers that can wait while others have
  to give up (`STREAMING-IO.md`, "Two classes, not one"). With a direction per
  consumer, none has to give up.
- **Gathering keeps its try:** the batch is cut only where a pull would wait.
  `sendAll` gathers as `pushWith` does today, and waits for room, or for a
  source that isn't ready once what is gathered before it has gone.

### Specialization

Code over the classes is `INLINABLE`, with a `SPECIALIZE` for each pair of
monad and link that a build uses, as code over `Link` is today. Specialized to
IO, it is today's code with the lock; specialized to `Loop`, the lock is gone.
Core checks confirm both.

## Monads

### IO: a thread per direction

```haskell
instance MonadWait IO where
  now = id
  waitFd fd Readable = threadWaitRead fd
  waitFd fd Writable = threadWaitWrite fd
  waitWatch = awaitWatch

instance MonadLock IO where
  type Lock IO = MVar ()
  newLock = newMVar ()
  withLock lock = withMVar lock . const
```

- **Waits:** a socket waits on its descriptor directly, with no watch.
- **Directions:** `spawn` forks a thread in the caller's scope, and its wait
  joins it. A failure of either thread cancels the other. A cancelled direction
  stops by an asynchronous exception, and its `bracketIn` releases run.
- **Parallelism:** the directions may run on different capabilities.

### Loop: both directions on one thread

```haskell
-- | Directions on one thread. It is IO underneath, so a bind is IO's; only
-- the waits differ.
newtype Loop a = Loop (IO a) deriving newtype (Functor, Applicative, Monad)

instance MonadWait Loop where
  now = Loop
  waitFd fd interest = Loop (suspend (fdWatch fd interest))
  waitWatch = Loop . suspend

instance MonadLock Loop where
  type Lock Loop = ()
  newLock = pure ()
  withLock () = Loop -- its directions never run at once

-- | What the scheduler runs: a direction until it waits, spawns or ends.
data Step = Wait (IO Watch) (IO Step) | Spawn (IO Step) (IO Step) | Finished

-- | Captures the direction up to the scheduler's prompt with control0#, and
-- gives the scheduler what it waits for and how to go on.
suspend :: IO Watch -> IO ()
suspend watching = control0 schedulerTag (\resume -> pure (Wait watching (resume (pure ()))))
```

- **The scheduler** runs on the thread that would otherwise run the request,
  Warp's connection thread. It runs each step under `prompt schedulerTag`,
  keeps the suspended directions with their watches, waits with one
  `awaitFirst` on the union, and resumes what fired. Readiness is a hint, so
  resuming all is correct; resuming what fired is cheaper.
- **One prompt tag** serves every scheduler: `control0#` captures up to the
  nearest prompt with the tag, which is the running step's.
- **The cost is per suspension, not per bind.** Suspending copies the stack
  between the prompt and the wait to the heap. A try that finds bytes copies
  nothing, and a bind is IO's.
- **Tickets are waited for as sockets are:** a ticket's watch is a `TVar`'s,
  and a watch was never tied to a descriptor.
- **Cancellation** drops a direction's continuation and runs its releases,
  innermost first. The scheduler catches what a step throws, which ends that
  direction the same way. No asynchronous exceptions pass between directions.
- **To be verified** before it is relied on (step 1): what `control0#` may
  capture (it is believed to refuse across an STM transaction or a thunk's
  evaluation), how masking and exception handlers in the captured stack
  behave, and what a suspension costs against a thread's wake-up.
- **This is the explicit state machine** of the proxy, derived from the same
  code.

### Simulation, for tests

- **What it is:** `Loop`'s scheduler with a seed that chooses which ready
  direction runs next, simulated links (in memory, with capacities) as an
  instance of `Link`, and a simulated clock for timers, which advances when
  every direction waits.
- **Interleavings are at waits only.** That is enough here: directions interact
  only where one waits, on a half, a ticket, a loan or a timer; calls into a
  link never wait, and the lock is a no-op on one thread.
- **What it tests:** what is timing-dependent and flaky against real sockets:
  early failures during an upload, a client that goes away, timeouts at every
  coordination point, pipelining. One suite runs in every monad, which is also
  what keeps their semantics the same.
- **No dependency.** `io-sim` would add a systematic search of thread orders,
  inside STM and `MVar` code too, which direction code doesn't have between
  waits.

## What it replaces

| today | becomes |
|---|---|
| `receiveWaiting`, `readStalls`, the push inside a read | gone: a read only reads |
| `pollStep`, `channelPoll`, `channelPush`, `Receiver`, `Stall`, `Poll`, `pollInput`, `borrowInputWaiting` | gone: they exist so that a read can give up and be waited for elsewhere |
| `Stalls`, `Resume`, `waitingStalls`, `stopping`, `Pushed`'s watch | gone: `sendAll` waits in `m` |
| `WaitingSource`, `Field`'s wait, `endingWaiting`'s wait, `watchStalled` | `pull` and `await` in `m` |
| `Source`'s stall callback | a pull that doesn't wait and gives a watch |
| `Link`'s non-waiting calls, `watchReadable`, `watchWritable`, `awaitAny`, `awaitReadOrWrite` | tries inside each instance; the class blocks in `m` |
| `Interest`'s record and `Blocked` | `Interest`: readable or writable |
| Warp's `answering`, its kept watch, the one-byte probe, `Client` | the answer direction's `sendAll`; the request direction reads the client |
| Warp's `Waiting` (`OnBytes`, `OnLoan`, `OnRegistration`) | a wait for a loan's registration in `m` |
| `Channel`'s fields | the connection's `Reader` and `Writer` |
| `contextPending` and the one batch of `queueContextOutput` | the queue of expected answers |
| `Watch`, `awaitFirst`, `Buffer`, `BufferPool`, `Scope`, `Output`, the gather | kept |

- **`STREAMING-IO.md`'s other goals stand:** one gathered write per batch, the
  header leaving at once when the body isn't ready, bounded memory, zero copy,
  specialized hot paths.
- **Two of its invariants change:** "One owner per link" becomes one owner per
  half, and "Teardown releases without joining" holds for `Loop` only: in IO
  the spawned direction is joined.
- **"Why not conduit or streamly" stands,** except the point that waiting on
  two peers is done with threads: here it may be, by choice.

## Costs and risks

- **A thread per request, in IO.** A fork costs microseconds and a small stack.
  The answer direction lives for a request, not for a keep-alive connection,
  whose idle threads would each hold a stack chunk.
- **A wake-up per request, in IO.** Starting the answer direction wakes a
  thread. It overlaps the server's work, so it is hidden while a query takes
  longer than the run queue's delay. At CPU saturation that delay has been
  measured in milliseconds in PostgREST load tests. A read pays it as much as an
  upload. `Loop` doesn't pay it, which is what the A/B between them is for.
- **TLS's lock, in IO.** Both halves share one session, so each try takes the
  lock. It is never held across a wait, so it is almost never contended, and a
  free lock costs tens of nanoseconds against microseconds of encryption per
  record. `Loop` has none. Removing it in IO needs the session split by
  direction:
  - **TLS 1.3 keeps keys and sequence numbers per direction.** After the
    handshake only key updates, alerts and session tickets cross, and they are
    control: a coordination point from the reader to the writer.
  - **kTLS on Linux** splits it in the kernel: the socket then reads and writes
    plaintext, so the TLS link is a socket's, with no lock, no TLS buffers in
    user space, and decryption in place. It needs the session's secrets, which
    rustls can export and rustls-ffi 0.15.3 can't, and a recent kernel to rekey
    on receive. macOS keeps the lock.
- **`control0#`.** What it may capture, and what a suspension costs, decide
  whether `Loop` is usable at all: step 1 settles them before anything depends
  on it.
- **Specialization is required.** Without a `SPECIALIZE` for a pair, every call
  goes through dictionaries.
- **Two meanings of cancellation:** asynchronous exceptions in IO, dropped
  continuations in `Loop`. Releases go through `bracketIn` only, and the shared
  suite checks both.
- **Warp's order.** Warp reads the next request only once the response has
  ended, so pipelining needs the fork to let the request direction read ahead.

## Order of changes

Each step is its own change, tests first, checked as before: no new warnings or
hlint hints, the suites, Core of the hot paths, and an allocation A/B within its
limits.

1. **Prototype.** The proxy, in IO and in `Loop`: what `control0#` may capture,
   how masking and exceptions behave across it, and what a suspension costs
   against a thread's wake-up.
2. **Links and capabilities.** The capability classes with their IO instances,
   `Link m` with the socket's and TLS's instances, `Room`, the halves, tickets
   and `shutdownSend`, in streaming-io and native-pg-tls. No users yet.
3. **Sources.** `Source`'s pull gives a watch instead of calling a stall, and
   `pull` waits for it. `WaitingSource` stays for the transition.
4. **The connection's halves.** native-pg's connection gets a `Reader`, a
   `Writer` and the queue of expected answers, and the current API runs on
   them: a batch is written by a spawned direction, which the reads join at its
   ReadyForQuery. `receiveWaiting`, the polls and the stalls go. A request has
   several batches until step 7, so this first A/B is an upper bound of what the
   threads cost.
5. **native-pg over the capabilities.** The API becomes polymorphic in its
   monad, specialized to IO. The A/B should show no difference.
6. **Warp.** The answer direction sends the answer with `sendAll`. `answering`,
   the kept watch, the probe and `WaitingSource` go.
7. **PostgREST.** The handler becomes two directions over tickets: the request
   direction sends statements and streams the body, and the answer direction
   decides the status from the first results and streams the response. The
   final-chunk rule becomes a wait for the ending's ticket.
8. **`Loop` in the server.** The A/B against IO is a switch of monad in one
   build.
9. **Simulation.** Simulated links and clock, and the interleaving tests.

## Open questions

- **Which direction runs on Warp's connection thread.**
  - **The answer direction:** keeps the fork's `Answer` API, where Warp sends
    the response on its own thread. The request direction is spawned and reads
    the body.
  - **The request direction:** makes Warp's thread the client's lasting reader,
    which pipelining needs, and moves sending to the spawned thread.
- **The queue as API.** native-pg's public API could be the queue itself (send a
  statement, get a ticket), with today's transaction API on top of it, or keep
  the queue internal.
- **The parser:** polymorphic in the monad, or in IO with its refill given as an
  IO action. Every monad here is IO underneath, so a direction's `receive`,
  unwrapped, is an IO action that waits or suspends as its monad does, and the
  hottest code needn't be polymorphic.
- **TLS in IO:** the lock, or a split session through kTLS, which needs an
  addition to rustls-ffi.
- **Pipelining:** how many requests are read ahead, and whether a request's
  statements share the previous request's connection.
