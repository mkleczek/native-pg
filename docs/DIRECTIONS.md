# I/O by data direction

Draft for review. It proposes replacing the hand-written wait loops of
native-pg, streaming-io and the Warp fork with code written once per data
direction, as arrows, against a few classes: a link whose calls block in the
arrow that runs them, and what an arrow can do for it (run an action that never
waits, block until an event fires, run two arrows at once). Two arrows implement
them: `Threads`, a function to IO with a thread for each concurrent part, and
`Loop`, a program as data that one thread runs, or a simulation runs in an order
a seed chooses. It adds no dependency. It builds on `STREAMING-IO.md`: it keeps
the layers below the waits (output, gathering, loans, scopes) and replaces the
drivers.

The prototype began as the package `directions/`, apart from the code that would
use it. Its runtime (the classes, the arrows, the scheduler, tickets, scopes,
the pool and the reader) now lives in `streaming-io`, under `Network.Streaming`;
`directions/` keeps what only the prototype uses: the proxy, the link over
bytes in memory, the row parsers and the benchmarks. References are to native-pg d9d4ed6c11b3 and the Warp fork b89c12ac0037,
and to postgrestream af328fe1386c for PostgREST.

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
with bounded memory. Written against the classes (`directions/src/Network/Directions/Proxy.hs`):

```haskell
-- | One direction: what the source sends goes to the sink through the buffer,
-- until the source ends; then the sink's output ends.
pump :: (ArrowChoice arr, Link arr source, Link arr sink) => Buffer -> source -> sink -> arr () ()
pump buffer source sink = loop
  where
    loop =
      const (roomOf buffer)
        ^>> receive source
        >>> (\count -> if count == 0 then Left () else Right [view buffer count])
        ^>> (shutdownSend sink ||| (sendAll sink >>> loop))

-- | A direction each way.
proxy :: (ArrowChoice arr, Concurrent arr, Link arr client, Link arr server) => Buffer -> Buffer -> client -> server -> arr () ()
proxy requests answers client server = together [pump requests client server, pump answers server client]
```

- **The buffer is the bound, and waiting is the backpressure.** A direction
  whose sink is full stops reading its source, and TCP passes that on.
- **The directions never wait for each other.** A full sink stops only its own
  direction.
- **One coordination point:** both pumps end, and a failure of either stops the
  other.
- **Nothing in it says how it runs.** The same `proxy` runs with a thread for
  each direction, or with both on one thread, or in a simulation.

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

## Why arrows

Direction code needs to wait, and the two ways to run it disagree on what a
wait leaves behind:

- **On a thread of its own,** a wait blocks the thread, and the rest of the
  direction is the thread's stack.
- **On one thread with other directions,** a wait has to hand the thread back,
  so the rest of the direction must be a value the scheduler keeps.

A monad's bind takes the rest as a function, `m a -> (a -> m b) -> m b`, which is
opaque: the one-thread runner can only keep it as a closure, built at every
bind, or capture the stack (`control0#`). Arrows were made for this (Hughes,
2000): composition takes the rest as an arrow, `arr a b -> arr b c -> arr a c`,
which a runner can keep as data.

- **What is lost:** a later step can't be chosen from a result at run time.
  `ArrowChoice` chooses between branches that are written down; `ArrowApply`,
  which runs an arrow computed from a result, is as opaque as a monad.
- **It isn't needed.** A direction's choices are fixed branches: end or go on,
  commit or roll back. PostgREST's are too (see "PostgREST as arrows").
- **Nothing else is lost:** `Kleisli IO` is an arrow, so monadic code still
  runs where it has to, as `Threads` does.

## The classes

`streaming-io/src/Network/Streaming/Classes.hs`. None has a superclass:
instances and users add the constraints they need.

```haskell
-- | A byte stream whose calls block in the arrow that runs them.
class Link arr link where
  receive :: link -> arr Room Int              -- 0 at the end of the stream
  sendAll :: link -> arr [ByteString] ()       -- all of them, written
  awaitInput :: link -> arr () ()              -- until there is input, taking none
  shutdownSend :: link -> arr () ()
  close :: link -> arr () ()

-- | An action that never waits.
class Effect arr where
  now :: (a -> IO b) -> arr a b

-- | Waiting until an event fires, given the registration of its watch.
class Block arr where
  block :: arr (IO Watch) ()

-- | Two arrows at once on the same input, as '&&&' runs them one after the
-- other: both outputs, once both have ended.
class Concurrent arr where
  concurrently :: arr a b -> arr a c -> arr a (b, c)
```

- **A link's instance is written once, for any arrow** that can run its tries
  and block. The tries are the link's own calls that never wait (the socket's
  are `recv` and `writev`), and two helpers of `Classes`, `receiveWith` and
  `sendAllWith`, make "try, and while nothing is ready, block on the event"
  of them:

  ```haskell
  instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr) => Link arr Socket where
    receive sock = receiveWith (tryReceive sock) (readable sock)
    awaitInput sock = now (\() -> readable sock) >>> block
    ...
  ```

- **The arrows know nothing of links.** They provide `now`, `block` and
  `concurrently`, and that is all a link or a direction needs from them.
- **Programs read as what they do:** `^>>` and `>>^` put a function before or
  after an arrow, in place of `arr` chains, and there is no arrow notation,
  whose desugaring would decide what `Loop` builds. All three operators are
  `infixr 1`, so a function in the middle of a chain goes `^>>` into what
  follows it.
- **A memory region is a `Room`**: a view of a `Buffer`, made of the buffer, an
  offset and a length. A `Buffer` is streaming-io's, the one type that owns
  memory; it gives its room and views of what it received.

## The two arrows

### Threads: a thread for each concurrent part

`streaming-io/src/Network/Streaming/Threads.hs`: a newtype over `Kleisli IO`.

- **`now`** is the action, and **`block`** is `awaitWatch`: the thread waits.
- **`concurrently`** runs each arrow on a thread of its own and joins them; a
  failure of either stops both and is rethrown.
- **The directions may run in parallel,** on different capabilities.

### Loop: every part on one thread

`streaming-io/src/Network/Streaming/Loop.hs`: a program as data.

```haskell
data Loop a b where
  Arr :: (a -> b) -> Loop a b
  Seq :: Loop a b -> Loop b c -> Loop a c
  First :: Loop a b -> Loop (a, c) (b, c)
  Choose :: Loop a b -> Loop (Either a c) (Either b c)
  Both :: Loop a b -> Loop a c -> Loop a (b, c)
  Now :: (a -> IO b) -> Loop a b
  Block :: Loop (IO Watch) ()
```

- **Two primitives:** `Now` and `Block`. The rest is the structure of arrows.
- **A part is the program left and what follows it, as data:** the rest of a
  sequence, the other half of a pair, a choice to wrap, or the join of a
  concurrent pair. A part that blocks keeps those and its watch; nothing is
  captured, and no `control0#` is needed.
- **`runLoop`** runs each part that can go on in turn, for up to 64 steps of
  its primitives, and when none can, waits once on all of their
  watches.
- **`runSimulation seed`** runs the same program one step at a time, a part
  picked at random among those that can go on. It never waits: when no part
  can go on, it looks at their watches and goes on with those that have
  fired; if none has, the parts wait for each other, which it reports with
  their numbers. Over simulated links (`Simulated.hs`, lanes that hold a set
  number of bytes), a seed always gives the same run.

## PostgREST as arrows

postgrestream already has the two directions' shapes (`MainTx.hs`):

- **The request side is `Applicative`,** which is less than an arrow needs.
  `Steps tx` builds a `Tx.Pipeline`: transaction variables, the pre-request
  function, the EXPLAIN count and the main statement are known, keyed and sent
  before any result is read.
- **The only request-side choice is the ending,** made before anything is sent:
  with no check that can roll back, `finalPipeline` sends COMMIT in the batch;
  otherwise the ending waits for the checks (point 3).
- **The answer side reads in order** with fixed branches: the EXPLAIN total if
  there is one, the first row, `checkCrud`, which condemns on a failure, the
  pure `actionResponse`, then owned bytes or a streamed field, whose last chunk
  waits for COMMIT's reply on a write. `ArrowChoice` covers it.
- **A request's program is built from its plan before it runs,** which is a
  value, not `ArrowApply`. Server tasks (accepting connections, the schema
  cache's listener, reloads, the admin server) aren't request programs and
  stay IO. Running several requests in one program, as multi-request
  transactions would, needs `ArrowApply`, or a fresh program for each request,
  which the driver can start.

## What it replaces

| today | becomes |
|---|---|
| `receiveWaiting`, `readStalls`, the push inside a read | gone: a read only reads |
| `pollStep`, `channelPoll`, `channelPush`, `Receiver`, `Stall`, `Poll`, `pollInput`, `borrowInputWaiting` | gone: they exist so that a read can give up and be waited for elsewhere |
| `Stalls`, `Resume`, `waitingStalls`, `stopping`, `Pushed`'s watch | gone: `sendAll` blocks in its arrow |
| `WaitingSource`, `Field`'s wait, `endingWaiting`'s wait, `watchStalled` | arrows that block |
| `Link`'s non-waiting calls | the tries inside each link's instance |
| `awaitAny`, `awaitReadOrWrite`, `Interest`'s record | `block` on the link's watch |
| Warp's `answering`, its kept watch, the one-byte probe, `Client` | the answer direction's `sendAll`; the request direction reads the client |
| Warp's `Waiting` (`OnBytes`, `OnLoan`, `OnRegistration`) | a `block` on a loan's registration |
| `Channel`'s fields | the connection's two halves, each a link of one direction |
| `contextPending` and the one batch of `queueContextOutput` | the queue of expected answers |
| `Watch`, `awaitFirst`, `Buffer`, `BufferPool`, `Scope`, `Output`, the gather | kept |

- **`STREAMING-IO.md`'s other goals stand:** one gathered write per batch, the
  header leaving at once when the body isn't ready, bounded memory, zero copy,
  specialized hot paths.
- **Two of its invariants change:** "One owner per link" becomes one owner per
  half, and "Teardown releases without joining" holds for `Loop` only: in
  `Threads` the concurrent parts are joined.
- **"Why not conduit or streamly" stands,** except the point that waiting on
  two peers is done with threads: here it may be, by choice.

## The prototype

The client sends a request through the proxy to an echo server, which sends it
back; the round trip is one program over four ends.

- **Tests** (`directions/test` and `streaming-io/test`, all pass): the round trip with `Threads`, with
  `Loop`, and in simulations with 20 seeds; over socket pairs with `Threads`
  and `Loop`; a transform on the client's bytes; the same seed giving the same
  run; a client in one direction (`sendThenReceive`) deadlocking, and the
  simulation naming the four parts that wait; the same client finishing while
  its request fits in the buffers on the way; a failing part stopping the
  others under `Threads`.
- **Bench** (`directions-bench 64 +RTS -T`, a Mac, 64 MiB each way, not
  optimized):

  | run | MiB/s | steps/MiB | waits/MiB | allocated/MiB |
  |---|---|---|---|---|
  | Threads, socket pairs | 300–400 | – | – | 4.1 MB |
  | Loop, socket pairs | 140–175 | 2,370 | 256 | 9.8 MB |
  | Loop, simulated links | 1,500–2,100 | 736 | 32 | 15.4 MB |

  The wide ranges are runs on a busy machine. Not optimized: every wait
  registers watches, and `runLoop` registers all of the waiting parts'; `Loop`
  interprets its program node by node; the simulated lanes copy.

## Costs and risks

- **`Loop` interprets.** Each step walks the program's nodes and allocates
  what follows as data. Whether a static program can be specialized or
  compiled ahead into a state machine, and what `Loop` costs against
  `Threads` in PostgREST, are open.
- **A thread per request, in `Threads`.** A fork costs microseconds and a small
  stack; starting the answer direction wakes a thread, which a query hides
  until the run queue's delay is longer than it (milliseconds at CPU
  saturation in PostgREST load tests). `Loop` doesn't pay it.
- **Waits that aren't links'.** The pool's checkout, buffer loans, admission
  and timeouts wait too, so each needs a try, and else a registration whose
  watch `block` waits for, keeping its place in its queue, as the registering
  borrow of loans does. The pool's state stays IO and STM.
- **Releases.** A connection or a loan has to go back however its direction
  ends, which needs one more capability, a bracket: masking and an exception
  handler in `Threads`, a frame that runs the release when a part ends or is
  dropped in `Loop`.
- **TLS's lock.** Both halves share one session. Under `Threads` each try takes
  a lock (never held across a wait); under `Loop` none is needed. The arrow
  picks it when the connection is made, as a type (`Lock Threads = MVar ()`,
  `Lock Loop = ()`); kTLS on Linux would remove it by splitting the session,
  which needs rustls-ffi to export the session's secrets (0.15.3 can't).
- **Read-ahead.** `pump` receives only between sends, so it holds what one
  receive brought, not a full buffer. A pump that reads ahead while it sends
  needs a buffer it can receive into meanwhile, which is a design choice per
  direction.
- **Specialization.** Code over the classes is polymorphic in the arrow; on hot
  paths it needs a `SPECIALIZE` for `Threads` and for `Loop`, which Core checks
  confirm.
- **Warp's order.** Warp reads the next request only once the response has
  ended, so pipelining needs the fork to let the request direction read ahead.

## Order of changes

Each step is its own change, tests first, checked as before: no new warnings or
hlint hints, the suites, Core of the hot paths, and an allocation A/B within its
limits.

1. **Prototype** (done): `directions/`, the classes, `Threads`, `Loop`, the
   simulation and the proxy.
2. **The classes in streaming-io,** with `Threads` and `Loop`, the socket's and
   TLS's `Link` instances, and a bracket.
3. **The other waits:** loans, admission, the pool's checkout and timers as a
   try and a registration that `block` waits for.
4. **native-pg's connection as two directions:** the request side's pipeline
   and the answer side's reading in order, joined by the queue of expected
   answers.
5. **Warp:** the answer direction sends the answer; the request direction reads
   the body. `answering`, the kept watch, the probe and `WaitingSource` go.
6. **PostgREST:** a request's program built from its plan, run by `Threads`.
7. **`Loop` in the server,** an A/B against `Threads` by switching the arrow.
8. **The simulation's tests** of interleavings: early failures, a client that
   goes away, timeouts at every coordination point, pipelining.

## Open questions

- **Which direction runs on Warp's connection thread:** the answer direction,
  which keeps the fork's `Answer` API, or the request direction, the client's
  lasting reader, which pipelining needs.
- **The queue as API:** native-pg's public API could be the queue itself (send
  a statement, get a ticket), with today's transaction API on top of it.
- **`Loop`'s cost:** interpreting against specializing or compiling static
  programs.
- **TLS in `Threads`:** the lock, or kTLS.
- **Pipelining:** how many requests are read ahead, and whether a request's
  statements share the previous request's connection.
