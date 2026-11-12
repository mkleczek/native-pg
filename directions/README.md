# directions: a prototype of I/O by data direction

A prototype that shows the arrow design, apart from the code that would use it.
The runtime it began with, the classes, the arrows, the scheduler, tickets,
scopes, the pool and the reader, is now in `streaming-io`, under
`Network.Streaming`, and is described below with the modules' new names. What
stays here is what only the prototype uses: the proxy and what it is tried
with (`Network.Directions.Proxy`), the ping-pong programs, the link over bytes in
memory, the row parsers (`Rows`, `Resumable`) and the benchmarks. Nothing in
native-pg, streaming-io or Warp depends on this package, but native-pg's cursor
tests and benchmark use its parsers.

- **The classes** (`Network.Streaming.Classes`): `Link arr link`, whose calls
  block in the arrow that runs them (`receive`, `sendAll`, `awaitInput`,
  `shutdownSend`, `close`), and take the link as an input, like the other
  values that are known only when the program runs: a reader, a cursor, a
  ticket; and what an arrow can do: `Effect` (`now`, an action that never
  waits), `Block` (wait until a watch fires), `Concurrent` (two arrows at
  once) and `Exchange arr r` (a ticket that one direction gives a value and
  another waits for; made as the program runs, `ticket :: arr () (Waiting arr r a, Giver r a)`,
  and its ends passed through arrows like any value; the ticket's reach `r` is
  in its type, below). None has a superclass: instances and users add what they
  need.
- **A link's instance is written once, for any arrow:** the socket's and the
  simulated end's are `(ArrowChoice arr, Effect arr, Block arr) => Link arr _`,
  a try with `now`, and a `block` on the link's event while nothing is ready.
  The tries are the link's own calls that never wait (`receiveWith` and
  `sendAllWith` of `Classes` build the calls of the class from them), and
  `awaitInput` blocks on the event of its input without taking any.
  `Network.Streaming.Simulated` is an in-memory link whose lanes hold a set
  number of bytes.
- **Two arrows provide the capabilities:**
  - `Threads` (`Network.Streaming.Threads`), a function to IO: `block` blocks
    the thread, each half of a concurrent pair has a thread of its own, and a
    ticket is a `TMVar` that blocks the thread waiting for it.
  - `Loop` (`Network.Streaming.Loop`), a program as data, whose only
    primitives are `now` and `block`. `runLoop` runs all of its concurrent
    parts on one thread; a part that blocks is the rest of its program, as
    data. A ticket is a slot on that thread: a part that waits for it is
    parked there, with no watch, and the scheduler takes it back when the
    ticket is given its value. `runSimulation` runs the same program in an
    order a seed chooses, never waits, and reports parts that wait for each
    other or for tickets that nothing will give a value.
- **The proxy** (`Network.Directions.Proxy`): `pump`, written once with
  `ArrowChoice` and `Link`; the proxy, a pump each way; an echo server; a
  client in two directions, and the same client in one, which deadlocks once
  its request outgrows the buffers on the way; and a transaction, whose
  request side waits for the decision of its answer side to send COMMIT or
  ROLLBACK.

- **Cores** (`Network.Streaming.Delimited`, `Pool`, `PingPong`): thread per
  core is one `Delimited` scheduler for each core, with its own poller and
  mailbox. The core is in the arrow's type, as `ST`'s state thread is:
  `Delimited s a b`, run by `runDelimited :: (forall s. Delimited s a b) -> ...`
  or by `runCores`. One ticket API, `Exchange arr r`, with the ticket's reach in
  its type: `data Reach = Here Type | Anywhere`. A ticket of reach `'Here s`
  (`Local (Delimited s)`) belongs to the one scheduler: its `Giver ('Here s) a`
  mentions `s`, so it and its `Waiting` end can't be given a value, or waited
  for, by the program of another core (`RegionSpec` shows the compiler refusing,
  with `-fdefer-type-errors`), and the arrow keeps it as a plain slot, with no
  atomic operation. A ticket of reach `'Anywhere` has a `Giver 'Anywhere a`,
  which has no core in its type and can be kept in shared structures, and a
  `Waiting` end on the core that made it. A `give` to a parked waiter puts its
  continuation in the waiter's core's mailbox and wakes that core's poller
  (`EVFILT_USER` on kqueue, an eventfd on epoll) if it sleeps in it; the
  scheduler takes its mailbox each round and before it sleeps. `Pool` is a
  connection pool written once for any arrow with `Exchange arr 'Anywhere`, with
  one reference to the free connections and the givers of the waiters.
  `Threads` has the one reach, `'Anywhere`, and `Compiled` and `Loop` only
  `Local`'s.
- **Helpers** (`Network.Streaming.Combinators`): what programs need so that a
  value is made where it is used and not carried through the arrows between:
  `timesWith` (a step this many times, on a state), `untilJust` (a retry that
  gives way), `keeping` (an arrow's result next to the input), and `expect` and
  `call` for a request that carries the giver its answer is given at, for a
  ticket of any reach.
- **An environment** (`Network.Streaming.With`): `With r arr a b` is an arrow of
  `arr` that is given an `r` besides its input, which `ask` gives back, so that
  a program that reads a link or a cursor in many steps doesn't thread it
  through them. It has the capabilities of the arrow it lifts, `lift` and `with`
  go between the two, and every method is INLINE, so that the environment is a
  part of the input for an arrow that is known where the program is built. A
  step on a hot path tells what comes next with a sum that has nothing in it,
  and keeps what the next step needs in the environment: a sum that carries a
  cursor is built, and allocates, each time it is chosen.

- **Scopes and cancellation** (`Scope`, `Race`; `Network.Streaming.Internal.Cancel`):
  `scope acquire release body` releases on every exit, behind a shield, and
  the release may wait; `tryA` gives a failure as a value and lets a
  cancellation pass; `shield` keeps the cancellations of scopes outside it
  until it ends; `concurrently` cancels the other half when one fails, waits
  for it, and rethrows the first failure; `race` cancels the loser and waits
  for it. A part is cancelled at its waits only (`block`, `await`,
  `giveWay`), so no `now` is interrupted. Every arrow has them:
  `Threads` throws once to a part that waits, `Delimited` resumes the part
  with the exception, `Compiled` unwinds explicit failure continuations, and
  `Loop` unwinds frames. A shutdown of a `Delimited` or `Compiled` run
  cancels every part that holds something, and runs its release. The pool's
  `checkout` gives up its place when cancelled.
- **Time** (`Time`, `Within`; `Network.Streaming.Internal.Timers`): `clock`, `block
  (Until instant)`, `sleep`, and `within d body`, a deadline that is a scope
  inherited by everything inside it. `Delimited` and `Compiled` keep a timer
  wheel for each core, read the clock once a round, and wait in the poller
  until the next timer; `Threads` uses GHC's timer manager; a `runSimulation`
  has a virtual clock that jumps to the earliest deadline. Every instant, and
  every turn of a part, is read from one clock, `monotonicNanoseconds`, and not
  from GHC's, which ticks in microseconds on macOS and has another base there.

Run from the repository's root, in its `nix-shell`:

```sh
cabal test streaming-io --test-options='+RTS -N4'
cabal test directions --test-options='+RTS -N4'
cabal run directions-bench -- 64 +RTS -T
cabal run directions-cores-bench -- pool delimited 4 16 8 2 +RTS -T -N4
```
