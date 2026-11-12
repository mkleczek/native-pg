# directions: a prototype of I/O by data direction

A fresh package that shows the arrow design, apart from the code that would use
it. Nothing in native-pg, streaming-io or Warp depends on it.

- **The classes** (`Network.Directions.Classes`): `Link arr link`, whose calls
  block in the arrow that runs them (`receive`, `sendAll`, `shutdownSend`,
  `close`); and what an arrow can do: `Effect` (`now`, an action that never
  waits), `Block` (wait until a watch fires), `Concurrent` (two arrows at
  once) and `Exchange` (a ticket that one direction gives a value and another
  waits for; made as the program runs, `newTicket :: arr () (Ticket arr a)`,
  and passed through arrows like any value). None has a superclass: instances
  and users add what they need.
- **A link's instance is written once, for any arrow:** the socket's and the
  simulated end's are `(ArrowChoice arr, Effect arr, Block arr) => Link arr _`,
  a try with `now`, and a `block` on the link's watch while nothing is ready.
  The tries are streaming-io's, as they are, plus `HalfClose`, which they lack.
  `Network.Directions.Simulated` is an in-memory link whose lanes hold a set
  number of bytes.
- **Two arrows provide the capabilities:**
  - `Threads` (`Network.Directions.Threads`), a function to IO: `block` blocks
    the thread, each half of a concurrent pair has a thread of its own, and a
    ticket is a `TMVar` that blocks the thread waiting for it.
  - `Loop` (`Network.Directions.Loop`), a program as data, whose only
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

- **Cores** (`Network.Directions.Delimited`, `Pool`, `PingPong`): thread per
  core is one `Delimited` scheduler for each core, with its own poller and
  mailbox. The core is in the arrow's type, as `ST`'s state thread is:
  `Delimited s a b`, run by `runDelimited :: (forall s. Delimited s a b) -> ...`
  or by `runCores`, so a `Ticket (Delimited s) a` belongs to the one scheduler
  and can't be given a value, or waited for, by the program of another core
  (`RegionSpec` shows the compiler refusing, with `-fdefer-type-errors`). What
  crosses cores is a `Remote` ticket: an `Awaiting` end, on the core that made
  it, and a `Giver`, which has no core in its type and can be kept in shared
  structures. A `give` to a parked waiter puts its continuation in the
  waiter's core's mailbox and wakes that core's poller (`EVFILT_USER` on
  kqueue, an eventfd on epoll) if it sleeps in it; the scheduler takes its
  mailbox each round and before it sleeps. `Pool` is a connection pool written
  once for any arrow with `Remote`, with one reference to the free connections
  and the givers of the waiters.
- **Helpers** (`Network.Directions.Combinators`): what programs need so that a
  value is made where it is used and not carried through the arrows between:
  `timesWith` (a step this many times, on a state), `untilJust` (a retry that
  gives way), `keeping` (an arrow's result next to the input), and `expect` and
  `call` for a request that carries the end its answer is waited at, with
  `expectLocal` and `callLocal` for tickets of a core.

- **Scopes and cancellation** (`Scope`, `Race`; `Network.Directions.Cancel`):
  `scope acquire release body` releases on every exit, behind a shield, and
  the release may wait; `tryA` gives a failure as a value and lets a
  cancellation pass; `shield` keeps the cancellations of scopes outside it
  until it ends; `concurrently` cancels the other half when one fails, waits
  for it, and rethrows the first failure; `race` cancels the loser and waits
  for it. A part is cancelled at its waits only (`block`, `await`,
  `awaitRemote`, `giveWay`), so no `now` is interrupted. Every arrow has them:
  `Threads` throws once to a part that waits, `Delimited` resumes the part
  with the exception, `Compiled` unwinds explicit failure continuations, and
  `Loop` unwinds frames. A shutdown of a `Delimited` or `Compiled` run
  cancels every part that holds something, and runs its release. The pool's
  `checkout` gives up its place when cancelled.
- **Time** (`Time`, `Within`; `Network.Directions.Timers`): `clock`, `block
  (Until instant)`, `sleep`, and `within d body`, a deadline that is a scope
  inherited by everything inside it. `Delimited` and `Compiled` keep a timer
  wheel for each core, read the clock once a round, and wait in the poller
  until the next timer; `Threads` uses GHC's timer manager; a `runSimulation`
  has a virtual clock that jumps to the earliest deadline. Every instant, and
  every turn of a part, is read from one clock, `monotonicNanoseconds`, and not
  from GHC's, which ticks in microseconds on macOS and has another base there.

Run from the repository's root, in its `nix-shell`:

```sh
cabal test directions --test-options='+RTS -N4'
cabal run directions-bench -- 64 +RTS -T
cabal run directions-cores-bench -- pool delimited 4 16 8 2 +RTS -T -N4
```
