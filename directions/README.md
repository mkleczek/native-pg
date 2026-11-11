# directions: a prototype of I/O by data direction

A fresh package that shows the arrow design, apart from the code that would use
it. Nothing in native-pg, streaming-io or Warp depends on it.

- **The classes** (`Network.Directions.Classes`): `Link arr link`, whose calls
  block in the arrow that runs them (`receive`, `sendAll`, `shutdownSend`,
  `close`); and what an arrow can do: `Effect` (`now`, an action that never
  waits), `Block` (wait until a watch fires) and `Concurrent` (two arrows at
  once). None has a superclass: instances and users add what they need.
- **A link's instance is written once, for any arrow:** the socket's and the
  simulated end's are `(ArrowChoice arr, Effect arr, Block arr) => Link arr _`,
  a try with `now`, and a `block` on the link's watch while nothing is ready.
  The tries are streaming-io's, as they are, plus `HalfClose`, which they lack.
  `Network.Directions.Simulated` is an in-memory link whose lanes hold a set
  number of bytes.
- **Two arrows provide the capabilities:**
  - `Threads` (`Network.Directions.Threads`), a function to IO: `block` blocks
    the thread, and each half of a concurrent pair has a thread of its own.
  - `Loop` (`Network.Directions.Loop`), a program as data, whose only
    primitives are `now` and `block`. `runLoop` runs all of its concurrent
    parts on one thread; a part that blocks is the rest of its program, as
    data. `runSimulation` runs the same program in an order a seed chooses,
    never waits, and reports parts that wait for each other.
- **The proxy** (`Network.Directions.Proxy`): `pump`, written once with
  `ArrowChoice` and `Link`; the proxy, a pump each way; an echo server; a
  client in two directions, and the same client in one, which deadlocks once
  its request outgrows the buffers on the way.

Run from the repository's root, in its `nix-shell`:

```sh
cabal test directions
cabal run directions-bench -- 64 +RTS -T
```
