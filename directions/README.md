# directions: a prototype of I/O by data direction

A fresh package that shows the design of `docs/DIRECTIONS.md` with commands
instead of a monad, apart from the code that would use it. Nothing in
native-pg, streaming-io or Warp depends on it.

- **Stages** (`Network.Directions.Stage`): a stage's state is data and its step
  a function fixed when it is made. A step never waits: it moves what it can
  and returns one of three commands, `Again`, `Await` with what it waits for
  (its input, room in its output, or an event outside the direction), or
  `Done`. Stages compose with `~>` through a bounded gap.
- **Directions** are stages whose first stage reads nothing and whose last
  writes nothing, such as `receiving link ~> sending other`.
- **Drivers** (`Network.Directions.Drive`) are the only code that waits, and
  they differ only in how: `runThreads` gives each direction a thread,
  `runLoop` runs all of them on one thread, and `runSimulation` runs them on
  one thread in an order a seed chooses, never waits, and reports directions
  that wait for each other.
- **Ends** (`Network.Directions.Ends`): stages over streaming-io's `Link`, as
  it is, plus `HalfClose`, which it lacks. `Network.Directions.Simulated` is an
  in-memory link whose lanes hold a set number of bytes.
- **The proxy** (`Network.Directions.Proxy`): a direction each way, an echo
  server, a client in two directions, and the same client as one stage, which
  deadlocks once its request outgrows the buffers on the way.

Run from the repository's root, in its `nix-shell`:

```sh
cabal test directions
cabal run directions-bench -- 64 +RTS -T
```
