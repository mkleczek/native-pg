# native-pg

A PostgreSQL client in Haskell that speaks PostgreSQL's protocol over a socket,
without libpq. It reads only when asked to, supports borrowed result and
parameter streams through caller-owned workspace, and sends statements in pipelines.

- **`native-pg`:** connections (libpq's connection strings, SCRAM), statements,
  pipelines and transactions, with streamed results and parameters, connection
  pooling.
- **`streaming-io`:** borrowed streams, shared request buffer capabilities and bounded body/setup pools.
- **`resource-admission`:** private waiter delivery, shared deadlines and opaque FIFO
  queues for independent resource pools.
- **`native-pg-tls`:** TLS through [rustls-ffi](https://github.com/rustls/rustls-ffi)
  0.15, with libpq's `sslmode`, client certificates and SCRAM channel binding.
  It's a package of its own because it's the only one with C code, and it has
  no Windows build.

The design is in [docs/DESIGN.md](docs/DESIGN.md). The API isn't stable yet.

## Operation workspace and result ownership

Bounded integrations use an operation-scoped `Session`. Its input cursor,
output workspace and pending writes belong to the operation; the persistent
`Connection` retains transport and protocol state.
`Database.PostgreSQL.Streaming.withOperation` receives a request's `BufferAccess`
and supplies a `Session`. Warp leases the primary before parsing each HTTP/1.1
request and passes this same capability through `Network.Wai.Streaming.Request`.
A body operation reuses the primary whenever it is free; overlapping input and
output may borrow one additional block, including from the protected reserve.
Additional blocks return promptly, while the primary stays with the request.
Parsed HTTP/protocol metadata is owned separately. Idle keep-alive and idle
PostgreSQL connections retain no body-pool lease.

`Pool.use` checks out only a database connection. The application supplies a
`Waiting` to `Pool.newPool waiting config` and
`Resources.withResources waiting settings action`. It chooses which pools share
a shutdown scope; each pool has its own waiting queue. Timed waits use GHC's
shared runtime timer manager, including across distinct `Waiting` instances.
Executables using pending timed waits must be linked with `-threaded`.
Buffer and connection returns deliver to selected private mailboxes, avoiding
resource-wide retry wakeups. Queue adapters return opaque removal handles from
enqueue; Waiting handles removal even when enqueue grants immediately.
Setup borrows a temporary pair from its separate
budget. The application keeps the service open through all pool uses and closes
it afterwards, typically with `Waiting.withWaiting`. Releasing, retiring or
collecting a pool leaves the supplied service open.

The scoped source contract is `Source { withChunk :: forall a.
(ByteString -> IO a) -> IO a }`: the bytes remain valid through the callback.
Forwarding finishes that callback before requesting another chunk. Statements
see streams, not buffer allocation. Native-pg's concrete parser remains
monomorphic. Explicit supplied-buffer `Streaming.withBuffers` and owned
sessions remain available for standalone integrations.

Client code uses `ConnectionString`, `Session`, `Streaming`, `Transaction`,
`Encoders`, `Decoding`, `Diagnostics`, `Snippet` and `Pool`.
`Database.PostgreSQL.Transaction` implements statements, applicative pipelines,
prepared-statement selection and transaction-scoped result cursors in one module.
`Transaction`, `Pipeline`, `Result`, `Field` and `RowDecoder` are abstract;
`Statement` describes SQL, parameters, its row schema and preparation eligibility.
Parameter types and preparation callbacks use ordinary `Word32` OIDs.
`Session` exposes connection controls, prepared-statement deallocation and
cancellation. Raw execution, batching, row access and protocol phases are private.
`Transport` is the extension API for TLS providers: implement `Link`, build an
abstract `Channel` with `newChannel`, and return it from a `TlsClient`.

Public modules contain their API implementations. Shared settings, connection
string types, stream types and link definitions live in public submodules.
`Internal` contains protocol exchange, startup negotiation, host selection and
opaque state representations. There are no separate statement, pipeline or pull
transaction modules.

`Transaction.runTransaction` runs a scoped `Transaction tx` action. The monad
supports `MonadIO` and `MonadUnliftIO`; use `withRunInIO` when an external API
requires an `IO` callback.
`pipeline` returns cursors without reading their results; `nextRow` advances a
cursor, and `streamingRow` exposes its last column as a nullable `Field tx`.
`fieldLength` reports that field's declared length. `nextChunk` returns borrowed
bytes valid until the next input operation or scope exit. Consume each chunk
before another pull, row advance, pipeline or transaction completion; copy bytes
that must survive those operations.

Owned results use the same cursors. `rowList`, `singleRowValue` and
`rowMaybeValue` collect rows whose schema uses `Const`; `foldRows` consumes any
row shape with a transaction action. With `Tx` importing
`Database.PostgreSQL.Transaction`, and `Const` and `Int64` imported, a query
returning one owned integer is run as:

```haskell
let query = Tx.Statement "SELECT 42::bigint" (const [])
      (Tx.singleRow (Tx.row @'[Int64] Const)) False
Tx.runTransaction Tx.ReadCommitted Tx.Read session $ do
  cursor <- Tx.pipeline (Tx.statement () query)
  value <- Tx.singleRowValue cursor
  Tx.finishTransaction
  pure value
```

`finishTransaction` drains unread rows, checks statement completion and observes
`COMMIT`, or `ROLLBACK` when condemned. An empty transaction sends nothing.
An unfinished scope drains and rolls back an open transaction. Ordinary
application exceptions raised through `liftIO` also permit this cleanup and are
rethrown with their original type; interrupted I/O and parser or parameter-source
failures discard the connection when its protocol state is unsafe.
For a streaming HTTP response, keep this scope open around `respond` and use
`withRunInIO` at the HTTP boundary to run transaction actions inside Warp's
response callback. Protocol operations check their scope and calling thread and
reject overlapping access, including re-entry from a parameter producer.
Synchronous callbacks between operations are allowed. The transaction driver
and result consumer run on the same calling thread.

For input parameters, `Encoders.Parameter` and `Streaming.ByteStream` provide
streamed values. `ParameterBorrowedStream` accepts a known length and a
callback-based `Source`; the sender consumes each chunk before ending its
callback and pulling the next one. Keep its source and storage alive throughout
the operation.

Field EOF says only that the selected value ended. A late server error remains
an operation error even after a field was delivered. A transaction succeeds only
when `finishTransaction` succeeds, including the queued ending of a
`finalPipeline`. Applications that require commit-safe responses must retain the
body until that completion.

Ordinary row decoding and leading fields of streaming decoders produce owned
values and can allocate memory proportional to each field. Owned collectors may
retain all selected rows. These paths support materialized responses and control
values such as headers and counts; they are not a bound on application-retained
memory. Use borrowed field chunks for body transfer.

## Building

`native-pg-tls` needs rustls-ffi 0.15, found through `pkg-config`. With Nix,
`nix-shell` has it, GHC, cabal and PostgreSQL.

```bash
cabal build all
```

### VS Code

Open the repository root with the Haskell extension installed. Workspace settings
start HLS through `.vscode/hls`, which enters `.vscode/shell.nix` automatically.
This uses the pinned development shell for native dependencies including rustls
and `pkg-config`. The launcher uses GHC, Cabal and HLS from GHCup consistently;
install HLS with support for the selected GHCup GHC version. The optional Nix
Environment Selector uses the same native-only shell. The launcher also uses
`cabal exec` to put project build tools, including `skeletest-preprocessor`, on
HLS's `PATH`; no separate global preprocessor installation is needed. VS Code
can be launched normally. The first start may download the pinned native
dependencies and build the test dependencies.
After changing the shell or editor settings, run **Developer: Reload Window**.
No Nix store paths or manual environment selection are needed for HLS.

## Testing

```bash
cabal test all --test-show-details=direct
```

All suites use skeletest 0.4.2. Cabal builds its preprocessor along with
the tests. Select a suite and optionally filter its test names:

```bash
cabal test native-pg-spec --test-show-details=direct
cabal test native-pg-integration --test-show-details=direct --test-option='[close]'
```

Check the installed client API, including rejected imports of private modules:

```bash
cabal build native-pg
cabal exec -- sh native-pg/test/check-api-boundary.sh
```

The integration tests start one temporary PostgreSQL cluster with the `initdb`
and `pg_ctl` found on the path. Each session wrapper opens a fresh connection
and closes it after the test, including on failure. The integration runner
registers its spec explicitly so that cluster setup stays outside the tests;
the core and TLS runners discover `*Spec.hs` modules automatically. The repository
root contains a test-only integration package that compiles both implementations
to exercise their private operations without adding client-facing exports.

For a test-first jj change, first run the regression without a marker and check
that it fails for the intended reason. Then mark that individual test with
`xfail`, giving the missing behavior or issue as the reason:

```haskell
xfail "issue-123: explain the missing behavior" $
  it "describes the regression" regression
```

Record the failing test in one change. Implement the behavior in a child change
and remove the marker there. An expected failure passes the suite; an unexpected
success (`XPASS`) fails it, so the marker cannot silently outlive the fix.
Ordinary exceptions also count as expected failures, so check the unmarked
failure before adding the marker. Existing passing tests stay unmarked.

Keep independent test/implementation pairs on separate branches and merge their
tips into the bookmarked feature change. To select expected failures in a suite,
pass `--test-option=@xfail` (a selection matching no tests fails).

## Licence

AGPL-3.0-or-later, see [LICENSE](LICENSE).
