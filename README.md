# native-pg

A PostgreSQL client in Haskell that speaks PostgreSQL's protocol over a socket,
without libpq. It reads only when asked to, supports borrowed result and
parameter streams through caller-owned workspace, and sends statements in pipelines.

- **`native-pg`:** connections (libpq's connection strings, SCRAM), statements,
  pipelines and transactions, with streamed results and parameters.
- **`native-pg-tls`:** TLS through [rustls-ffi](https://github.com/rustls/rustls-ffi)
  0.15, with libpq's `sslmode`, client certificates and SCRAM channel binding.
  It's a package of its own because it's the only one with C code, and it has
  no Windows build.

The design is in [docs/DESIGN.md](docs/DESIGN.md). The API isn't stable yet.

## Operation workspace and result ownership

Bounded integrations use an operation-scoped `Session`. Its input cursor,
output workspace and pending writes belong to the operation; the persistent
`Connection` retains transport and protocol state.
`Database.PostgreSQL.Streaming.withOperation` borrows two initialized buffers
from shared `Resources` and supplies a `Session` to its callback. Integrations
that already hold a pair use `Streaming.withBuffers`; it passes the shared descriptors directly without allocating,
copying or clearing their storage. Resource integrations request fixed shapes,
such as `(Buffer, Buffer)`, and receive that shape directly; single-buffer grants
use `Solo Buffer`. The owned connection path through `ConnectionString.connectFirst` remains
available; `Session.connection` retrieves its persistent handle.

Client code uses `ConnectionString`, `Session`, `Streaming`, `Statement`,
`Encoders`, `Decoding`, `Diagnostics`, `Snippet`, `Pipeline` and `Transaction`.
`Statement.Result`, `Session.Session` and `Session.Connection` are abstract;
statement parameter types and preparation callbacks use ordinary `Word32` OIDs.
`Encoders.Parameter` and `Streaming.ByteStream` provide streamed parameters.
The current PostgREST integration determines the supported API. `Session`
exposes the connection handle, connection closing and usability checks,
prepared-statement deallocation and cancellation. Queries and result streaming use `Pipeline`,
`Transaction` and `Statement`; raw execution, batching and row access are private.
`Session` denotes a ready application session; protocol phases are private.
Public modules live directly under `src/Database/PostgreSQL`; protocol codecs,
authentication machinery and mutable buffers live under its `Internal/`
directory and are not exposed by the library. `Transport` is the extension API
for TLS providers:
implement `Link`, build an abstract `Channel` with `newChannel`, and return it
from a `TlsClient`.

Public modules contain the API implementations. Shared settings, connection
string types, stream types and link definitions live in public submodules;
the existing imports above remain supported. `Internal` contains protocol
exchange, startup negotiation, host selection and opaque state representations.

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

For input parameters, `ParameterBorrowedStream` accepts a length-bearing
`ByteStream`; the sender consumes each chunk before pulling the next one.
Keep its source and storage alive throughout the operation.

Field EOF says only that the selected value ended. The enclosing execution
still reads `CommandComplete` and `ReadyForQuery`; a transaction succeeds only
when its completion, including `COMMIT`, succeeds. A late server error remains
an operation error even after a field was delivered. Applications that require
commit-safe responses must retain the body until the transaction completes.

Ordinary row decoding and leading fields of streaming decoders
produce owned values and can allocate memory proportional to each field.
`streamChunk` also returns an owned chunk. These paths support materialized
responses and control values such as headers and counts; they are not a bound
on application-retained memory. Use the borrowed APIs for body transfer.

## Building

`native-pg-tls` needs rustls-ffi 0.15, found through `pkg-config`. With Nix,
`nix-shell` has it, GHC, cabal and PostgreSQL.

```bash
cabal build all
```

## Testing

```bash
cabal test all --test-show-details=direct
```

All three suites use skeletest 0.4.2. Cabal builds its preprocessor along with
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
