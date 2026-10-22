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

Bounded integrations use an explicit opaque `Context`. Its input cursor,
output workspace and pending writes belong to the operation; the persistent
`Connection` retains transport and protocol state. The integration-only
`Database.PostgreSQL.Internal.Context.withContext` binds two disjoint initialized
buffers for its callback. `Buffer.bufferFromInitializedMemory` adapts storage
already initialized by a shared pool without allocating, copying or clearing it.
The existing owned `Session` API remains available.

`columnChunks` and typed `streamChunks` consume borrowed chunks synchronously.
`withColumnStream` exposes a length-bearing `ByteStream`: its next-pull action
is a borrowed source, and each chunk expires on the next pull or scope exit.
The source excludes concurrent pulls and row advancement. An unfinished pull is
cancelled and joined before its scope releases storage. Copy any bytes that
must remain valid later.

Field EOF says only that the selected value ended. The enclosing execution
still reads `CommandComplete` and `ReadyForQuery`; a transaction succeeds only
when its completion, including `COMMIT`, succeeds. A late server error remains
an operation error even after a field was delivered. Applications that require
commit-safe responses must retain the body until the transaction completes.

`columnBytes`, ordinary row decoding and leading fields of streaming decoders
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

The integration tests start one temporary PostgreSQL cluster with the `initdb`
and `pg_ctl` found on the path. Each session wrapper opens a fresh connection
and closes it after the test, including on failure. The integration runner
registers its spec explicitly so that cluster setup stays outside the tests;
the core and TLS runners discover `*Spec.hs` modules automatically.

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
