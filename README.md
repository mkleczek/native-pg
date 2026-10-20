# native-pg

A PostgreSQL client in Haskell that speaks PostgreSQL's protocol over a socket,
without libpq. It reads only when asked to, so results and parameters stream in
bounded memory, and statements go in pipelines.

- **`native-pg`:** connections (libpq's connection strings, SCRAM), statements,
  pipelines and transactions, with streamed results and parameters.
- **`native-pg-tls`:** TLS through [rustls-ffi](https://github.com/rustls/rustls-ffi)
  0.15, with libpq's `sslmode`, client certificates and SCRAM channel binding.
  It's a package of its own because it's the only one with C code, and it has
  no Windows build.

The design is in [docs/DESIGN.md](docs/DESIGN.md). The API isn't stable yet.

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
