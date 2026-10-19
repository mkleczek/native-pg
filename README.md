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
cabal test all
```

The integration tests start a temporary PostgreSQL cluster with the `initdb`
and `pg_ctl` found on the path.

## Licence

AGPL-3.0-or-later, see [LICENSE](LICENSE).
