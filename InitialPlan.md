# Initial Plan File And Architecture For A Pipelined PostgreSQL Client

## Summary
- First repo mutation: add `docs/initial-postgresql-protocol-plan.md` and capture this plan before any code changes.
- Build a pure-Haskell PostgreSQL frontend client on top of a reusable wire core, but design for pipelining from day one rather than bolting it on later.
- Core goals: non-blocking IO, zero-copy decoding where possible, precise typed errors, parsed SQLSTATE diagnostics, reusable connection state after operations where possible, typed transaction/savepoint APIs, and sink-first streaming for large results such as PostgREST’s single-row large payload case.

## Architecture
- Use `network` directly with non-blocking sockets, `STM`, and `async`.
- Run one reader loop and one writer loop per connection, coordinated by an internal scheduler.
- Track exact protocol state from every `ReadyForQuery` as `Idle`, `InTransaction`, or `FailedTransaction`.
- Treat “unknown but maybe reusable” as forbidden: if post-state cannot be proven, close the connection and mark it broken.
- Use a pinned sliding receive buffer.
- Decode small/control messages from contiguous frame slices.
- Decode large `DataRow` payloads incrementally so a single field can be streamed across multiple socket reads without allocating the whole field or whole row.
- Keep the runtime parser custom and internal. Do not depend on `ptr-peeker`, `attoparsec`, `binary`, `cereal`, or a streaming framework at the protocol boundary.

## Public API Shape
- Modules:
  - `Database.PostgreSQL.Protocol`
  - `Database.PostgreSQL.Error`
  - `Database.PostgreSQL.Client`
  - `Database.PostgreSQL.Transaction`
  - `Database.PostgreSQL.Pipeline`
  - `Database.PostgreSQL.Stream`
- Normal client API:
  - `connect`, `close`, `cancel`
  - `prepare`, `execute`, `simpleQuery`
  - `foldRows`, `foldRawRows`
  - `streamSingleColumn`, `streamRows`
- Typed execution:
  - `Statement params row`
  - `Params params`
  - `RowCodec row`
  - runtime validation against `RowDescription` OIDs and format codes before typed decoding begins
- Streaming API:
  - primary abstraction is `ByteSink` with synchronous `pushChunk` and `flush`
  - `streamSingleColumn` is the PostgREST path and must stream one selected column directly to the sink without full buffering
  - `streamRows` exposes lower-level row and field events
  - only one active streaming consumer may exist per connection at a time
  - a streaming operation in a pipeline must be the last operation in its sync-delimited segment
- Pipelining API:
  - explicit advanced API, not transparent automatic pipelining
  - only extended-query operations are pipelinable in v1
  - one pipeline is one logical unit of work: either sync-delimited implicit segments or at most one explicit transaction with optional savepoints
  - each queued operation gets a typed result handle
  - the scheduler tracks segment ids, operation ids, sync count, and skipped-after-error operations

## Transaction And Savepoint Model
- Expose typed handles:
  - `Connection`
  - `Tx tx`
  - `Savepoint tx sp`
- High-level API:
  - `withTransaction`
  - `begin`, `commit`, `rollback`
  - `withSavepoint`
  - `rollbackToSavepoint`, `releaseSavepoint`
- Use opaque client-generated savepoint names in the safe API so savepoint identity is unambiguous even though PostgreSQL allows same-name shadowing.
- High-level transactional operations default to savepoint-wrapped execution so a statement failure can roll back to the nearest client-managed savepoint and keep the transaction reusable where possible.
- Also expose a lower-level raw transactional path that skips automatic savepoints for trusted hot paths.
- Manual transaction-control SQL through the safe high-level query API is out of scope; tracked transaction state is owned by the dedicated transaction/savepoint API.
- On pipeline errors:
  - operations after the first error in the same segment are surfaced as `PipelineAborted`, not as if they had executed
  - recovery occurs at the next `Sync`
  - if an explicit transaction is left in failed state after the sync, the next safe action must be `ROLLBACK` or `ROLLBACK TO SAVEPOINT`

## Error Model
- Every public failure is a typed value with exact post-condition metadata.
- Top-level families:
  - `ConnectionError`
  - `QueryError`
  - `TransportError`
  - `ProtocolError`
  - `AuthenticationError`
  - `DecodeError`
  - `StreamError`
  - `PipelineError`
  - `TransactionError`
  - `CancellationError`
- Every error includes `PostState = ReusableIdle | ReusableInTransaction | FailedTransactionState | Closed | Broken`.
- Parse `ErrorResponse` and `NoticeResponse` into `ServerDiagnostic`.
- Parse SQLSTATE into:
  - `RawSqlState`
  - `SqlStateClass`
  - `KnownSqlState`
  - `SqlState` containing both raw and known-code views
- Generate `KnownSqlState` from PostgreSQL Appendix A and keep unknown-code fallback for forward compatibility.
- Preserve unknown error-field tags in diagnostics.
- For pipeline result mapping, include an operation/segment identifier in pipeline-originated errors.

## Reuse And Streaming Recovery Rules
- No call reports success until its result stream is fully consumed and the connection has reached a known reusable state for that operation or segment.
- Default stream abort handling is configurable per connection and per call:
  - `DrainAndReuse`
  - `CancelAndClose`
- `DrainAndReuse` is the default for safe client APIs. If draining fails, times out, or hits a protocol violation, escalate to `CancelAndClose`.
- For PostgREST-style large single-column output, the implementation streams field bytes directly from the receive buffer to the sink with backpressure and without materializing the full payload.

## Implementation Order
- Step 1: write `docs/initial-postgresql-protocol-plan.md`.
- Step 2: define protocol state machine, typed error hierarchy, SQLSTATE codegen strategy, and connection scheduler model.
- Step 3: implement non-blocking transport, frame assembler, and incremental `DataRow` field streaming.
- Step 4: implement startup/auth, parameter status, backend key data, and `ReadyForQuery` tracking.
- Step 5: implement normal extended-query execution plus explicit pipeline segments and error recovery at `Sync`.
- Step 6: implement transaction/savepoint APIs and automatic savepoint-wrapped transactional operations.
- Step 7: implement sink streaming APIs and PostgREST-oriented large-result path.
- Step 8: add performance work and benchmarks.

## Test Plan
- Framing and parser tests for partial reads, exact-size messages, oversized frames, EOF mid-frame, and large field streaming across buffer boundaries.
- SQLSTATE and diagnostic golden tests from PostgreSQL docs.
- Pipeline tests for sync counting, skipped-after-error operations, reusable-state recovery, and explicit transaction failure paths.
- Transaction/savepoint tests for nested savepoints, rollback-to-savepoint reuse, release semantics, and failed-transaction recovery.
- Streaming tests for bounded memory on huge single-row single-column results and for sink failure under both abort policies.
- Integration tests against PostgreSQL for startup/auth, prepared statements, notices/errors, cancellation, pipelined extended queries, explicit transactions, and savepoints.

## Assumptions
- v1 still excludes COPY and replication.
- Simple query remains a convenience API and is isolated from the pipelined scheduler; the advanced pipeline API is extended-query-only.
- Multiple explicit transactions inside one pipeline are intentionally excluded from v1 to keep recovery and reusability well defined.
