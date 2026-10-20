# native-pg design

Draft for review. `PLAN.md` is the original plan from haskell-native-pg; where
the two differ, this document wins.

## Goals

- Replace hasql in PostgREST with a pure-Haskell PostgreSQL client.
- **Bounded memory.** PostgREST must run in memory-constrained environments
  without OOMs or allocation failures. The library reads only when the
  client asks for data and holds at most one read buffer; anything allocated
  beyond that is the client's choice.
- Never materialize large values. A request body goes from the WAI request
  into `Bind`; the response body column goes from the socket to the WAI
  response.
- Work as a streaming parser and serializer over a raw socket, keeping no
  state beyond what the protocol needs.
- Usable from plain WAI with no special treatment. No WAI dependency, but the
  streaming types have the same shape as WAI's.

## Client responsibilities

The library does none of the following:

- **Memory beyond the read buffer.** The library reads the socket only when
  the client pulls the next message or chunk and the read buffer is empty. A
  client that stops pulling stops the reads; TCP flow control then fills and
  PostgreSQL waits in `send()`. Buffer pools, read-ahead and limits on what
  the client keeps are built by the client on top of that.
- **Prepared statement caching.** The client names its statements and
  decides when to `Parse` and `Close` them. Only the client knows a good
  cache key: PostgREST, for example, can key on the request plan instead of
  generating SQL text just to look a statement up.
- **Connection pooling.**
- **Threads.** The library starts none. A session operation sends pending
  output as it waits for input, in the calling thread. Clients can use
  different connections concurrently. Direct users of transport links or
  channels must ensure exclusive access to each connection.

## State

The library's runtime state is only:

- the read buffer: bytes read from the socket but not yet consumed;
- the protocol state machine's position: phase, transaction status and the
  responses still expected;
- notifications that arrived during an operation, until the client takes
  them with `awaitNotification`.

Everything else lives in types or belongs to the client.

## Principles

1. **Type level first, through typeclass metaprogramming.** Computation over
   types is done by instance resolution over promoted data, with associated
   types where needed. There are no top-level type families.
2. **Static information stays static.** What's known at compile time (wire
   layouts, constants, the messages a state accepts) lives in types, where
   GHC can use it. Constants are type-level literals (`KnownNat`,
   `KnownChar`), which fold even across modules. Instance methods are
   `INLINE`, so a derived parser or serializer specializes into straight-line
   code.
3. **Declarative code, minimal allocation, zero copy wherever feasible.**
   PostgREST should become a thin REST adapter for PostgreSQL that serves
   many databases from one small pod.
4. **Every layer pulls from the one below.** The application pulls from the
   protocol state machine, the state machine is a client of the parser and
   pulls messages from it, and the parser pulls bytes from its input source.
   Every pull blocks until data is available.

## Ideas from the original code

The original code is a guideline, not a literal requirement.

1. **Wire layout is a type-level schema.** The `Payload` kind
   (`Codec.Payload`) has atoms (`CStringAtom`, `FixedBytesAtom n`,
   `RemainingBytesAtom`, `MaybeBytes32Atom`) and combinators
   (`PayloadStruct`, `PayloadCounted16`, `PayloadZeroTerminated`,
   `PayloadBounded`, `PayloadConst`, `PayloadField`). Framing is part of the
   schema: the tag byte is `PayloadConst 1 tag`, the length prefix is
   `PayloadBounded`.
2. **Each message type is a singleton GADT indexed by its payload**, e.g.
   `DataRowType :: DataRowType (BackendSessionMessage 'D' (PayloadCounted16 'MaybeBytes32Atom))`.
   Shared shapes are named (`OidType`, `RowFieldType`) and referenced with
   `PayloadField`.
3. **Semantic values are plain data, separate from the schema**, and
   polymorphic in the byte representation (`DataRow bytes`, `RowField bytes`).
   `Envelope a s` ties a value to its schema; `Scalar` wraps primitive-like
   values.
4. **Codecs are derived from the schema, not hand-written.** `HasConstructor`
   supplies the constructor, `ParseConstructor` folds it over the payload's
   fields, and the overlappable `Parsable a singleton` instance ties them
   together. `Serializable` is derived the same way. Atom instances are
   explicit and sized exactly, so platform-sized `Int` can't slip in.
5. **Input is pulled incrementally.** `MonadInput` reads only what a parser
   needs from an `InputSource`; `withInputLimit` scopes a parser to a length
   prefix and discards whatever it leaves.
6. **Messages are grouped by protocol phase**: `Startup`, `AuthBackend`,
   `AuthFrontend`, `FrontendSession`, `BackendSession`, `Copy`.
7. **Existentials are zero-cost** (`Some`, `SomeEnvelope`) and eliminated in
   continuation-passing style.

## Gaps in the current code

- Typed decoding trusts that the columns have the expected types. Without a
  `Describe`, a column of another type with the same size decodes as
  garbage, e.g. a `float4` read as `Int32`.
- No typed parameters, and none planned: PostgREST passes parameters as raw
  bytes (text format, with the type left to the server or to a cast), or as
  `bytea` in binary format. The exceptions are `text[]` values: variadic RPC
  arguments and the schema cache's lists of schemas and settings. A small
  helper for those comes with the integration.
- Inlined row decoders duplicate the slow paths and error messages of every
  column. Moving those out of line would shrink the code.
- Notices and `ParameterStatus` reports during a session are skipped.
- If a row step throws, the session is broken rather than drained.
- A streamed column allocates a new read buffer whenever one fills, because
  chunks already handed out are never overwritten. A borrowed chunk, valid
  only until the next pull, would let a consumer that copies or sends
  synchronously stream with no allocation.
- SCRAM passwords aren't SASLprep-normalized. A password whose normalized
  form differs from its bytes (possible only with non-ASCII characters)
  fails to authenticate.

## New parts

### Encoding

The mirror of parsing, derived from the same schema.
`ConstructorOf s a r` is a curried function from the payload's fields to
`r`. With `r ~ a` it's the constructor parsers apply. `destructorOf` feeds a
value's fields to one with any other `r`, which is how serializers take a
value apart. `SerializeConstructor` mirrors `ParseConstructor`: it builds the
function that takes the fields and serializes them. `PayloadBounded` writes
its length, which counts its own four bytes, from the computed size before
the content. So a field's bytes can be streamed whenever its length is known.

Output is its length, how many of its bytes are copied, and a function that
writes them. Composing outputs composes the functions, so a message's fields
become one function, not a structure built at run time, and a batch is
written in one pass into one buffer of the copied bytes' size. Client-supplied
byte strings of 8 KiB or more aren't copied: they're sent from where they
are, as parts of their own. A parameter can also be given in chunks, each
sent so, without copying them into one first, or be a `ByteStream`. Its
chunks are pulled from the client's source, after the declared length. A
stream that gives a different number of bytes leaves the message incomplete,
so the session breaks.

Each send is one `writev` of the parts up front, with a stream's chunks
gathered up to 256 KiB or 16 pieces. warp gives a body in chunks of up to
16 KiB, and sending each on its own woke the server once per chunk: about
3,300 times for a 50 MB body, against about 200. Pulling a chunk may wait
for the client, so the bytes before a stream are sent before it's pulled,
and the server starts on its statement, and its `statement_timeout`, as
without the stream.

### Receiving and dispatch

`PayloadChoice` lists alternatives told apart by their leading constants. Its
constructor is an `Alternatives` value: one function per alternative, all
producing the same result.

- **Enums** use constant alternatives (`TransactionStatus`, `FormatCode`).
- **Protocol states** use message alternatives. The messages a state accepts
  are a `PayloadChoice` of `PayloadField`s, and the state machine passes one
  handler per message to `parseChoice`. No sum value is built.

The `PayloadPrefix` class checks each alternative's leading constants in
order: the tag byte, and for `'R'` messages also the subcode after the
length. It peeks one byte at a time, so it never waits for bytes that a short
message doesn't have. Input that no alternative accepts is a protocol error
and marks the connection broken.

### State machine

The state machine is a separate layer and a client of the parser. In each
state it pulls the next message with the choice of messages legal there.

Phases form a promoted kind and index the session, `Session (phase :: Phase)`.
Each phase is named after what the client waits for in it, and its
`Accepts` instance lists, as an associated type, the server messages it
accepts. Transitions are functions from a session in one phase to a session
in the next:

```
Authentication -> AuthenticationResult -> Initialization -> Ready
Authentication -> SaslContinue -> SaslFinal -> AuthenticationResult  (SCRAM)
Authentication -> Initialization                        (no password needed)
Ready -> [ParseResult ->] BindResult -> Rows -> SyncResult -> Ready    (execute)
Ready -> ParseResult -> SyncResult -> Ready                            (prepare)
Ready -> CloseResult -> SyncResult -> Ready                     (closeStatement)
Ready -> CopyIn | CopyOut -> Ready                                       (later)
any -> Broken | Closed
```

- A server error in any phase skips to `SyncResult`, which waits for the
  `ReadyForQuery`. The error is returned as a value, with the transaction
  status, and the session stays usable.
- Errors carry `Diagnostics`: every field of the protocol, typed and copied
  out of the read buffer. That includes the severity (the untranslated one
  where the server sends it), the SQLSTATE as its five bytes, the message,
  detail, hint, position, context, and the schema, table, column and
  constraint involved.
- After a FATAL or PANIC error the server ends the session. Such an error is
  thrown as `ConnectionTerminated`, and the session is broken, so a pool
  drops it. During startup any error is thrown as `ConnectionRejected`.
- SQLSTATEs are plain byte strings, like PostgREST's own literals and the
  constants of `postgresql-error-codes`. The library needs only the severity,
  so it doesn't depend on that package.

- The client asks for protocol 3.0 by default. Version 3.2 is a setting:
  servers before 18 negotiate it down, and some connection poolers reject
  it.
- `Ready` carries the transaction status from the last `ReadyForQuery`.
  Transactions are plain SQL sent by the client; the library only reports
  the status.
- In `ExtendedQuery`, the expected responses follow from the frontend
  messages sent: `Parse` gives `ParseComplete`, `Bind` gives `BindComplete`,
  `Execute` gives `DataRow*` then `CommandComplete`, `EmptyQueryResponse` or
  `PortalSuspended`, and `Sync` gives `ReadyForQuery`. When the client sends
  a pipeline as one typed value, the expected responses are computed from
  its type rather than kept in a runtime queue. After an `ErrorResponse`,
  everything up to the next `ReadyForQuery` is skipped.
- `NoticeResponse`, `ParameterStatus` and `NotificationResponse` are accepted
  in every phase after authentication. Notices and parameter reports are
  skipped.
- **Notifications.** The client listens with plain SQL (`LISTEN`) and pulls
  each notification with `awaitNotification`, which blocks on an idle
  session. One that arrives during an operation, e.g. a session's own
  `NOTIFY`, which comes before its `ReadyForQuery`, is kept until the next
  `awaitNotification`. Notifications come out in the order they arrived. A
  listener whose session the server ends gets `ConnectionTerminated`, so it
  knows to reconnect.
- **Cancelling.** `cancel` sends a `CancelRequest` with the session's backend
  key over a new connection, and waits for the server to close it, as libpq
  does. The cancelled statement fails in its own session with SQLSTATE
  57014, and that session stays usable.
- Haskell can't stop a stale session handle from being reused, so the
  connection also tracks at run time whether it's idle, busy, broken or
  closed. An operation on a busy, broken or closed session fails. An
  operation that throws leaves the session broken, because its protocol
  state is then unknown. State transitions are masked against asynchronous
  exceptions, while the operation itself remains interruptible.
- `close` marks an idle or broken session closed before ending the
  connection. Repeated calls do nothing, including after a close that
  throws, so the channel and TLS connection are closed only once. Closed
  sessions report `isUsable = False` and reject operations with
  `SessionUnusable`. Closing a busy session fails without changing its
  state; interrupt the operation and wait for it to finish first.

### Socket I/O

- Raw `Network.Socket` (TCP and Unix domain), with `sendMany` for output.
- Reads are buffered. The socket `InputSource` calls `recvBuf` into the free
  part of an allocation of the read size the client sets (16 KiB by
  default). Unconsumed bytes stay for the next message. When a message
  doesn't fit the rest of an allocation, only its unconsumed bytes are copied
  to a new one. Filled bytes are never written again, so slices of them stay
  valid. A client that manages its own buffers can supply its own
  `InputSource`.
- The parser monad `Input` passes the buffer along in unboxed tuples. Once
  inlined, a parser keeps the buffer in registers and refills it only on a
  slow path.
- Values the library returns are slices of the read buffer, so it copies
  nothing. A client that keeps a value for long should copy it, so the value
  doesn't keep the whole read buffer alive.
- The parser and serializer expose the wire protocol through `InputSource`
  and `Output`, without transport-specific operations. Tests can feed plain
  byte strings.
- A `Link` supplies a connection's reads and writes, and its `Channel` is
  made from code specialized to that link. Both require exclusive access
  to the connection: reads, writes, flushing, readiness waits and closing
  must not overlap, including reads through the channel's `InputSource`.
  A read may also send pending output. Ownership may pass to another thread
  between calls. Session operations enforce exclusive access during query
  I/O; callers using links or channels directly enforce it themselves.
- A `Link` exposes `awaitReadable`, `awaitWritable` and
  `awaitReadableOrWritable`, keeping its underlying connection private.
  These interruptible waits suspend the calling thread without doing I/O.
  The combined wait returns `Readable` or `Writable`, preferring input when
  both events have been observed. It lets a channel send pending output
  while waiting for responses, so neither peer has to finish sending before
  reading. Socket event registrations are released even if waiting throws.
  After waking, the channel retries the nonblocking operation: readiness
  needn't mean progress, e.g. when only part of a TLS record has arrived.
- native-pg-tls supplies a TLS `Link` through rustls-ffi. Its public API is
  just `tlsClient`, which a client gives native-pg as a `TlsClient`.
  TLS configuration construction, connection operations and FFI helpers
  stay in a private implementation module. This keeps native-pg free of C.
- Dependencies: `network`, plus `crypton` for SCRAM and MD5. PostgREST
  already trusts `crypton` for verifying JWT signatures, through `jose-jwt`.
  Its hashing and PBKDF2 are C code. All of the library's cryptography is in
  `Database.PostgreSQL.Authentication`, so it could move to libcrypto through
  FFI without touching anything else.

### Pull-based reading

- All reading is blocking and driven by client requests: the next message,
  the next column of a row, the next chunk of a stream. A request reads the
  socket only when the read buffer can't satisfy it, and blocks until it
  can. Without a request nothing is read; a read made for a request may fill
  the read buffer up to its size.
- The parser reads only what it needs to advance: tags, lengths and the
  fields of the message being parsed.
- Row values are produced on request. The client sees a value's length
  before taking it, and decides whether to take it whole or as a stream. So
  the client, not the server, decides what gets allocated for data.

### Streaming API

A streamed value has the same shape in both directions:

```haskell
data ByteStream = ByteStream
  { byteStreamLength :: Int            -- known before the first byte
  , byteStreamNext :: IO ByteString    -- empty means end
  }
```

- **Parameters.** The client supplies either strict bytes or a `ByteStream`.
  A WAI request body with `KnownLength n` becomes
  `ByteStream n (getRequestBodyChunk req)`. A body without a length (chunked
  upload) has to be buffered by the client, or rejected.
- **Results.** `execute` folds the rows with a step, and the step pulls
  each row's columns in order with `nextColumn`. For each column the client
  sees its length from the `DataRow` prefix, then takes the value whole
  (`columnBytes`) or as a `ByteStream` (`columnStream`). `byteStreamNext`
  returns slices of the read buffer, each at most the read size; the library
  makes no copies. Reads are limited to the row, so a column can't run past
  it.
- **Scope.** A row is valid only during its step, and a column's stream only
  until the next column is pulled; later use throws `RowMisuse`. Whatever the
  client leaves unread is skipped.
- **Measured.** A 64 MiB column streamed to a slow consumer through the
  default 16 KiB buffer didn't grow the live heap at all, sampled at 16 major
  collections on PostgreSQL 14, 17 and 18.

### Typed decoding

Values come in PostgreSQL's binary format (the client asks for it in the
execution's result formats). The per-value decoders come from
`postgresql-binary`, which PostgREST already depends on. Rows are typed on
top of them, through typeclass metaprogramming:

- **The columns are a type-level list.** In
  `decodeRow @'[Int32, Maybe Text] row (\count name -> ...)`, an associated
  type computes the function's type from the list. `DecodeRow` folds over the
  list and decodes all the columns in one pass over the read buffer.
  `decodeRow` is inlined, so each call site's decoding is specialized to its
  columns, with no dictionaries left.
- **A streamed last column.** `decodeRowStreaming` takes one more column, not
  in the list, and passes it to the function as a `ByteStream`. A
  `ByteStream` in the list is a type error that points to it.
- **Value types.** `DecodeValue` maps a Haskell type to a decoder:
  - integers, floats, `Bool`, `Scientific`, `Char`, `Text`, `UUID`, dates and
    times;
  - `ByteString` for `bytea`, and `Json` / `Jsonb` for JSON text. These
    three are slices of the read buffer;
  - lists for one-dimensional arrays;
  - user types as composites, with `composite (Pair <$> field <*> field)`.
- **NULL.** `DecodeField` lets `Maybe` accept NULL (in a column, an array
  element or a composite field), and makes any other type reject it.
- **Sizes.** Fixed-size types check the value's size, so an `int4` read as
  `Int64` fails instead of being misread. That matters because
  `postgresql-binary`'s integer decoder takes any width.
- **Errors are values.** A wrong column count, an unexpected NULL or a value
  that doesn't decode is returned as a `DecodingError`. The rest of the row is
  skipped and the session stays usable.

### Prepared statements

- The operations map onto protocol messages. `prepare` sends `Parse` and
  `Sync`, `closeStatement` sends `Close` and `Sync`, and `execute` sends
  `Bind`, `Execute` and `Sync`, all in one write. An `Unprepared` statement
  adds a `Parse` of the unnamed statement to the same write, so a one-off
  query costs one round trip.
- A `Prepared` statement is referred to by its name. That doesn't mean it
  exists on a connection: prepared statements are per session. The client
  tracks which connections have which statements, and re-prepares when it
  gets SQLSTATE 26000 (`invalid_sql_statement_name`).
- Parameters stay untyped (see the gaps above).

### WAI usage

```haskell
app request respond =
  withSession pool $ \session -> do   -- the client's own pool
    let body = ByteStream size (getRequestBodyChunk request)
    (result, _) <- execute session (Execution statement [] [Just (ParameterStream body)] []) answer Nothing
    ...
  where
    -- The status is decoded; the body, last, is streamed.
    answer _ row =
      fmap (either (const Nothing) Just) . decodeRowStreaming @'[Int32] row $ \status body ->
        respond $
          responseStream (toStatus status) [(hContentLength, showLength (byteStreamLength body))] $ \write flush ->
            forEachChunk body (write . byteString) >> flush
```

With `Content-Length` set, Warp sends the body as it is; it only falls back
to chunked encoding when no length is given, and HTTP/2 has no chunked
encoding at all. `respond` runs inside the connection scope, which is what
WAI's continuation-passing `Application` type is for.

## Consequences for PostgREST

- **Body column last.** `body` must be the last column of the main queries.
  The status, headers, counts and the checks that depend on them
  (`max-affected`, singular object) then come before it.
- **Writes stay buffered.** Statement errors, including AFTER triggers of
  mutating CTEs, are reported before the row. Commit-time failures (deferred
  constraints, serialization failures) arrive after it. So only read-only
  transactions stream. PostgREST buffers write responses until `COMMIT`
  succeeds, as a list of chunks rather than one contiguous block, up to a
  configured limit.
- **Request bodies.** A body with `Content-Length` streams into `Bind`. A
  chunked upload is buffered up to a configured limit, or rejected.
- **Respond inside the DB scope.** Handlers call `respond` inside the
  database scope instead of returning a `Response` after the transaction
  ends.
- **Pool and statement cache move into PostgREST.** The vendored
  `Hasql.Pool` logic carries over. The statement cache is keyed on the
  request plan, so a cache hit skips generating SQL.
- **JSON keys.** Inserts without `?columns=` parse the JSON body for its keys
  before the SQL can be built. Streaming them needs that moved into SQL, or
  it stays a buffering case.
- **Connection strings.** libpq features reachable through `db-uri`
  (multiple hosts, `sslmode`, `.pgpass`, service files, `PG*` variables) are
  either reimplemented or explicitly unsupported. `sslmode` and the other
  TLS options work as in libpq, except `verify-ca` and a few others (step 8
  of the integration plan).

## Implementation order

1. Pure codec: encoder producing `sendMany` pieces, missing `HasConstructor`
   instances, fallible mappings, derived dispatch, round-trip tests. (Done.)
2. Socket I/O: a buffered `InputSource` over `recv` with a client-set read
   size, then startup and authentication (SCRAM-SHA-256, MD5, cleartext) up
   to `Ready`, tested against a real PostgreSQL. (Done.)
3. Extended query: named and unnamed statements, a streamed parameter, and
   row columns pulled one at a time, one of them streamed. Check that memory
   stays flat with a slow consumer. (Done.) Typed row decoding on top of
   `postgresql-binary`. (Done.)
4. Errors (`ErrorResponse` to typed diagnostics with SQLSTATE) and
   transaction status tracking. (Done; transaction status came with step 3.)
5. Cancel and `LISTEN`/`NOTIFY`. (Done.)
6. PostgREST integration: see `INTEGRATION.md`.
