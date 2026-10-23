# native-pg design

Draft for review. `PLAN.md` is the original plan from haskell-native-pg; where
the two differ, this document wins.

## Goals

- Replace hasql in PostgREST with a pure-Haskell PostgreSQL client.
- **Bounded request storage.** Caller-owned buffer capacity and availability
  control transport scratch space and request admission.
  Streaming reads follow consumer demand, so a slow consumer applies
  backpressure instead of accumulating the rest of the transfer.
- Stream large values where their semantics permit it: a request body goes
  from WAI into `Bind`, and a response column goes from PostgreSQL to WAI.
  Features that require the full body remain supported, with owned storage
  outside the reusable transport buffers.
- Work as a streaming parser and serializer over a raw socket, keeping no
  state beyond what the protocol needs.
- Usable from plain WAI with no special treatment. No WAI dependency, but the
  streaming types have the same shape as WAI's.

## Client responsibilities

The library does none of the following:

- **Application memory policy.** The library reads the socket only when the
  client pulls the next message or chunk and the read buffer is empty. A
  client that stops pulling stops the reads; TCP flow control then makes
  PostgreSQL wait. The client owns buffer pools and the values, callbacks and
  caches that it retains itself. There are no separate library memory quotas.
- **Prepared statement caching.** The client names its statements and
  decides when to `Parse` and `Close` them. Only the client knows a good
  cache key: PostgREST, for example, can key on the request plan instead of
  generating SQL text just to look a statement up.
- **Tenant policy.** Applications choose connection-pool sizes, create tenant
  pools and decide which requests use them. The optional pool API supplies
  connection lifecycle and targeted waiting.
- **Query workers.** A session operation sends pending output as it waits for
  input, in the calling thread. Pool maintenance and admission use background
  workers. Clients can use different connections concurrently. Direct users of
  transport links or channels must ensure exclusive access to each connection.

Pipeline planning completes before `pipeline` queues its output. It retains the
statement descriptions, parameters and result cursors for that pipeline;
parameter sources are not pulled during planning. Buffer capacity governs the
subsequent transfer, but does not bound this retained plan. Incremental planning
would require a separate protocol/transaction design, rather than a quota on
the current API.

The TLS adapter sets rustls's output buffer limit to 65536 bytes for every
native connection, before its handshake. This bounds unsent application
plaintext before handshake completion and unsent TLS records; it does not
promise a 64 KiB total TLS allocation or cap inbound records, certificate and
handshake state, or kernel socket buffers. `sendReady` can accept a prefix and
then returns zero when the peer stops reading. The caller retains the remaining
application output and retries after readiness; draining the peer resumes
without losing or duplicating bytes. This existing fixed policy is sufficient
for the outbound TLS staging dimension of bounded-memory streaming.

## State

A persistent `Connection` retains the transport channel, protocol usability
state and owned notification metadata. It does not retain an operation's input
cursor, pending output or output scratch allocation.

A private `Context` binds that connection to its input cursor, output workspace
and pending writes. The private supplied-buffer constructor claims the
connection for its callback and expires the context before releasing storage.
A standalone owned `Session` carries its own context rather than installing
request buffers into connection IORefs. Both buffer callbacks supply a public
`Session` directly; `Transaction.runTransaction` accepts that handle. These
paths share the same state machine and concrete `Input` parser.

A channel's input-source constructor receives the current context's pending
writes explicitly. It can advance output while waiting for input without
capturing another operation's output reference. Supplied input reads only the
header needed to learn a protocol envelope, then batches refills up to that
envelope's boundary. The following envelope remains in the transport when an
operation completes; no request-byte leftovers are installed on the connection.

## Shared operation resources

The independent `streaming-io` package owns initialized blocks, with separate
body and setup inventories. `newBufferPool acquisition settings` allocates ordinary
GC-owned inventory. Any retained `BufferPool` remains usable, including after its
allocating helper returns or throws. The supplied acquisition context borrows the
caller's timer manager; application task scopes manage request lifetimes. Each
pool allocates one contiguous block with `calloc` at startup; its `ForeignPtr`
owns the backing allocation until the final pool or buffer alias is collected.
Loans neither allocate body storage nor clear it again.

`withRequestBuffers` admits one primary, leaving `reserveBufferCount` blocks
unavailable to ordinary admission. It passes an opaque `BufferAccess`. Warp
starts this scope after the first request byte arrives and before parsing the
request. The primary remains owned until response completion and request cleanup;
idle keep-alive connections hold none. Headers and protocol metadata are copied
to ordinary owned memory, outside the body budget. Body read-ahead remains
loan-backed, and body consumption follows application demand.

`borrowBuffer` lends the free primary first. While it is in use, one additional
loan may borrow globally, including from the protected reserve. A third
overlapping acquisition fails promptly. Returning the primary wakes that
capability's pending borrower directly, even if global inventory is still empty.
Returning an additional loan makes it globally available immediately. A request
can hold at most two loans, including a pending borrow. Callers use explicit
`BufferLoan` lifetimes for unread cursors and stop accessing every alias before
returning a loan. They cancel or drain their readers and producers and wait for
those children to finish before request teardown. Teardown expires access and
returns outstanding loans without waiting for escaped users.

The reserve provides progress capacity when a producer holds one block while
its consumer needs another. This relies on bounded two-loan nesting, prompt
release, and cancellable I/O; it is neither a latency guarantee nor useful
concurrency by itself. Slow peers can hold a loan and a database connection.

The WAI streaming request carries this capability alongside ordinary request
metadata. PostgREST checks out a database connection with `Pool.use`, then calls
`Streaming.withOperation access connection`. It does not acquire a buffer pair.
The Session's concrete input cursor borrows on refill and returns exhausted
storage; a borrowed field chunk keeps its input loan until the next input
operation. An outgoing borrowed parameter is sent directly from its source
chunk. Small serialization/protocol metadata remains separately owned.
Statements describe parameters and rows without knowing how their buffers
were acquired.

`Streaming.connect` and `connectFirst` borrow a temporary setup pair for startup,
authentication and qualification. The returned `Connection` retains protocol
and transport state but no body storage. `Streaming.withBuffers` retains the explicit
supplied-descriptor path, including capacity and alias validation. Every path
requires exclusive connection access and uses the same monomorphic parser.

The fixed budget covers borrowed body workspace. Control-plane operations,
deliberately materialized values, TLS internal allocations and kernel buffers
remain separate. Full-body application features are retained.

`PoolSettings` specifies `bufferSize`, `ordinaryBufferCount`,
`reserveBufferCount` and `setupBufferCount`, all opaque `PositiveInt` values.
The body pool contains exactly ordinary plus reserve blocks; the separate setup
pool contains exactly the setup count. Every settings value provides at least
one ordinary, reserve and setup block. Reserve can exceed ordinary capacity;
there is no cross-field validation or separate `validateSettings` operation.
Grant sizes are determined by their tuple types and checked against the pool
when acquired.

`positiveInt @n` checks constant positivity and the target `Int` range at compile
time; `mkPositiveInt` checks runtime configuration before narrowing from
`Integer`. Pool allocation adds counts and multiplies by block size in `Integer`,
then narrows each pool's byte count once through `mkPositiveInt` before calling
the allocator. An unsupported allocation size fails before `calloc`; ordinary
allocation failure also propagates as an exception. The independent pools have
no combined `Int` byte-total constraint.
The compiler boundary fixtures can be checked from `streaming-io/` with
`GHC=ghc sh test/check-positive-int.sh`, alongside the Skeletest runtime tests.

## Caller-owned buffers

`Buffer`, defined in `Network.Streaming.Buffer`, identifies initialized, fixed-capacity storage.
Native-pg uses this shared descriptor directly. Allocation
helpers live alongside it in `Network.Streaming.Buffer`; pool
creation uses the same helpers. Each pool owns one contiguous initialized block
and lends disjoint slices; any retained slice keeps the backing block alive.
New allocations use `calloc`, satisfying TLS's initialized-destination requirement
without clearing storage on each loan.
The public allocation helpers keep their byte-budget and block-size API, both
requiring `PositiveInt`: constants are checked at compile time, while dynamic
parser capacities use checked runtime construction. `newBuffers` returns the
whole-block count alongside slices from its byte budget, leaving trailing bytes
unused. The count is calculated from the budget and block size without traversing
the slice list. Buffer pools supply exact multiples of block size, so every
configured block is available.
Externally supplied descriptors still undergo operation-boundary validation.
`Network.Streaming.BufferPool` owns configuration, acquisition and release; its
`BufferPool`, `BufferAccess` and `BufferLoan` representations are private.
Setup uses `withSetupBuffers` for whole tuple grants; body storage is available
through request scopes and explicit loans. The private lease machinery backs
these scopes without exposing raw admission or return operations. The generic `ListPrefix`
implementation is hidden, and no internal streaming-io module is exposed.
Manually constructed descriptors must refer to already-initialized storage.
A loan gives one operation exclusive access; sharing a pool does
not permit two connections to use the same storage concurrently. TLS can
reuse initialized storage without clearing it before every read.

Reusable parser storage requires a separate borrowed-chunk API. Existing
byte-string results must keep their ownership semantics: they cannot become
aliases of storage that the next read overwrites. Retained values are copied;
borrowed values are consumed synchronously before the storage is reused.

The operation boundary validates positive capacities and requires at least four
output bytes before entering the callback. Input and output ranges must be disjoint, including descriptors
whose `ForeignPtr`s point into the same allocation at different offsets.

Borrowed field consumers are separate from materializers. Public `nextChunk`
returns a slice of input storage, valid until the next input operation or scope
exit. Advancing a row, submitting another pipeline or finishing the transaction
expires its field. Consumers copy bytes they need to retain.

Private protocol helpers also support synchronous `columnChunks` sinks and a
length-bearing `withColumnStream` source. In that private source interface,
pulls are exclusive and row advancement is blocked while the source owns the
column. Teardown cancels and joins an unfinished pull before returning storage;
repeated cancellation cannot interrupt the join. Parser or borrowed-callback
failures invalidate the row because input may already have compacted.

Explicit owned operations (private `columnBytes`, decoded columns, and leading columns
of a streaming decoder) assemble stable owned chunks across small input blocks.
They may allocate the complete field size. PostgreSQL metadata C strings and
remaining authentication payloads also assemble owned values across refills.
The low-level `readInputExactly` primitive retains its contiguous-capacity bound.
The body workspace budget therefore applies to borrowed transfer, not to values
the application deliberately materializes or retains.

Field EOF is not an execution or transaction success signal. Execution consumes
trailing command completion and ready messages, and a transaction can still fail
at `COMMIT`. A streamed write response can withhold one owned final chunk until
transaction completion. Earlier bytes may already be visible; a failure aborts
the incomplete HTTP response instead of replacing it with an error response.

## Connection pools and waiting

`Database.PostgreSQL.Pool` owns connection inventory, opening capacity, idle and
maximum lifetimes, reset generations, observations and reservation cleanup.
Its connection and error parameters remain generic so the existing Hasql
control pool can use the same lifecycle implementation. PostgREST retains its
configuration, statement cache and application error mapping.

Each connection pool owns one FIFO of pending checkouts. `newPool acquisition config`
uses the supplied timer context, without registering connection inventory
elsewhere. The application passes the context explicitly to connection and
buffer constructors. Sharing a timer manager does not couple resource lifetimes.
Warp already owns the request capability before choosing a tenant; waiting for a
connection does not reserve extra body blocks. Connection startup borrows from
the separate setup pool. `Pool.release` resets connections without retiring the
pool.

The `resource-admission` package supplies two independent primitives.
`Control.Concurrent.Resource.Acquisition` provides private delivery and deadlines.
`Control.Concurrent.Resource.Queue` provides an opaque FIFO with direct removal
handles. The connection and buffer pools combine these primitives; Acquisition
has no dependency on Queue. Each entry carries its owning queue, so `remove`
takes only the entry and cannot be directed at another queue. Resource owners
decide whom to grant. There is no central Admission registry, global connection
ordering, or dispatcher shared by tenants.

Queue order comes from its links: append adds at the tail, and removal connects
the predecessor and successor in one STM transaction. The queue stores only head
and tail; each entry stores its owner, payload and two neighbour links. No
positions or ordering counter are needed because entries are never reinserted
or moved. The public operations are `newQueue`, `append`, `first`, `value`,
`remove` and `isEmpty`. Consumers examine or remove the current head; the API
does not expose traversal or snapshots of entry handles.

A connection return or newly available opening slot serves that pool's FIFO.
Registration and cancellation also reconsider its head, so an available resource
cannot be stranded behind a removed waiter. Reserving inventory, removing the
waiter and publishing its grant commit in one STM transaction. Waiting threads
retry on their own delivery state, never on shared resource inventory. A return
to one tenant does not inspect or mutate another tenant's queue. Creating a pool
adds no timer or scheduling worker; the existing idle reaper remains per pool.

Buffer pools own their reserve-aware selection policy. An ordinary grant must
leave the protected reserve available; an extra loan may consume it. A blocked
ordinary request must therefore not prevent an eligible extra loan from making
progress. Each required block count has its own FIFO. Selection compares the
oldest fitting heads, so its cost depends on the number of grant sizes rather
than the number of blocked requests. Larger grants can wait while smaller
arrivals fit; eligible grants retain FIFO order.
Temporary setup grants use a separate inventory and queue. Returning a
request's primary can deliver directly to that capability's pending borrower
without returning storage globally.
Ordinary primary reuse changes only request-local state.

Uncontended acquisition reserves directly when the pool's queues are empty.
The queue check and reservation share a transaction, so new arrivals cannot
bypass existing waiters. Buffer pools check that all waiting lanes are empty;
otherwise their existing scheduler selects eligible requests, even when some
inventory is available. This preserves reserve rules and ordering without
duplicating selection logic in the fast path. The direct path skips acquisition
state, queue entries, lane creation and arrival tickets; resource ownership
still has its normal lease or reservation state.

Each queued pool request has one queue entry, private delivery state and at most
one timer registration. Grant, timeout, owner failure and cancellation detach the
queue entry in STM. The waiting thread owns its timer key and unregisters it
before transferring a grant to its caller; unregistering is separate from the
grant transaction. Cancellation either removes the pending request or cleans up
its committed but unclaimed grant. Removing an entry
invalidates its handle: callers must neither remove it again nor inspect its
payload or links. Entries retain their references without a liveness flag or
duplicate-removal checks. Acquisition's once-only removal hook enforces this
discipline for both pool adapters; its protection remains necessary for competing
outcomes. Shutdown repeatedly takes the current queue head because failing one
waiter may also remove later entries through scheduling. `value` is a pure
payload projection, used before an operation that can invalidate the handle.

`acquire` accepts a fair immediate reservation and an enqueue hook that returns
the resource owner's opaque removal handle:

```haskell
acquire
  :: Context -> Deadline
  -> STM a         -- reserve immediately, or retry to use the queue
  -> (Request a -> STM handle)
  -> (handle -> STM ())
  -> (a -> IO ())  -- reclaim a grant that never reaches the caller
  -> IO (Maybe a)
```

`Nothing` means acquisition timed out; `Just value` transfers ownership to the
caller. The cleanup argument handles only grants that cannot be transferred,
including cancellation during an interruptible wait and a deadline crossed
before delivery. Acquisition does not run a user action or release a successfully
returned value. The caller uses `bracket` or equivalent masking to protect the
handoff and establish normal cleanup:

```haskell
bracket
  (acquire context deadline reserveNow enqueue remove release)
  (traverse_ release)
  (traverse use)
```

Masking protects the handoff while leaving a blocked STM wait interruptible.
The pool interprets its grant: an existing connection reservation or an opening
slot. Opening, validation, use and normal release belong to the pool, after
acquisition returns.

The immediate reservation and fallback registration are alternatives in one STM
transaction. A retry rolls back reservation effects before allocating waiting
state and invoking enqueue. An immediate success allocates no waiting state,
invokes neither queue hook and registers no timer. It remains masked through the
final deadline check and transfer; cleanup reclaims it if the deadline has
passed. An exception rolls back the reservation and propagates to the caller.
Buffer acquisitions use `NoDeadline`, so their fast-path benefit is avoiding
waiting-state allocation and delivery transactions, rather than timer work.

The enqueue hook can return a FIFO entry directly; no waiter IDs, lookup table
or caller-owned handle slot are needed. It may also grant or fail immediately.
Acquisition marks that delivery terminal first, remembers the removal request, and
invokes the removal hook once enqueue supplies its handle, before the same STM
transaction commits. Subsequent grants and cancellations remove immediately in
their own transactions. Removal is consumed before invoking the hook, so a hook
can reconsider its queue without granting the same waiter again. Removal hooks
must not retry or throw; an entry granted during enqueue can remain linked until
enqueue returns, and scheduling must tolerate its grant operation returning `False`.
If the deadline has already expired, neither the immediate reservation nor the
queue hooks run, and acquisition returns `Nothing`. Failed enqueue transactions
roll back both registration and any
immediate reservation. `tryGrant` still takes an `STM a` reservation: a retry
leaves the request pending and rolls back reservation effects; an exception is
delivered to the requesting thread after rolling those effects back. There is
no exported rejection operation or acquisition-specific closed error. A resource
owner can fail a pending request with `tryGrant request (throwSTM failure)`.

An acquisition `Context` is an immutable wrapper around a caller-supplied
`GHC.Event.TimerManager`. `newContext :: TimerManager -> Context` does not start
workers or acquire the system manager. At bootstrap an application can obtain
`GHC.Event.getSystemTimerManager` once and share it through one or more contexts.
A context has no open/closed state and no shutdown operation. Pending timed
requests require an executable linked with `-threaded`; untimed requests need no
timer key. A custom manager must remain operational until its requests finish.

A timer callback only sets its request's private expiration flag. It retains no
resource and never calls queue or resource hooks; the requesting thread or
resource owner performs detachment and grant selection in STM. A callback racing
with unregistration can only set that flag, so late callbacks are harmless.
Active connection startup and validation keep their existing short-lived IO
timeouts. Resource closure errors are defined by their owners and propagated as
exceptions; they do not close the acquisition context or change other requests.

The application orchestrates lifecycle: stop admitting new work, drain or cancel
its tasks and wait for their cleanup before dropping its resource references.
Acquisition owns the
mechanical safety of an individual request: detach once, unregister its timer,
and reclaim any committed grant that never reaches the caller. After ownership
transfers, the caller is responsible for cleanup on completion or exception.
Buffer inventories have no close or retirement operation. Queued acquisitions
keep their inventory reachable until granted or cancelled; callers cancel and
join their waiting tasks when stopping work. Request scopes still expire their
`BufferAccess` capability and return outstanding loans, and stale returns remain
harmless. Connection pools retain their cleanup and lifetime anchors through
queued checkout. `Pool.release` closes idle connections and marks borrowed
connections for closure on return, while keeping the pool usable. Construction
failure, pool release and pool collection leave the context and timer manager
untouched. The acquisition timeout bounds queueing, while connection startup
belongs to the acquired slot.

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
3. **Complete message types carry their framing.** Each is an
   `Envelope PrivatePayload Schema` alias, polymorphic in its byte representation
   where needed. Bidirectional patterns preserve construction and matching without
   exporting raw payloads. `serialize SSLRequest` infers its schema from the
   argument; no canonical-schema type family is needed. `Envelope` is a newtype
   with a nominal schema role, so framing cannot be changed with `coerce`.
   Complete-message parsers take that type first: `parseMessage @ReadyForQuery`
   checks a byte string, while `parser @ReadyForQuery` runs in monomorphic
   `Input` and leaves subsequent messages for the next parser. Both infer the
   schema; private payload and schema parameters are invisible.
   Primitive fields remain schema-directed: `serializeField` and `fieldParser`
   explicitly select encodings such as a C string or remaining bytes. `Scalar`
   wraps typed fields.
   `DataRowHeader` is an incomplete streaming prefix and stays schema-directed.
4. **Codecs are derived from the schema, not hand-written.** `HasConstructor`
   supplies the constructor, `ParseConstructor` folds it over the payload's
   fields, and the overlappable `Parsable a singleton` instance ties them
   together. `Serializable` is derived the same way for the complete framed
   type. `parseMessage` returns that type without adding another envelope.
   Atom instances are
   explicit and sized exactly, so platform-sized `Int` can't slip in.
5. **Input is pulled incrementally.** Monomorphic `Input` parsers read only
   what they need from an `InputSource`; `withInputLimit` scopes a parser to a length
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

Output records its length, how many bytes need copying, and a resumable writer.
Composition joins writers directly, so adjacent fields can be written without
building intermediate instruction nodes. The bounded sender and the contiguous
serializer run the same writer. It stops when scratch space fills or a direct
byte string or stream must be sent. Its continuation retains the remaining
values, not the old scratch pointer, so sending can resume in different storage.
Inspecting lengths or composing writers never pulls parameter sources. Arrays
and chunk lists scan their sizes without retaining a serializer for each
element; their writers are built as sending advances. Client-supplied byte strings of
8 KiB or more aren't copied: they're sent from where they are, as parts of
their own. A parameter can also be given in chunks, each
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

Scalar fields decode directly from the input pointer and force their values
before advancing the cursor. This avoids copying one-, two- and four-byte
fields into temporary byte strings, while keeping results safe when storage
is reused. Owned byte-string fields still copy out of reusable storage;
borrowed body chunks retain their existing lifetime contract.

### State machine

The state machine is a separate layer and a client of the parser. In each
state it pulls the next message with the choice of messages legal there.

Private phases form a promoted kind and index the protocol engine's
`Session (phase :: Phase)`. The public `Session` fixes the ready phase.
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
- `ReadyForQuery` supplies the transaction status. The private protocol
  layer reports it; the canonical transaction runtime sends `BEGIN`, `COMMIT`
  or `ROLLBACK` and checks that completion leaves an idle session.
- In `ExtendedQuery`, the expected responses follow from the frontend
  messages sent: `Parse` gives `ParseComplete`, `Bind` gives `BindComplete`,
  `Execute` gives `DataRow*` then `CommandComplete`, `EmptyQueryResponse` or
  `PortalSuspended`, and `Sync` gives `ReadyForQuery`. When the client sends
  a pipeline, each private protocol phase accepts its expected messages while
  the transaction runtime tracks queued statements and cursor order. After
  an `ErrorResponse`, everything up to the next `ReadyForQuery` is skipped.
- `NoticeResponse`, `ParameterStatus` and `NotificationResponse` are accepted
  in every phase after authentication. Notices and parameter reports are
  skipped.
- **Notifications (private protocol support).** Tests listen with plain SQL (`LISTEN`) and pull
  each notification with `awaitNotification`, which blocks on an idle
  session. One that arrives during an operation, e.g. a session's own
  `NOTIFY`, which comes before its `ReadyForQuery`, is kept until the next
  `awaitNotification`. Notifications come out in the order they arrived. A
  listener whose session the server ends gets `ConnectionTerminated`, so it
  knows to reconnect. PostgREST does not currently require this interface, so
  it is not part of the public client API.
- **Cancelling.** `cancel` sends a `CancelRequest` with the session's backend
  key over a new connection, and waits for the server to close it, as libpq
  does. The cancelled statement fails in its own session with SQLSTATE
  57014, and that session stays usable.
- Haskell can't stop a stale session handle from being reused, so the
  connection also tracks at run time whether it's idle, busy, broken or
  closed. An operation on a busy, broken or closed session fails. An
  exception escaping a raw protocol operation leaves the session broken,
  because its protocol state is then unknown. The transaction scope catches
  query and ordinary application failures, drains and rolls back when safe,
  and rethrows after cleanup. State transitions are masked against asynchronous
  exceptions, while the operation itself remains interruptible.
- `close` marks an idle or broken session closed before ending the
  connection. Repeated calls do nothing, including after a close that
  throws, so the channel and TLS connection are closed only once. Closed
  sessions report `isUsable = False` and reject operations with
  `SessionUnusable`. Closing a busy session fails without changing its
  state; interrupt the operation and wait for it to finish first.

### Socket I/O

- Raw `Network.Socket` (TCP and Unix domain), with `sendMany` for output.
- The private parser reads through an `InputSource` into initialized storage.
  An operation's input cursor and output workspace expire with its callback;
  the persistent connection keeps transport and protocol state. Public clients
  obtain this scope through `Streaming.withOperation`, or pass two shared
  initialized buffer descriptors to `Streaming.withBuffers`.
- The parser monad `Input` passes state in unboxed tuples, allowing its binds
  and schema operations to inline. Reads whose bytes are already available
  skip loan acquisition, but still check frame and supplied-storage bounds.
  Refills and loan acquisition stay on the slow path. Cursor records and
  owned results can still allocate; an unboxed return tuple alone does not
  eliminate those allocations. Codec tests feed plain byte strings through
  this private API. Scalar readers and discriminator lookahead combine bytes
  at machine-word width and narrow once, avoiding repeated masking between
  shifts. Bounds checks and portable byte loads remain unchanged.
- The public row-decoder constructors expose their bodies to optimized callers.
  GHC can then specialize a concrete column list before the decoder is stored,
  removing per-row `DecodeRow` dispatch while keeping the parser monomorphic.
  Private rows store their arity and end position as strict unpacked integers,
  so repeated column and chunk operations can use them without following
  separate numeric boxes. Escaping boxed selector results may still allocate.
- Borrowed result chunks from `nextChunk` expire on the next input operation
  or when the transaction scope ends. Owned field decoders return stable
  values. Applications copy borrowed data before retaining it across an
  input operation.
- There is one library. Public modules live in `src/Database/PostgreSQL` and
  are listed in `exposed-modules`; implementation modules live below its
  `Internal/` directory and are listed in `other-modules`. Protocol messages,
  codecs, authentication helpers and buffer constructors cannot be imported
  by clients. Whitebox unit tests compile the same sources directly to reach
  hidden modules. Public parameter OIDs are `Maybe Word32` and become protocol
  scalars only when a Parse message is built.
- Public modules own their API implementations rather than forwarding whole
  interfaces to internal counterparts. Shared concrete types live in public
  `Types`, `Session.Settings`, `ConnectionString.Types`, `Streaming.Types` and
  `Transport.Link` modules. Private helpers are split by responsibility:
  `Exchange`, `Startup`, `HostSelection`, `Channel` and `Socket`. `Transaction`
  defines its opaque query types directly; their constructors remain hidden
  from clients. Shared internal connection state remains private.
- PostgREST's current production usage defines the client boundary. `Session`
  exposes two opaque handles: `Connection` for pooled transport state and
  `Session` for query operations with storage. `connection` retrieves the
  persistent handle from a session, including the standalone owned path.
  `close` and `isUsable` operate on `Connection`; `closeStatement` takes a
  `Session`, and `cancel` uses connection settings and a backend key. `Context`
  and buffer-to-session construction remain private. Configuration
  belongs to `Session.Settings` and errors to `Diagnostics`. Execution and row
  primitives are private. The public `Transaction` module defines statements,
  pipelines, preparation, row schemas and scoped cursors, and composes those
  primitives through one transaction engine.
  The test-only integration package compiles both core and TLS sources, so
  whitebox coverage does not require publishing low-level operations.
- `Transport` deliberately exposes `Link` and TLS configuration for alternative
  transport providers. A provider builds an abstract `Channel` with `newChannel`
  and supplies it through `TlsClient`. Channel input, pending output and mutable
  connection state remain private. Session operations enforce exclusive access
  during query I/O; providers enforce it when operating on their own links.
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
- A private TLS configuration is tied to one server name. rustls-ffi 0.15
  borrows that name for both the connection and its configuration's session
  cache, so a temporary `withCString` allocation is insufficient even after
  handshaking. The configuration owns one NUL-terminated, GHC-managed pinned
  buffer. Both configuration and connection finalizers keep it alive through
  native teardown; either owner can outlive the other. This adds setup-time
  storage, with no copying or synchronization on the I/O path.
- TLS setup returns a `TlsChannel`: a channel builder and optional
  `tls-server-end-point` binding data. The TLS client derives this data from
  the server's certificate; authentication consumes the hash without
  receiving the certificate. `Link` and `Channel` contain only I/O,
  readiness and closing operations, with no authentication metadata.
  An unavailable binding and a failure to compute one are distinct.
  The latter becomes an authentication error only if SCRAM-PLUS is selected,
  so other authentication methods, disabled channel binding and cancellation
  don't fail because of an unsupported certificate hash.
- Dependencies: `network`, plus `crypton` for SCRAM and MD5. PostgREST
  already trusts `crypton` for verifying JWT signatures, through `jose-jwt`.
  Its hashing and PBKDF2 are C code. All of the library's cryptography is in
  `Database.PostgreSQL.Internal.Authentication`, so it could move to libcrypto through
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

### Transaction and streaming API

`Database.PostgreSQL.Transaction` owns the query API: `Statement`, `Pipeline`,
`RowDecoder`, `Result`, `Field`, preparation selection and transaction actions.
Statements describe SQL, parameters, row decoding, cardinality and preparation
eligibility. They do not refer to HTTP, sinks, sessions or buffer allocation.
There are no separate statement, pipeline or pull transaction modules.

`runTransaction` runs a rank-2 `Transaction tx` action and owns its cleanup.
`Transaction` wraps `ReaderT Runtime IO` and derives its monad, `MonadIO` and
`MonadUnliftIO` instances. `withRunInIO` captures that runtime for external
`IO` callbacks; it does not open another transaction or allocate body storage.

`Pipeline` wraps strict `StateT [Pending] (ReaderT Runtime IO)` and derives only
`Functor` and `Applicative`. Each run starts with an empty pending list. Statements
prepend to it in constant time, then one reversal restores wire order before
submission. Preparation effects and cursor values retain applicative order.
This avoids quadratic list append in left-associated batches; planning still
retains metadata proportional to the number of statements.

`pipeline` returns result cursors without reading their results; `nextRow` and
`nextChunk` advance them on demand. Protocol operations enforce one calling
thread, exclusive use and scope lifetime. Synchronous callback nesting between
operations is allowed; re-entry while an operation is active is rejected.
Escaped actions cannot access the database after the scope ends. The transaction
driver starts no result worker. The parser remains the concrete `Input`
implementation; transaction composition adds no `MonadInput` dictionary to
parsing.

`row` decodes owned metadata. `streamingRow` decodes that metadata followed by
one nullable `Field tx`; `fieldLength` reports its declared length. `nextChunk`
returns borrowed bytes until field EOF, represented by an empty chunk. A chunk
expires at the next input operation or scope exit. Advancing its row, submitting
another pipeline or finishing the transaction expires the field itself. Consume
each chunk before those operations, or copy it before retaining it.

Owned results use this same cursor engine. `rowList`, `singleRowValue` and
`rowMaybeValue` consume `Result tx (Const a)` values; `singleRow` and `rowMaybe`
constrain a statement's row schema. Owned single-row collectors count extra
rows without decoding unused values, preserving cardinality errors.
`foldRows` runs a transaction action for each decoded row, so a step can pull a
field and send its chunks synchronously before the next row is requested.
Collectors establish statement completion; successful transaction completion
still requires `finishTransaction`.

`finishTransaction` drains and validates remaining results, then observes the
transaction ending. With `finalPipeline`, that ending was queued with the batch;
otherwise it is sent after the results. A condemned transaction rolls back.
Finishing an empty scope sends nothing. Leaving the scope unfinished drains
pending results and rolls back any still-open transaction; an ending already
queued by `finalPipeline` cannot be changed.

Lifting and unlifting preserve application exceptions without wrapping them.
An ordinary application exception between operations allows the scope to drain
and roll back. Each protocol operation catches unsafe failures before returning
to application code, so catching such a failure cannot make a broken connection
reusable. Interrupted I/O and asynchronous exceptions escaping an unfinished
scope discard the connection without waiting to drain, as do unsafe parser and
parameter-source failures. A successfully finished transaction remains usable
if later HTTP output fails.

A streamed input parameter has this shape:

```haskell
data ByteStream = ByteStream
  { byteStreamLength :: Int            -- known before the first byte
  , byteStreamNext :: IO ByteString    -- empty means end
  }
```

Owned input uses strict bytes or `ByteStream`. Borrowed input uses
`ParameterBorrowedStream n source`, whose chunks remain valid during their
source callback. The PostgreSQL wire length must be known; unknown-length
uploads remain an explicit materialization case.

A 64 MiB column streamed to a slow consumer through the default 16 KiB buffer
didn't grow the live heap, sampled at 16 major collections on PostgreSQL 14, 17
and 18.

### Typed decoding

Values come in PostgreSQL's binary format (the client asks for it in the
execution's result formats). The per-value decoders come from
`postgresql-binary`, which PostgREST already depends on. Rows are typed on
top of them, through typeclass metaprogramming:

- **The columns are a type-level list.** In
  `Transaction.row @'[Int32, Maybe Text] (\count name -> Const (count, name))`, an associated
  type computes the function's type from the list. Private `DecodeRow`
  machinery decodes the columns in one pass. The public combinators inline
  through it, specializing each call site's decoding to its columns.
- **A streamed last column.** `streamingRow` takes one more column, outside
  the type-level list, and passes it to the constructor as `Maybe (Field tx)`.
  A `ByteStream` in the list is a type error that points to it.
- **Value types.** `DecodeValue` maps a Haskell type to a decoder:
  - integers, floats, `Bool`, `Scientific`, `Char`, `Text`, `UUID`, dates and
    times;
  - `ByteString` for `bytea`, and `Json` / `Jsonb` for JSON text. These
    three produce owned values, independent of subsequent buffer reuse;
  - lists for one-dimensional arrays;
  - user types as composites, with `composite (Pair <$> field <*> field)`.
- **NULL.** `DecodeField` lets `Maybe` accept NULL (in a column, an array
  element or a composite field), and makes any other type reject it.
- **Sizes.** Fixed-size types check the value's size, so an `int4` read as
  `Int64` fails instead of being misread. That matters because
  `postgresql-binary`'s integer decoder takes any width.
- **Errors.** A wrong column count, an unexpected NULL or a value that does
  not decode produces a `DecodingError`. The transaction runner reports it
  through `QueryError`, then skips unread data and rolls back an open
  transaction so the connection can be reused.

### Prepared statements

- The private execution engine maps operations onto protocol messages.
  Applications supply a `Prepare` callback to `Transaction.runTransactionWith`
  and select eligible statements with `statementPreparable`. The callback
  returns `PreparedAs` for an existing name or `PrepareAs` with an action run
  after the server accepts its parse, even if execution later fails. Applications
  can deallocate with `Session.closeStatement`. Internally, `prepare` sends
  `Parse` and `Sync`, `closeStatement` sends `Close` and `Sync`, and `execute` sends
  `Bind`, `Execute` and `Sync`, all in one write. An `Unprepared` statement
  adds a `Parse` of the unnamed statement to the same write, so a one-off
  query costs one round trip.
- A `Prepared` statement is referred to by its name. That doesn't mean it
  exists on a connection: prepared statements are per session. The client
  tracks which connections have which statements, and re-prepares when it
  gets SQLSTATE 26000 (`invalid_sql_statement_name`).
- Parameter OIDs are optional, allowing the server to infer their types.

### HTTP/1.1 usage

PostgREST connects request-body input to a streamed statement parameter and
pulls the result's last column within the database transaction scope. It calls
`respond` before that scope returns. App uses `withRunInIO` at this HTTP
boundary to run `nextChunk` and `finishTransaction` inside Warp's response
callback. MainTx and other transaction code use ordinary monadic composition
without a runner argument. The HTTP sink finishes
writing each borrowed chunk before the next native input operation.
For a write, PostgREST copies only the final chunk, finishes the transaction
inside Warp's response callback, and sends the copy on success. Both protocols
run sequentially on the request thread. Buffer provisioning stays below the
handler in the transport and connection adapters.

Warp uses `Content-Length` when available and chunked encoding otherwise.
This integration covers HTTP/1.1 only. A field's EOF does not establish
transaction success; finishing the response requires successful transaction
completion. If the entire body fits in its first chunk, PostgREST can own that
chunk and complete the transaction before sending HTTP headers.

## Consequences for PostgREST

- **Body column last.** `body` must be the last column of the main queries.
  The status, headers, counts and the checks that depend on them
  (`max-affected`, singular object) then come before it.
- **Writes retain only their tail.** Statement errors, including AFTER triggers of
  mutating CTEs, are reported before the row. Commit-time failures (deferred
  constraints, serialization failures) arrive after it. Borrowed prefixes stream
  directly; one copied final chunk waits for `COMMIT`. That extra chunk is outside
  the shared buffer budget and is bounded by the transfer chunk size. A failed
  commit aborts an already-started response. Required materialization for other
  application features remains supported.
- **Request bodies.** A raw body with `Content-Length` streams into `Bind`.
  PostgreSQL requires a parameter length before its bytes, so an upload of
  unknown length must be materialized first. Existing application body policy
  still applies; buffer provisioning adds no separate limit.
- **Respond inside the DB scope.** Handlers call `respond` inside the
  database scope instead of returning a `Response` after the transaction
  ends.
- **Pool and statement cache.** Native-pg owns connection lifecycle; PostgREST
  owns configuration, the adapter and statement caching keyed on request plans.
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

### Bounded serialization

`outputBufferFrom` lends initialized caller storage to a resumable serializer.
It requires at least four bytes, enough for the largest indivisible protocol
scalar. Copied fields fill that storage incrementally; a partial send keeps
the current slice alive and unchanged before the next fill. The sender resumes
with the remaining writer, without rebuilding the already emitted fields.
After running out of space, it resumes in an empty buffer of at least four
bytes; the replacement may have a different capacity. The contiguous serializer
instead resumes after a direct part at its existing scratch offset.
Existing immutable
byte strings may be sent directly. This bounds serialization scratch space,
not the caller-owned message values or pipeline metadata. The legacy allocating
serializer remains available when no caller buffer is supplied.

List serialization computes wire size and copied size together, then builds
writers as the sender advances. It does not retain a serializer per element.
Output batching tracks its piece count alongside byte count, preserving the
16-piece and 256 KiB limits without repeatedly traversing the gathered list.
Pending stream parts keep their byte counters unpacked, avoiding a separate
boxed counter allocation when advancing to the next chunk. These changes affect
protocol bookkeeping only; borrowed storage and partial-send lifetimes are
unchanged.

### Borrowed upload chunks

`ParameterBorrowedStream length source` accepts a callback-based `Source`.
Its producer keeps each callback open while the single protocol driver sends
partial slices. A one-chunk acknowledgement bridge permits the driver to read
earlier replies while later upload bytes are pending. The producer never calls
`Link`, preserving exclusive TLS access. Source readiness and readable transport
both wake progress; an unfinished producer is cancelled and joined before its
borrowed storage can return. No next chunk is pulled until the previous callback
has ended. Ordinary `ParameterStream` retains its owned-value contract.

### Values outside transport storage

The low-level contiguous `readInputExactly` primitive requires its value to
fit the supplied input region. Borrowed body chunks use repeated refills and
keep their original supplied storage. Explicit owned field decoders instead
assemble stable values across refills and can allocate the whole field.

Protocol discriminators may need lookahead beyond a tiny body block. The
concrete parser temporarily owns that metadata, then restores the supplied
buffer before exposing body slices. C strings, authentication payloads and
other owned metadata also span refills. A C string ending in the current
buffer returns its owned copy directly; only fragmented strings need chunk
assembly. This allocation is protocol overhead,
not an expansion of the borrowed body workspace. Session has no streamed COPY
parser; materializing a `CopyData` message remains an owned-value path.

In the private protocol engine, notifications received during a query are copied into a FIFO until
`awaitNotification` can return them. Stopping socket reads at a queued
notification could prevent the query itself from completing on the same stream.
Buffer availability therefore cannot bound this queue under that internal interface.
A synchronous notification consumer or a dedicated notification connection
would need a separate design if that queue must also apply backpressure.

