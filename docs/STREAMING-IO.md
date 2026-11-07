# Shared pull-based I/O in streaming-io

Draft for review. It proposes moving native-pg's transport, serializer and
gathering sender into `streaming-io`, and replacing the callback-scoped
`Source` with a pull that can report "not ready". Warp's streaming responses
and native-pg's uploads then use the same code, gather into one `writev`, and
need no helper threads. `DESIGN.md` remains the native-pg design; the sections
listed under "Changes to DESIGN.md" would be updated when this lands.

## Why

Measured on postgrestream-mt (native-pg `4a51dc34692e`, wai fork `51b66138`),
Mix M at 3,000 req/s, 1,500 keep-alive connections:

- **Warp sends the response header alone.** The fork's `RspBytes` path sends
  the header with `connSendAll` before the body runs, then one send per
  chunk (`Response.hs:380-409`). That is 1.00 header-only send per request,
  2.19 sends in all. A chunked response would take three sends per chunk
  (size line, data, CRLF). Warp sends the header at once on purpose: the
  client sees a response while the body is slow. Only the fork's
  `ResponseBytes` does; the legacy `ResponseStream` keeps the header in its
  builder until the first flush or 16 KiB. The answer loop of the runner
  design keeps the former behaviour.
- **native-pg cuts its gather at every borrowed stream.** `sendSome` sends the
  bytes before a stream before pulling it, "so the server starts on its
  statement" (`Channel.hs:282-287`), and sends each borrowed upload chunk
  alone (`Channel.hs:291-295`). The reason is the same as Warp's: a pull may
  wait for the stream's client.
- **native-pg forks for uploads.** `Source` lends a chunk only inside its
  callback (`withChunk :: (ByteString -> IO a) -> IO a`). To keep a chunk
  while a partial send waits and PostgreSQL replies are read, a producer
  thread sits inside the callback (`newBorrowedPull`, `Channel.hs:154-180`).
  Waiting for either the producer or the socket forks two more threads per
  wait (`waitForSourceOrReadable`, `Channel.hs:189-201`).

All three come from one gap: a consumer can't ask a source whether a chunk
is ready, and can't hold a chunk outside a callback.

## Goals

- **One gathered write per batch, in both directions.** Owned framing and
  borrowed chunks go out in one `writev` whenever the chunk is ready.
- **The header still goes out at once when the body isn't ready.** The rule
  is "cut the batch only where a pull would wait", for HTTP headers and
  PostgreSQL framing alike.
- **No helper threads on the request path.** One thread drives both sockets;
  readiness of sockets, sources and buffer loans composes in STM.
- **Bounded memory and backpressure as today** (see "Invariants").
- **Zero copy as today.** Chunks are slices of pool loans, sent by reference.
- **Hot paths specialize.** Transport and source are type classes with
  `INLINABLE` operations, specialized at their call sites, as `newChannel`
  is for `Socket` today (`Transport.hs:181-183`).

Not goals: HTTP/2 and warp-tls (warp-tls has no `connRecvInto` and doesn't
run the streaming runner), stream combinators, changes to loan limits.

## Layers

All in `streaming-io`, with the PostgreSQL- and HTTP-specific parts left
where they are.

### Link: transport

Moved from `Database.PostgreSQL.Transport.Link`, with readiness as STM:

```haskell
class Link link where
  receiveReady :: link -> Ptr Word8 -> Int -> IO (Maybe Int)  -- as today
  sendReady :: link -> [ByteString] -> IO Int                  -- as today, gathered
  flushReady :: link -> IO Bool                                -- as today
  watchReadable :: link -> IO Watch
  watchWritable :: link -> IO Watch
  awaitAny :: link -> Interest -> IO ()
  closeLink :: link -> IO ()

-- | A registered readiness event. 'watchReady' retries until it fires;
-- 'watchCancel' releases the registration and is idempotent.
data Watch = Watch {watchReady :: STM (), watchCancel :: IO ()}

-- | What a caller is blocked on: input, and what its output waits for.
data Interest = Interest {interestInput :: !Bool, interestOutput :: !Blocked}

data Blocked = Done | OnWritable | OnSource (IO Watch)
```

- **One wait.** A caller works out from its state what it can react to,
  waits for any of it with `awaitAny`, and then retries everything it was
  blocked on. Readiness is a hint, so `awaitAny` returns nothing and no
  caller branches on which event fired. It replaces `awaitReadable`,
  `awaitWritable` and `awaitReadableOrWritable`, and the rule that each
  branch repeated.
- **The link chooses the cheapest wait for the interest.** The default
  registers the watches of the interest, waits on them with one `atomically`
  over their `orElse`, and releases them. The socket instance waits for one
  direction in `threadWaitRead` or `threadWaitWrite`, for both in one
  registration with the IO manager, and on watches only for a source; with
  nothing to wait for it returns at once. TLS delegates to its socket.
- **Watches.** The socket instance builds them from
  `threadWaitReadSTM`/`threadWaitWriteSTM` (`Socket/Link.hs:37-40`); that
  stops being private.
- **Sends take at most 16 pieces.** `sendReady` offers the pieces to one
  `writev`, and the socket link uses the iovec limit of 16, so a caller
  offers no more, in as many sends as it needs. It offers at least one: an
  empty `sendReady` fails with `EINVAL` on macOS.
- **Try before watching.** A watch is only valid after the matching try
  returned `Nothing`, as today. That keeps the TLS link correct without
  TLS-aware watches: rustls may hold plaintext while the socket is quiet,
  and `receiveReady` returns it first.
- **Instances.** The `Socket` instance moves with the class (adding a
  `network` dependency to `streaming-io`, which wai and warp already have).
  The rustls instance stays in native-pg-tls. Warp's plain HTTP/1.1
  connection uses the `Socket` instance.

### Output: framing

Moved from `Internal/Protocol/Codec/Output.hs`: `Output` (wire length,
copied length, writer), `Writer`, `WriteResult` (`Finished`, `NeedSpace`,
`Yield`), scalar writers in network byte order, `bytes` with its copy
threshold, `outputs`. The resume rules stay as in "Bounded serialization".

- **Uncopied parts:** `UncopiedBytes ByteString`, `UncopiedOwned` (today's
  `ByteStream`, owned chunks) and `UncopiedSource` (a declared length and a
  borrowed source, below).
- **Stays in native-pg:** `cstring`, length prefixes, `Parameter` and the
  message codecs, all built on the moved core.
- **New in Warp:** the status line, headers, and chunked framing (size line
  and CRLF around a slice, and the final `0\r\n\r\n`) as `Output`s.

### Source: pull without a callback

```haskell
class Source source where
  -- | Takes the next chunk if it's available without waiting. A chunk stays
  -- valid until the next pull from the same source or the end of its scope.
  pullReady :: source -> IO Pulled
  -- | After 'Pending': fires when a pull may succeed.
  watchSource :: source -> IO Watch

data Pulled = Pulled !ByteString | Pending | Ended
```

- **Lifetime by pull, not by callback.** This is the rule native-pg's
  `nextChunk` already has ("valid until the next input operation or scope
  exit", `DESIGN.md:200-204`). A consumer can hold a chunk across a partial
  send and return to its event loop; it pulls again only once the chunk has
  been accepted, which is the backpressure rule anyway.
- **What "ready" means.** A chunk as large as a blocking read of the same
  source would give: bytes already buffered, and nonblocking reads into the
  source's own loan, for as long as bytes keep arriving, until it holds that
  many. A `Pending` pull may hold the bytes it has received, in its loan, and
  keeps them for the next pull.
- **Warp's request body.** `pullReady` serves read-ahead, then tries a
  receive that doesn't wait into the request's loan. The Content-Length and
  chunked decoders (`Conduit.hs`) were not resumable; since step 3 of the
  runner `lengthStep` and `chunkStep` keep their state as data between pulls,
  and a pull that finds no bytes is `Pending`. Its watch is the link's
  readable watch. The graceful-shutdown give-up of `makeGracefulRecvInto` is
  not part of a pull: `Interest` has no slot for a give-up, so the runner's
  idle receive waits with `OnSource` of a shutdown watch.
- **native-pg's response column.** `fieldSource` is the field of a row as a
  `Source`: `pullReady` gives a chunk as large as `nextChunk`'s, which is the
  field's rest or the room the input buffer has up to its end, whichever is
  less. It takes the bytes the input holds and receives, without waiting, for
  as long as bytes arrive, until it has that many; with fewer, when nothing
  more arrives, it is `Pending` and keeps them. One receive brings only what
  the sender sent at once, about 8 KiB from PostgreSQL, so a pull that gave
  what one receive brought gave twice the chunks of a 16 KiB blocking read.
  The watch is one watch of what the pull was stalled on (`watchInterest`):
  the link readable, and, while output is unsent, the link writable or the
  upload's source, so that a pull never waits for the answer to a request that
  is not fully sent. It sends unsent output as far as the link takes it before
  it is `Pending`, after it has tried to receive. `watchInterest` has one
  copy for each link type, made where the instance is (`SPECIALIZE`, not
  inlined), so the channel's watch picks the link's watches at compile time
  and passes no `Link` dictionary at run time.
- **Buffer loans.** A pull that needs a loan while none is free returns
  `Pending` with a watch on the pending borrow. `BufferAccess` gets a
  registering borrow: it joins the FIFO lane as `borrowBuffer` does today, so
  fairness is unchanged, and the next pull takes the granted loan. A request
  buffer returns its loan whenever it is fully consumed, so a pull of the
  next chunk, and `endingReady` too, may find none.
- **`watchBorrow` and the waits that cancel.** `watchBorrow`'s docs say to
  cancel its watch on exceptions, as `awaitWatch` does. But `awaitWatch`,
  `awaitFirst` and native-pg's `awaitAny` also cancel after the watch fired,
  before the consumer pulls again, and `watchBorrow`'s cancel returns a loan
  that was granted to the registration, and gives up its place in the queue.
  A source that hands out `watchBorrow` therefore keeps the registration
  itself and hands out a watch whose cancel does nothing and which fires
  once the registration is granted; its next pull claims the loan with
  `tryBorrowBuffer`, which takes the registration's loan first. The request
  body does so (runner design §5). So do `fieldSource` and `endingReady`: the
  input keeps the registration, the next pull or `endingReady` takes the loan
  with `tryBorrowBuffer`, and one that finds the registration still pending
  doesn't try, since a pending registration holds the place of the second loan.
  The stall of such a pull is `StallOnLoan`, which also says what unsent output
  waits for, so that the watch includes the link writable or the upload's
  source and the output keeps moving. The registration is withdrawn when the
  input's buffer is released, which gives back the loan it holds, and before
  any blocking borrow of the loan, as a blocking read takes or a scope's drain.
- **Old callback sources.** `Network.Streaming.Source` becomes
  `ScopedSource` for the transition and is removed when its last user is.
  Nothing implements the new class from a callback, because that's exactly
  what needs a thread.

### Sender: gathering and partial sends

Moved from `Channel.hs`: `Part`, `Unsent`, `renderOutput`, `sendSome` and
the gather limits (16 pieces, 256 KiB).

```haskell
-- | Sends what the link takes without waiting and says what to wait for.
sendSome :: Link link => link -> Unsent -> IO (Unsent, Blocked)

-- | 'sendSome' and then 'flushReady', so that the wait it reports is enough.
pushOutput :: Link link => link -> Unsent -> IO (Unsent, Blocked)
```

- **Gather rule.** At a source part, pull it: `Pulled` gathers the chunk
  behind the bytes already gathered; `Ended` checks the declared length and
  continues; `Pending` cuts the batch, sends what was gathered, and reports
  `OnSource` once the link has taken it. So a ready first chunk leaves with
  the HTTP header or PostgreSQL framing in front of it, and a slow one never
  holds them back.
- **Partial sends.** The unsent tail keeps pointing at the held chunk; the
  source isn't pulled again until the chunk is fully accepted. A TLS link
  accepts by copying into rustls, so its chunks are released at once.
- **Length checks.** A source that delivers more or fewer bytes than
  declared fails with `StreamLengthError`, for both protocols (Warp's
  "exceeded" and "ended before Content-Length" errors become this one).
- **Flush before waiting for a source.** `pushOutput` owns the rule that
  records the link took but hasn't written (TLS) go out before any wait for a
  source, which they would stay behind: it reports `OnWritable` instead of
  `OnSource` until the link is flushed, and `Done` only when nothing is
  unsent and all is flushed. Callers don't flush.
- **No registry, no threads.** `BorrowedPull`, `waitForSourceOrReadable` and
  the thread registry in `Unsent` go away; `discardUnsent` only drops
  references.

### Drive: the loop around a step

```haskell
data Progress s a = Ready a | Again s | Blocked s Interest

-- | Runs the step until it is ready, and waits for the link when it is blocked.
drive :: Link link => link -> (s -> IO (Progress s a)) -> s -> IO a
```

- **One loop.** `drive` is the only recursion and the only `awaitAny` of its
  users. A step tries what it can and never waits: it gives the result, or
  says whether it made progress, or what it is blocked on.
- **`Again` goes before a wait.** It means "made progress, try again before
  waiting", for example when the rest of the output was sent and the server
  may have answered meanwhile. `Blocked` waits once for the interest and
  steps again from the state it gave. Readiness is a hint, so the step tries
  every operation it is blocked on again, whichever fired.
- **No cost.** `drive` is `INLINE`, so a step that is known where it is run
  becomes part of the loop: `Progress` is matched where it is made and never
  allocated, and the wait stays the known call it is without `drive`.
- **Exceptions** from a step or a wait propagate. `drive` holds nothing to
  release.

## Drivers

- **native-pg input** (`linkInputSource`), a step run by `drive`. After
  `receiveReady` returns `Nothing`: `pushOutput`, then blocked on
  `Interest True blocked` with what it reports, and receive again. Input is
  tried first, as today, on the request thread. When `pushOutput` has sent the
  rest of the output (`Done`), the step makes progress instead of blocking, so
  it receives again without waiting, since the server may have answered
  meanwhile: nothing waits while a read could succeed. Sending output is the
  same step without input: ready once `pushOutput` reports `Done`, otherwise
  blocked on `Interest False blocked`.
- **Warp response.** The application returns an `Answer`: a status, headers
  and a body source. Warp sends it in its own loop, with `pushOutput` on the
  client link and `awaitAny link (Interest input blocked)`, where `blocked` is
  the link writable or the source's watch, until `pushOutput` reports `Done`.
  The timeout is resumed only around waits for the link, so a slow database
  never times the client out. The runner design describes it.
- **PostgREST.** Instead of pushing chunks into a sink (`consumeNext`,
  `App.hs:390-392`), it hands Warp the column's source with the advertised
  length. Write mode's rule that the final chunk waits for COMMIT becomes a
  source wrapper that copies the final chunk, reports `Pending` until
  `endingReady` has the ending's reply, and gives the chunk then, or throws
  the failure of COMMIT; `retainFinalChunk` moves into it.

## Scope: resources that learn how a request ended

`Network.Streaming.Scope` is a bracket for any number of resources, shared by
Warp's request and native-pg's connections.

```haskell
withScope :: (forall s. Scope s -> IO a) -> IO a
acquireIn :: Scope s -> IO a -> (Ending -> a -> IO ()) -> IO (Owned s a)
data Ending = Completed | Aborted SomeException
```

- **Releases run last in, first out, once each,** when the scope closes, on
  success, on an exception and on an asynchronous one. Every release runs even
  if one throws. As with nested brackets, the exception of the last release
  to throw replaces those before it, and the body's; a release after a failed
  one learns `Aborted` with it.
- **A release learns how the scope ended,** so that it can give back what it
  holds when all went well, and discard it when it didn't. `Ending` is
  streaming-io's own type: IO has no abort case, and `Aborted` carries the
  exception, so a release can tell an interruption from an error.
- **`acquireIn` acquires under `mask_`** and registers the release, so no
  asynchronous exception comes between them; the acquisition can be
  interrupted where it blocks. One that throws registers nothing.
- **No early release.** An `Owned` lives as long as the scope. A resource that
  has to go earlier has a scope or a bracket of its own.
- **A scope belongs to the thread that opened it,** as an `STRef` does: releases
  are registered and run without synchronization, which keeps a scope with two
  resources as cheap as two nested brackets (about 750 bytes allocated at
  `-O2`, where a synchronized registry took about a third more).
- **The type parameter is rank-2,** as `runST`'s is: neither a `Scope s` nor
  an `Owned s a`, nor anything whose type mentions `s`, can leave `withScope`.
  The tag protects only what carries it: `owned` takes the `a` out as plain
  as its library made it. So native-pg's handles carry the tag (`Checkout s`,
  `Tx s`, `Field s`, `FieldSource s`), and what is plain, the `Session`, is
  rejected at run time once its operation has ended.
  `streaming-io/test/check-scope-escape.sh` checks the escapes fail to compile.

### native-pg in a scope

The pieces compose, since the pool is generic over its connections and
PostgREST wraps its own:

- **`Pool.checkoutIn`** checks a connection out as `useChecked` does, with the
  same admission deadline and check; acquisition failures are values. The
  connection goes back when the scope closes: `Succeeded` if it completed, and
  if it aborted `Failed` or `Broke` by `configUsableAfterException`, as `use`
  decides. `markFailed` and `markBroken` decide earlier for the code in the
  scope.
- **`Streaming.operationIn`** binds the request's `BufferAccess` to the
  connection, as `withOperation` does. When the scope aborted, the
  transaction decides what becomes of the connection.
- **`Transaction.transactionIn`** begins a transaction on that session, and
  `runIn` runs a `Transaction s a` against it. Completed, it finishes the
  transaction as `finishTransaction` does (a failure the code handled is
  rolled back instead). Aborted, an interruption or a field left unread in the
  middle of its stream abandons it and breaks the connection; any other failure
  drains and rolls back. A connection never goes back in an unknown state.
- **`Transaction.fieldSource`** is a field as a `Source` that never waits, as
  described above.
- **`Transaction.endingReady`** reads what follows the body and the ending's
  reply when all of it is in the input, or arrives in receives that don't wait;
  `Nothing` until then, `Right` once the ending was observed, `Left` for its
  failure (a deferred constraint at COMMIT). `endingWatch` is the watch for
  it. The replies after the field have to fit the input, which is checked and
  raised instead of waiting forever.

## Invariants

- **Every borrowed byte is in a `BufferAccess` loan, at most two per
  request.** An upload holds Warp's body loan and native-pg's input loan, as
  today; a response holds native-pg's input loan. Gathering several chunks
  from one source would need more loans, so a batch holds at most one chunk
  per source.
- **Owned memory stays bounded:** per-output framing buffers up to 64 KiB,
  and the HTTP header.
- **Nothing is pulled before the previous chunk is accepted.** A slow HTTP
  client stops Warp's pulls, which stops native-pg's reads, which fills
  PostgreSQL's socket buffer. A slow PostgreSQL stops native-pg's pulls of
  the upload, and TCP throttles the HTTP client.
- **A pending pull or a wait holds no new data.** Watches only register
  interest.
- **One owner per link.** Only the request thread calls a link, as
  native-pg requires today; the waiter threads that broke this briefly are
  gone.
- **Scopes:** every acquisition in a scope is released exactly once, when the
  scope closes, and nothing tagged with the scope outlives it.
- **Teardown releases without joining.** With no producer or waiter
  threads, scope exit cancels watches and returns loans; there is nothing
  to kill and join.

## Why not conduit or streamly

They compose transformations over streams of owned values. This layer is
about buffer ownership, readiness and syscalls, which they leave out:

- chunks are GC-owned values that any stage may keep, so pool-backed slices
  would need a copy at every boundary;
- a pull blocks, with no way to ask whether the next chunk is ready, which
  is what gathering the header with a ready first chunk needs;
- waiting on two peers at once is done with concurrency, i.e. threads;
- a sink takes one element at a time: no gathered write, no partial-send
  tail pointing at borrowed storage;
- the pipelines here are assembled at run time across packages, where
  fusion doesn't apply, and per-step continuations allocate per chunk.

WAI 3.0 replaced conduit in WAI's types with IO callbacks for similar
reasons. An adapter from `Source` to a streaming library can still serve
code that transforms bodies, copying at that boundary.

## Changes to DESIGN.md

- **Principle 4:** "Every pull blocks until data is available" becomes "a
  pull takes what is ready or reports that it isn't; a blocking pull is a
  try followed by a wait". Nothing is still read without a request.
- **Socket I/O:** waits become one `awaitAny` over what the caller is
  blocked on, which the link waits for with watches or more directly.
- **Pull-based reading:** the same amendment as principle 4.
- **Borrowed upload chunks:** the producer thread, the acknowledgement
  bridge and the joined waiters are replaced by the source's pull and watch.
- **HTTP/1.1 usage:** PostgREST passes a source instead of a sink;
  "the HTTP sink finishes writing each borrowed chunk before the next native
  input operation" becomes "Warp pulls the next chunk only after the client
  accepted the previous one".

## Order of changes

Each step is its own change, tests first.

1. **Move, unchanged.** `Link` (with the `Socket` instance), the generic
   `Output` core and the sender move into `streaming-io`; native-pg uses
   them. No behaviour change.
2. **STM readiness.** Add watches and `awaitAny` to `Link`, which replaces
   its three waits; native-pg's combined waits use it. The waiter threads go.
3. **Pull sources.** Add the `Source` class and the registering borrow;
   `ParameterBorrowedStream` takes a pull source; the gather rule pulls when
   ready. The producer thread goes, and an upload's framing leaves with its
   first chunk.
4. **Warp.** The request body becomes a pull source with resumable
   decoders; streaming responses take a source and go through the sender.
   The header leaves with a ready first chunk; chunked framing is gathered.
5. **PostgREST.** Hand Warp the column source; move the final-chunk rule
   into a source wrapper. Count sends per request and compare CPU locally.

## Open questions

- **Chunk validity.** "Valid until the next pull" is documented, not
  enforced, as for `nextChunk` today. A debug build could check a pull
  generation on use.
- **Where the `Socket` instance lives:** in `streaming-io` (a `network`
  dependency) or in a small companion package.
- **Warp's 1-byte metadata read** for the next keep-alive request could use
  the same try and watch; it is outside this change.
- **Upload size limit.** A streamed body above
  `server-request-stream-threshold` bypasses `server-request-body-limit`
  (`ApiRequest.hs:142-152`). Unrelated to this design, found while reading.
