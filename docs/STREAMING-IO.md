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
  client sees a response while the body is slow.
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
- **What "ready" means.** Bytes already buffered, or a nonblocking read into
  the source's own loan that returned some. A `Pending` pull has read
  nothing beyond its loan's free space.
- **Warp's request body.** `pullReady` serves read-ahead, then tries
  `recvBufNoWait` into the request's loan. The Content-Length and chunked
  decoders (`Conduit.hs`) keep their state between pulls and report
  `Pending` when a size line or data is incomplete. The watch is the client
  socket's readable watch `orElse` graceful shutdown (today's give-up action
  in `makeGracefulRecvInto`).
- **native-pg's response column.** `pullReady` returns the field bytes
  already in the input buffer, or tries one receive into the input loan's
  free tail. The watch is the PostgreSQL link's readable watch.
- **Buffer loans.** A pull that needs a loan while none is free returns
  `Pending` with a watch on the pending borrow. `BufferAccess` gets a
  registering borrow: it joins the FIFO lane as `borrowBuffer` does today, so
  fairness is unchanged, and the next pull takes the granted loan.
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

## Drivers

- **native-pg input** (`linkInputSource`). After `receiveReady` returns
  `Nothing`: `pushOutput`, then `awaitAny link (Interest True blocked)` with
  what it reports, and receive again. Input is tried first, as today, on the
  request thread. When `pushOutput` has sent the rest of the output (`Done`),
  it receives again without waiting, since the server may have answered
  meanwhile: nothing waits while a read could succeed.
- **Warp response.** Loop `pushOutput` on the client link and
  `awaitAny link (Interest False blocked)` until it reports `Done`. The
  timeout is resumed only around waits for the link, as `sendFragment` does
  today, so a slow database never times the client out.
- **PostgREST.** Instead of pushing chunks into a sink (`consumeNext`,
  `App.hs:390-392`), it hands Warp the column's source with the advertised
  length. Write mode's rule that the final chunk waits for COMMIT becomes a
  source wrapper that reports `Pending` for the final chunk until the
  transaction has finished; `retainFinalChunk` moves into it.

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
