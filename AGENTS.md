# AGENTS

Orientation for anyone (human or agent) working on this library.

## What this is

A generic, extensible HTTP/1.1 application framework on **standalone Asio** (C++20
coroutines). It puts the application *above* the server, behind one seam: the
application implements one interface,
`HttpHandler::handle(const Request&, ResponseWriter&)`, and the framework parses,
routes, negotiates, frames, and writes. Header-only. Read `README.md` for the shape,
`ARCHITECTURE.md` for the request lifecycle; this is the working notes.

## File map

```
http_message.h        Request + the buffered Response value + reason_phrase().
http_handler.h        HttpHandler (the seam; handle() is a coroutine) + ResponseWriter
                      (buffered/streamed output) + BodySink (push streaming).
http_body_stream.h    BodyStream: a request body read with co_await (a bounded channel the
                      connection feeds); the handler-facing async_read and the feed side.
http_router.h         Router — method/path dispatch; a thin binding over Kronuz/radix-router.
http_accept.h         Accept — content negotiation (Accept / Accept-Encoding / …), RFC 7231.
http_compression.h    Transparent response compression (Accept-Encoding → zstd/gzip).
http_conditional.h    Conditional requests — a weak ETag from the body + If-None-Match → 304.
http_range.h          Range requests — parse a single byte range (bytes=N-M / N- / -M).
http_request_parser.h RequestParser — transport-agnostic http-parser wrapper: bytes → Request,
                      with buffered-or-streamed body intake (the headers-complete hook).
http_asio.h           The Asio transport: the connection as a C++20 coroutine (the
                      reactor::Session), the buffered ResponseWriter with compression,
                      keep-alive, the per-request watch (ConnectionInput), the per-request
                      abort hook (RequestAbort, a reactor::Abortable), the streamed-body
                      feed, and HttpAsioService (a thin adapter over reactor::TcpServer).
http_deferred.h       DeferredReply: an owned, delayed buffered reply (ResponseWriter::defer()).
http_owned_stream.h   OwnedStreamCapability: owned response streaming (ResponseWriter::stream()).
http_owned_stream_asio.h  Its Asio implementation, plain TCP and TLS.
http_owned_storage.h  Owned chunk storage and allocation refusal for owned streams.
http_asio_tls.h       Optional TLS adapter (tls_connection_factory); the only OpenSSL user.
reactor (dep)         The generic Asio server runtime (Kronuz/reactor): the shared-nothing
                      reactor pool, accept/bind strategies, admission gate, and graceful
                      shutdown. Extracted from http_asio.h so other services share it.
examples/kv_store.cc  A complete REST app: buffered routes, a streamed response route, a
                      streamed request-body route (POST /bulk).
examples/bench_asio.cc Raw-Asio throughput ceiling (no framework) — the reference bench_http
                      is measured against.
examples/bench_http.cc Framework throughput: the same fixed response through HttpAsioService.
test/test.cc          ctest: method dispatch, 404 vs 405, param capture (transport-agnostic).
test/deferred_test.cc, deferred_asio_test.cc, owned_stream_test.cc,
owned_stream_asio_test.cc
                      ctest: deferred replies, owned streaming.
test/coroutine_test.cc ctest "coroutine": coroutine handlers, BodyStream, disconnect cancels
                      reads not writes, the admission 503, the body cap's 413, shutdown.
docs/mechanisms/coroutine-handlers.md  How a request runs, is cancelled, admitted and stopped.
docs/status/phase7c-l3.md  The L3 ledger.
docs/TRANSPORT.md     Deferred replies, TLS, connection and request limits, rejections.
docs/OWNED_STREAMING.md  Owned response streaming: framing, backpressure, footprint.
docs/AUDIT.md         Transport audit findings with their before/after measurements.
test/asio_test.cc     ctest "asio": the transport end-to-end over real TCP — suspended
                      handlers, 503 backpressure, N reactors (both bind modes), keep-alive,
                      compression, streamed request bodies, error mapping.
```

## Dependencies (FetchContent, version-pinned)

- **asio** — [chriskohlhoff/asio](https://github.com/chriskohlhoff/asio) `asio-1-36-0`, header-only, `ASIO_STANDALONE` (no Boost). The runtime + TCP + coroutine substrate. Set `-DASIO_INCLUDE_DIR=<dir>` to reuse an existing checkout instead of fetching.
- **http-parser** — the Joyent `http_parser` fork (accepts custom methods).
- **radix-router** — the radix-tree path router behind `Router`.
- **compressors** — the deflate/gzip + zstd codecs behind response compression.

There is **no dependency on Kronuz/server** — the Asio transport replaced the libev
one. Xapiand still uses Kronuz/server for its other (non-HTTP) services, but this
library does not.

## The seam — invariants that keep the moving parts contained

- **The handler is parser/transport/concurrency-agnostic.** It gets a `Request` and
  writes a `ResponseWriter`. It never sees the parser, the socket, or the loop. Don't
  leak any of those across the seam.
- **Scoped response ownership.** Synchronous handlers finish before returning. Deferred handlers transfer authority through `ResponseWriter::defer()` and retain only the owned capability. Never retain a stack writer or Request. Original socket IO must be reaped before releasing request or reply leases; acceptance is distinct from delivery.

- **Routing is application config.** `Router` (per-method radix trees + 404/405) is
  what Xapiand's hardcoded `prepare()` method-switch becomes.

## Parser choice (why http-parser, not llhttp)

`http_parser` accepts arbitrary request methods; Xapiand's REST API uses custom verbs
(`COUNT`, `INFO`, `DUMP`, `RESTORE`, …) that a method-validating parser (llhttp)
rejects with `HPE_INVALID_METHOD`. The seam is parser-agnostic, so the choice is
contained in `http_request_parser.h`. Note http_parser has no
`on_header_value_complete`, so headers are paired via the field→value→field
transition (`reading_value_`).

## The un-stallable model (Asio coroutine)

One connection is one coroutine (`detail::serve_connection` in `http_asio.h`, run as a
`reactor::Session`), and a handler is a coroutine awaited from it on the same loop:

```cpp
co_await handler->handle(request, writer);   // suspends on whatever it waits for
```

A handler that waits `co_await`s, the loop serves other connections, and the coroutine
resumes on the connection's own `io_context`. There is no worker pool and no thread
handoff. Invariants -- do not break these when touching `serve_connection`:

- **One in-flight handler per connection.** The coroutine is the single owner of its
  `Request` and `ResponseWriter`; it does not read the next request until this one's
  response is written. So there is no pause/resume bookkeeping and no cross-thread
  handoff of the connection state -- the race class a callback FSM hides cannot arise.
- **A request's frame is never destroyed early.** Cancellation is disabled on the
  connection coroutine from just before the handler runs until its response is written
  (`reset_cancellation_state(disable_cancellation())`, re-enabled at the top of the next
  request). Shutdown and hang-ups arrive as `Request::cancel`, never as a destroyed
  frame. `reactor->stopping()` is checked at the top of the keep-alive loop because a
  session cancellation that arrived while disabled is not replayed.
- **Admitted at headers-complete, released when the response is settled.**
  `reactor->gate().enter()` in the headers hook; empty means `RequestRejected(503, ...,
  retry_after 1)`. The lease is a connection-scope local, reset after the write.
- **Tracked and watched for the whole run.** `RequestAbort` (a stack `reactor::Abortable`)
  is on the reactor's intrusive list from before the handler to after the write:
  `abort()` is `Request::cancel(Shutdown)`, `force()` closes the socket after the grace.
  `ConnectionInput::watch` keeps one one-byte read pending: EOF or a read error (not our
  own cancellation) calls `Request::cancel(Disconnect)`. That is the per-request rule and
  it differs from the deferred-reply monitor's (EOF is a half-close there). A pipelined
  byte ends the watch for the rest of the request. `WatchGuard` unwatches on every exit.
- **Cancel reads, never writes.** A handler stops reading at its next checkpoint when
  `Request::cancelled()`; the transport still writes the response (a half-closed client
  gets its answer). Nothing in the transport aborts a write for a disconnect.
- **`ConnectionInput::drain` must end when cancelled.** A cancelled waiter cancels the
  pending one-byte read (`lowest_layer().cancel()`); otherwise an idle keep-alive
  connection would hold shutdown until its peer sent a byte.
- **Handlers are single-threaded per reactor but run on every reactor.** State shared
  across reactors still needs its own synchronization.

`test/coroutine_test.cc` proves each of these over real TCP; `docs/mechanisms/
coroutine-handlers.md` is the long form.

## Multi-reactor binding (portable)

`HttpAsioService` runs on a `reactor::TcpServer`: N `io_context`s on N threads
(shared-nothing). Binding N reactors to one port (handled by the runtime):

- **`reuse_port` true (Linux):** each reactor binds its own acceptor; the kernel
  load-balances. `AsioBindOptions{reuse_port=true}` (`AsioBindOptions` is an alias for
  `reactor::BindOptions`).
- **`reuse_port` false / macOS/BSD:** a second same-port bind is rejected, so one
  acceptor on reactor 0 distributes accepted connections round-robin (accepting each
  new socket directly onto its target reactor's `io_context`). Chosen automatically
  when `reuse_port` is off and there is more than one reactor. `asio_test [D2]` covers
  it.

## Request-body intake (three modes)

Chosen at headers-complete:

- **Buffered** (default): the body lands in `Request::body`. The up-front reservation
  from `Content-Length` is capped (`kMaxBufferedReserve`) so a hostile length can't
  force a giant allocation.
- **Push-streamed** (`on_request_body` returns a `BodySink`): body chunks go to the
  sink as parsed, on the reactor, nothing accumulated. For light per-chunk work.
  `asio_test [H]` streams a 2.4 MB NDJSON body and asserts `Request::body` stays empty.
- **Streamed** (`wants_body_stream` returns true): `handle()` is spawned on the
  connection's loop as soon as the headers are in and reads `Request::body_stream` (a
  `BodyStream`) with `co_await`. The connection coroutine feeds a bounded
  `asio::experimental::channel` (capacity 8 chunks) and suspends in `push()` when it is
  full, so back-pressure reaches the socket. `finish()` ends the stream (`eof`, sticky),
  `fail(ec)` ends it with an error after the buffered chunks, `abandon()` (the handler
  completed) releases a push that is waiting. The feed runs in `feed() || handler_done`:
  if the handler answers without reading it all, the feed is cancelled and the
  connection closes after the response. The join on the handler is uncancellable
  (`HandlerDone::wait` with cancellation disabled), so the request and writer outlive it.
  The body is not capped. `asio_test [K]` (2 MB through a slow consumer, all reads on the
  one loop thread, a fast request served mid-stream, an over-read returns eof, a tiny
  body arriving with the headers) and `[L]` (stop() while a stream is mid-flight).
- The **buffered cap** (`AsioResponseOptions::max_body`, 16 MiB by default) is enforced
  by `RequestParser::set_body_limit`: a `Content-Length` over it stops the parse at
  headers-complete, and a chunked body stops it as bytes arrive; the connection throws
  `RequestRejected(413)`. The callbacks return -1 (http_parser treats 1 as "no body").

## Response transforms (generic knobs)

The Asio `ResponseWriter` applies three optional transforms at `serialize()`, in
order **conditional → range → compression**, each enabled on the service
(`enable_conditional()` / `enable_ranges()` / `enable_compression()`), all buffered
GET-oriented (the Asio writer buffers the whole response):

- **Conditional** (`http_conditional.h`): a weak ETag `W/"<fnv1a64 hex>"` from the
  body + `If-None-Match` → `304` (bodyless). Runs first so the ETag identifies the
  resource, not the wire bytes; a handler that set its own `ETag` is left alone.
  GET/HEAD 200s only.
- **Range** (`http_range.h`): a single byte range → `206 Partial Content`
  (`Content-Range`) / `416`; advertises `Accept-Ranges: bytes`. Forms `bytes=N-M`,
  `bytes=N-`, `bytes=-N`; a multi-range/unrecognized header serves the full 200. A
  206 is served uncompressed (range + content-coding is a tar pit). GET 200s only.
- **Compression** (`http_compression.h`): negotiates `Content-Encoding` from
  `Accept-Encoding` (via `http::Accept`) and compresses with Kronuz/compressors,
  setting `Content-Encoding` + `Vary`. Policy: compress only when the client
  *explicitly* advertised a coding we produce (absent `Accept-Encoding` → identity);
  skip below `min_size`, bodyless statuses (204/304/1xx), a 206 slice, an
  already-`Content-Encoding`'d response, and a result that didn't shrink. zstd
  preferred over gzip; HTTP `deflate` intentionally omitted (raw-vs-zlib ambiguity).

A `304` short-circuits range + compression; a `206` skips compression. `asio_test`
[I]/[J] cover conditional/range; the parsers (`if_none_match`, `parse_byte_range`)
also have direct unit coverage in `test/test.cc`.

## Limitations

- Ordinary Asio `write()` calls remain buffered. Optional response-side streaming uses `ResponseWriter::stream()` and its independently owned capability; exact-length and bounded chunked modes are described in `docs/OWNED_STREAMING.md`. Request-side push and concurrent pull streaming remain available.

## Build / test

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
ctest --test-dir build              # http (unit) + asio (transport, ~2s)
./build/kv_store 8080
#   curl -XPUT localhost:8080/kv/k -d v ; curl localhost:8080/kv/k   # buffered
#   curl localhost:8080/stream/5                                     # chunked response
#   curl -XPOST localhost:8080/bulk --data-binary @big.ndjson        # streamed request body
./build/bench_http 8080 4 &  ./build/bench_asio 8081 4 &             # framework vs raw ceiling
```

`HttpAsioService` preserves the existing ConnectionFactory and constructor APIs. An additive TimedConnectionFactory receives reactor's acceptance timestamp and an explicit sixth constructor argument sets a global connection cap before destination dispatch. `set_connection_limit()` and `connection_stats()` expose the same gate for existing factories. The timed path requires the matching reactor connection-admission API; upgrade the sibling reactor headers together when adopting it.

For TLS, `tls_connection_factory_timed()` uses the original accept timestamp so destination queue wait consumes the handshake deadline. Pass it with an explicit service connection bound. The original `tls_connection_factory()` remains source-compatible and keeps its destination-side handshake/idle-connection limits; without a service-level gate it does not bound sockets queued before the factory starts. TLS allocation follows capacity and elapsed-deadline checks. Transport leases survive original serving IO and refund on retirement.
