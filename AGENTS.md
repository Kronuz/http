# AGENTS

Orientation for anyone (human or agent) working on this library.

## What this is

A generic, extensible HTTP/1.1 application framework on **standalone Asio** (C++20
coroutines) — **Leg 2** of inverting Xapiand, the seam where the application sits
*above* the server. An application implements one interface,
`HttpHandler::handle(const Request&, ResponseWriter&)`, and the framework parses,
routes, negotiates, frames, and writes. Header-only. Read `README.md` for the shape,
`ARCHITECTURE.md` for the request lifecycle; this is the working notes.

## File map

```
http_message.h        Request + the buffered Response value + reason_phrase().
http_handler.h        HttpHandler (the seam) + ResponseWriter (buffered/streamed output)
                      + BodySink (push streaming) + BodyReader (concurrent pull streaming).
http_router.h         Router — method/path dispatch; a thin binding over Kronuz/radix-router.
http_accept.h         Accept — content negotiation (Accept / Accept-Encoding / …), RFC 7231.
http_compression.h    Transparent response compression (Accept-Encoding → zstd/gzip).
http_conditional.h    Conditional requests — a weak ETag from the body + If-None-Match → 304.
http_range.h          Range requests — parse a single byte range (bytes=N-M / N- / -M).
http_request_parser.h RequestParser — transport-agnostic http-parser wrapper: bytes → Request,
                      with buffered-or-streamed body intake (the headers-complete hook).
http_asio.h           The Asio transport: the connection as a C++20 coroutine, the buffered
                      ResponseWriter with compression, bounded offload, keep-alive, and
                      HttpAsioService (N io_contexts on N threads, one port).
examples/kv_store.cc  A complete REST app: buffered routes, a streamed response route, a
                      streamed request-body route (POST /bulk).
examples/bench_asio.cc Raw-Asio throughput ceiling (no framework) — the reference bench_http
                      is measured against.
examples/bench_http.cc Framework throughput: the same fixed response through HttpAsioService.
test/test.cc          ctest: method dispatch, 404 vs 405, param capture (transport-agnostic).
test/asio_test.cc     ctest "asio": the transport end-to-end over real TCP — un-stallable
                      offload, 503 backpressure, N reactors (both bind modes), keep-alive,
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
- **Completion is `ResponseWriter::end()`, not "handle() returned".** This is
  load-bearing: it lets a handler finish the response *later* — off the offload pool
  — with no handler-code change. Do not reintroduce "the response is done when
  handle() returns".
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

One connection is one coroutine (`serve_connection` in `http_asio.h`). When the
service is created with a worker count, each `AsioReactor` owns an Asio `thread_pool`
and a bounded offload window. A request that opts in (`should_offload`) is run with:

```cpp
co_await asio::co_spawn(reactor->pool->get_executor(),
    [&]() -> asio::awaitable<void> { handler->handle(request, writer); co_return; },
    asio::use_awaitable);
```

The handler runs on a pool thread, the reactor stays free to serve other connections,
and the coroutine resumes on the connection's own `io_context`. Invariants — do not
break these when touching `serve_connection`:

- **One in-flight handler per connection.** The coroutine is the single owner of its
  `Request` and `ResponseWriter`; it does not read the next request until this one's
  response is written. So there is no pause/resume bookkeeping and no cross-thread
  handoff of the connection state — the race class a callback FSM hides cannot arise.
- **Bounded offload → 503.** `OffloadGate::try_enter()` fails when the window is full;
  answer 503 inline rather than grow an unbounded backlog.
- **Offloaded handlers run concurrently across connections, so the `HttpHandler` must
  be thread-safe** when a worker count is set. With `workers = 0`, `handle()` runs
  inline on the reactor — the single-threaded fast path.
- **Per-route fast path.** `should_offload(request)` (default true) is consulted per
  request; return false to keep a cheap route (health check, metrics) inline so it is
  served even when every worker is busy. A route classified cheap must actually be
  cheap — it runs on the reactor.

`test/asio_test.cc` proves it: fast-request latency stays ~15 ms under offloaded slow
load vs ~450 ms inline (same load).

## Multi-reactor binding (portable)

`HttpAsioService` runs N `io_context`s on N threads (shared-nothing). Binding N
reactors to one port:

- **`reuse_port` true (Linux):** each reactor binds its own acceptor; the kernel
  load-balances. `AsioBindOptions{reuse_port=true}`.
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
- **Pull-streamed / concurrent** (`wants_body_stream` returns true, needs a worker
  pool): the framework runs `handle()` on a worker *concurrently* with the body read
  and gives it a `BodyReader` (`Request::body_reader`) to PULL raw chunks from.
  `read()` blocks the worker on a condition variable until the next chunk; the reactor
  feeds through a bounded queue and, when it is full, yields via a short timer and
  retries (flow control -> the reactor stays free, back-pressure reaches the socket).
  O(buffer) memory for any size. The handler is launched with an explicit
  `co_spawn(pool_exec, handle, done_signal)` (so it really runs on the pool -- the
  `&&`/awaitable-operators form ran it on the reactor and deadlocked); the reactor
  feeds, then co_awaits the done signal. feed never throws (a socket/parse error
  `abort()`s the reader so `read()` returns false). `read()` is sticky at end (an
  over-read never blocks). The consumer waking on a condvar -- not the io_context --
  is what lets shutdown work: `HttpAsioService::stop()` calls `AsioReactor::abort_readers()`
  (a per-reactor registry of in-flight readers) so a handler blocked mid-stream unwedges
  before the pool join, even after `io.stop()`. An empty chunk is the end-of-body marker;
  a streaming handler MUST drain the reader.
  `asio_test [K]` (2 MB through a slow consumer: no loss under back-pressure + a fast
  request served mid-stream + an over-read returns false + a tiny body arriving with the
  headers) and `[L]` (stop() while a stream is mid-flight must not hang).

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

## Roadmap

- **Response-side streaming** (chunked `Transfer-Encoding`) in the Asio writer — it
  buffers the whole response today, so a huge `DUMP` is held in memory.
- The Xapiand HTTP path already runs on this framework (`SearchApplication` is the
  `HttpHandler`); see the Xapiand repo for the manager integration + RESTORE streaming.

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
