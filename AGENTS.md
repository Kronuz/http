# AGENTS

Orientation for anyone (human or agent) working on this library.

## What this is

A generic HTTP/1.1 application layer on top of Kronuz/server — **Leg 2** of
inverting Xapiand, the seam where the application sits *above* the server. An
application implements one interface, `HttpHandler::handle(const Request&,
ResponseWriter&)`, and the engine parses, routes, frames, and writes. Header-only.
Read `README.md` for the shape and the forward-compatibility rationale; this is
the working notes.

## File map

```
http_message.h     Request + the buffered Response value + reason_phrase().
http_handler.h     HttpHandler (the seam) + ResponseWriter (buffered/streamed output).
http_router.h      Router — method/path dispatch; a thin binding over Kronuz/radix-router.
http_accept.h      Accept — content negotiation (Accept / Accept-Encoding / …), RFC 7231.
http_compression.h Transparent response compression (Accept-Encoding → zstd/gzip).
http_conditional.h Conditional requests — a weak ETag from the body + If-None-Match → 304.
http_range.h       Range requests — parse a single byte range (bytes=N-M / N- / -M).
http_dispatcher.h  Dispatcher — a bounded worker pool (Kronuz/queue) for off-reactor work.
http_watchdog.h    StallWatchdog — flags the loop if it stops ticking (offload observability).
http_connection.h  The connection: http_parser parsing + HTTP/1.1 framing + the one
                   handler call site (inline or offloaded), over BaseClient.
http_server.h      HttpServer (accept loop) + HttpService (worker-tree root; owns the
                   per-reactor Dispatcher + the optional StallWatchdog).
examples/kv_store.cc        A complete REST app: buffered routes + a streaming route.
examples/dispatcher_bench.cc Throughput of the Dispatcher queue (single vs sharded).
test/test.cc       ctest: method dispatch, 404 vs 405, param capture, streamed body.
test/loadtest.cc   ctest "loadtest": the un-stallable model over real TCP (a slow
                   handler must not stall the loop) + 503 backpressure.
```

## Dependencies (Kronuz family, FetchContent at tip)

- **server** — the libev reactor + TCP + the `BaseClient` connection FSM.
- **http-parser** — the Joyent `http_parser` fork (accepts custom methods).
- **radix-router** — the radix-tree path router behind `Router`.

## The seam — invariants that keep the eventual migrations cheap

- **The handler is parser/transport/concurrency-agnostic.** It gets a `Request`
  and writes a `ResponseWriter`. It never sees the parser, the socket, or the
  loop. Don't leak any of those across the seam.
- **Completion is `ResponseWriter::end()`, not "handle() returned".** This is
  load-bearing: it is what lets a handler finish the response *later* — off a
  worker thread (the un-stallable model) or as a coroutine whose dispatch call
  site becomes `co_await` — with no handler-code change. Do not reintroduce
  "the response is done when handle() returns" anywhere.
- **Framing lives in the connection's Writer.** It buffers the first chunk to
  decide: a single `write()` → `Content-Length`; a second `write()` before
  `end()` → `Transfer-Encoding: chunked`. Keep-alive is honored on both. An
  unhandled handler exception is a 500 backstop, never a hung connection.
- **Routing is application config.** `Router` (per-method radix trees + 404/405)
  is what Xapiand's hardcoded `prepare()` method-switch becomes.

## Parser choice (why http-parser, not llhttp)

`http_parser` accepts arbitrary request methods; Xapiand's REST API uses custom
verbs (`COUNT`, `INFO`, `DUMP`, `RESTORE`, …) that a method-validating parser
(llhttp) rejects with `HPE_INVALID_METHOD`. The seam is parser-agnostic, so the
choice is contained in `HttpConnection`. Note http_parser has no
`on_header_value_complete`, so headers are paired via the field→value→field
transition (`reading_value_`). Planned follow-up (separate commit): refresh the
fork 2.7.1 → 2.9.4 for upstream security/correctness fixes.

## Never-stalling the loop (the un-stallable model — built)

When `HttpService` is created with a worker count, it owns a **per-reactor
`Dispatcher`** (a bounded worker pool) and `HttpConnection` runs `handle()` on a
worker so a slow or blocking handler never stalls the loop. The reactor keeps
accepting, reading, and serving other connections. `test/loadtest.cc` proves it:
~8 ms fast-request latency under offload vs ~450 ms inline (same slow load).

Invariants — do not break these when touching `HttpConnection`:

- **One in-flight handler per connection.** On offload the parser is paused
  (`http_parser_pause`) at message-complete and `io_read` is stopped, so the
  worker has sole, race-free access to `request_`/`writer_`, responses stay
  ordered, and the stopped read is TCP backpressure. Pipelined bytes already
  received are stashed in `pending_input_` and re-fed when the handler completes.
- **The worker never touches reactor-owned state.** It reads `request_` and drives
  the writer (whose `write()` path is thread-safe — Kronuz/server enqueues to the
  per-connection `write_queue` and wakes the reactor). Completion (`end()`) is
  handed back to the reactor via `complete_async_` (an `ev::async`); the reactor
  runs `complete_response` + resumes reading, because that step touches the ev
  watchers / `close()` and must run on the loop. The task captures
  `share_this<HttpConnection>` so the connection outlives the work.
- **`submit()` false → 503, inline.** A full bounded queue is the backpressure
  signal; answer 503 on the reactor rather than block it or grow an unbounded
  backlog.
- **Offloaded handlers run concurrently across connections, so the `HttpHandler`
  must be thread-safe** when a Dispatcher is configured. With no Dispatcher
  (`create(handler)`), `handle()` runs inline on the reactor — the cheap fast path,
  single-threaded, unchanged.
- **Per-route fast path.** `HttpHandler::should_offload(request)` (default true) is
  consulted per request when a Dispatcher exists; return false to keep a cheap,
  non-blocking route (health check, metrics) inline on the reactor so it is served
  even when every worker is busy. A route classified cheap must actually be cheap —
  it runs on the reactor.
- **Observability.** `HttpService::watch_stalls(threshold, on_stall)` arms a
  `StallWatchdog`: a periodic timer pets the loop and a monitor thread fires
  `on_stall` (default: stderr) on the healthy→stalled edge. The timer forces the
  loop to wake even when idle, so a stale pet means genuinely stuck, not waiting on
  I/O. In the offload model the loop shouldn't stall; this catches a misclassified
  cheap handler or an engine bug. Stop the monitor before breaking the loop
  (`HttpService::stop()` does) so it can't false-fire on the shutdown quiescence.

Per-reactor, not global: one `HttpService` == one loop == one Dispatcher
(shared-nothing across cores). To scale, run several `HttpService` instances on
several threads (SO_REUSEPORT). `examples/dispatcher_bench.cc` shows the single
queue is not the bottleneck for ms-scale handlers and that sharding the queue
would not help (the cost at high worker counts is condvar wakeup latency, not lock
contention) — size the pool to the task rate instead.

Both follow-ups are now built: the **stall watchdog** (`http_watchdog.h`) and
**per-route classification** (`should_offload`). Validated race-/leak-free under TSAN
and ASAN+UBSAN (Homebrew LLVM); the only sanitizer reports are libev's own async
primitive (hand-rolled fences TSAN can't model), not the offload path.

## Response compression (a generic knob)

`HttpService::enable_compression(opts)` turns on transparent response compression
(`http_compression.h`). The connection's Writer, at `end()` on the buffered path,
negotiates the body's `Content-Encoding` from the request's `Accept-Encoding`
(reusing `http::Accept`) and compresses with Kronuz/compressors, setting
`Content-Encoding` + `Vary: Accept-Encoding` and a `Content-Length` from the
compressed size. The application just writes bytes; the lib never learns its model.

- **Codings: zstd (preferred) and gzip.** HTTP `deflate` is intentionally omitted —
  it is ambiguously raw-vs-zlib in the wild and the backend emits raw deflate, which
  some clients reject. `Accept::best({"zstd","gzip"})` favours zstd on ties.
- **Conservative triggers.** Compress only when the client *explicitly* advertised a
  coding we produce (absent `Accept-Encoding` → identity, not RFC 7231's "anything");
  skip bodies below `min_size`, bodyless statuses (204/304/1xx), a response that
  already set `Content-Encoding`, and the result if it didn't actually shrink.
- **Buffered path only.** A streamed/chunked response is left as-is (the first chunk
  already committed the framing); streaming compression is a later refinement.
- **The CPU lands where the handler ran** — on the worker for an offloaded handler,
  which is where it belongs. Validated round-trip per codec + race-/leak-free with
  compression running on workers (loadtest phase G).

## Conditional requests (a generic knob)

`HttpService::enable_conditional()` (`http_conditional.h`) makes a repeat GET of an
unchanged resource cost a hash and a header instead of the body. At `end()` on the
buffered path (GET/HEAD 200s only), the Writer derives a **weak ETag** from the body
(`W/"<fnv1a64 hex>"`) and, if the request's `If-None-Match` already holds it (or is
`*`), drops the body and answers `304 Not Modified` (no `Content-Length` — 304 is
defined bodyless). It runs *before* compression, so the ETag identifies the resource
(not the wire bytes — which is exactly why it is weak) and a 304 skips compression
entirely. Last-Modified/If-Modified-Since is intentionally absent: it needs an
app-supplied mtime the library can't know. A handler that sets its own `ETag` is
left alone.

## Range requests (a generic knob)

`HttpService::enable_ranges()` (`http_range.h`) lets a buffered GET 200 advertise
`Accept-Ranges: bytes` and serve a single byte `Range` as `206 Partial Content`
(`Content-Range`), or `416` when unsatisfiable — what media players and resumable
downloads use to seek. Forms handled: `bytes=N-M`, `bytes=N-`, `bytes=-N`. A
multi-range (`multipart/byteranges`) or any other unit is left unrecognized and the
full 200 is served — always correct, just not partial. It runs after conditional and
before compression; a 206 is served **uncompressed** (range + content-coding is a tar
pit, and ranges are for already-compressed media).

The transforms run in order at `end()`: **conditional → range → compression**, so a
304 short-circuits everything and a 206 skips compression. All four are configured
through one `ResponseOptions` (held by `HttpService`, a `const ResponseOptions*`
reaches the connection — the same plumbing the Dispatcher uses), enabled by
`enable_compression()` / `enable_conditional()` / `enable_ranges()`. The parsers
(`if_none_match`, `parse_byte_range`) have direct unit coverage in `test/test.cc`.

## Roadmap (Leg 2 productionization)

Migrate Xapiand's ~20 search `*_view` endpoints onto this seam: a
`SearchApplication` `HttpHandler`/`Router`, with MsgPack serialization and
`catch_http_errors` (Xapian-exception → HTTP) moved to the app boundary. Buffered
+ response-streaming covers most endpoints; RESTORE/bulk *request-body* streaming
waits for the Asio + coroutine migration. Verify against the doc-driven E2E suite
(`Xapiand/docs_to_postman.py | newman run`).

## Build / test

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
ctest --test-dir build              # http (unit) + loadtest (un-stallable, ~2s)
./build/kv_store 8080
#   curl -XPUT localhost:8080/kv/k -d v ; curl localhost:8080/kv/k   # buffered
#   curl localhost:8080/stream/5                                     # chunked
./build/dispatcher_bench            # queue throughput / sharding analysis
```

Concurrency validation (Homebrew LLVM — Apple clang's sanitizers misbehave here):

```sh
cmake -B build-tsan -S . -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++ \
  -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm/bin/clang -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread -g -O1" -DCMAKE_C_FLAGS="-fsanitize=thread -g -O1" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"   # + FETCHCONTENT_SOURCE_DIR_* to reuse deps
cmake --build build-tsan --target http_loadtest
# suppress libev's async primitive (race:evpipe_write / ev_async_send / pipecb)
TSAN_OPTIONS="suppressions=tsan.supp" ./build-tsan/http_loadtest
```
