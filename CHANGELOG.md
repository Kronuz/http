# Changelog

Newest first. Dates are 2026.

Upgrading a pin from before 2026-10-30: [the consumer upgrade note](https://github.com/Kronuz/cpp-libraries/blob/main/docs/UPGRADING-2026-10.md#http) lists what changes for callers, how to find affected inputs, the commits not to pin, and the speedup.

## 2026-11-03: handlers, bodies and the connection on `task::task`

`task::task` ([Kronuz/task](https://github.com/Kronuz/task)) is the one coroutine type of
the API and of the connection: handlers, routes and `BodyStream` reads are tasks, the
connection is a task on its reactor's `task::asio_scheduler`, and a handler is awaited as a
child task, so one that finishes in its first run costs no post. Cancellation is one token
per request. Design `docs/design/task-handlers.md` revision 2; ledgers
`docs/status/task-h1.md` and `docs/status/task-h2.md`. Not timed yet: the throughput and
CPU rows are the lead's (the design's T6).

### Upgrade note

Every consumer changes in the same wave; no alias or second API remains.

| Before (http:679c0f8) | Now |
| --- | --- |
| `asio::awaitable<void> HttpHandler::handle(Request&, ResponseWriter&)` | `task::task<void> handle(Request&, ResponseWriter&)` |
| `Router::Route = std::function<asio::awaitable<void>(Request&, ResponseWriter&, const Params&)>`, `asio::awaitable<void> Router::handle(...)` | `std::function<task::task<void>(Request&, ResponseWriter&, const Params&)>`, `task::task<void> Router::handle(...)` |
| `co_await body.async_read(chunk, asio::redirect_error(asio::use_awaitable, ec))` | `ec = co_await body.read(chunk)`; `read(std::string&, task::cancel_token = {})` returns `task::task<asio::error_code>` with the same values, plus `operation_aborted` for its own requested token |
| `BodyStream(asio::any_io_executor, capacity)`, `asio::awaitable<bool> push(std::string)` | `BodyStream(task::asio_scheduler&, capacity)`, `task::task<bool> push(std::string, task::cancel_token)` |
| `Request::cancelled()`, `cancel_reason()`, `on_cancel(std::function<void(CancelReason)>)`, `enum class CancelReason { None, Disconnect, Shutdown }` | removed: `task::cancel_token Request::cancel_token()` (`requested()`, `reason()`), a `task::cancel_callback` on it, `http::disconnected` (`task::cancel_reason::user`) and `task::cancel_reason::shutdown` |
| `Request::cancel(CancelReason)` | `Request::cancel(task::cancel_reason)`, first call wins |
| `co_await asio::this_coro::executor` in a handler | `request.scheduler().executor()`; `request.scheduler()` is the reactor's `task::asio_scheduler` |
| `ConnectionFactory` (4 arguments) and `TimedConnectionFactory` (plus `reactor::SessionContext`), both `asio::awaitable<void>` | one `ConnectionFactory = std::function<task::task<void>(asio::ip::tcp::socket, HttpHandler*, reactor::Reactor*, const AsioResponseOptions*, reactor::SessionContext)>`; `SessionContext::stop` is the session's shutdown token, which a factory honors in its waits |
| `HttpAsioService(handler, reactors, in_flight_limit, ConnectionFactory = {})` and `HttpAsioService(handler, reactors, in_flight_limit, TimedConnectionFactory, maximum_connections)` | `HttpAsioService(handler, reactors, in_flight_limit, ConnectionFactory = {}, maximum_connections = 0)` |
| `tls_connection_factory(context, deadline, handshakes, connections)` and `tls_connection_factory_timed(..., TlsFailureHook)` | `tls_connection_factory(context, deadline = 5s, handshakes = 64, connections = 1024, TlsFailureHook = {})`, timed from the accept |
| `stop()`, then destruction | `stop()`, `close()`, `destroy_roots()`, then destruction (task's teardown phases; each runs the earlier ones it needs) |

Find what changes in a consumer with
`git grep -nE 'asio::awaitable|use_awaitable|co_spawn|this_coro|async_read\(|cancelled\(\)|cancel_reason\(\)|on_cancel\(|CancelReason|TimedConnectionFactory|tls_connection_factory_timed'`.
A handler that polled `cancelled()` polls `cancel_token().requested()`; one that registered
`on_cancel(cb)` keeps a `task::cancel_callback cb(request.cancel_token(), ...)` alive for as
long as it wants to be told (a throwing callback is dropped). A computation in slices writes
`task::budget slice(200us, request.cancel_token())` and calls `co_await slice.checkpoint()`.
Asio operations are awaited with `task::await_asio`, with an optional token.
`task::sync_wait` on a reactor thread deadlocks the loop.

### Changed

- **Pins.** task at task:6558c2e (new; `task::asio` is linked into `http`) and reactor at
  reactor:2fd30c8, whose `Session` is a task with its stop token and whose `stop()` is
  bounded by about twice the grace.
- **The handler is a child task.** No driver, post or shared block; the request, writer and
  `BodyStream` are locals of the connection declared before the handler.
- **Timeouts are `task::deadline`s** on the reactor's scheduler, linked to the session token
  for the next request's reads and the TLS handshake. A timed-out streamed body read returns
  `timed_out`.
- **The streamed body joins with `task::when_all`.** The feed and the handler are children of
  the connection; the handler's completion stops the feed, and shutdown stops it through the
  linked token. A truncated body still reads `connection_reset`, never `eof`. The disconnect
  watch after the body waits for the handler to ask for its token, as a buffered request's
  does, unless it asked during the body.
- **An escaped `task::cancelled`** while the request's token is requested is answered `503`
  with `Connection: close` when `on_error` leaves it unanswered; an owned stream whose head is
  on the wire closes without its terminal chunk.
- **Affinity.** The writer and `BodyStream::read` assert in Debug builds that they run on the
  connection's scheduler, or in a raw handler on its loop.

### Added

- `coroutine_test` [T] (the design's T4 (a)-(h): no handler post, demotion, a cross-thread
  resume, teardown with suspended handlers and zero live frames, the 503, a hang-up within one
  slice, an owned stream cancelled after its head, affinity including a body read) and [X]
  (the teardown phases); [S] (the streamed join: a hang-up after the body, a handler that
  answers without reading it); T4 (d) includes a body read at shutdown; `handler_count_test`
  (io handlers per request, buffered and streamed); [B] covers `read()` with a token and a
  deadline.
- `bench/server_compare` kind `httpb`: a 1 KiB streamed POST (`bench/post_body.lua`).
- `timeouts_test`'s slow-body case measures its own client: an attempt whose host stalled the
  client past the inactivity timeout is void and retried, not failed.

## 2026-11-01: the disconnect watch waits for the handler to ask

`bench_http` under `wrk` (a 14-byte reply through `HttpAsioService`, 2 reactors, keep-alive,
4 s runs, START, `e70ba34` and this change built and run in one hold, median of 4 rounds, A/A
is a second copy of the START binary): on the i9 `e70ba34` served 15% and 17% fewer requests
per second than START (`c23bb99`, before coroutine handlers), and this change serves as many or
more.

| i9-9980HK, req/s (CPU per request) | START `c23bb99` | `e70ba34` | this change | A/A |
| --- | ---: | ---: | ---: | ---: |
| 8 connections | 87,316 (11.1 us) | 74,503 (13.3 us) | 93,657 (10.6 us) | 0.95 |
| 64 connections | 97,986 (10.2 us) | 82,380 (12.1 us) | 98,730 (10.1 us) | 0.99 |

Against START: 0.85 and 0.83 before, 1.08 and 0.99 after (the A/A spread is 0.84 to 1.02).
On the M4 Pro (3 rounds) `wrk` sets the rate at about 37,000 req/s for all three, and the CPU
per request shows the change: START 22.5 and 22.1 us, `e70ba34` 22.7 and 22.4 us, this change
21.7 and 21.5 us at 8 and 64 connections (req/s against START 1.02 and 1.01, A/A 1.01).

### Changed

- **The disconnect watch reads only once the handler asks about cancellation.** The watch
  started a read before every handler: a `recv` that found nothing ahead of every reply, and a
  wake-up through a channel post when the next request arrived. A handler that never asks
  about cancellation cannot observe a hang-up, so on plain TCP the request now carries a
  `CancelWatch` hook (`Request::set_cancel_watch`) that the handler's first `on_cancel()`,
  `cancelled()` or `cancel_reason()` engages. Engaging peeks at the socket (`MSG_PEEK |
  MSG_DONTWAIT`) and applies the watch's rules to what the client did meanwhile: data waiting
  ends the watch, EOF or a socket error cancels the request before the call returns (so
  `cancelled()` is already true and an `on_cancel` callback has already run), and otherwise
  the read-ahead watch starts as before. Cancellation, admission, the body cap and the
  smuggling defenses are unchanged; TLS and the streamed path watch from the start.
- **A buffered reply goes out at once.** On plain TCP the connection's socket is non-blocking
  and the reply's head and body leave in one `write_some`; only what the send buffer does not
  take is written asynchronously (with its deadlines). The head's string is reused across the
  connection's replies, and a read with nothing read ahead is the socket's own operation.

### Added

- `readahead_test` [L] (the lazy watch and the `Request` hook), [Q] (a hang-up before the
  first question, pipelined input before it, a handler that never asks) and [N] (replies of
  1 byte to 4 MiB to a client that reads late). Removing the laziness, the peek, the hook in
  `on_cancel()`, the buffer advance after a partial send, or the partial-send check each fails
  named checks.

## 2026-11-01: Beast and cpp-httplib in the server comparison

### Added

- `bench/server_beast.cc` (`-DHTTP_BENCH_BEAST=ON`, Boost 1.92) and `bench/server_httplib.cc`
  (`-DHTTP_BENCH_HTTPLIB=ON`, cpp-httplib v0.64.0 fetched by the bench target only) answer the
  comparison's reply on the same number of threads. `bench/verify.py` sends the bench request to
  every server and aborts `bench/compare.sh` unless the answers are the same bytes, headers
  included. `compare.sh` takes `BUILD`, `KINDS`, `CONNS` and `REPS` to run a piece of the
  comparison and prints the wrk, Asio, Boost and cpp-httplib versions it ran.

### Fixed

- The minimal Asio server and the `reactor` kind answered without the `Connection: keep-alive`
  field that `http` always states, so the servers did not send the same bytes. They send it
  now, and the README table is measured with identical replies.

## 2026-11-01: trailers, CONNECT, and http-parser's hardening

A security review of http-parser's hardening against request smuggling found two ways to
smuggle through this library whatever the parser does, and one the parser now closes.

### Security

- **Trailer fields are not headers.** The trailer section of a chunked request went into
  `Request::headers`: every trailer field but the last was merged there, after the front end
  had set or stripped the headers it controls, so `X-User: admin` in a trailer reached the
  handler as a header. Trailers now go to `Request::trailers` (RFC 9110 section 6.5.1), and
  `header()` reads the header section only.
- **CONNECT closes the connection.** The parser stops at the end of a `CONNECT` request's
  header section, and the connection read on: the bytes a front end framed as its body by
  `Content-Length` were served as the next request. A `CONNECT` is now answered and the
  connection closed, since the service opens no tunnel and a `CONNECT` has no content
  (RFC 9110 section 9.3.6).
- **http-parser pinned to its hardened head (ef12ec6).** It rejects what request smuggling relies on
  (bare LF, obs-fold, control bytes in values, `Transfer-Encoding` other than `chunked`,
  malformed `Content-Length` and chunk lines), and closes the connection after an HTTP/1.0
  request with `Transfer-Encoding` (RFC 9112 section 6.1): such a request with
  `Connection: keep-alive` had the request behind it served.

### Fixed

- The docs and the CMake comment said http-parser accepts arbitrary request methods. Its
  method table is fixed, with Xapiand's verbs in it; they say so now.

## 2026-11-01: the README follows the family standard

### Changed

- The README states the description as its first sentence, puts Quick start before
  Installation, pins FetchContent to a commit, lists the limits as "Not for" bullets, and
  takes its Performance table from `docs/perf/timeouts-cpu.md` (the M4 Pro row held older
  numbers). It names CMake 3.16, the version `CMakeLists.txt` requires, where it said 3.14.

### Removed

- The README diagram (`docs/http.d2`, `docs/http.svg`). It drew a worker pool and a
  `should_offload` hook that the library does not have; `ARCHITECTURE.md` maps the layers.

## 2026-10-31: the disconnect watch reads ahead

A keep-alive request cost 91 us at one connection against 60 us for a minimal Asio server, and
with the production timeouts on, 45 us of CPU per request and 27.5k requests per second at 64
connections against 24 us and 43.5k. A profile of the server at one connection traced it to
the connection's disconnect watch and to the timer and the split write around it
(`docs/perf/timeouts-cpu.md`).

### Changed

- **The watch reads ahead.** `ConnectionInput` keeps one read pending while a handler runs, as
  before, but into its own 4096-byte buffer (`kReadAhead`) instead of one byte. The pending read
  is also the read for the next request, so a keep-alive request costs one `recv` where it cost a
  one-byte `recv` and a second `recv` for the rest. `read()` returns the bytes the watch took
  first, in order, in whatever pieces its caller asks for, and no further read-ahead is armed
  until they are consumed, so a connection holds at most 4096 bytes of input outside the kernel.
  Every rule is unchanged: EOF or a read error calls `Request::cancel(Disconnect)` while a
  request runs, only a read error other than EOF ends a deferred or owned-stream reply, and input
  the watch reads (formerly one byte, now up to 4096) ends the watch for the rest of that request.
- **`drain()` waits on a channel, not a timer.** A completed read wakes it through an
  `asio::experimental::channel` of capacity 1. A timer used as a condition variable costs a
  wake-up pipe `write` and `read` and a `kevent` per use on kqueue when it is the earliest timer.
- **`write_with_progress` slices across buffers.** With an inactivity timeout a reply is written in
  64 KiB slices of its head and body taken together, each slice with its own deadline; the head
  and a body of up to 64 KiB less its length leave in one `sendmsg`, where each buffer used to be
  sliced and sent on its own. The bytes on the wire are identical.
- `read()` no longer creates a coroutine frame for `drain()` when no read is pending.
- http-parser is pinned at 476ba60 (it was 178f873). The parser takes runs of URL and header
  bytes in one step and continues straight into the next state, 1.3x to 2.3x faster on its own
  benchmarks; its differential test holds every callback, its bytes and every field of
  `struct http_parser` to the previous parser, so nothing the transport sees changes. Its
  CHANGELOG has the numbers.
- `examples/bench_http` takes a third argument, `--timeouts`, that turns on the request, idle,
  header and inactivity timeouts of a production service.
- `bench/` holds the server comparison behind `docs/perf/timeouts-cpu.md`: `http_server_compare`
  (`-DHTTP_BUILD_BENCH=ON`) serves the same bytes from a minimal Asio server, `reactor::TcpServer`
  and `http`, `bench/run.sh` and `bench/compare.sh` drive it with `wrk`, and `bench/summarize.py`
  takes medians.

### Performance

`http_server_compare` (`bench/`) answers a 14-byte keep-alive reply through `HttpAsioService`
with one reactor, driven by `wrk` (4 s runs); "off" has no timeouts and "on" has the production
ones (`bench_http PORT 1 --timeouts`). Latency is in us, req/s is at 64 connections, CPU is the
server's user plus system time per request in us. Medians of three rounds, the order of the
variants rotating by round. "Before" is main with the http-parser it pinned (178f873), "parser"
is main with http-parser 476ba60, "after" is this change with 476ba60, and Asio is a minimal Asio
server answering the same bytes.

Apple M4 Pro (arm64, Apple clang 17, `-O3 -DNDEBUG`; the host was shared, so the spread is wider):

| row | before | parser | after | Asio |
|---|---|---|---|---|
| 1 connection, off: latency / CPU | 94.5 / 42.3 | 99.5 / 42.2 | 80.8 / 35.9 | 64.3 / 30.2 |
| 1 connection, on: latency / CPU | 149.1 / 82.3 | 142.4 / 78.9 | 97.5 / 45.7 | |
| 64 connections, off: req/s / CPU | 42,483 / 25.6 | 42,843 / 26.0 | 42,424 / 25.3 | 42,062 / 24.6 |
| 64 connections, on: req/s / CPU | 27,601 / 44.9 | 27,936 / 44.9 | 42,214 / 28.7 | |

Intel i9-9980HK (x86_64, Apple clang 17, `-O3 -DNDEBUG`):

| row | before | parser | after | Asio |
|---|---|---|---|---|
| 1 connection, off: latency / CPU | 72.5 / 46.3 | 72.5 / 46.0 | 55.2 / 36.1 | 33.2 / 22.9 |
| 1 connection, on: latency / CPU | 145.9 / 113.4 | 146.8 / 114.6 | 86.8 / 67.6 | |
| 64 connections, off: req/s / CPU | 71,366 / 17.8 | 73,204 / 17.3 | 85,420 / 14.9 | 130,595 / 9.6 |
| 64 connections, on: req/s / CPU | 26,974 / 47.0 | 27,406 / 46.3 | 48,423 / 26.1 | |

On arm64 the spread over the rounds is 1-8% except one connection (4-20%); on x86_64 it is 0-6%.
No row is slower, on either architecture, than before or than the parser pin alone, which moves
no row beyond its spread: a 14-byte reply spends almost nothing in the parser. With the timeouts
on, the watch, the channel and the single slice take a request from 146 to 87 us and from 113 to 68
us of CPU at one connection on x86_64, and 64 connections from 27.0k to 48.4k req/s. On arm64 the
client saturates first at 64 connections with the timeouts off, so that row does not move there.

### Tests

- `test/readahead_test.cc` (ctest `readahead`): the watch's 4096-byte bound, order and disconnect
  rules directly on `ConnectionInput`, plus a seeded random walk of sends, watches and reads; a
  pipelined request arriving inside the read-ahead at every alignment of the 4096-byte window
  against the request line, headers, CRLF CRLF, body and next request; 3000 pipelined requests
  behind a held one; a half-close after pipelined input; a body cut short (close, half-close,
  reset, chunked) never reaches the handler and releases its admission; request smuggling
  (Content-Length with Transfer-Encoding, repeated and malformed Content-Length, a bad chunk
  size) is refused as the first request and as one arriving inside the read-ahead, and a request
  inside a Content-Length or chunked body is data; the sliced write delivers every byte at every
  alignment of head and body against the 64 KiB slice. The reply monitor (deferred and owned-stream
  replies) is tested on `ConnectionInput` too: it arms nothing over waiting bytes, EOF is not a
  disconnect for it and a reset is. The integration sections also pass against the header before
  this change (`-DREADAHEAD_TEST_INTEGRATION_ONLY`), so they pin behavior the watch keeps. Seven
  mutants each fail the test: a read-ahead of one byte, bytes returned from the wrong offset, a
  read armed over unread bytes, data that does not end the watch, a byte dropped per slice, a
  completion that never wakes `drain()`, and a consumed buffer that leaves the ready flag set.
- `test/timeouts_test.cc`: headers trickled on a reused connection, a slow client under the
  header timeout (fresh and reused connection), a reply the client stops reading is cut at the
  inactivity timeout, a slow reader that keeps moving receives the whole reply.
- ASan with UBSan and TSan, on Linux CI and on an M1 Pro, report nothing on `readahead_test`,
  `coroutine_test` and `timeouts_test`.

## 2026-10-30 and earlier: `response_ended()`

- Buffered response decorators can tell `response_started()` from `response_ended()`:
  setting a status or writing bytes does not complete a response, and Asio reports
  completion only after `end()` succeeds. The query defaults to the started approximation
  for existing custom `BufferedResponse` writers; override it for accurate middleware error
  mapping. The change is source compatible but extends a virtual interface, so rebuild all
  consumers together instead of mixing header revisions. It does not indicate confirmed
  network delivery.
