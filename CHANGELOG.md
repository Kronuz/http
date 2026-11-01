# Changelog

Newest first. Dates are 2026.

## 2026-10-31: the disconnect watch reads ahead

A keep-alive request cost 91 us at one connection against 60 us for a minimal Asio server, and
with the production timeouts on, 45 us of CPU per request and 27.5k requests per second at 64
connections against 24 us and 43.5k. The survey (`campaign/survey/J.md`, cluster J) traced it to
the connection's disconnect watch and to the timer and the split write around it.

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
- `examples/bench_http` takes a third argument, `timeouts`, that turns on the request, idle,
  header and inactivity timeouts of a production service.

### Performance

`server_min` (campaign/bench/J2) answers a 14-byte keep-alive reply through `HttpAsioService`
with one reactor, driven by `wrk` (4 s runs); "off" has no timeouts and "on" has the production
ones (`bench_http PORT 1 timeouts`). Latency is in us, req/s is at 64 connections, CPU is the
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
