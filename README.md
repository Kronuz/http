# http

A generic, extensible HTTP/1.1 application framework on
[standalone Asio](https://think-async.com/Asio/) (C++20 coroutines).

This is **Leg 2** of inverting Xapiand: it draws the seam where the application
sits *above* the server. An application implements one interface —

```cpp
struct HttpHandler {
    virtual void handle(const Request&, ResponseWriter&) = 0;   // request in, response written out
};
```

— and the framework does everything else: an Asio coroutine connection parses bytes
with **http-parser**, builds a `Request`, hands it to the handler, and frames the
written response back over the socket. The search engine becomes one `HttpHandler`;
the demo is another.

The handler writes through a `ResponseWriter`, which serves both shapes with one
interface and hides the HTTP/1.1 framing:

```cpp
resp.send(200, body);              // buffered: the writer sets Content-Length
// or
resp.status(200);                  // streamed: multiple writes -> chunked encoding,
for (auto& row : rows) resp.write(row);   // so a DUMP or a large result set never
resp.end();                        // has to sit in memory all at once
```

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
./build/kv_store 8080          # a REST key/value app, served through the seam
curl -XPUT localhost:8080/kv/greeting -d 'hello'
curl       localhost:8080/kv/greeting        # -> hello (Content-Length)
curl       localhost:8080/stream/5           # -> five lines (chunked)
```

`examples/kv_store.cc` is a complete REST app in a `Router` of closures. It never
names http-parser, a socket, or the event loop.

## Why the seam has this shape

The handler takes a `Request` and writes a response — it is never a callback into
the connection, and it never sees the parser, the socket, or the loop. That value
semantic keeps the moving parts contained behind the seam, so none of them touch
application code:

- **Parser (http-parser).** Parsing lives only inside the connection and just
  *produces* a `Request`. The parser is the Joyent `http_parser` fork
  ([Kronuz/http-parser](https://github.com/Kronuz/http-parser)), chosen because it
  accepts arbitrary request methods — Xapiand's REST API uses custom verbs
  (`COUNT`, `INFO`, `DUMP`, `RESTORE`, …) that a stricter parser like llhttp
  rejects. Swapping it for another parser is contained here; the handler and the
  `Request`/`ResponseWriter` are parser-agnostic.
- **Concurrency (inline or offloaded).** Completion is signalled by
  `ResponseWriter::end()`, not by `handle()` returning, so a handler may finish the
  response *later*. That is the **un-stallable model**: give the service a worker
  count and each request that opts in (`should_offload`) runs `handle()` on the
  reactor's Asio `thread_pool` while the reactor stays free — a single
  `co_await co_spawn(pool, …)`, one in-flight per connection, resuming back on the
  connection's `io_context`. A full bounded window answers 503. With no pool,
  `handle()` runs inline on the reactor (the cheap fast path). Because completion is
  the writer's `end()`, there is no pause/resume bookkeeping and no cross-thread
  handoff — the coroutine owns the connection, so that race class cannot arise.
- **Request body (buffered or streamed).** A handler opts into streaming per
  endpoint (`on_request_body` returns a `BodySink`): the body then flows to the sink
  chunk-by-chunk as it is parsed, never accumulated, so a multi-gigabyte `RESTORE`
  or bulk load runs in O(read-buffer) memory. Otherwise the body is buffered into
  `Request::body` (the convenient path for small requests).
- **Runtime (Asio).** The handler never sees the reactor. The transport rides on
  [Kronuz/reactor](https://github.com/Kronuz/reactor), a generic Asio server runtime:
  N Asio `io_context`s on N threads (thread-per-core, shared-nothing), all bound to one
  port via `SO_REUSEPORT` where available, with a portable single-acceptor fallback
  elsewhere (macOS/BSD). Swapping the runtime never reaches the HTTP layer or the app.

The routing table (`Router`) is application configuration — what Xapiand's
hardcoded `prepare()` method-switch becomes once search is an `HttpHandler`.

## Files

| File | Role |
|---|---|
| `http_message.h` | `Request` and the `Response` value (for buffered handlers), plus `reason_phrase`. |
| `http_handler.h` | `HttpHandler` (the seam) + `ResponseWriter` (buffered `send` / streamed `write`+`end`) + `BodySink` (streamed request-body intake). |
| `http_router.h` | `Router` — method/path dispatch, a thin binding over `Kronuz/radix-router`. |
| `http_accept.h` | `Accept` — content negotiation (Accept / Accept-Encoding / …) per RFC 7231. |
| `http_compression.h` | Transparent response compression — negotiate `Accept-Encoding` → zstd/gzip (Kronuz/compressors). |
| `http_conditional.h` | Conditional requests — a weak ETag from the body + `If-None-Match` → `304 Not Modified`. |
| `http_range.h` | Range requests — a single byte range → `206 Partial Content` (`Content-Range`), `416` if unsatisfiable. |
| `http_request_parser.h` | `RequestParser` — the transport-agnostic http-parser wrapper: received bytes → `Request` values, with buffered-or-streamed body intake. |
| `http_asio.h` | The Asio transport: the connection as a C++20 coroutine (parse → handler seam → frame) run as a `reactor::Session`, the buffered `ResponseWriter` with compression, keep-alive, the streaming `ChannelBodyReader`, and `HttpAsioService` (a thin adapter over `reactor::TcpServer`). |

## Dependencies

All via FetchContent, so the build is self-contained and version-pinned (no
system/brew install):

- [standalone Asio](https://github.com/chriskohlhoff/asio) (`asio-1-36-0`) — the reactor + TCP + C++20-coroutine runtime. Header-only, `ASIO_STANDALONE` (no Boost).
- [`Kronuz/reactor`](https://github.com/Kronuz/reactor) — the generic Asio server runtime (shared-nothing reactor pool + accept + graceful shutdown) `http_asio.h` rides on. Header-only.
- [`Kronuz/http-parser`](https://github.com/Kronuz/http-parser) — the HTTP parser (accepts custom methods).
- [`Kronuz/radix-router`](https://github.com/Kronuz/radix-router) — the radix-tree path router behind `Router`.
- [`Kronuz/compressors`](https://github.com/Kronuz/compressors) — the deflate/gzip + zstd codecs behind response compression.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for how the pieces fit and the request
lifecycle; [`AGENTS.md`](AGENTS.md) for the working notes and invariants.
