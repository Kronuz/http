# http

A generic, extensible HTTP/1.1 application framework on
[standalone Asio](https://think-async.com/Asio/) (C++20 coroutines).

It puts the application *above* the server behind a single seam: an application
implements one interface —

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
- **Concurrency (inline or offloaded).** A handler selected by `should_offload` runs on the reactor's bounded worker pool; excess requests receive 503. Inline handlers run on the reactor and must remain nonblocking. The scoped writer and request remain valid only during the handler call. Delayed work transfers exclusive reply authority through `ResponseWriter::defer()` and retains the returned move-only `DeferredReply`, never the writer or request. The connection awaits one reply before handling the next keep-alive request.
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

## Owned delayed replies

`defer(DeferredOptions)` returns an owned capability when supported, or null for an existing synchronous transport. Call it before writing any response fields. Supply a finite steady-clock deadline and bounded header/body limits. `complete(Response, reply_retention)` moves one buffered response into the transport and reports acceptance; acceptance does not establish delivery. Request retention and accepted reply retention remain held until the original socket operation completes or is canceled and reaped.

Settlement runs once, outside the exchange lock, with Written, TimedOut, Disconnected, Abandoned, Shutdown or Failed. Written means the local write completed, not that the client received or acted on it. Dropping an unanswered capability abandons the reply; dropping it after acceptance preserves the accepted response. Late submission after shutdown returns Closed without accessing the retired runtime. A sending half-close preserves response interest. Deferred replies skip compression, range and conditional transforms so their owned buffers and limits remain stable. Framing headers are transport-owned.

### Optional HTTPS transport

`http_asio_tls.h` adds `tls_connection_factory(shared_context, handshake_deadline, maximum_handshakes)`, passed as the optional fifth `HttpAsioService` constructor argument. Link OpenSSL only when using this adapter. The caller configures certificates and verification policy. Incoming TLS allocation is capped by a shared handshake limit (64 by default), and each handshake has one absolute deadline (five seconds by default). The factory retains the context through original operations.

The common HTTP parser and response path operate on the plaintext stream. A deferred response retains one owned read-ahead byte across its write, then returns that byte to the next request. Fatal read errors notify the current deferred exchange. The original four-argument TCP service API remains available without an OpenSSL dependency.

### Streaming request retirement

Shutdown aborts the body reader, then joins the original pool handler with cleanup cancellation disabled. The request and response writer remain alive until the handler returns, including after a canceled body read. This uses the reactor session completion barrier in reactor `863bed0`.
