# Architecture

How the pieces fit, and the path a request takes. Read `README.md` first for the
seam and the rationale; this is the map.

Buffered response completion is independent of response start. Asio's `BufferedResponse::response_ended()` exposes its existing `ended_` flag, letting decorators preserve the transport's rule: map an exception only before successful `end()`, then fall back to 500 if mapping still leaves the response unfinished. The compatibility default for custom writers approximates completion with `response_started()`; it is not a delivery or ownership-transfer observation.

## Two layers

The framework is split into a transport-agnostic **protocol layer** and one
**transport**. The protocol layer is pure value logic — it never names a socket, a
loop, or a thread — so it is testable in isolation and portable to any runtime.

```
        application:  HttpHandler  (Request in, response written out)
       ---------------- the seam ----------------------------------
  protocol layer   http_message   Request / Response values
  (agnostic)       http_router    method + radix path dispatch (404/405)
                   http_accept    content / encoding negotiation (RFC 7231)
                   http_compression  Accept-Encoding -> zstd/gzip
                   http_conditional  weak ETag + If-None-Match -> 304
                   http_range        byte range -> 206 / 416
                   http_request_parser  bytes -> Request (buffered or streamed body)
       ---------------------------------------------------------------
  transport        http_asio      Asio C++20 coroutine connection (the reactor::Session)
                                  + the buffered ResponseWriter (compression / conditional
                                  / range), riding on the Kronuz/reactor runtime below
  runtime          reactor        N shared-nothing reactors on N threads, one port,
                                  per-reactor admission gate, graceful shutdown (extracted)
```

The files that name Asio are `http_asio.h` (and the generic `reactor` runtime it
rides on) and the coroutine seam itself (`handle` returns `asio::awaitable<void>`;
`BodyStream` is an Asio operation). The only file that knows the parser is `http_request_parser.h`. Everything
else is plain C++ over the `Request` / `ResponseWriter` values.

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
- **Concurrency (coroutines on the loop).** `handle` is a coroutine awaited on the connection's loop. A handler that must wait `co_await`s and the loop serves other connections; nothing runs off the loop, so there is no worker pool to size. Excess requests receive 503 from the reactor's admission gate. The scoped writer and request remain valid until the coroutine completes, and a request's frame is never destroyed early. Delayed work transfers exclusive reply authority through `ResponseWriter::defer()` and retains the returned move-only `DeferredReply`, never the writer or request. The connection awaits one reply before handling the next keep-alive request.
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

## Runtime: shared-nothing reactors

The runtime is [Kronuz/reactor](https://github.com/Kronuz/reactor), a generic Asio
server runtime extracted from this file: `HttpAsioService` is now a thin adapter that
turns the `HttpHandler` + response options into a per-connection `reactor::Session`
(`detail::serve_connection`) and hands it to a `reactor::TcpServer`. The server runs
**N `io_context`s on N threads** — thread-per-core, no shared state between them, no
global lock, no cross-core bounce. Each `reactor::Reactor` also owns a bounded in-flight
admission gate (`AdmissionGate`).

Binding N reactors to one port has two modes, chosen automatically by the runtime:

- **`SO_REUSEPORT` (Linux):** each reactor binds its own acceptor and the kernel
  load-balances incoming connections across them. The fast path.
- **Shared acceptor (macOS/BSD, or `reuse_port` off):** a second same-port bind is
  rejected there, so one acceptor on reactor 0 binds the port and hands each accepted
  connection to a reactor round-robin (accepting the socket directly onto the target
  reactor's `io_context`). Only the cheap accept funnels; read/handler/write shard.

See the reactor repo's `ARCHITECTURE.md` for the runtime internals; below is the HTTP
request lifecycle that runs on top of it.

## The request lifecycle

One connection is one coroutine (`serve_connection`). Its keep-alive loop:

```
  read bytes  ->  RequestParser.feed
                     |
                     | on headers complete:
                     |   admit: reactor gate, else 503 + Retry-After (rejected, connection closes)
                     |   build the app extension (create_extension)
                     |   ask the app: BodyStream? sink? else buffer (capped: 413)
                     |
                     | on each body chunk:
                     |   sink present  -> sink.write(chunk)     (pushed, never held)
                     |   else          -> Request::body.append  (buffered, up to the cap)
                     v
  Request ready (or, for a BodyStream, as soon as the headers are)
     |
     | track the request for shutdown, watch the socket for a hang-up
     | co_await handler->handle(request, writer)       (the loop serves others while it waits)
     |    a hang-up or shutdown -> Request::cancel(...)  (the handler's callback; frame stays)
     | handler throws -> on_error(...) maps it (default 500)
     v
  ResponseWriter.serialize:  content negotiation + compression, HTTP/1.1 framing
     |
     v
  async_write (finishes even when cancelled) -> release the gate -> keep-alive? loop : close
```

### The un-stallable property

A handler is a coroutine, so waiting is `co_await`: the coroutine suspends, the loop is
free to serve other connections, and it resumes on the connection's own `io_context`.
Nothing runs off the loop. There is **one in-flight handler per connection** (the
connection coroutine is the single owner of its `Request` and `ResponseWriter`), so
there is no pause/resume bookkeeping and no cross-thread completion handoff -- the race
class that a callback-FSM version hides cannot arise. A saturated reactor sheds load as
`503` instead of growing an unbounded queue. `docs/mechanisms/coroutine-handlers.md`
walks through cancellation, admission and shutdown.

### Bodies: bounded memory for any size

Request-body intake has three modes, chosen at headers-complete:

- **Buffered** (default): the whole body lands in `Request::body`, up to the service's
  cap (16 MiB; `413` beyond it, from `Content-Length` or as chunked bytes arrive).
  Convenient for small requests; the up-front reservation from `Content-Length` is
  capped so a hostile length can't force a giant allocation.
- **Push-streamed** (`on_request_body` returns a `BodySink`): each body chunk is
  handed to the sink as it is parsed, on the reactor, and nothing is accumulated.
  For light per-chunk work (hashing, line-counting). Not capped.
- **Streamed** (`wants_body_stream` returns true): `handle()` starts as soon as the
  headers are in and reads the body from `Request::body_stream` (a `BodyStream`) with
  `co_await ... async_read(chunk, token)`. The connection coroutine feeds chunks
  through a bounded channel and **suspends when it is full** (the loop stays free, and
  back-pressure reaches the socket). A multi-gigabyte `RESTORE`/bulk load is indexed as
  it arrives, in O(buffer) memory. The app parses the chunks in its own format; the
  framework owns the transport and the back-pressure. Not capped.

Push-streamed sinks run inside the parser, so keep their work light; heavy consumption
belongs on the streamed path, where the handler can suspend.

## Extension points

An application plugs into the framework through the `HttpHandler` seam, never by
editing it:

| Hook | Purpose |
|---|---|
| `handle(request, writer)` | the request handler, a coroutine (route + respond). |
| `create_extension(request)` | attach typed per-request state, built at headers-complete. |
| `on_request_body(request)` | opt into push streaming: return a `BodySink` (light per-chunk work on the reactor), or nullptr to buffer. |
| `wants_body_stream(request)` | opt into a streamed body: `handle()` runs while the body arrives and reads raw chunks from `Request::body_stream` with `co_await`, flow-controlled (O(buffer) memory). |
| `Request::on_cancel(cb)` | run `cb(reason)` on the loop when the client closes or the service stops; reads stop, writes finish. |
| `on_error(exc, request, writer)` | map an exception to a response (default 500). |
| `CompressionOptions::add_coding` | register a content-coding (the app owns the codec). |
| `Accept` / `negotiate` | pick a media type from what the app can serialize. |

## Response transforms

At `serialize()` the buffered response passes through three optional transforms, in
order, each a generic knob the app turns on (`enable_conditional` / `enable_ranges` /
`enable_compression` on the service):

1. **Conditional** (`http_conditional.h`) — a weak ETag `W/"<fnv1a64 hex>"` from the
   body; if the request's `If-None-Match` already holds it, drop the body and answer
   `304`. Runs first so the ETag identifies the resource, not the wire bytes.
2. **Range** (`http_range.h`) — a single byte `Range` → `206 Partial Content` with
   `Content-Range` (or `416` if unsatisfiable); a `206` is served uncompressed.
3. **Compression** (`http_compression.h`) — negotiate `Content-Encoding` from
   `Accept-Encoding` and compress with Kronuz/compressors; self-skips bodyless
   statuses (204/304/1xx), a 206 slice, an already-encoded body, and a result that
   didn't shrink.

A `304` short-circuits range + compression; a `206` skips compression.

## Limitations

Response-side *streaming* (chunked `Transfer-Encoding`) is not in the Asio writer yet
— it buffers the whole response, so the transforms above always have the full body to
work on, but a huge response is held in memory. Request-side streaming (both push and
concurrent pull, with flow control) is done.

## Deferred response ownership

The scoped ResponseWriter and Request cannot escape the handler. `defer()` transfers exclusive buffered response authority into a move-only capability backed by a mutex-protected exchange. Application submission retains owned response bytes and a reply lease; the transport retains the request lease and waits on its connection executor. Completion before waiting is observable, and a handler that returns after `defer()` releases its in-flight admission when the reply settles.

The exchange advances Waiting → Ready → Writing → Written, or enters Closing for deadline, disconnect, abandonment or shutdown. Closing retains all leases until the original socket operation has been canceled and reaped. Settlement is exactly once; hooks and lease destructors execute outside the lock. Escaped capabilities retain closed metadata, not a socket, stack writer or live executor. One bounded response is accepted, keep-alive handlers remain ordered, and sending half-close remains valid. Deferred response transforms are disabled.
