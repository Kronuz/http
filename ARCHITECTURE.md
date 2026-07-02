# Architecture

How the pieces fit, and the path a request takes. Read `README.md` first for the
seam and the rationale; this is the map.

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
                                  per-reactor offload pool, graceful shutdown (extracted)
```

The only file that knows Asio is `http_asio.h` (and the generic `reactor` runtime it
rides on). The only file that knows the parser is `http_request_parser.h`. Everything
else is plain C++ over the `Request` / `ResponseWriter` values.

## Runtime: shared-nothing reactors

The runtime is [Kronuz/reactor](https://github.com/Kronuz/reactor), a generic Asio
server runtime extracted from this file: `HttpAsioService` is now a thin adapter that
turns the `HttpHandler` + response options into a per-connection `reactor::Session`
(`detail::serve_connection`) and hands it to a `reactor::TcpServer`. The server runs
**N `io_context`s on N threads** — thread-per-core, no shared state between them, no
global lock, no cross-core bounce. Each `reactor::Reactor` also owns an optional Asio
`thread_pool` for offload and a bounded offload window (`OffloadGate`).

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
                     |   build the app extension (create_extension)
                     |   ask the app: stream this body? (on_request_body -> BodySink?)
                     |
                     | on each body chunk:
                     |   sink present  -> sink.write(chunk)     (streamed, never held)
                     |   else          -> Request::body.append  (buffered, small bodies)
                     |
                     | on message complete: sink.end(); request ready
                     v
  take() the Request
     |
     | should_offload(request) && a pool exists?
     |    yes, window has room  -> co_await co_spawn(pool, handle)   (reactor free)
     |    yes, window full      -> 503 Service Unavailable
     |    no                    -> handle() inline on the reactor    (fast path)
     |
     | handler throws -> on_error(...) maps it (default 500)
     v
  ResponseWriter.serialize:  content negotiation + compression, HTTP/1.1 framing
     |
     v
  async_write  ->  keep-alive? loop : close
```

### The un-stallable property

Offload is one `co_await co_spawn(pool, work)`: the blocking work runs on a pool
thread, the reactor is free to serve other connections, and the coroutine resumes on
the connection's own `io_context` when the work is done. There is **one in-flight
handler per connection** (the coroutine is the single owner of its `Request` and
`ResponseWriter`), so there is no pause/resume bookkeeping and no cross-thread
completion handoff — the race class that a callback-FSM version hides cannot arise.
A saturated offload window sheds load as `503` instead of growing an unbounded queue.

### Bodies: bounded memory for any size

Request-body intake has three modes, chosen at headers-complete:

- **Buffered** (default): the whole body lands in `Request::body`. Convenient for
  small requests; the up-front reservation from `Content-Length` is capped so a
  hostile length can't force a giant allocation.
- **Push-streamed** (`on_request_body` returns a `BodySink`): each body chunk is
  handed to the sink as it is parsed, on the reactor, and nothing is accumulated.
  For light per-chunk work (hashing, line-counting).
- **Pull-streamed / concurrent** (`wants_body_stream` returns true): the framework
  runs `handle()` on a worker *concurrently* with the body read and gives it a
  `BodyReader` to PULL raw chunks from. `read()` blocks the worker until the next
  chunk (or end); the reactor feeds chunks through a bounded, flow-controlled channel
  and **suspends when it is full** (stays free to serve others, and back-pressure
  reaches the socket). A multi-gigabyte `RESTORE`/bulk load is indexed as it arrives,
  in O(buffer) memory. The app parses the chunks in its own format; the framework owns
  the transport, the concurrency, and the back-pressure.

Push-streamed sinks run on the reactor, so keep their work light; heavy consumption
belongs on the pull path (a worker).

## Extension points

An application plugs into the framework through the `HttpHandler` seam, never by
editing it:

| Hook | Purpose |
|---|---|
| `handle(request, writer)` | the request handler (route + respond). |
| `create_extension(request)` | attach typed per-request state, built at headers-complete. |
| `on_request_body(request)` | opt into push streaming: return a `BodySink` (light per-chunk work on the reactor), or nullptr to buffer. |
| `wants_body_stream(request)` | opt into concurrent pull streaming: `handle()` runs on a worker and pulls raw chunks from `Request::body_reader`, flow-controlled (O(buffer) memory). |
| `should_offload(request)` | run this request on the offload pool vs inline. |
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

## Not yet here

Response-side *streaming* (chunked `Transfer-Encoding`) is not in the Asio writer yet
— it buffers the whole response, so the transforms above always have the full body to
work on, but a huge `DUMP` response is held in memory. Request-side streaming (both
push and concurrent pull, with flow control) is done.
