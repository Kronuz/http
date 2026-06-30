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
http_connection.h  The connection: http_parser parsing + HTTP/1.1 framing + the one
                   handler call site, over BaseClient.
http_server.h      HttpServer (accept loop) + HttpService (worker-tree root).
examples/kv_store.cc  A complete REST app: buffered routes + a streaming route (/stream/:n).
test/test.cc       ctest: method dispatch, 404 vs 405, param capture, streamed body.
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

## Never-stalling the loop (design, not yet built)

The target concurrency model is multiple per-core reactors + a bounded worker
pool: heavy handlers run *only* on the pool (never the reactor), bounded queues
with 503 backpressure, a reactor-stall watchdog, and the worker hands the response
back to the owning reactor via `ev::async`. The value-semantic seam plus
`end()`-completion is exactly what makes this additive — design new work so it
stays behind the seam.

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
ctest --test-dir build
./build/kv_store 8080
#   curl -XPUT localhost:8080/kv/k -d v ; curl localhost:8080/kv/k   # buffered
#   curl localhost:8080/stream/5                                     # chunked
```
