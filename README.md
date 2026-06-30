# http

A generic HTTP/1.1 application layer on top of [Kronuz/server](https://github.com/Kronuz/server).

This is **Leg 2** of inverting Xapiand: it draws the seam where the application
sits *above* the server. An application implements one interface —

```cpp
struct HttpHandler {
    virtual void handle(const Request&, Response&) = 0;   // request in, response out
};
```

— and the engine does everything else: a generic `HttpConnection` parses bytes
with **llhttp**, builds a `Request`, hands it to the handler, and writes the
`Response` back over a `Kronuz/server` connection. The search engine becomes one
`HttpHandler`; the demo is another.

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
./build/kv_store 8080          # a REST key/value app, served through the seam
curl -XPUT localhost:8080/kv/greeting -d 'hello'
curl       localhost:8080/kv/greeting        # -> hello
```

`examples/kv_store.cc` is a complete REST app in a `Router` of closures. It never
names llhttp, a socket, or the event loop.

## Why the seam has this shape (forward-compatibility)

The handler is **value-semantic** — `Request` in, `Response` out — rather than a
callback into the connection. That single choice keeps three eventual migrations
contained behind the seam, so none of them touch application code:

- **Parser (llhttp today).** Parsing lives only inside `HttpConnection` and just
  *produces* a `Request`. Swapping llhttp for another parser is contained there;
  the handler and the `Request`/`Response` structs are parser-agnostic.
- **Concurrency (synchronous today).** Because the handler returns its response
  by value, the coroutine upgrade is additive: `handle()` gains a `task<>`
  variant and the *one* call site in `HttpConnection::dispatch()` becomes
  `co_await handler.handle(...)`. No handler's logic changes.
- **Reactor (libev, via Kronuz/server today).** The handler never sees the
  reactor; an Asio port of `Kronuz/server` keeps `BaseClient`'s `on_read`/`write`
  surface and so never reaches the HTTP layer or the app.

The routing table (`Router`) is application configuration — what Xapiand's
hardcoded `prepare()` method-switch becomes once search is an `HttpHandler`.

## Files

| File | Role |
|---|---|
| `http_message.h` | `Request` / `Response` — the plain data that crosses the seam, with `Response::serialize()`. |
| `http_handler.h` | `HttpHandler` (the seam) + `Router` (method/path dispatch). |
| `http_connection.h` | The generic connection: llhttp parsing + the single handler call site, over `BaseClient`. |
| `http_server.h` | `HttpServer` (accept loop) + `HttpService` (the worker-tree root). |

## Dependencies

Both via FetchContent, so the build is self-contained and version-pinned (no
system/brew install): `Kronuz/server` (reactor + TCP + connection FSM) and
`nodejs/llhttp` at a `release/*` tag (which ships pre-generated C, so no Node
toolchain is needed).
