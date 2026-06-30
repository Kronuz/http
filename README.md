# http

A generic HTTP/1.1 application layer on top of [Kronuz/server](https://github.com/Kronuz/server).

This is **Leg 2** of inverting Xapiand: it draws the seam where the application
sits *above* the server. An application implements one interface —

```cpp
struct HttpHandler {
    virtual void handle(const Request&, ResponseWriter&) = 0;   // request in, response written out
};
```

— and the engine does everything else: a generic `HttpConnection` parses bytes
with **http-parser**, builds a `Request`, hands it to the handler, and frames the
written response back over a `Kronuz/server` connection. The search engine becomes
one `HttpHandler`; the demo is another.

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

## Why the seam has this shape (forward-compatibility)

The handler takes a `Request` and writes a response — it is never a callback into
the connection, and it never sees the parser, the socket, or the loop. That keeps
three eventual migrations contained behind the seam, so none of them touch
application code:

- **Parser (http-parser today).** Parsing lives only inside `HttpConnection` and
  just *produces* a `Request`. The parser is the Joyent `http_parser` fork
  ([Kronuz/http-parser](https://github.com/Kronuz/http-parser)), chosen because it
  accepts arbitrary request methods — Xapiand's REST API uses custom verbs
  (`COUNT`, `INFO`, `DUMP`, `RESTORE`, …) that a stricter parser like llhttp
  rejects. Swapping it for another parser is contained here; the handler and the
  `Request`/`ResponseWriter` are parser-agnostic.
- **Concurrency (synchronous today).** Completion is signalled by `ResponseWriter::end()`,
  not by `handle()` returning. So a handler is free to finish the response *later* —
  the coroutine upgrade makes `handle()` a `task<>` and the *one* call site in
  `HttpConnection::dispatch()` becomes `co_await handler.handle(...)`; the
  un-stallable model hands the writer to a worker thread so heavy work never runs
  on the reactor. Neither changes a handler's logic, because the writer is the same.
- **Reactor (libev, via Kronuz/server today).** The handler never sees the
  reactor; an Asio port of `Kronuz/server` keeps `BaseClient`'s `on_read`/`write`
  surface and so never reaches the HTTP layer or the app.

The routing table (`Router`) is application configuration — what Xapiand's
hardcoded `prepare()` method-switch becomes once search is an `HttpHandler`.

## Files

| File | Role |
|---|---|
| `http_message.h` | `Request` and the `Response` value (for buffered handlers), plus `reason_phrase`. |
| `http_handler.h` | `HttpHandler` (the seam) + `ResponseWriter` (buffered `send` / streamed `write`+`end`). |
| `http_router.h` | `Router` — method/path dispatch, a thin binding over `Kronuz/radix-router`. |
| `http_connection.h` | The generic connection: http-parser parsing, HTTP/1.1 framing (Content-Length / chunked), and the single handler call site, over `BaseClient`. |
| `http_server.h` | `HttpServer` (accept loop) + `HttpService` (the worker-tree root). |

## Dependencies

All via FetchContent, so the build is self-contained and version-pinned (no
system/brew install):

- [`Kronuz/server`](https://github.com/Kronuz/server) — reactor + TCP + connection FSM.
- [`Kronuz/http-parser`](https://github.com/Kronuz/http-parser) — the HTTP parser (accepts custom methods).
- [`Kronuz/radix-router`](https://github.com/Kronuz/radix-router) — the radix-tree path router behind `Router`.
