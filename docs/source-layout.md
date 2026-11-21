# Source layout

The library is header-only. A header too large to read whole is split by responsibility into
pieces that its own header includes, so a caller keeps naming the one header.

## `http_asio.h` and `http_asio/`

`http_asio.h` is the include callers name. It holds the overview comment and includes the pieces
below, in dependency order; each piece includes the one before it that it needs.

| File | Holds |
|---|---|
| `http_asio/options.h` | the includes the pieces share, `AsioBindOptions`, `AsioResponseOptions`, `Limits`, `TlsFailure`, `Tls` |
| `http_asio/response_writer.h` | `AsioResponseWriter`: the buffered `ResponseWriter`, its transforms and HTTP/1.1 framing |
| `http_asio/connection_io.h` | `detail::write_before_deadline`, `write_with_progress` and `ConnectionInput`, the connection's owned read-ahead |
| `http_asio/connection_state.h` | `detail`: the owned-stream reply write, `map_handler_error`, `RequestAbort`, `WatchGuard`, `send_now`, `IdleList`, `ServiceState`, `IdleWait` |
| `http_asio/serve_connection.h` | `detail::serve_connection_body` and `serve_connection`: the keep-alive loop of a connection |
| `http_asio/service.h` | `HttpAsioService`: the service over a `reactor::TcpServer` |

## `test/coroutine_test.cc` and `test/coroutine/`

`coroutine_test.cc` holds `main()`, which runs the sections in order. The sections are functions
in the pieces below, all linked into the one `coroutine_test` target (`ctest` name `coroutine`).

| File | Holds |
|---|---|
| `test/coroutine/support.h` | the shared helpers in `detail`: `check`, the raw client (`Conn`, `get`, `post`, `one`), `until`, `Obs`, `wrapper_scheduler` |
| `test/coroutine/app.h` | `App`, the `HttpHandler` the sections drive |
| `test/coroutine/sections.h` | the declarations of the sections |
| `test/coroutine/requests.cc` | sections [A] [G] [D] [S] [E] and the BodyStream tests [B]: suspension, pipelining, the in-flight gate, the streamed join, the body cap |
| `test/coroutine/disconnects.cc` | sections [C] [J] [H] [I]: reset, half-close, a body cut short, a throwing cancel callback |
| `test/coroutine/lifecycle.cc` | sections [F] [T] [X] and the affinity death test: shutdown, the task loop, the teardown phases |
