# Source layout

The library is header-only. A header too large to read whole is split by responsibility into
pieces that its own header includes, so a caller keeps naming the one header.

## `http_asio.h` and `http_asio/`

`http_asio.h` is the include callers name (Xapiand and Detent). It holds the overview comment and
includes the pieces below, in dependency order; each piece includes the one before it, so each
compiles alone. The pieces are the units a maintainer opens separately: the configuration and the
response, the socket I/O, the serving of a connection, and the service.

| File | Holds |
|---|---|
| `http_asio/response.h` | the includes the pieces share, `AsioBindOptions`, `AsioResponseOptions`, `Limits`, `TlsFailure`, `Tls` and `AsioResponseWriter` (the buffered `ResponseWriter`, its transforms and HTTP/1.1 framing) |
| `http_asio/connection_io.h` | `detail::write_before_deadline`, `write_with_progress` and `ConnectionInput`, the connection's owned read-ahead |
| `http_asio/connection.h` | `detail`: the owned-stream reply write, `map_handler_error`, `RequestAbort`, `WatchGuard`, `send_now`, `IdleList`, `ServiceState`, `IdleWait`, then `serve_connection_body` and `serve_connection`, the keep-alive loop |
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

## `test/readahead_test.cc` and `test/readahead/`

`readahead_test.cc` holds `main()`, which runs the sections in order. The sections are functions
in the pieces below, all linked into the one `readahead_test` target (`ctest` name `readahead`).
The [U] pieces are left out by `-DREADAHEAD_TEST_INTEGRATION_ONLY`, as before.

| File | Holds |
|---|---|
| `test/readahead/support.h` | the shared helpers in `detail`: `check`, `until`, `pattern`, the raw client, `Obs`, `App`, `Server`, `Held` |
| `test/readahead/input.h` | `Pair` and `read_once`, the socket pair the [U] sections drive, and their declarations |
| `test/readahead/sections.h` | the declarations of the integration sections |
| `test/readahead/input_watch.cc` | [U] sections on the lazy watch, a held read and the waiter's lane |
| `test/readahead/input_reads.cc` | [U] sections on `read()` and `read_ahead()` |
| `test/readahead/requests.cc` | sections [I] [P] [F] [S] [N] [W]: idle scratch, pipelining, smuggling, buffered and sliced replies |
| `test/readahead/disconnects.cc` | sections [D] [B] [Q]: hang-up, a body cut short, the lazy first question |
