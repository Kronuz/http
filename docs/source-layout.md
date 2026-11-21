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

`coroutine_test.cc` keeps `main()`: its twelve sections run in order against one shared application
and each is a block of `main`, so cutting `main` would change code, not move it. It stays one file
(about 770 lines) for that reason. The pieces that sections use, and the one test that stands alone,
moved out; they link into the one `coroutine_test` target (`ctest` name `coroutine`).

| File | Holds |
|---|---|
| `test/coroutine/support.h` | the shared helpers in `detail`: `check`, the raw client (`Conn`, `get`, `post`, `one`), `until`, `Obs`, `wrapper_scheduler` |
| `test/coroutine/app.h` | `App`, the `HttpHandler` the sections drive |
| `test/coroutine/body_stream.cc` | `body_stream_tests`: BodyStream semantics (section [B]) on a private loop |

## `test/readahead_test.cc` and `test/readahead/`

`readahead_test.cc` keeps `main()` and its integration sections [I] [P] [F] [D] [B] [S] [Q] [N] [W]
(about 430 lines): each is a block of `main`, so cutting `main` would change code, not move it. The
[U] sections, which test `ConnectionInput` alone over a socket pair, are functions of their own and
moved out with their helpers. Both link into the one `readahead_test` target (`ctest` name
`readahead`); `-DREADAHEAD_TEST_INTEGRATION_ONLY` leaves the [U] file empty, as before.

| File | Holds |
|---|---|
| `test/readahead/support.h` | the shared helpers in `detail`: `check`, `until`, `pattern`, the raw client, `Obs`, `App`, `Server`, `Held`, and the [U] functions' prototypes |
| `test/readahead/input_tests.cc` | `ConnectionInput` on its own: the lazy watch, a held read, the waiter's lane, `read_ahead()`, the bound and the disconnect rules (about 600 lines, one topic, kept whole) |
