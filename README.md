# http

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/http/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/http/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Header-only](https://img.shields.io/badge/header--only-yes-green.svg)

Header-only HTTP/1.1 framework for C++20 on [task](https://github.com/Kronuz/task) coroutines over Asio: write one handler, get parsing, routing and streaming.
The application implements one coroutine, `task::task<void> handle(Request&, ResponseWriter&)`, and never sees
the parser, the socket or the event loop; each connection is a task on its reactor's loop that parses bytes into
a `Request` and frames what the handler writes. Reach for it when a C++ service needs an
HTTP/1.1 front: a REST API, a search or storage server, an admin endpoint.

## ✨ Highlights

- **One seam.** The handler takes a `Request` and writes through a `ResponseWriter`; the
  parser, the socket and the loop stay behind it.
- **Shared-nothing reactors.** One Asio `io_context` per thread, nothing off the loop: a
  handler that waits `co_await`s and the loop serves other connections. Past the
  per-reactor in-flight limit, a request gets `503` with `Retry-After`.
- **Cancellation.** A client hang-up or a shutdown requests the running request's
  `task::cancel_token` (`Request::cancel_token()`); reads stop at their next checkpoint,
  writes finish.
- **Bounded memory for any body.** Read a request body chunk by chunk from a `BodyStream`
  or push it to a `BodySink`; a buffered body has a cap (`413`). Responses stream with
  backpressure.
- **Batteries.** A radix-tree `Router`, content negotiation, and opt-in zstd and gzip
  compression, ETags with `304`, and byte ranges with `206`.
- **Optional TLS and limits.** `http_asio_tls.h` adds TLS over OpenSSL, linked only when you
  use it; connection caps, timeouts and per-request deadlines are settings.
- **Custom methods.** [http-parser](https://github.com/Kronuz/http-parser) accepts any verb,
  so `COUNT` or `RESTORE` route like `GET`.

## 🚀 Quick start

```cpp
#include "http_asio.h"
#include "http_router.h"

#include <chrono>
#include <string>
#include <thread>

struct App : http::HttpHandler {
	http::Router router;
	App() {
		router.route("GET", "/hello/:name", [](http::Request&, http::ResponseWriter& resp, const http::Params& p) -> task::task<void> {
			resp.send(200, "hello, " + std::string(p.get("name")) + "\n");
			co_return;
		});
	}
	task::task<void> handle(http::Request& req, http::ResponseWriter& resp) override {
		co_await router.handle(req, resp);
	}
};

int main() {
	App app;
	http::HttpAsioService service(app, /*reactors=*/4, /*in_flight_limit=*/256);
	service.start(8080);  // curl localhost:8080/hello/world prints "hello, world"
	std::this_thread::sleep_for(std::chrono::minutes(1));
	service.stop();
}
```

[`examples/kv_store.cc`](examples/kv_store.cc) is a complete REST key/value app, with a
streamed response and a request body pushed to a sink:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
./build/kv_store 8080 &
curl -XPUT localhost:8080/kv/greeting -d 'hello'
curl localhost:8080/kv/greeting
```

## 📦 Installation

Requires a C++20 compiler and CMake 3.16 or newer; it fetches standalone Asio 1.36.0,
[http-parser](https://github.com/Kronuz/http-parser),
[radix-router](https://github.com/Kronuz/radix-router),
[compressors](https://github.com/Kronuz/compressors),
[task](https://github.com/Kronuz/task) and
[reactor](https://github.com/Kronuz/reactor) at pinned commits. compressors needs zlib and
uses the system's LZ4 and zstd when it finds them, fetching them otherwise; TLS needs
OpenSSL 3. CI builds it on Linux (x86_64, arm64), macOS (arm64, x86_64), and FreeBSD,
plus ASan, UBSan, and TSan builds.

```cmake
include(FetchContent)
FetchContent_Declare(http
  GIT_REPOSITORY https://github.com/Kronuz/http.git
  GIT_TAG        c9777e49f04ca493004728ef932f23fec43b278e)  # or main, to track the tip
FetchContent_MakeAvailable(http)

target_link_libraries(my_app PRIVATE http::http)
```

```cpp
#include "http_asio.h"
```

## 🎯 When to use it

Use it for a C++ service that answers HTTP/1.1 and wants coroutine handlers, streamed bodies
and per-core reactors without writing the connection layer.

Not for:

- **HTTP/2, HTTP/3 or WebSockets.** None is implemented.
- **Blocking handlers.** A handler runs on its connection's loop, so a blocking call stalls
  every connection of that reactor; `co_await` instead, or hand the work to your own pool
  through `ResponseWriter::defer()`.
- **HTTP clients.** The library is a server.

## ⚡ Performance

One request, `GET /`, answered with the same 103-byte keep-alive reply (a 14-byte
`Hello, World!\n` body) by every server on one thread, driven by
[wrk](https://github.com/wg/wrk) 4.2.0 on the same host over loopback for 4 s, at one
connection (`-t1 -c1`) and at 64 (`-t2 -c64`). A cell is "latency / CPU" at one connection
and "req/s / CPU" at 64: latency is the `wrk` average in microseconds, req/s is requests per
second, and CPU is the server's user plus system time per request in microseconds. Lower is
better, except for req/s.

Each row is a server. **Minimal Asio** is the lower bound, not a framework: a hand-written
read-and-reply loop on standalone [Asio](https://github.com/chriskohlhoff/asio) 1.36.0, the
tag `CMakeLists.txt` fetches on both machines. **`http`** is this library, and
**`http`, timeouts on** is the same server with a production service's request, idle, header
and inactivity timeouts set: a configuration of `http`, not another server.
[Boost.Beast](https://github.com/boostorg/beast) (Boost 1.92.0, using Boost.Asio rather than
the standalone Asio) and [cpp-httplib](https://github.com/yhirose/cpp-httplib) v0.64.0 are
the comparators, each on one io_context or one pool thread. Before timing, `bench/verify.py`
sends the request to every server and aborts the run unless each answers the same bytes,
headers included.

Medians of three rounds on an Apple M4 Pro (macOS) and an Intel Core i9-9980HK (macOS 15,
x86-64), Apple clang 17, `-O3 -DNDEBUG`, measured on 2026-11-01 with `asio::awaitable`
handlers, before the move to `task::task`; this build has not been timed against them yet:

| server | M4 Pro, 1 connection: latency / CPU | M4 Pro, 64 connections: req/s / CPU | i9, 1 connection: latency / CPU | i9, 64 connections: req/s / CPU |
| --- | ---: | ---: | ---: | ---: |
| minimal Asio | 70.8 / 32.7 | 34,823 / 28.7 | 33.8 / 23.4 | 126,259 / 9.9 |
| `http` | 87.7 / 38.4 | 35,232 / 29.5 | 56.6 / 36.9 | 84,642 / 15.0 |
| `http`, timeouts on | 104.0 / 48.5 | 35,773 / 32.6 | 87.1 / 68.1 | 47,946 / 26.5 |
| Boost.Beast | 73.2 / 35.5 | 35,312 / 29.9 | 39.6 / 31.2 | 86,066 / 14.7 |
| cpp-httplib | 73.7 / 33.2 | 13,326 / 33.4 | 43.0 / 29.1 | 22,472 / 29.1 |

At one connection `http` takes 1.2 to 1.4 times Beast's latency and 1.1 to 1.2 times its
CPU, and cpp-httplib is faster than `http`; at 64 connections `http` and Beast tie. `wrk`
saturates at about 35,000 req/s on the M4 Pro, so there the CPU column separates the
servers. cpp-httplib serves a connection on one pool thread until it closes, so with one
thread only one of the 64 connections is answered and its 64-connection row is that
connection's throughput. [docs/perf/timeouts-cpu.md](docs/perf/timeouts-cpu.md) has the
method, the spread, and where the time goes. Rebuild and rerun the table (Boost from
Homebrew for Beast):

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON -DHTTP_BENCH_BEAST=ON -DHTTP_BENCH_HTTPLIB=ON
cmake --build build --target http_server_compare http_server_beast http_server_httplib
sh bench/compare.sh > out.txt
python3 bench/summarize.py < out.txt
```

## 📚 Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md): the two layers, the shape of the seam, the request
  lifecycle and the response transforms.
- [docs/mechanisms/coroutine-handlers.md](docs/mechanisms/coroutine-handlers.md): how a
  request runs, is cancelled, is admitted and stops at shutdown.
- [docs/TRANSPORT.md](docs/TRANSPORT.md): deferred replies, TLS, connection caps, request
  deadlines and early rejections.
- [docs/OWNED_STREAMING.md](docs/OWNED_STREAMING.md): owned response streaming.
- [docs/perf/](docs/perf/): per-request CPU and latency against a minimal Asio server, Boost.Beast and cpp-httplib.
- [CHANGELOG.md](CHANGELOG.md): behavior and performance changes, newest first.
- [AGENTS.md](AGENTS.md): file map, invariants and dependencies.
- [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries): the catalog;
  [prism](https://github.com/Kronuz/prism) is an app server built on this library.

## 🛠️ Building and testing

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```

Add `-DHTTP_BUILD_BENCH=ON` to build the `http_server_compare` benchmark server, and
`-DHTTP_FUZZ=ON` (Clang) for the libFuzzer target; the checked-in fuzz corpus replays as a
CTest test in every build.

## 🤝 Contributing

Read [AGENTS.md](AGENTS.md) and the [catalog's rules](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md)
first.

## 📄 License

MIT, Copyright (c) 2026 Germán Méndez Bravo. See [LICENSE](LICENSE).
