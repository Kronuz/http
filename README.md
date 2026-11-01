# http

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/http/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/http/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Header-only](https://img.shields.io/badge/header--only-yes-green.svg)

Header-only HTTP/1.1 framework for C++20 on Asio coroutines: write one handler, get parsing, routing and streaming.
The application implements one coroutine, `handle(Request&, ResponseWriter&)`, and never sees
the parser, the socket or the event loop; each reactor's connection coroutine parses bytes into
a `Request` and frames what the handler writes. Reach for it when a C++ service needs an
HTTP/1.1 front: a REST API, a search or storage server, an admin endpoint.

## ✨ Highlights

- **One seam.** The handler takes a `Request` and writes through a `ResponseWriter`; the
  parser, the socket and the loop stay behind it.
- **Shared-nothing reactors.** One Asio `io_context` per thread, nothing off the loop: a
  handler that waits `co_await`s and the loop serves other connections. Past the
  per-reactor in-flight limit, a request gets `503` with `Retry-After`.
- **Cancellation.** A client hang-up or a shutdown cancels the running request
  (`Request::on_cancel`); reads stop at their next checkpoint, writes finish.
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
		router.route("GET", "/hello/:name", [](http::Request&, http::ResponseWriter& resp, const http::Params& p) -> asio::awaitable<void> {
			resp.send(200, "hello, " + std::string(p.get("name")) + "\n");
			co_return;
		});
	}
	asio::awaitable<void> handle(http::Request& req, http::ResponseWriter& resp) override {
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
[compressors](https://github.com/Kronuz/compressors) and
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

A 14-byte keep-alive reply on one reactor, driven by `wrk` for 4 s at one connection
(`-t1 -c1`) and at 64 (`-t2 -c64`). "Timeouts on" sets the request, idle, header and
inactivity timeouts of a production service; the minimal server is plain Asio 1.36.0
answering the same bytes. Latency is the `wrk` average and CPU is the server's user plus
system time per request, both in microseconds; medians of three rounds, Apple clang 17
with `-O3 -DNDEBUG`:

| machine | server | 1 connection: latency / CPU | 64 connections: req/s / CPU |
| --- | --- | ---: | ---: |
| Apple M4 Pro | minimal Asio | 60.9 / 29.1 | 41,785 / 24.5 |
| | `http` | 80.1 / 35.6 | 42,059 / 25.4 |
| | `http`, timeouts on | 95.6 / 45.8 | 41,888 / 28.7 |
| Intel Core i9-9980HK | minimal Asio | 33.2 / 22.9 | 130,595 / 9.6 |
| | `http` | 55.2 / 36.1 | 85,420 / 14.9 |
| | `http`, timeouts on | 86.8 / 67.6 | 48,423 / 26.1 |

`wrk` saturates at about 43k req/s on the M4 Pro, so there the CPU column separates the
servers at 64 connections. [docs/perf/timeouts-cpu.md](docs/perf/timeouts-cpu.md) has the
method and where the time goes. Run it with:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON
cmake --build build --target http_server_compare
sh bench/compare.sh build/http_server_compare > out.txt
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
- [docs/perf/](docs/perf/): per-request CPU and latency against a minimal Asio server.
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
