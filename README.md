# http

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/http/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/http/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Header-only](https://img.shields.io/badge/header--only-yes-green.svg)

An HTTP/1.1 application framework for C++20 on standalone Asio coroutines. Your app
implements one coroutine, `handle(Request&, ResponseWriter&)`; the framework handles
sockets, parsing, keep-alive, negotiation, compression, and backpressure. Handlers
run on the connection's loop and suspend instead of block.

Buffered response decorators can distinguish `response_started()` from `response_ended()`: setting a status or writing bytes does not complete a response; Asio reports completion only after `end()` succeeds. The new query defaults to the historical started approximation for existing custom `BufferedResponse` writers. Override it for accurate middleware error mapping. This is source compatible but extends a virtual interface: rebuild all consumers together rather than mixing header revisions. It does not indicate confirmed network delivery.

![http: connections on per-core reactors parse into a Request, the handler writes a response](docs/http.svg)

## ✨ Highlights

- 🔌 **One seam**: the handler never sees the parser, the socket, or the event loop.
- 🧵 **Shared-nothing reactors**: one Asio `io_context` per core and nothing off it.
  A handler that must wait `co_await`s, and the loop serves other connections. Past
  the per-reactor in-flight limit, requests get `503` with `Retry-After`.
- ✂️ **Cancellation**: a client hang-up or a shutdown cancels a running request
  (`Request::on_cancel`); reads stop at their next checkpoint, writes finish.
- 🌊 **Bounded memory for any body**: read request bodies with `co_await` from a
  `BodyStream`, or push them to a `BodySink`, chunk by chunk; a buffered body is capped
  (`413`). Responses stream with backpressure.
- 🤝 **Batteries**: a radix-tree `Router`, content negotiation, zstd/gzip compression,
  ETags with `304`, and byte ranges with `206`.
- 🔒 **Optional TLS**, connection caps, and per-request deadlines; OpenSSL is linked
  only when you use the TLS adapter.
- 🗣️ **Custom methods**: built on [http-parser](https://github.com/Kronuz/http-parser),
  so verbs like `COUNT` or `RESTORE` parse.

## 📦 Installation

Requires a C++20 compiler and CMake 3.14 or newer. Asio and the Kronuz dependencies
are fetched at configure time. CI builds it on Linux (x86_64, arm64), macOS (arm64,
x86_64), and FreeBSD, plus ASan, UBSan, and TSan builds.

```cmake
include(FetchContent)
FetchContent_Declare(http
  GIT_REPOSITORY https://github.com/Kronuz/http.git
  GIT_TAG        main)
FetchContent_MakeAvailable(http)

target_link_libraries(my_app PRIVATE http::http)
```

## 🚀 Quick start

```cpp
#include "http_asio.h"
#include "http_router.h"

#include <chrono>
#include <thread>

struct App : http::HttpHandler {
	http::Router router;
	App() {
		router.route("GET", "/hello/:name", [](http::Request&, http::ResponseWriter& resp, const http::Params& p) -> asio::awaitable<void> {
			resp.send(200, "hello, " + std::string(p.get("name")) + "\n");
			co_return;
		});
	}
	asio::awaitable<void> handle(http::Request& req, http::ResponseWriter& resp) override { co_await router.handle(req, resp); }
};

int main() {
	App app;
	http::HttpAsioService service(app, /*reactors=*/4, /*in_flight_limit=*/256);
	service.start(8080);   // curl localhost:8080/hello/world
	std::this_thread::sleep_for(std::chrono::minutes(1));
	service.stop();
}
```

[`examples/kv_store.cc`](examples/kv_store.cc) is a complete REST key/value app,
including a streamed response and a request body pushed to a sink:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
./build/kv_store 8080 &
curl -XPUT localhost:8080/kv/greeting -d 'hello'
curl localhost:8080/kv/greeting
```

## 🎯 When to use it

- ✅ A C++ service that needs a solid HTTP/1.1 front: REST APIs, search or storage
  servers, admin endpoints.
- ❌ HTTP/2, HTTP/3, or WebSockets: not implemented.

## 📚 Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md): the two layers, why the seam has this shape,
  the request lifecycle, and the response transforms.
- [docs/mechanisms/coroutine-handlers.md](docs/mechanisms/coroutine-handlers.md): how a
  request runs, is cancelled, is admitted, and stops at shutdown.
- [docs/TRANSPORT.md](docs/TRANSPORT.md): deferred replies, TLS, connection caps,
  request deadlines, and early rejections.
- [docs/OWNED_STREAMING.md](docs/OWNED_STREAMING.md): owned response streaming.
- [docs/AUDIT.md](docs/AUDIT.md): transport audit findings, measured before and after.
- [AGENTS.md](AGENTS.md): file map, invariants, and dependencies.
- [prism](https://github.com/Kronuz/prism): a full app server built on this library.

## 🛠️ Building and testing

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
ctest --test-dir build
```

## 🤝 Contributing

Issues and pull requests are welcome. Read [AGENTS.md](AGENTS.md) first. CI must
pass on every platform.

## 📄 License

MIT, Copyright (c) 2026 Germán Méndez Bravo. See [LICENSE](LICENSE).
