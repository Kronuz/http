// bench_http -- the current Kronuz/http stack, keep-alive, serving a fixed response
// inline (no Dispatcher -> the fast path), N reactors via the ReactorPool runtime,
// one port (SO_REUSEPORT). The apples-to-apples counterpart of bench_asio for the
// throughput/latency comparison. Runs until killed.
//   usage: bench_http <port> <reactors>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "http_handler.h"
#include "http_server.h"
#include "reactor_pool.h"

struct BenchHandler : http::HttpHandler {
	void handle(const http::Request& /*request*/, http::ResponseWriter& response) override {
		response.send(200, "Hello, World!\n", "text/plain");
	}
};

int main(int argc, char** argv) {
	unsigned short port = static_cast<unsigned short>(argc > 1 ? std::atoi(argv[1]) : 8080);
	int reactors = argc > 2 ? std::atoi(argv[2]) : 4;

	BenchHandler handler;
	// create(handler) with no Dispatcher -> handlers run inline on the reactor (the
	// fast path), matching bench_asio's inline serve.
	ReactorPool<http::HttpService> pool(reactors, [&handler] {
		auto svc = http::HttpService::create(handler);
		http::BindOptions bind;
		bind.reuse_port = true;
		svc->set_bind_options(bind);
		return svc;
	});
	pool.start(port);
	std::printf("bench_http: %d reactors on :%u\n", reactors, port);

	for (;;) { std::this_thread::sleep_for(std::chrono::hours(1)); }
	return 0;
}
