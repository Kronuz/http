/*
 * bench_http -- framework throughput: the same fixed "Hello, World!" response
 * served through the real HttpAsioService + a trivial HttpHandler (router, request
 * parsing, response framing all exercised). Compare its numbers against bench_asio
 * (the raw-Asio ceiling with no framework) to see the framework's overhead.
 *
 * The fast path is inline on the reactor (no offload), which is what a cheap handler
 * wants -- max throughput, the loop never leaves the core.
 *
 *   ./bench_http <port> <reactors>
 *   wrk -t4 -c100 -d10s http://127.0.0.1:<port>/
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "http_asio.h"
#include "http_handler.h"

// A handler that answers every request with a fixed body -- the minimum work, so the
// measurement is the framework's parse/route/frame overhead, not the app's.
class HelloApp : public http::HttpHandler {
public:
	void handle(const http::Request& /*request*/, http::ResponseWriter& response) override {
		response.status(200);
		response.set_header("Content-Type", "text/plain");
		response.write("Hello, World!\n");
		response.end();
	}
};

static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop.store(true); }

int main(int argc, char** argv) {
	unsigned short port = static_cast<unsigned short>(argc > 1 ? std::atoi(argv[1]) : 8080);
	std::size_t reactors = static_cast<std::size_t>(argc > 2 ? std::atoi(argv[2]) : 4);
	if (reactors == 0) { reactors = 1; }

	HelloApp app;
	// workers=0 -> the handler runs inline on the reactor (the fast path). SO_REUSEPORT
	// so each reactor binds its own acceptor (best on Linux); the portable shared
	// acceptor kicks in automatically where SO_REUSEPORT is unavailable.
	http::HttpAsioService service(app, reactors, /*workers=*/0, /*queue_limit=*/0);
	http::AsioBindOptions bind;
	bind.reuse_port = true;
	service.set_bind_options(bind);

	std::signal(SIGINT, on_signal);
	std::signal(SIGTERM, on_signal);

	service.start(port);
	std::printf("bench_http: %zu reactors on :%u (framework path)\n", reactors, port);
	std::fflush(stdout);

	while (!g_stop.load()) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

	service.stop();
	return 0;
}
