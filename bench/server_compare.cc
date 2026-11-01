// http_server_compare: servers that answer the same fixed HTTP request with the same fixed bytes, so a
// wrk run separates the HTTP layer from the transport underneath it.
//   asio     plain Asio: N io_contexts, one acceptor on loop 0 handing sockets round-robin (the layout
//            reactor::TcpServer uses where SO_REUSEPORT does not balance, as on macOS)
//   reactor  reactor::TcpServer with the same session body (the runtime without the http library)
//   http     http::HttpAsioService and a handler that writes the same body, no timeouts
//   httpt    the same with the request, idle, header and inactivity timeouts a production service sets
//
//   http_server_compare KIND PORT REACTORS
//   http_server_compare version
//
// SIGUSR2 prints the process's user and system CPU time (microseconds) to stderr as "RUSAGE <user> <system>";
// bench/run.sh reads it to report CPU per request. The Asio server is the lower bound: Asio is already the
// library's transport dependency, so nothing is fetched for it. The Beast and cpp-httplib comparators are
// bench/server_beast.cc and bench/server_httplib.cc. Built only with -DHTTP_BUILD_BENCH=ON.
#include <asio.hpp>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bench_common.h"
#include "http_asio.h"
#include "http_handler.h"
#include "reactor.h"

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

// The same request loop for the asio and reactor kinds: read, find the blank line, answer.
static awaitable<void> serve(tcp::socket socket) {
	try {
		socket.set_option(tcp::no_delay(true));
		std::string buf;
		char tmp[2048];
		for (;;) {
			std::size_t n = co_await socket.async_read_some(asio::buffer(tmp), use_awaitable);
			buf.append(tmp, n);
			std::size_t pos;
			while ((pos = buf.find("\r\n\r\n")) != std::string::npos) {
				buf.erase(0, pos + 4);
				co_await asio::async_write(socket, asio::buffer(RESP), use_awaitable);
			}
		}
	} catch (const std::exception&) {
	}
}

static awaitable<void> asio_acceptor(std::vector<std::unique_ptr<asio::io_context>>& ios, unsigned short port) {
	auto ex = co_await asio::this_coro::executor;
	tcp::acceptor acceptor(ex);
	tcp::endpoint ep(tcp::v4(), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	acceptor.bind(ep);
	acceptor.listen(1024);
	std::size_t next = 0;
	for (;;) {
		auto& target = *ios[next++ % ios.size()];
		tcp::socket socket = co_await acceptor.async_accept(target, use_awaitable);
		asio::co_spawn(target, serve(std::move(socket)), asio::detached);
	}
}

class HelloApp : public http::HttpHandler {
public:
	asio::awaitable<void> handle(http::Request&, http::ResponseWriter& response) override {
		response.status(200);
		response.set_header("Content-Type", "text/plain");
		response.write("Hello, World!\n");
		response.end();
		co_return;
	}
};

int main(int argc, char** argv) {
	std::string kind = argc > 1 ? argv[1] : "asio";
	if (kind == "version") {
		std::printf("asio %d.%d.%d\n", ASIO_VERSION / 100000, ASIO_VERSION / 100 % 1000, ASIO_VERSION % 100);
		return 0;
	}
	unsigned short port = static_cast<unsigned short>(argc > 2 ? std::atoi(argv[2]) : 8080);
	std::size_t reactors = static_cast<std::size_t>(argc > 3 ? std::atoi(argv[3]) : 1);
	std::signal(SIGINT, on_signal);
	std::signal(SIGTERM, on_signal);
	std::signal(SIGUSR2, on_usr2);

	if (kind == "asio") {
		std::vector<std::unique_ptr<asio::io_context>> ios;
		std::vector<asio::executor_work_guard<asio::io_context::executor_type>> guards;
		std::vector<std::thread> threads;
		for (std::size_t i = 0; i < reactors; ++i) {
			ios.push_back(std::make_unique<asio::io_context>(1));
			guards.emplace_back(asio::make_work_guard(*ios.back()));
		}
		asio::co_spawn(*ios[0], asio_acceptor(ios, port), asio::detached);
		for (auto& io : ios) threads.emplace_back([p = io.get()] { p->run(); });
		std::printf("ready asio %zu\n", reactors);
		std::fflush(stdout);
		while (!g_stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
		for (auto& io : ios) io->stop();
		for (auto& t : threads) t.join();
	} else if (kind == "reactor") {
		reactor::TcpServer server(reactors, 0, [](tcp::socket s, reactor::Reactor&) -> awaitable<void> { co_await serve(std::move(s)); });
		server.start(port);
		std::printf("ready reactor %zu\n", reactors);
		std::fflush(stdout);
		while (!g_stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
		server.stop();
	} else {
		HelloApp app;
		http::HttpAsioService service(app, reactors, /*in_flight_limit=*/0);
		if (kind == "httpt") {
			service.set_request_timeout(std::chrono::seconds(30));
			service.set_idle_timeout(std::chrono::seconds(60));
			service.set_header_timeout(std::chrono::seconds(10));
			service.set_inactivity_timeout(std::chrono::seconds(30));
		}
		service.start(port);
		std::printf("ready http %zu\n", reactors);
		std::fflush(stdout);
		while (!g_stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
		service.stop();
	}
	return 0;
}
