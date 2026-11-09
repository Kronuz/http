// http_server_compare: servers that answer the same fixed HTTP request with the same fixed bytes, so a
// wrk run separates the HTTP layer from the transport underneath it.
//   asio     plain Asio: N io_contexts, one acceptor on loop 0 handing sockets round-robin (the layout
//            reactor::TcpServer uses where SO_REUSEPORT does not balance, as on macOS)
//   reactor  reactor::TcpServer with the same session body (the runtime without the http library)
//   http     http::HttpAsioService and a handler that writes the same body, no timeouts
//   httpt    the same with the request, idle, header and inactivity timeouts a production service sets
//   httpr    http through a Router whose one route yields once (task::yield) before it answers: the cost of a
//            handler that suspends, which "http" never pays
//   httpc    http with a handler that asks for its cancel token first, as a search does: the cost of the
//            disconnect watch that question engages, which "http" never pays
//
//   http_server_compare KIND PORT REACTORS
//   http_server_compare version
//
// SIGUSR2 prints the process's user and system CPU time (microseconds) to stderr as "RUSAGE <user> <system>";
// bench/run.sh reads it to report CPU per request. The Asio server is the lower bound: Asio is already the
// library's transport dependency, so nothing is fetched for it. With -DHTTP_BENCH_COUNT_ALLOCS=ON (bench/counting.h)
// it also prints "ALLOCS <operator new calls>" and "HANDLERS <Asio handler invocations>"; bench/run.sh divides them by
// the requests served. That build is for counting, never for timing. The Beast and cpp-httplib comparators are
// bench/server_beast.cc and bench/server_httplib.cc. Built only with -DHTTP_BUILD_BENCH=ON.
#ifdef HTTP_BENCH_COUNT_ALLOCS
#include "counting.h"
#endif
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
#include "http_router.h"
#include "reactor.h"

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

// The request loop of the asio kind: read, find the blank line, answer.
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

// The same loop as a task, for the reactor kind (reactor::Session).
static task::task<void> serve_task(tcp::socket socket) {
	asio::error_code ignored;
	socket.set_option(tcp::no_delay(true), ignored);
	std::string buf;
	char tmp[2048];
	for (;;) {
		auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto h) { socket.async_read_some(asio::buffer(tmp), std::move(h)); });
		if (error) { co_return; }
		buf.append(tmp, n);
		std::size_t pos;
		while ((pos = buf.find("\r\n\r\n")) != std::string::npos) {
			buf.erase(0, pos + 4);
			auto [write_error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
				[&](auto h) { asio::async_write(socket, asio::buffer(RESP), std::move(h)); });
			(void)written;
			if (write_error) { co_return; }
		}
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
	task::task<void> handle(http::Request&, http::ResponseWriter& response) override {
		response.status(200);
		response.set_header("Content-Type", "text/plain");
		response.write("Hello, World!\n");
		response.end();
		co_return;
	}
};

// The same reply from a handler that first asks for its cancel token (the httpc kind), as a search that bounds
// its work by the request's cancellation does: the question engages the connection's disconnect watch.
class AskingApp : public http::HttpHandler {
public:
	task::task<void> handle(http::Request& request, http::ResponseWriter& response) override {
		if (request.cancel_token().requested()) { co_return; }
		response.status(200);
		response.set_header("Content-Type", "text/plain");
		response.write("Hello, World!\n");
		response.end();
	}
};

// The same reply after reading a streamed request body to its end (the httpb kind, a POST).
class StreamedApp : public http::HttpHandler {
public:
	bool wants_body_stream(const http::Request&) override { return true; }
	task::task<void> handle(http::Request& request, http::ResponseWriter& response) override {
		std::string chunk;
		while (!co_await request.body_stream->read(chunk)) {}
		response.status(200);
		response.set_header("Content-Type", "text/plain");
		response.write("Hello, World!\n");
		response.end();
	}
};

// The same reply through a Router route that suspends once on the reactor before it writes.
static http::Router make_yield_router() {
	http::Router router;
	router.route("GET", "/", [](http::Request&, http::ResponseWriter& response, const http::Params&) -> task::task<void> {
		co_await task::yield();
		response.status(200);
		response.set_header("Content-Type", "text/plain");
		response.write("Hello, World!\n");
		response.end();
	});
	return router;
}

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
		reactor::TcpServer server(reactors, 0, [](tcp::socket s, reactor::Reactor&, reactor::SessionContext) { return serve_task(std::move(s)); });
		server.start(port);
		std::printf("ready reactor %zu\n", reactors);
		std::fflush(stdout);
		while (!g_stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
		server.stop();
	} else {
		HelloApp hello;
		http::Router yielding = make_yield_router();
		StreamedApp streamed;
		AskingApp asking;
		http::HttpHandler& app = kind == "httpr" ? static_cast<http::HttpHandler&>(yielding)
			: kind == "httpb" ? static_cast<http::HttpHandler&>(streamed)
			: kind == "httpc" ? static_cast<http::HttpHandler&>(asking) : hello;
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
