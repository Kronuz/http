// http_server_beast: Boost.Beast answering the same fixed reply as http_server_compare, for the comparison in
// docs/perf/timeouts-cpu.md. A plain asynchronous server on Boost.Asio (not standalone Asio): N io_contexts with one
// thread each, one acceptor on loop 0 handing sockets round-robin, HTTP/1.1 keep-alive, no Server field, the
// same `Connection: keep-alive` field the others state.
//
//   http_server_beast KIND PORT REACTORS      KIND is "beast"
//   http_server_beast version
//
// SIGUSR2 prints the process's CPU time as http_server_compare does. Built only with -DHTTP_BENCH_BEAST=ON.
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/version.hpp>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bench_common.h"

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

static awaitable<void> serve(tcp::socket socket) {
	try {
		socket.set_option(tcp::no_delay(true));
		beast::flat_buffer buffer;
		for (;;) {
			http::request<http::empty_body> req;
			co_await http::async_read(socket, buffer, req, use_awaitable);
			http::response<http::string_body> res{http::status::ok, req.version()};
			res.set(http::field::content_type, "text/plain");
			res.body() = "Hello, World!\n";
			res.prepare_payload();
			res.set(http::field::connection, "keep-alive");
			co_await http::async_write(socket, res, use_awaitable);
			if (!req.keep_alive()) { break; }
		}
	} catch (const std::exception&) {
	}
}

static awaitable<void> acceptor_loop(std::vector<std::unique_ptr<asio::io_context>>& ios, unsigned short port) {
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

int main(int argc, char** argv) {
	if (argc > 1 && std::string(argv[1]) == "version") {
		std::printf("Boost.Beast in Boost %d.%d.%d\n", BOOST_VERSION / 100000, BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100);
		return 0;
	}
	unsigned short port = static_cast<unsigned short>(argc > 2 ? std::atoi(argv[2]) : 8080);
	std::size_t reactors = static_cast<std::size_t>(argc > 3 ? std::atoi(argv[3]) : 1);
	std::signal(SIGINT, on_signal);
	std::signal(SIGTERM, on_signal);
	std::signal(SIGUSR2, on_usr2);

	std::vector<std::unique_ptr<asio::io_context>> ios;
	std::vector<asio::executor_work_guard<asio::io_context::executor_type>> guards;
	std::vector<std::thread> threads;
	for (std::size_t i = 0; i < reactors; ++i) {
		ios.push_back(std::make_unique<asio::io_context>(1));
		guards.emplace_back(asio::make_work_guard(*ios.back()));
	}
	asio::co_spawn(*ios[0], acceptor_loop(ios, port), asio::detached);
	for (auto& io : ios) { threads.emplace_back([p = io.get()] { p->run(); }); }
	std::printf("ready beast %zu\n", reactors);
	std::fflush(stdout);
	while (!g_stop.load()) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
	for (auto& io : ios) { io->stop(); }
	for (auto& t : threads) { t.join(); }
	return 0;
}
