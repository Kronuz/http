// bench_asio -- a keep-alive Asio coroutine server for the throughput/latency bench.
// Serves a fixed response inline on the reactor (the fast path), N io_contexts on N
// threads sharing one port via SO_REUSEPORT. Runs until killed.
//   usage: bench_asio <port> <reactors>

#include <asio.hpp>
#include <netinet/in.h>
#include <sys/socket.h>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using asio::ip::tcp;
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::use_awaitable;
namespace this_coro = asio::this_coro;
using reuse_port = asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>;

static const std::string RESP =
	"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\n\r\nHello, World!\n";

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
				buf.erase(0, pos + 4);   // consume the request (no body for GET)
				co_await asio::async_write(socket, asio::buffer(RESP), use_awaitable);
			}
		}
	} catch (const std::exception&) {
	}
}

static awaitable<void> listener(unsigned short port) {
	auto ex = co_await this_coro::executor;
	tcp::acceptor acceptor(ex);
	tcp::endpoint ep(tcp::v4(), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	acceptor.set_option(reuse_port(true));
	acceptor.bind(ep);
	acceptor.listen(1024);
	for (;;) {
		tcp::socket socket = co_await acceptor.async_accept(use_awaitable);
		co_spawn(ex, serve(std::move(socket)), detached);
	}
}

int main(int argc, char** argv) {
	unsigned short port = static_cast<unsigned short>(argc > 1 ? std::atoi(argv[1]) : 8080);
	int reactors = argc > 2 ? std::atoi(argv[2]) : 4;
	std::vector<std::unique_ptr<asio::io_context>> ios;
	std::vector<std::thread> threads;
	for (int i = 0; i < reactors; ++i) { ios.push_back(std::make_unique<asio::io_context>(1)); }
	std::printf("bench_asio: %d reactors on :%u\n", reactors, port);
	for (auto& io : ios) {
		asio::io_context* iop = io.get();
		threads.emplace_back([iop, port] {
			co_spawn(*iop, listener(port), detached);
			iop->run();
		});
	}
	for (auto& t : threads) { t.join(); }
	return 0;
}
