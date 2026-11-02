// The benchmark counters (bench/counting.h): operator new calls and Asio handler invocations are counted exactly.
// The handler count is checked against io_context::run()'s own count of the handlers it ran, on posts, awaitables and
// a socket round trip; the allocation count against known numbers of operator new calls and coroutine frames.
#include "counting.h"

#include <asio.hpp>
#include <cstdio>
#include <cstdlib>

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

static int failures = 0;
#define CHECK(cond)                                                      \
	do {                                                                 \
		if (!(cond)) {                                                   \
			std::fprintf(stderr, "FAIL %s:%d %s\n", __FILE__, __LINE__, #cond); \
			++failures;                                                  \
		}                                                                \
	} while (0)

static unsigned long long allocs() { return g_allocs.load(); }
static unsigned long long handlers() { return g_handlers.load(); }

static awaitable<void> leaf() { co_return; }

static awaitable<void> hop(int n) {
	auto ex = co_await asio::this_coro::executor;
	for (int i = 0; i < n; ++i) co_await asio::post(ex, use_awaitable);
}

static awaitable<void> echo_server(tcp::acceptor& acceptor) {
	tcp::socket s = co_await acceptor.async_accept(use_awaitable);
	char b[1];
	co_await asio::async_read(s, asio::buffer(b), use_awaitable);
	co_await asio::async_write(s, asio::buffer(b), use_awaitable);
}

static awaitable<void> client(tcp::endpoint ep) {
	tcp::socket s(co_await asio::this_coro::executor);
	co_await s.async_connect(ep, use_awaitable);
	char b[1] = {'x'};
	co_await asio::async_write(s, asio::buffer(b), use_awaitable);
	co_await asio::async_read(s, asio::buffer(b), use_awaitable);
}

int main() {
	// operator new: every replaceable form counts once; delete does not count.
	{
		unsigned long long a = allocs();
		void* p = ::operator new(24);
		void* q = ::operator new[](24);
		void* r = ::operator new(24, std::align_val_t(64));
		void* t = ::operator new(24, std::nothrow);
		CHECK(allocs() == a + 4);
		CHECK(reinterpret_cast<std::uintptr_t>(r) % 64 == 0);
		::operator delete(p);
		::operator delete[](q);
		::operator delete(r, std::align_val_t(64));
		::operator delete(t);
		CHECK(allocs() == a + 4);
	}
	// posts: one handler each, and the count agrees with what run() ran.
	{
		asio::io_context io(1);
		unsigned long long h = handlers();
		for (int i = 0; i < 100; ++i) asio::post(io, [] {});
		std::size_t ran = io.run();
		CHECK(ran == 100);
		CHECK(handlers() - h == ran);
	}
	// awaitables: the count agrees with run(), and each spawned frame reaches operator new (recycling is off).
	{
		asio::io_context io(1);
		unsigned long long h = handlers(), a = allocs();
		for (int i = 0; i < 50; ++i) asio::co_spawn(io, leaf(), asio::detached);
		std::size_t ran = io.run();
		CHECK(handlers() - h == ran);
		CHECK(allocs() - a >= 50);
		io.restart();
		h = handlers();
		asio::co_spawn(io, hop(10), asio::detached);
		ran = io.run();
		CHECK(ran >= 10);
		CHECK(handlers() - h == ran);
	}
	// a socket round trip: connect, accept, read, write on one loop.
	{
		asio::io_context io(1);
		tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
		unsigned long long h = handlers();
		asio::co_spawn(io, echo_server(acceptor), asio::detached);
		asio::co_spawn(io, client(acceptor.local_endpoint()), asio::detached);
		std::size_t ran = io.run();
		CHECK(ran >= 6);
		CHECK(handlers() - h == ran);
	}
	std::printf(failures ? "bench_counting: %d failure(s)\n" : "bench_counting: ok\n", failures);
	return failures ? 1 : 0;
}
