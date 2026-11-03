// The benchmark counters (bench/counting.h): operator new calls and Asio handler invocations are counted exactly.
// The handler count is checked against io_context::run()'s own count of the handlers it ran, on posts, tasks and
// a socket round trip; the allocation count against known numbers of operator new calls and coroutine frames.
#include "counting.h"

#include <asio.hpp>
#include <cstdio>
#include <cstdlib>

#include "task_asio.hh"

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

static task::task<void> leaf() { co_return; }

static task::task<void> hop(int n) {
	for (int i = 0; i < n; ++i) co_await task::yield();
}

static task::task<void> echo_server(tcp::acceptor& acceptor, tcp::socket& s) {
	co_await task::await_asio<void(asio::error_code)>([&](auto h) { acceptor.async_accept(s, std::move(h)); });
	char b[1];
	co_await task::await_asio<void(asio::error_code, std::size_t)>([&](auto h) { asio::async_read(s, asio::buffer(b), std::move(h)); });
	co_await task::await_asio<void(asio::error_code, std::size_t)>([&](auto h) { asio::async_write(s, asio::buffer(b), std::move(h)); });
}

static task::task<void> client(tcp::socket& s, tcp::endpoint ep) {
	co_await task::await_asio<void(asio::error_code)>([&](auto h) { s.async_connect(ep, std::move(h)); });
	char b[1] = {'x'};
	co_await task::await_asio<void(asio::error_code, std::size_t)>([&](auto h) { asio::async_write(s, asio::buffer(b), std::move(h)); });
	co_await task::await_asio<void(asio::error_code, std::size_t)>([&](auto h) { asio::async_read(s, asio::buffer(b), std::move(h)); });
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
	// tasks: the count agrees with run(), and each spawned frame reaches operator new (TASK_FRAME_POOL=0).
	{
		asio::io_context io(1);
		task::asio_scheduler scheduler(io.get_executor());
		const auto on_loop = task::scheduler_ref::of(scheduler);
		unsigned long long h = handlers(), a = allocs();
		for (int i = 0; i < 50; ++i) task::spawn(on_loop, leaf());
		std::size_t ran = io.run();
		CHECK(ran == 50);
		CHECK(handlers() - h == ran);
		CHECK(allocs() - a >= 100);   // each spawn: the task's frame and its root's
		io.restart();
		h = handlers();
		task::spawn(on_loop, hop(10));
		ran = io.run();
		CHECK(ran == 11);
		CHECK(handlers() - h == ran);
	}
	// a socket round trip: connect, accept, read, write on one loop.
	{
		asio::io_context io(1);
		task::asio_scheduler scheduler(io.get_executor());
		const auto on_loop = task::scheduler_ref::of(scheduler);
		tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
		tcp::socket server(io), peer(io);
		unsigned long long h = handlers();
		task::spawn(on_loop, echo_server(acceptor, server));
		task::spawn(on_loop, client(peer, acceptor.local_endpoint()));
		std::size_t ran = io.run();
		CHECK(ran >= 6);
		CHECK(handlers() - h == ran);
	}
	std::printf(failures ? "bench_counting: %d failure(s)\n" : "bench_counting: ok\n", failures);
	return failures ? 1 : 0;
}
