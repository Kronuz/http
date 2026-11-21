#pragma once

// ConnectionInput on its own: the socket pair the [U] sections drive, and the sections.

#include "support.h"

#ifndef READAHEAD_TEST_INTEGRATION_ONLY

namespace detail {

// ---- [U] ConnectionInput on its own ----
// (-DREADAHEAD_TEST_INTEGRATION_ONLY builds [P]..[W] without this section, so they also run
// against a header that has no read-ahead: they pin the behavior the watch must keep.)
using Socket = asio::ip::tcp::socket;
using Input = http::detail::ConnectionInput<Socket>;
inline int pending_bytes(int fd) { int n = 0; ::ioctl(fd, FIONREAD, &n); return n; }

struct Pair {
	asio::io_context io;
	Socket server{io};
	task::asio_scheduler scheduler{io.get_executor()};   // after io and server: destroyed first
	int client = -1;
	Pair() {
		asio::ip::tcp::acceptor acceptor(io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
		client = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(acceptor.local_endpoint().port());
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		::connect(client, reinterpret_cast<sockaddr*>(&a), sizeof(a));
		acceptor.accept(server);
	}
	~Pair() { io.stop(); if (client >= 0) { ::close(client); } }   // a stopped loop: the scheduler may destroy a read left pending
	void send(const std::string& s) { std::size_t o = 0; while (o < s.size()) { ssize_t n = ::send(client, s.data() + o, s.size() - o, 0); if (n <= 0) { break; } o += static_cast<std::size_t>(n); } }
	void settle() { io.restart(); io.run_for(150ms); }   // the loop runs what is ready, then idles out
	// Waits until `n` bytes sit in the server socket's receive queue.
	bool arrived(int n) { return until([&] { return pending_bytes(server.native_handle()) >= n; }); }
};
struct Read { std::string data; asio::error_code error; bool done = false; };
// One read(output) of up to `max` bytes, driven on the pair's loop.
inline Read read_once(Pair& p, const std::shared_ptr<Input>& in, std::size_t max) {
	Read r;
	auto reader = [&, in]() -> task::task<void> {
		std::string buf(max, '\0');
		const http::detail::ReadResult got = co_await in->read(asio::buffer(buf), {});
		if (got.error) { r.error = got.error; } else { r.data.assign(buf, 0, got.bytes); }
		r.done = true;
	};
	task::spawn(task::scheduler_ref::of(p.scheduler), reader());
	p.io.restart();
	for (auto end = clk::now() + 2s; !r.done && clk::now() < end;) { p.io.run_one_for(5ms); }
	return r;
}


}  // namespace detail

void lazy_watch_tests();
void held_read_tests();
void lane_tests();
void read_ahead_tests();
void input_tests();

#endif  // READAHEAD_TEST_INTEGRATION_ONLY
