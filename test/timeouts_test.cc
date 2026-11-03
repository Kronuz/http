// The idle, header, and inactivity timeouts (AsioResponseOptions) over real TCP: a silent
// connection closes at the idle timeout, from its start and after a reply; headers trickled
// past the header timeout close even though bytes keep arriving; a body that stops closes at
// the inactivity timeout while a slow one that moves completes; a reply the client stops
// reading is cut at the inactivity timeout while a slow reader that moves gets all of it; a
// slow client that stays under a timeout is served, also on a reused connection; unset, none
// of them fires.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "http_asio.h"
#include "test_loop.h"

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

static int failures = 0;
static void check(bool ok, const char* what) {
	std::printf("  %s: %s\n", ok ? "ok" : "FAILED", what);
	if (!ok) { ++failures; }
}

// How the last streamed body ended for the handler reading it.
static std::atomic<int> stream_end{0};

struct Echo : http::HttpHandler {
	bool wants_body_stream(const http::Request& r) override { return r.path == "/stream"; }
	task::task<void> handle(http::Request& req, http::ResponseWriter& w) override {
		if (req.path == "/stream") {
			std::string chunk;
			asio::error_code ec;
			while (!(ec = co_await req.body_stream->read(chunk))) {}
			stream_end = ec.value();
			w.send(200, "streamed");
			co_return;
		}
		if (req.path == "/big") { w.send(200, std::string(std::stoul(req.query.substr(2)), 'x')); co_return; }   // query "n=<bytes>"
		w.send(200, std::to_string(req.body.size()));
		co_return;
	}
};

static unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}

static int dial(unsigned short port) {
	for (int i = 0; i < 300; ++i) {
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0) {
			timeval tv{.tv_sec = 5, .tv_usec = 0};
			::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
			return fd;
		}
		::close(fd);
		std::this_thread::sleep_for(5ms);
	}
	return -1;
}

static void put(int fd, const std::string& s) { ::send(fd, s.data(), s.size(), 0); }

// Reads until the peer closes; the bytes read, and the seconds it took.
static std::pair<std::string, double> until_closed(int fd) {
	const auto t0 = clk::now();
	std::string got;
	char b[4096];
	for (;;) {
		const ssize_t n = ::recv(fd, b, sizeof(b), 0);
		if (n <= 0) { break; }
		got.append(b, static_cast<std::size_t>(n));
	}
	return {got, std::chrono::duration<double>(clk::now() - t0).count()};
}

int main() {
	std::signal(SIGPIPE, SIG_IGN);  // a send to a closed connection fails instead
	Echo echo;
	const unsigned short port = free_port();
	http::HttpAsioService svc(echo, 1, 64);
	svc.set_idle_timeout(300ms);
	svc.set_header_timeout(400ms);
	svc.set_inactivity_timeout(300ms);
	svc.start(port);

	{  // a connection that never sends: closed at the idle timeout
		int fd = dial(port);
		auto [got, s] = until_closed(fd);
		check(got.empty() && s > 0.2 && s < 2.0, "a silent connection closes at the idle timeout");
		::close(fd);
	}
	{  // after a reply, the idle timeout applies again
		int fd = dial(port);
		put(fd, "GET / HTTP/1.1\r\nHost: x\r\n\r\n");
		auto [got, s] = until_closed(fd);
		check(got.rfind("HTTP/1.1 200", 0) == 0 && s > 0.2 && s < 2.0, "idle after a reply closes at the idle timeout");
		::close(fd);
	}
	{  // headers trickled a byte every 100 ms: the header timeout closes it, though bytes keep coming
		int fd = dial(port);
		const std::string req = "GET / HTTP/1.1\r\nHost: x\r\nX-Pad: " + std::string(40, 'a') + "\r\n\r\n";
		const auto t0 = clk::now();
		bool closed = false;
		for (char c : req) {
			if (::send(fd, &c, 1, 0) <= 0) { closed = true; break; }
			std::this_thread::sleep_for(100ms);
			char probe;
			if (::recv(fd, &probe, 1, MSG_DONTWAIT) == 0) { closed = true; break; }
		}
		const double s = std::chrono::duration<double>(clk::now() - t0).count();
		check(closed && s < 2.0, "trickled headers close at the header timeout");
		::close(fd);
	}
	{  // a body that stops: closed at the inactivity timeout
		int fd = dial(port);
		put(fd, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789");
		auto [got, s] = until_closed(fd);
		check(got.empty() && s > 0.2 && s < 2.0, "a stalled body closes at the inactivity timeout");
		::close(fd);
	}
	{  // a streamed body that stops: its reader sees timed_out at the inactivity timeout
		int fd = dial(port);
		put(fd, "POST /stream HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789");
		auto [got, s] = until_closed(fd);
		check(s > 0.2 && s < 2.0 && stream_end == asio::error_code(asio::error::timed_out).value(),
			"a stalled streamed body fails its reader with timed_out at the inactivity timeout");
		::close(fd);
	}
	{  // a slow body that keeps moving, every 150 ms for 1.5 s: completes
		int fd = dial(port);
		put(fd, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\nConnection: close\r\n\r\n");
		for (int i = 0; i < 10; ++i) {
			std::this_thread::sleep_for(150ms);
			put(fd, "x");
		}
		auto [got, s] = until_closed(fd);
		check(got.rfind("HTTP/1.1 200", 0) == 0 && got.find("\r\n\r\n10") != std::string::npos, "a slow but moving body completes");
		::close(fd);
	}
	{  // headers trickled after a reply on a keep-alive connection (the first byte arrives through the
	   // watch's read-ahead): the header timeout still counts from that byte and closes it
		int fd = dial(port);
		put(fd, "GET / HTTP/1.1\r\nHost: x\r\n\r\n");
		char reply[256];
		const ssize_t got = ::recv(fd, reply, sizeof(reply), 0);
		const std::string req = "GET / HTTP/1.1\r\nHost: x\r\nX-Pad: " + std::string(40, 'a') + "\r\n\r\n";
		const auto t0 = clk::now();
		bool closed = false;
		for (char c : req) {
			if (::send(fd, &c, 1, 0) <= 0) { closed = true; break; }
			std::this_thread::sleep_for(100ms);
			char probe;
			if (::recv(fd, &probe, 1, MSG_DONTWAIT) == 0) { closed = true; break; }
		}
		const double s = std::chrono::duration<double>(clk::now() - t0).count();
		check(got > 0 && closed && s < 2.0, "headers trickled on a reused connection close at the header timeout");
		::close(fd);
	}
	{  // a reply stalled by a client that does not read: closed at the inactivity timeout
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		int small = 4096;
		::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
		timeval tv{.tv_sec = 5, .tv_usec = 0};
		::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		constexpr std::size_t big = 48u << 20;
		put(fd, "GET /big?n=" + std::to_string(big) + " HTTP/1.1\r\nHost: x\r\n\r\n");
		std::this_thread::sleep_for(1200ms);   // not reading: the server's slice stalls and times out
		auto [got, s] = until_closed(fd);
		check(got.size() < big && s < 3.0, "a reply the client stops reading is cut at the inactivity timeout");
		::close(fd);
	}
	svc.stop();

	{  // a slow reader that keeps moving: every 64 KiB slice completes in time, though the whole reply takes longer than the bound
		const unsigned short p4 = free_port();
		http::HttpAsioService mover(echo, 1, 64);
		mover.set_inactivity_timeout(1000ms);
		mover.start(p4);
		int fd = dial(p4);
		constexpr std::size_t size = 4u << 20;
		put(fd, "GET /big?n=" + std::to_string(size) + " HTTP/1.1\r\nConnection: close\r\nHost: x\r\n\r\n");
		std::string got;
		char b[16384];
		const auto t0 = clk::now();
		for (;;) {
			const ssize_t n = ::recv(fd, b, sizeof(b), 0);
			if (n <= 0) { break; }
			got.append(b, static_cast<std::size_t>(n));
			std::this_thread::sleep_for(8ms);   // 256 or more reads: at least 2 s in all, four or more reads per 64 KiB slice
		}
		const double s = std::chrono::duration<double>(clk::now() - t0).count();
		check(got.size() > size && got.compare(got.size() - size, size, std::string(size, 'x')) == 0 && s > 1.0,
			"a slow reader that keeps moving receives the whole reply, taking longer than the inactivity timeout");
		::close(fd);
		mover.stop();
	}

	{  // under the header timeout a slow client is served: a request trickled a byte every 10 ms
		const unsigned short p3 = free_port();
		http::HttpAsioService slow(echo, 1, 64);
		slow.set_idle_timeout(10s);
		slow.set_header_timeout(10s);
		slow.set_inactivity_timeout(10s);
		slow.start(p3);
		const std::string req = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";
		const auto trickle = [&](int fd) {
			for (char c : req) {
				::send(fd, &c, 1, 0);
				std::this_thread::sleep_for(10ms);
			}
			char reply[256];
			const ssize_t n = ::recv(fd, reply, sizeof(reply), 0);
			return n > 0 && std::string(reply, static_cast<std::size_t>(n)).rfind("HTTP/1.1 200", 0) == 0;
		};
		int fd = dial(p3);
		check(trickle(fd), "headers trickled under the header timeout are served");
		check(trickle(fd), "and on the same connection after a reply, through the read-ahead");
		::close(fd);
		slow.stop();
	}

	{  // unset: a silent connection stays open past every bound above
		const unsigned short p2 = free_port();
		http::HttpAsioService plain(echo, 1, 64);
		plain.start(p2);
		int fd = dial(p2);
		timeval tv{.tv_sec = 1, .tv_usec = 200000};
		::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		char b;
		const ssize_t n = ::recv(fd, &b, 1, 0);
		check(n < 0, "with the timeouts unset, a silent connection stays open");
		::close(fd);
		plain.stop();
	}
	std::printf("timeouts: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
