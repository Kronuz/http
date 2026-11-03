// handler_count_test -- what the task loop costs in io_context handlers per request (design T4 (a),
// and (f)'s second half), counted exactly by Asio's custom handler tracking (bench/counting.h's).
// The reference is a raw reactor session on the same task mechanism: a read awaited with the
// session's token, then the reply sent at once. A request served through HttpAsioService by a
// handler that never suspends costs exactly what one round trip of that session costs, so the
// handler (and a chain of child tasks it awaits) adds no handler; a handler that asks for its
// cancel token pays for the disconnect watch's read, and one that never asks does not.
#define ASIO_CUSTOM_HANDLER_TRACKING "counting_tracking.h"
#include <asio.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "http_asio.h"

static int failures = 0;
static void check(bool ok, const std::string& what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what.c_str());
	if (!ok) { ++failures; }
}

static const std::string kReply = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";

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
		if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0) { return fd; }
		::close(fd);
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	return -1;
}
// Sends `path` `n` times on one keep-alive connection, one at a time, each answered with a
// 2-byte body; returns the server's handler invocations per request over those requests.
static double handlers_per_request(unsigned short port, const std::string& path, int warm, int n) {
	const int fd = dial(port);
	const std::string request = "GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n";
	auto round = [&] {
		::send(fd, request.data(), request.size(), 0);
		std::string got;
		char b[512];
		while (got.size() < 4 || got.compare(got.size() - 4, 4, "\r\nok") != 0) {
			ssize_t k = ::recv(fd, b, sizeof b, 0);
			if (k <= 0) { return false; }
			got.append(b, static_cast<std::size_t>(k));
		}
		return true;
	};
	for (int i = 0; i < warm; ++i) { if (!round()) { ::close(fd); return -1; } }
	std::this_thread::sleep_for(std::chrono::milliseconds(20));   // the warm-up's tail has run
	const auto before = g_handlers.load();
	for (int i = 0; i < n; ++i) { if (!round()) { ::close(fd); return -1; } }
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	const auto after = g_handlers.load();
	::close(fd);
	return double(after - before) / n;
}

// The reference: a session that reads with the session's token and sends the reply at once.
static task::task<void> raw_session(asio::ip::tcp::socket socket, reactor::Reactor&, reactor::SessionContext context) {
	asio::error_code ignored;
	socket.set_option(asio::ip::tcp::no_delay(true), ignored);
	std::string buffered;
	char tmp[4096];
	for (;;) {
		const auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto h) { socket.async_read_some(asio::buffer(tmp), std::move(h)); }, context.stop);
		if (error) { co_return; }
		buffered.append(tmp, n);
		for (std::size_t end; (end = buffered.find("\r\n\r\n")) != std::string::npos;) {
			buffered.erase(0, end + 4);
			asio::write(socket, asio::buffer(kReply), ignored);
		}
	}
}

static task::task<int> depth(int n) {
	if (n == 0) { co_return 0; }
	co_return 1 + co_await depth(n - 1);
}

struct App : http::HttpHandler {
	task::task<void> handle(http::Request& req, http::ResponseWriter& w) override {
		if (req.path == "/deep") { co_await depth(3); }                  // three child tasks that never suspend
		if (req.path == "/asks") { (void)req.cancel_token(); }          // engages the disconnect watch
		w.send(200, "ok", "");
		co_return;
	}
};

int main() {
	std::printf("== handler counts per request ==\n");
	constexpr int warm = 200, n = 1000;
	double raw = 0;
	{
		unsigned short port = free_port();
		reactor::TcpServer server(1, 0, raw_session);
		server.start(port);
		raw = handlers_per_request(port, "/", warm, n);
		server.stop();
	}
	App app;
	unsigned short port = free_port();
	http::HttpAsioService service(app, 1, 0);
	service.start(port);
	const double flat = handlers_per_request(port, "/flat", warm, n);
	const double deep = handlers_per_request(port, "/deep", warm, n);
	const double asks = handlers_per_request(port, "/asks", warm, n);
	service.stop();
	std::printf("  raw session %.3f, http /flat %.3f, /deep %.3f, /asks %.3f handlers per request\n", raw, flat, deep, asks);
	check(raw > 0, "the reference session served");
	check(flat == raw, "(a) a handler that never suspends adds no io_context handler to the session's");
	check(deep == raw, "(a) nor does a chain of child tasks it awaits that never suspend");
	check(asks > raw, "(f) a handler that asks for its cancel token pays for the watch's read; one that never asks pays none");
	std::printf("\n%s (%d failures)\n", failures == 0 ? "PASS" : "FAIL", failures);
	return failures == 0 ? 0 : 1;
}
