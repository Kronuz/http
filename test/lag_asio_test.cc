// lag_asio_test -- the service's reactor lag probe: off unless enabled, one per reactor when
// enabled, and a request keeps the loop at the 1 ms interval (note_activity per request).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "http_asio.h"

using namespace std::chrono_literals;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

struct App : http::HttpHandler {
	asio::awaitable<void> handle(http::Request&, http::ResponseWriter& w) override { w.send(200, "ok\n"); co_return; }
};

// A keep-alive client: one connection, sequential GET /x, each answer read in full.
struct Client {
	int fd = -1;
	explicit Client(unsigned short port) {
		fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
		if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) { ::close(fd); fd = -1; }
	}
	~Client() { if (fd >= 0) { ::close(fd); } }
	bool get() {
		if (fd < 0) { return false; }
		const std::string req = "GET /x HTTP/1.1\r\nHost: t\r\n\r\n";
		if (::send(fd, req.data(), req.size(), 0) < 0) { return false; }
		std::string got;
		char buf[512];
		while (got.find("ok\n") == std::string::npos) {
			const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
			if (n <= 0) { return false; }
			got.append(buf, static_cast<std::size_t>(n));
		}
		return got.find("200") != std::string::npos;
	}
};

static unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
	socklen_t len = sizeof(addr);
	::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
	::close(fd);
	return ntohs(addr.sin_port);
}

int main() {
	std::printf("== http reactor lag probe test ==\n");
	App app;
	{
		http::HttpAsioService off(app, 2, 16);
		const auto port = free_port();
		off.start(port);
		Client c(port);
		for (int i = 0; i < 4; ++i) { check(c.get(), "a request is served"); }
		std::this_thread::sleep_for(250ms);
		check(off.reactor(0).lag().snapshot().wakes == 0 && off.reactor(1).lag().snapshot().wakes == 0,
			"a service without enable_lag_probe() records nothing");
		off.stop();
	}
	{
		http::HttpAsioService on(app, 2, 16);
		on.enable_lag_probe();
		const auto port = free_port();
		on.start(port);
		std::this_thread::sleep_for(450ms);
		const auto idle0 = on.reactor(0).lag().snapshot().wakes + on.reactor(1).lag().snapshot().wakes;
		check(idle0 >= 4 && idle0 <= 14, "idle, the two probes wake about 10 times a second each");
		// One connection: the accept marks the loop busy for a second, so only the requests on it
		// keep the 1 ms interval past that second.
		Client c(port);
		auto wakes = [&] { return on.reactor(0).lag().snapshot().wakes + on.reactor(1).lag().snapshot().wakes; };
		const auto t0 = std::chrono::steady_clock::now();
		int served = 0;
		std::uint64_t before = 0;
		bool marked = false;
		while (std::chrono::steady_clock::now() - t0 < 1800ms) {
			served += c.get() ? 1 : 0;
			std::this_thread::sleep_for(5ms);
			if (!marked && std::chrono::steady_clock::now() - t0 >= 1200ms) { before = wakes(); marked = true; }
		}
		const auto busy = wakes() - before;
		std::printf("     %d requests, %llu wakes in the last 600 ms\n", served, (unsigned long long)busy);
		check(served > 50, "requests were served");
		check(busy > 60, "requests alone keep the probes at the 1 ms interval (per-request note_activity)");
		on.stop();
	}
	std::printf(g_fail ? "FAILED (%d)\n" : "PASSED\n", g_fail);
	return g_fail ? 1 : 0;
}
