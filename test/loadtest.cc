/*
 * Load test for the un-stallable model (the connection-offload dispatch).
 *
 * The unit test (test.cc) covers the seam logic above the wire. This one drives a
 * real server over TCP to prove the property that matters under load and that a
 * single-threaded test cannot show: a slow or blocking handler must NOT stall the
 * event loop -- other connections keep being served while it runs.
 *
 * Three checks:
 *   A. Offload non-stall. With a Dispatcher, a handful of slow handlers occupy
 *      some workers while many fast requests on other connections are served with
 *      low latency. Races here will not reliably surface in the doc E2E suite, so
 *      this is the validation for the offload wiring.
 *   B. Inline control. With no Dispatcher the same slow handler runs ON the reactor
 *      and blocks it: the fast requests are delayed ~the slow handler's duration.
 *      This proves the harness actually detects a stall -- and that A's low latency
 *      is a real improvement, not a measurement artifact.
 *   C. Backpressure. When the bounded queue saturates, submit() fails and the
 *      reactor answers 503 instead of growing an unbounded backlog.
 *
 * Build target: http_loadtest (also registered with ctest as "loadtest").
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "compressor_deflate.h"   // for decompressing gzip responses in the test
#include "compressor_zstd.h"      // for decompressing zstd responses in the test
#include "http_handler.h"
#include "http_router.h"
#include "http_server.h"

using clock_type = std::chrono::steady_clock;
static double ms_since(clock_type::time_point t0) {
	return std::chrono::duration<double, std::milli>(clock_type::now() - t0).count();
}

static int g_failures = 0;
#define CHECK(cond, msg)                                                            \
	do {                                                                            \
		if (!(cond)) { ++g_failures; std::printf("  FAIL: %s\n", msg); }             \
		else { std::printf("  ok:   %s\n", msg); }                                   \
	} while (0)


// ---- a minimal blocking HTTP/1.1 client (one request per connection) ----------
struct Result {
	int status = 0;
	double ms = 0;
	bool ok = false;
};

static Result do_get(uint16_t port, const std::string& path) {
	Result r;
	auto t0 = clock_type::now();
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) { return r; }
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
	// Don't let a wedged loop hang the whole test forever.
	timeval tv{.tv_sec = 5, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
		::close(fd);
		return r;
	}
	std::string req = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
	if (::send(fd, req.data(), req.size(), 0) < 0) { ::close(fd); return r; }
	std::string resp;
	char buf[4096];
	ssize_t n;
	while ((n = ::recv(fd, buf, sizeof(buf), 0)) > 0) { resp.append(buf, static_cast<size_t>(n)); }
	::close(fd);
	r.ms = ms_since(t0);
	if (resp.rfind("HTTP/1.1 ", 0) == 0 && resp.size() >= 12) {
		r.status = std::atoi(resp.c_str() + 9);
		r.ok = r.status != 0;
	}
	return r;
}

// Fire `count` concurrent GETs of `path` and return their results.
static std::vector<Result> fire(uint16_t port, const std::string& path, int count) {
	std::vector<Result> out(static_cast<size_t>(count));
	std::vector<std::thread> ts;
	ts.reserve(static_cast<size_t>(count));
	for (int i = 0; i < count; ++i) {
		ts.emplace_back([&out, port, path, i] { out[static_cast<size_t>(i)] = do_get(port, path); });
	}
	for (auto& t : ts) { t.join(); }
	return out;
}

static double max_latency(const std::vector<Result>& v) {
	double m = 0;
	for (const auto& r : v) { if (r.ms > m) { m = r.ms; } }
	return m;
}
static int count_status(const std::vector<Result>& v, int status) {
	int n = 0;
	for (const auto& r : v) { if (r.status == status) { ++n; } }
	return n;
}


// ---- a header+body-capturing client (for the compression checks) --------------
struct Resp {
	int status = 0;
	http::Headers headers;
	std::string body;   // raw, possibly binary (compressed)
	bool ok = false;
	std::string header(std::string_view name) const {
		for (const auto& [k, v] : headers) { if (http::iequal(k, name)) { return v; } }
		return {};
	}
};

static Resp do_request(uint16_t port, const std::string& path, const std::string& accept_encoding) {
	Resp r;
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) { return r; }
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
	timeval tv{.tv_sec = 5, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) { ::close(fd); return r; }
	std::string req = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n";
	if (!accept_encoding.empty()) { req += "Accept-Encoding: " + accept_encoding + "\r\n"; }
	req += "\r\n";
	::send(fd, req.data(), req.size(), 0);
	std::string raw;
	char buf[8192];
	ssize_t n;
	while ((n = ::recv(fd, buf, sizeof(buf), 0)) > 0) { raw.append(buf, static_cast<size_t>(n)); }
	::close(fd);

	auto sep = raw.find("\r\n\r\n");
	if (sep == std::string::npos) { return r; }
	std::string head = raw.substr(0, sep);
	r.body = raw.substr(sep + 4);   // binary-safe: the rest of the buffer
	auto eol = head.find("\r\n");
	std::string status_line = head.substr(0, eol);
	if (status_line.rfind("HTTP/1.1 ", 0) == 0) { r.status = std::atoi(status_line.c_str() + 9); }
	for (std::size_t pos = (eol == std::string::npos ? head.size() : eol + 2); pos < head.size();) {
		auto e = head.find("\r\n", pos);
		if (e == std::string::npos) { e = head.size(); }
		std::string line = head.substr(pos, e - pos);
		pos = e + 2;
		auto colon = line.find(':');
		if (colon != std::string::npos) {
			std::string v = line.substr(colon + 1);
			auto s = v.find_first_not_of(" \t");
			r.headers.emplace_back(line.substr(0, colon), s == std::string::npos ? std::string() : v.substr(s));
		}
	}
	r.ok = r.status != 0;
	return r;
}

static std::string gunzip(std::string_view s) {
	std::string out;
	DeflateDecompressData d(s.data(), s.size(), /*gzip=*/true);
	for (auto it = d.begin(); it; ++it) { out.append(*it); }
	return out;
}


// ---- the application: a fast route and a deliberately-slow route --------------
static constexpr int SLOW_MS = 500;

class LoadHandler : public http::HttpHandler {
	http::Router router_;
public:
	LoadHandler() {
		router_.route("GET", "/fast", [](const http::Request&, http::ResponseWriter& resp, const http::Params&) {
			resp.send(200, "ok\n");
		});
		router_.route("GET", "/slow", [](const http::Request&, http::ResponseWriter& resp, const http::Params&) {
			std::this_thread::sleep_for(std::chrono::milliseconds(SLOW_MS));   // blocking, CPU-bound stand-in
			resp.send(200, "slow\n");
		});
		router_.route("GET", "/big", [](const http::Request&, http::ResponseWriter& resp, const http::Params&) {
			resp.send(200, big_body());          // large, highly compressible
		});
		router_.route("GET", "/small", [](const http::Request&, http::ResponseWriter& resp, const http::Params&) {
			resp.send(200, "tiny\n");            // below the compression threshold
		});
	}

	// A large, very compressible body (repeated text) for the compression checks.
	static const std::string& big_body() {
		static const std::string body = [] {
			std::string s;
			while (s.size() < 20000) { s += "The quick brown fox jumps over the lazy dog. "; }
			return s;
		}();
		return body;
	}
	void handle(const http::Request& req, http::ResponseWriter& resp) override { router_.handle(req, resp); }
};


// Same routes, but classifies /fast as cheap (run inline on the reactor) and /slow
// as heavy (offload). Demonstrates the per-route fast path: a cheap route is served
// even while every worker is busy on heavy ones.
class ClassifiedHandler : public LoadHandler {
public:
	bool should_offload(const http::Request& req) const override { return req.path != "/fast"; }
};


// ---- server fixture: create + listen + run on one thread, torn down cleanly ---
static uint16_t find_free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;   // let the kernel pick a free port
	::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
	socklen_t len = sizeof(addr);
	::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
	uint16_t port = ntohs(addr.sin_port);
	::close(fd);
	return port;
}

static bool wait_until_listening(uint16_t port) {
	for (int i = 0; i < 600; ++i) {   // up to ~3s
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
		bool up = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
		::close(fd);
		if (up) { return true; }
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	return false;
}

struct ServerFixture {
	std::shared_ptr<http::HttpService> service;
	std::thread thr;
	uint16_t port;

	ServerFixture(std::function<std::shared_ptr<http::HttpService>()> factory, uint16_t p) : port(p) {
		std::promise<std::shared_ptr<http::HttpService>> ready;
		auto fut = ready.get_future();
		thr = std::thread([factory = std::move(factory), p, &ready] {
			auto svc = factory();
			svc->listen(p);
			ready.set_value(svc);   // published only after the port is bound + listening
			svc->run();             // the reactor loop (this thread owns it end to end)
		});
		service = fut.get();
		wait_until_listening(port);
	}

	~ServerFixture() {
		service->stop();                 // break_loop (async, thread-safe) -> run() returns
		if (thr.joinable()) { thr.join(); }
		service.reset();                 // join the Dispatcher's workers + destroy the loop
	}
};


int main() {
	std::printf("== http un-stallable load test ==\n");

	LoadHandler handler;

	// ---- A. Offload: a slow handler does not stall the loop -------------------
	// 8 workers; occupy 4 with slow requests, leaving 4 free. The reactor is never
	// blocked, so the many fast requests are read + dispatched immediately and run
	// on the free workers with low latency.
	double offload_fast_max = 0;
	{
		uint16_t port = find_free_port();
		ServerFixture fx([&handler] { return http::HttpService::create(handler, /*workers=*/8, /*queue_limit=*/256); }, port);

		std::vector<Result> slow;
		std::thread slow_t([&] { slow = fire(port, "/slow", 4); });
		std::this_thread::sleep_for(std::chrono::milliseconds(100));   // let the 4 slow handlers occupy 4 workers

		auto fast = fire(port, "/fast", 40);
		slow_t.join();

		offload_fast_max = max_latency(fast);
		std::printf("  [A] offload: 40 fast under 4 slow -> %d/40 got 200, max fast latency %.1f ms (slow=%d ms)\n",
		            count_status(fast, 200), offload_fast_max, SLOW_MS);
		CHECK(count_status(fast, 200) == 40, "all fast requests served (200)");
		CHECK(offload_fast_max < SLOW_MS / 2.0, "fast latency stays well under the slow handler's duration");
		CHECK(count_status(slow, 200) == 4, "the slow requests also complete (200)");
	}

	// ---- B. Inline control: the same slow handler DOES stall the loop ---------
	// No Dispatcher -> the slow handler runs on the reactor; one slow request
	// monopolizes the loop, so fast requests on other connections wait for it.
	{
		uint16_t port = find_free_port();
		ServerFixture fx([&handler] { return http::HttpService::create(handler); }, port);

		std::thread slow_t([&] { do_get(port, "/slow"); });
		std::this_thread::sleep_for(std::chrono::milliseconds(50));   // the reactor is now inside the slow handler

		auto fast = fire(port, "/fast", 20);
		slow_t.join();

		double inline_fast_max = max_latency(fast);
		std::printf("  [B] inline:  20 fast under 1 slow -> max fast latency %.1f ms\n", inline_fast_max);
		CHECK(inline_fast_max > SLOW_MS * 0.5, "inline fast latency is dominated by the blocked loop (stall detected)");
		CHECK(inline_fast_max > offload_fast_max * 2, "offload is materially faster than inline under the same slow load");
	}

	// ---- C. Backpressure: a saturated bounded queue sheds load as 503 ---------
	// 2 workers + queue of 2 = capacity 4 in-flight/queued. Fire 16 concurrent slow
	// requests; only ~4 are accepted, the rest are rejected with 503 immediately.
	{
		uint16_t port = find_free_port();
		ServerFixture fx([&handler] { return http::HttpService::create(handler, /*workers=*/2, /*queue_limit=*/2); }, port);

		auto res = fire(port, "/slow", 16);
		int n200 = count_status(res, 200);
		int n503 = count_status(res, 503);
		std::printf("  [C] backpressure: 16 slow @ cap 4 -> %d x 200, %d x 503\n", n200, n503);
		CHECK(n503 >= 1, "overload is shed as 503 (submit() returned false)");
		CHECK(n200 >= 1, "requests within capacity are still served (200)");
		CHECK(n200 + n503 == 16, "every request gets a definite answer (200 or 503)");
		CHECK(n200 <= 8, "accepted count is bounded by the pool + queue, not unbounded");
	}

	// ---- D. Per-route classification: a cheap route keeps the inline fast path -
	// 2 workers, both saturated by heavy /slow requests. /fast is classified cheap
	// (should_offload=false) -> it runs inline on the reactor and is served at once,
	// instead of queueing behind the busy workers (which is what a heavy route does).
	{
		ClassifiedHandler classified;
		uint16_t port = find_free_port();
		ServerFixture fx([&classified] { return http::HttpService::create(classified, /*workers=*/2, /*queue_limit=*/64); }, port);

		std::vector<Result> slow;
		std::thread slow_t([&] { slow = fire(port, "/slow", 2); });   // occupy both workers
		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		auto fast = fire(port, "/fast", 20);
		slow_t.join();

		double cheap_max = max_latency(fast);
		std::printf("  [D] classification: 20 cheap /fast under saturated workers -> %d/20 got 200, max %.1f ms\n",
		            count_status(fast, 200), cheap_max);
		CHECK(count_status(fast, 200) == 20, "cheap route served even with every worker busy");
		CHECK(cheap_max < SLOW_MS / 2.0, "cheap route runs inline (does not queue behind the busy pool)");
	}

	// ---- E. Stall watchdog: an inline blocking handler trips it ---------------
	// No Dispatcher -> /slow runs on the reactor and blocks it for 500 ms, well past
	// the 250 ms threshold, so the monitor thread observes the stall and fires.
	{
		std::atomic<int> stalls{0};
		uint16_t port = find_free_port();
		ServerFixture fx([&handler, &stalls] {
			auto svc = http::HttpService::create(handler);
			svc->watch_stalls(std::chrono::milliseconds(250), [&stalls](std::chrono::milliseconds) { stalls.fetch_add(1); });
			return svc;
		}, port);
		std::this_thread::sleep_for(std::chrono::milliseconds(60));   // let the loop pet healthily first

		do_get(port, "/slow");   // blocks the reactor ~500 ms
		std::this_thread::sleep_for(std::chrono::milliseconds(60));
		std::printf("  [E] watchdog (inline): a blocking handler fired the watchdog %d time(s)\n", stalls.load());
		CHECK(stalls.load() >= 1, "the watchdog observes a stalled (blocked) reactor");
	}

	// ---- F. Stall watchdog: offloaded work keeps the loop healthy -------------
	// With a Dispatcher the same /slow runs on a worker; the reactor keeps ticking,
	// so the watchdog stays silent -- exactly the property the offload model buys.
	{
		std::atomic<int> stalls{0};
		uint16_t port = find_free_port();
		ServerFixture fx([&handler, &stalls] {
			auto svc = http::HttpService::create(handler, /*workers=*/4, /*queue_limit=*/64);
			svc->watch_stalls(std::chrono::milliseconds(250), [&stalls](std::chrono::milliseconds) { stalls.fetch_add(1); });
			return svc;
		}, port);
		std::this_thread::sleep_for(std::chrono::milliseconds(60));

		fire(port, "/slow", 4);   // offloaded; the loop should stay responsive throughout
		std::this_thread::sleep_for(std::chrono::milliseconds(60));
		std::printf("  [F] watchdog (offload): healthy loop fired the watchdog %d time(s)\n", stalls.load());
		CHECK(stalls.load() == 0, "the watchdog stays silent while heavy work runs off-reactor");
	}

	// ---- G. Response compression: negotiate Accept-Encoding, round-trip --------
	// Compression enabled + offloaded (the CPU lands on a worker). A large body is
	// compressed to the client's advertised codec; small / unsupported / absent
	// Accept-Encoding are left identity.
	{
		const std::string& big = LoadHandler::big_body();
		uint16_t port = find_free_port();
		ServerFixture fx([&handler] {
			auto svc = http::HttpService::create(handler, /*workers=*/4, /*queue_limit=*/64);
			svc->enable_compression();
			return svc;
		}, port);

		Resp gz = do_request(port, "/big", "gzip");
		std::printf("  [G] compression: /big %zuB -> gzip %zuB, zstd negotiated, identity fallbacks\n",
		            big.size(), gz.body.size());
		CHECK(gz.status == 200 && gz.header("Content-Encoding") == "gzip", "gzip negotiated when advertised");
		CHECK(gz.header("Vary") == "Accept-Encoding", "Vary: Accept-Encoding is set on a compressed response");
		CHECK(gz.body.size() < big.size() && gunzip(gz.body) == big, "gzip body is smaller and round-trips");

		Resp zs = do_request(port, "/big", "zstd");
		CHECK(zs.header("Content-Encoding") == "zstd" && decompress_zstd(zs.body) == big, "zstd body round-trips");

		Resp both = do_request(port, "/big", "gzip, zstd");
		CHECK(both.header("Content-Encoding") == "zstd", "server prefers zstd when the client accepts both");

		Resp raw = do_request(port, "/big", "");
		CHECK(raw.header("Content-Encoding").empty() && raw.body == big, "no Accept-Encoding -> identity (uncompressed)");

		Resp br = do_request(port, "/big", "br");
		CHECK(br.header("Content-Encoding").empty() && br.body == big, "unsupported coding -> identity");

		Resp small = do_request(port, "/small", "gzip");
		CHECK(small.header("Content-Encoding").empty() && small.body == "tiny\n", "body below min_size is not compressed");
	}

	std::printf("\n%s (%d failures)\n", g_failures == 0 ? "PASS" : "FAIL", g_failures);
	return g_failures == 0 ? 0 : 1;
}
