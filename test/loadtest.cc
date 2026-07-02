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

static Resp do_request(uint16_t port, const std::string& path, const std::string& accept_encoding,
                       const std::string& if_none_match = "", const std::string& range = "") {
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
	if (!if_none_match.empty()) { req += "If-None-Match: " + if_none_match + "\r\n"; }
	if (!range.empty()) { req += "Range: " + range + "\r\n"; }
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

static std::string raw_inflate(std::string_view s) {
	std::string out;
	DeflateDecompressData d(s.data(), s.size(), /*gzip=*/false);
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

	// ---- H. Conditional requests: ETag / If-None-Match -> 304 ------------------
	// Conditional + compression enabled together, offloaded. A buffered GET gets a
	// weak ETag; a matching If-None-Match (or *) short-circuits to a bodyless 304,
	// before compression; a miss returns the body.
	{
		const std::string& big = LoadHandler::big_body();
		uint16_t port = find_free_port();
		ServerFixture fx([&handler] {
			auto svc = http::HttpService::create(handler, /*workers=*/4, /*queue_limit=*/64);
			svc->enable_compression();
			svc->enable_conditional();
			return svc;
		}, port);

		Resp first = do_request(port, "/big", "", "");
		std::string etag = first.header("ETag");
		std::printf("  [H] conditional: ETag %s -> 304 on match/star, 200 on miss, 304 skips compression\n", etag.c_str());
		CHECK(first.status == 200 && etag.rfind("W/\"", 0) == 0, "a buffered GET gets a weak ETag");

		Resp hit = do_request(port, "/big", "", etag);
		CHECK(hit.status == 304 && hit.body.empty(), "If-None-Match hit -> 304 with no body");
		CHECK(hit.header("Content-Length").empty() && hit.header("ETag") == etag, "304 has no Content-Length and echoes the ETag");

		Resp star = do_request(port, "/big", "", "*");
		CHECK(star.status == 304 && star.body.empty(), "If-None-Match: * -> 304");

		Resp miss = do_request(port, "/big", "", "\"deadbeefdeadbeef\"");
		CHECK(miss.status == 200 && miss.body == big, "If-None-Match miss -> 200 with body");

		Resp cond_gz = do_request(port, "/big", "gzip", etag);
		CHECK(cond_gz.status == 304 && cond_gz.body.empty() && cond_gz.header("Content-Encoding").empty(),
		      "a 304 short-circuits before compression (no body to encode)");
	}

	// ---- I. Range requests: Range -> 206 Partial Content -----------------------
	// Ranges enabled, offloaded. Prefix / open / suffix ranges return 206 with the
	// right slice + Content-Range; past-the-end -> 416; no Range -> 200 advertising
	// Accept-Ranges; a multi-range we don't handle -> the full 200.
	{
		const std::string& big = LoadHandler::big_body();
		const std::size_t total = big.size();
		uint16_t port = find_free_port();
		ServerFixture fx([&handler] {
			auto svc = http::HttpService::create(handler, /*workers=*/4, /*queue_limit=*/64);
			svc->enable_ranges();
			return svc;
		}, port);

		Resp pre = do_request(port, "/big", "", "", "bytes=0-9");
		std::printf("  [I] ranges: bytes=0-9 -> %d %s, len %zu; past-end -> 416; no-range -> 200+Accept-Ranges\n",
		            pre.status, pre.header("Content-Range").c_str(), pre.body.size());
		CHECK(pre.status == 206 && pre.body == big.substr(0, 10), "prefix range -> 206 with the right 10 bytes");
		CHECK(pre.header("Content-Range") == "bytes 0-9/" + std::to_string(total), "Content-Range names the slice + total");
		CHECK(pre.header("Accept-Ranges") == "bytes", "206 advertises Accept-Ranges");

		Resp open = do_request(port, "/big", "", "", "bytes=10-");
		CHECK(open.status == 206 && open.body == big.substr(10), "open range bytes=N- -> to end");

		Resp suffix = do_request(port, "/big", "", "", "bytes=-5");
		CHECK(suffix.status == 206 && suffix.body == big.substr(total - 5), "suffix range bytes=-N -> last N bytes");

		Resp unsat = do_request(port, "/big", "", "", "bytes=" + std::to_string(total + 10) + "-");
		CHECK(unsat.status == 416 && unsat.header("Content-Range") == "bytes */" + std::to_string(total), "past-the-end range -> 416");

		Resp norange = do_request(port, "/big", "", "", "");
		CHECK(norange.status == 200 && norange.body == big && norange.header("Accept-Ranges") == "bytes",
		      "no Range -> full 200 that advertises Accept-Ranges");

		Resp multi = do_request(port, "/big", "", "", "bytes=0-9,20-29");
		CHECK(multi.status == 200 && multi.body == big, "unhandled multi-range -> full 200");
	}

	// ---- J. SO_REUSEPORT: several reactors share one port ----------------------
	// Two independent HttpServices (each its own loop + pool, shared-nothing) bind the
	// SAME port with reuse_port=true. Without SO_REUSEPORT the second bind would fail
	// with EADDRINUSE; with it both listen and the kernel spreads connections across
	// them. This is the scaling primitive num_http_servers-style deployments rely on.
	{
		uint16_t port = find_free_port();
		auto reuseport_factory = [&handler] {
			auto svc = http::HttpService::create(handler, /*workers=*/4, /*queue_limit=*/64);
			http::BindOptions bind;
			bind.reuse_port = true;
			svc->set_bind_options(bind);
			return svc;
		};
		ServerFixture fx1(reuseport_factory, port);   // both bind the same port -- only
		ServerFixture fx2(reuseport_factory, port);   // possible because reuse_port is set

		auto res = fire(port, "/fast", 40);
		int n200 = count_status(res, 200);
		std::printf("  [J] reuse_port: 2 reactors on one port -> %d/40 served 200\n", n200);
		CHECK(n200 == 40, "every request is served with two reactors sharing the port (SO_REUSEPORT)");
	}

	// ---- K. Custom content-coding: an app registers "deflate" ------------------
	// The library ships zstd/gzip; an app adds a coding it doesn't (raw "deflate")
	// via CompressionOptions::add_coding, and the transport -- not the app -- then
	// negotiates and applies it. App codings are preferred over the built-ins, so a
	// client accepting "deflate, gzip" gets deflate.
	{
		const std::string& big = LoadHandler::big_body();
		uint16_t port = find_free_port();
		ServerFixture fx([&handler] {
			auto svc = http::HttpService::create(handler, /*workers=*/4, /*queue_limit=*/64);
			http::CompressionOptions opt;
			opt.add_coding("deflate", [](std::string_view body) {
				std::string out;
				DeflateCompressData c(body.data(), body.size(), /*gzip=*/false);
				for (auto it = c.begin(); it; ++it) { out.append(*it); }
				return out;
			});
			svc->enable_compression(opt);
			return svc;
		}, port);

		Resp df = do_request(port, "/big", "deflate");
		std::printf("  [K] custom coding: /big %zuB -> deflate %zuB, app coding preferred over gzip\n",
		            big.size(), df.body.size());
		CHECK(df.status == 200 && df.header("Content-Encoding") == "deflate", "the app-registered deflate coding is negotiated");
		CHECK(df.body.size() < big.size() && raw_inflate(df.body) == big, "deflate body is smaller and round-trips");

		Resp both = do_request(port, "/big", "deflate, gzip");
		CHECK(both.header("Content-Encoding") == "deflate", "an app coding is preferred over a built-in on a tie");

		Resp gz = do_request(port, "/big", "gzip");
		CHECK(gz.header("Content-Encoding") == "gzip" && gunzip(gz.body) == big, "the built-in gzip still works alongside the app coding");
	}

	// ---- L. Error mapping hook: app maps exceptions, transport owns the 500 -----
	// A handler throws; its on_error() maps a known exception to a chosen status, and
	// anything it doesn't map falls back to the connection's generic 500. This is how
	// error-to-status stays an application concern without the app wrapping every
	// handler in a try/catch. Runs on the offloaded path (a Dispatcher is configured).
	{
		struct NotThere {};   // an application exception type
		struct ErrHandler : http::HttpHandler {
			void handle(const http::Request& req, http::ResponseWriter& /*resp*/) override {
				if (req.path == "/missing") { throw NotThere{}; }
				throw std::runtime_error("boom");   // an exception the app does NOT map
			}
			void on_error(std::exception_ptr err, const http::Request& /*req*/, http::ResponseWriter& resp) override {
				try {
					std::rethrow_exception(err);
				} catch (const NotThere&) {
					resp.send(404, "Not Found\n");
				} catch (...) {
					// leave it: the connection's generic 500 backstop answers
				}
			}
		};
		ErrHandler eh;
		uint16_t port = find_free_port();
		ServerFixture fx([&eh] { return http::HttpService::create(eh, /*workers=*/2, /*queue_limit=*/16); }, port);

		Resp mapped = do_request(port, "/missing", "");
		Resp fallback = do_request(port, "/boom", "");
		std::printf("  [L] error hook: mapped -> %d, unmapped -> %d\n", mapped.status, fallback.status);
		CHECK(mapped.status == 404 && mapped.body == "Not Found\n", "on_error maps the app's exception to 404");
		CHECK(fallback.status == 500, "an exception the app doesn't map falls back to the generic 500");
	}

	std::printf("\n%s (%d failures)\n", g_failures == 0 ? "PASS" : "FAIL", g_failures);
	return g_failures == 0 ? 0 : 1;
}
