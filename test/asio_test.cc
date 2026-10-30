// asio_test -- validates the Asio-based Kronuz/http transport (http_asio.h) against
// the same properties the libev loadtest checks, driving the REAL protocol libs
// (router, RequestExtension seam, codings) over the Asio runtime.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "compressor_deflate.h"   // gunzip the compressed responses
#include "http_asio.h"
#include "http_router.h"

static constexpr int SLOW_MS = 500;

static std::string big_body() {
	std::string s;
	for (int i = 0; i < 2000; ++i) { s += "the quick brown fox jumps over the lazy dog "; }
	return s;
}

struct App : http::HttpHandler {
	http::Router router;
	std::string big = big_body();
	App() {
		router.route("GET", "/fast", [](http::Request&, http::ResponseWriter& w, const http::Params&) -> asio::awaitable<void> {
			w.send(200, "fast\n");
			co_return;
		});
		// Waits on a timer: the coroutine suspends and the loop serves other connections.
		router.route("GET", "/slow", [](http::Request&, http::ResponseWriter& w, const http::Params&) -> asio::awaitable<void> {
			asio::steady_timer timer(co_await asio::this_coro::executor, std::chrono::milliseconds(SLOW_MS));
			co_await timer.async_wait(asio::use_awaitable);
			w.send(200, "slow\n");
		});
		router.route("GET", "/big", [this](http::Request&, http::ResponseWriter& w, const http::Params&) -> asio::awaitable<void> {
			w.send(200, big, "text/plain");
			co_return;
		});
		router.route("GET", "/throw", [](http::Request&, http::ResponseWriter&, const http::Params&) -> asio::awaitable<void> {
			throw std::runtime_error("boom");
			co_return;
		});
		router.route("GET", "/remote", [](http::Request& req, http::ResponseWriter& w, const http::Params&) -> asio::awaitable<void> {
			w.send(200, req.remote);
			co_return;
		});
	}
	asio::awaitable<void> handle(http::Request& req, http::ResponseWriter& resp) override { co_await router.handle(req, resp); }
	void on_error(std::exception_ptr e, const http::Request&, http::ResponseWriter& resp) override {
		try { std::rethrow_exception(e); }
		catch (const std::exception& ex) { resp.send(500, std::string("mapped: ") + ex.what() + "\n"); }
	}
};

// ---- streaming request-body app: counts an NDJSON body chunk-by-chunk without ever
// buffering it, proving the gigabyte-safe path. The count lives in a ctx shared with
// Request::user_data, so handle() reads it back after the sink is gone.
struct BulkCtx { std::atomic<size_t> lines{0}; std::atomic<size_t> bytes{0}; };
struct CountSink : http::BodySink {
	std::shared_ptr<BulkCtx> ctx;
	explicit CountSink(std::shared_ptr<BulkCtx> c) : ctx(std::move(c)) {}
	void write(std::string_view chunk) override {
		ctx->bytes.fetch_add(chunk.size(), std::memory_order_relaxed);
		for (char c : chunk) { if (c == '\n') { ctx->lines.fetch_add(1, std::memory_order_relaxed); } }
	}
	void end() override {}
};
struct StreamApp : http::HttpHandler {
	asio::awaitable<void> handle(http::Request& req, http::ResponseWriter& resp) override {
		auto ctx = std::static_pointer_cast<BulkCtx>(req.user_data);
		size_t lines = ctx ? ctx->lines.load() : 0;
		// req.body must be EMPTY if the body streamed to the sink (never buffered).
		bool streamed = req.body.empty();
		resp.send(200, std::to_string(lines) + (streamed ? " streamed\n" : " buffered\n"));
		co_return;
	}
	std::unique_ptr<http::BodySink> on_request_body(http::Request& req) override {
		if (req.path == "/bulk") {
			auto ctx = std::make_shared<BulkCtx>();
			req.user_data = ctx;
			return std::make_unique<CountSink>(ctx);
		}
		return nullptr;   // every other route buffers into Request::body
	}
};

// ---- streamed body: a handler reads the body with co_await on the connection's loop
// (slowly), while the connection feeds it flow-controlled. Also serves a fast route to
// prove the loop stays free during a back-pressured stream, and records the threads its
// reads ran on.
struct ConcurrentStreamApp : http::HttpHandler {
	std::mutex mutex;
	std::set<std::thread::id> threads;
	std::atomic<bool> all_inside_run{true};
	bool wants_body_stream(const http::Request& r) override { return r.method == "POST" && r.path == "/restore"; }
	asio::awaitable<void> handle(http::Request& req, http::ResponseWriter& resp) override {
		if (req.path == "/fast") { resp.send(200, "fast\n"); co_return; }
		if (req.path == "/restore" && req.body_stream) {
			std::size_t total = 0;
			std::uint64_t sum = 0;
			std::string chunk;
			auto ex = co_await asio::this_coro::executor;
			for (;;) {
				asio::error_code ec;
				co_await req.body_stream->async_read(chunk, asio::redirect_error(asio::use_awaitable, ec));
				{ std::lock_guard<std::mutex> lk(mutex); threads.insert(std::this_thread::get_id()); }
				if (!reactor::inside_run()) { all_inside_run = false; }
				if (ec == asio::error::eof) { break; }
				if (ec) { resp.send(400, "body failed: " + ec.message() + "\n"); co_return; }
				total += chunk.size();
				for (unsigned char c : chunk) { sum += c; }
				asio::steady_timer slow(ex, std::chrono::milliseconds(2));   // a slow consumer
				co_await slow.async_wait(asio::use_awaitable);
			}
			// Over-read past the end: must complete with eof immediately, never wait (a parser
			// that flushes a trailing item then loops does exactly this).
			std::string extra;
			asio::error_code again;
			co_await req.body_stream->async_read(extra, asio::redirect_error(asio::use_awaitable, again));
			bool empty_body = req.body.empty();
			resp.send(200, std::to_string(total) + " " + std::to_string(sum) +
				(empty_body ? " empty" : " buffered") + (again == asio::error::eof ? " end\n" : " overread\n"));
			co_return;
		}
		resp.send(404, "nope\n");
	}
};

// ---- client ----
struct Resp { int status = 0; std::string body; double ms = 0; std::string headers; };

static Resp do_request(unsigned short port, const std::string& path, const std::string& accept_enc = "") {
	Resp r;
	auto t0 = std::chrono::steady_clock::now();
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) { return r; }
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	timeval tv{.tv_sec = 10, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return r; }
	std::string req = "GET " + path + " HTTP/1.1\r\nHost: x\r\n";
	if (!accept_enc.empty()) { req += "Accept-Encoding: " + accept_enc + "\r\n"; }
	req += "Connection: close\r\n\r\n";
	::send(fd, req.data(), req.size(), 0);
	std::string resp;
	char b[8192];
	for (;;) { ssize_t n = ::recv(fd, b, sizeof(b), 0); if (n <= 0) { break; } resp.append(b, static_cast<size_t>(n)); }
	::close(fd);
	if (resp.rfind("HTTP/1.1 ", 0) == 0) { r.status = std::atoi(resp.c_str() + 9); }
	auto bp = resp.find("\r\n\r\n");
	if (bp != std::string::npos) { r.headers = resp.substr(0, bp); r.body = resp.substr(bp + 4); }
	r.ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
	return r;
}

// POST a body with Content-Length, sending it in chunks so the server reads it
// incrementally (exercising the streaming intake). Returns the response.
static Resp do_post(unsigned short port, const std::string& path, const std::string& body) {
	Resp r;
	auto t0 = std::chrono::steady_clock::now();
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) { return r; }
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	timeval tv{.tv_sec = 15, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return r; }
	std::string hdr = "POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: " +
		std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n";
	::send(fd, hdr.data(), hdr.size(), 0);
	size_t off = 0;
	while (off < body.size()) {
		size_t n = std::min<size_t>(32 * 1024, body.size() - off);
		ssize_t s = ::send(fd, body.data() + off, n, 0);
		if (s <= 0) { break; }
		off += static_cast<size_t>(s);
	}
	std::string resp;
	char b[8192];
	for (;;) { ssize_t n = ::recv(fd, b, sizeof(b), 0); if (n <= 0) { break; } resp.append(b, static_cast<size_t>(n)); }
	::close(fd);
	if (resp.rfind("HTTP/1.1 ", 0) == 0) { r.status = std::atoi(resp.c_str() + 9); }
	auto bp = resp.find("\r\n\r\n");
	if (bp != std::string::npos) { r.headers = resp.substr(0, bp); r.body = resp.substr(bp + 4); }
	r.ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
	return r;
}

static std::vector<Resp> fire(unsigned short port, const std::string& path, int n) {
	std::vector<Resp> out(static_cast<size_t>(n));
	std::vector<std::thread> ts;
	for (int i = 0; i < n; ++i) { ts.emplace_back([&out, port, path, i] { out[static_cast<size_t>(i)] = do_request(port, path); }); }
	for (auto& t : ts) { t.join(); }
	return out;
}
static int count_status(const std::vector<Resp>& v, int s) { int c = 0; for (auto& r : v) { if (r.status == s) { ++c; } } return c; }
static double max_ms(const std::vector<Resp>& v) { double m = 0; for (auto& r : v) { if (r.status == 200) { m = std::max(m, r.ms); } } return m; }
static bool has_header(const std::string& hdrs, const std::string& name, const std::string& val) {
	std::string needle = name + ": " + val;
	std::string low = hdrs;
	for (auto& c : low) { c = static_cast<char>(::tolower(c)); }
	std::string nl = needle;
	for (auto& c : nl) { c = static_cast<char>(::tolower(c)); }
	return low.find(nl) != std::string::npos;
}

// Extract a header's value from the raw header block (case-insensitive name match).
static std::string header_value(const std::string& hdrs, const std::string& name) {
	std::string low = hdrs, ln = name;
	for (auto& c : low) { c = static_cast<char>(::tolower(c)); }
	for (auto& c : ln) { c = static_cast<char>(::tolower(c)); }
	auto p = low.find("\n" + ln + ":");
	size_t vs;
	if (p == std::string::npos) {
		if (low.compare(0, ln.size() + 1, ln + ":") == 0) { vs = ln.size() + 1; } else { return ""; }
	} else {
		vs = p + 1 + ln.size() + 1;
	}
	auto ve = hdrs.find("\r\n", vs);
	if (ve == std::string::npos) { ve = hdrs.size(); }
	std::string v = hdrs.substr(vs, ve - vs);
	size_t b = v.find_first_not_of(" \t");
	return b == std::string::npos ? "" : v.substr(b);
}

// GET with one arbitrary extra header line ("Name: value").
static Resp do_get_h(unsigned short port, const std::string& path, const std::string& extra) {
	Resp r;
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) { return r; }
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	timeval tv{.tv_sec = 10, .tv_usec = 0};
	::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return r; }
	std::string req = "GET " + path + " HTTP/1.1\r\nHost: x\r\n";
	if (!extra.empty()) { req += extra + "\r\n"; }
	req += "Connection: close\r\n\r\n";
	::send(fd, req.data(), req.size(), 0);
	std::string resp; char b[8192];
	for (;;) { ssize_t n = ::recv(fd, b, sizeof(b), 0); if (n <= 0) { break; } resp.append(b, static_cast<size_t>(n)); }
	::close(fd);
	if (resp.rfind("HTTP/1.1 ", 0) == 0) { r.status = std::atoi(resp.c_str() + 9); }
	auto bp = resp.find("\r\n\r\n");
	if (bp != std::string::npos) { r.headers = resp.substr(0, bp); r.body = resp.substr(bp + 4); }
	return r;
}

static unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}
static void wait_listen(unsigned short port) {
	for (int i = 0; i < 300; ++i) {
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		bool up = ::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0;
		::close(fd);
		if (up) { return; }
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}
static std::string gunzip(const std::string& s) {
	std::string out;
	DeflateDecompressData d(s.data(), s.size(), /*gzip=*/true);
	for (auto it = d.begin(); it; ++it) { out.append(*it); }
	return out;
}

static int g_fail = 0;
static void check(bool ok, const std::string& m) { std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str()); if (!ok) { ++g_fail; } }

int main() {
	std::printf("== Asio Kronuz/http transport test ==\n");
	{
		http::Request request;
		http::AsioResponseWriter writer(request, nullptr);
		check(!writer.response_started() && !writer.response_ended(), "fresh response is neither started nor ended");
		writer.status(200);
		check(writer.response_started() && !writer.response_ended(), "status does not complete response");
		writer.write("partial");
		check(!writer.response_ended(), "body write does not complete response");
		writer.end();
		check(writer.response_ended(), "successful end completes response");
	}
	App app;
	double offload_fast_max = 0;

	// [A] Handlers that wait suspend: fast stays low-latency under slow load, on ONE loop.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 256);
		svc.start(port); wait_listen(port);
		std::vector<Resp> slow;
		std::thread st([&] { slow = fire(port, "/slow", 4); });
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		auto fast = fire(port, "/fast", 40);
		st.join();
		offload_fast_max = max_ms(fast);
		std::printf("  [A] suspended: 40 fast under 4 slow -> %d/40 200, max fast %.1f ms\n", count_status(fast, 200), offload_fast_max);
		check(count_status(fast, 200) == 40, "all fast served");
		check(offload_fast_max < SLOW_MS / 2.0, "fast stays under the slow duration");
		check(count_status(slow, 200) == 4, "slow requests complete");
	}
	// [C] Backpressure: in-flight cap 4 -> excess sheds 503.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 4);
		svc.start(port); wait_listen(port);
		auto res = fire(port, "/slow", 16);
		int n200 = count_status(res, 200), n503 = count_status(res, 503);
		std::printf("  [C] backpressure: 16 slow @ cap 4 -> %d x 200, %d x 503\n", n200, n503);
		check(n200 >= 1 && n200 <= 8, "bounded admission");
		check(n503 >= 6, "excess shed as 503");
		check(n200 + n503 == 16, "every request answered");
	}
	// [D] N reactors on one port (SO_REUSEPORT).
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 3, 64);
		http::AsioBindOptions b; b.reuse_port = true; svc.set_bind_options(b);
		svc.start(port); wait_listen(port);
		auto res = fire(port, "/fast", 60);
		std::printf("  [D] 3 reactors on one port -> %d/60 served\n", count_status(res, 200));
		check(count_status(res, 200) == 60, "all served across shared-nothing reactors");
	}
	// [D2] N reactors WITHOUT SO_REUSEPORT -> the shared-acceptor path (portable; the
	// macOS/BSD default, where a second same-port bind is rejected). One acceptor binds
	// and fans connections out round-robin to all reactors.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 4, 64);
		http::AsioBindOptions b; b.reuse_port = false; svc.set_bind_options(b);
		svc.start(port); wait_listen(port);
		auto res = fire(port, "/fast", 60);
		std::printf("  [D2] 4 reactors, shared acceptor (no reuse_port) -> %d/60 served\n", count_status(res, 200));
		check(count_status(res, 200) == 60, "all served via the shared acceptor");
	}
	// [E] Keep-alive: three sequential requests on one connection.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.start(port); wait_listen(port);
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		timeval tv{.tv_sec = 5, .tv_usec = 0}; ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
		int ok = 0;
		for (int i = 0; i < 3; ++i) {
			std::string req = "GET /fast HTTP/1.1\r\nHost: x\r\n\r\n";   // keep-alive (no close)
			::send(fd, req.data(), req.size(), 0);
			char b[512]; ssize_t n = ::recv(fd, b, sizeof(b), 0);
			if (n > 0 && std::string(b, static_cast<size_t>(n)).find("200") != std::string::npos) { ++ok; }
		}
		::close(fd);
		std::printf("  [E] keep-alive: 3 requests on 1 connection -> %d/3 ok\n", ok);
		check(ok == 3, "keep-alive serves multiple requests per connection");
	}
	// [F] Compression: enable_compression + Accept-Encoding: gzip -> gzip round-trips.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.enable_compression();
		svc.start(port); wait_listen(port);
		auto gz = do_request(port, "/big", "gzip");
		std::printf("  [F] compression: /big -> %zuB, Content-Encoding present=%d\n", gz.body.size(), has_header(gz.headers, "content-encoding", "gzip"));
		check(gz.status == 200 && has_header(gz.headers, "content-encoding", "gzip"), "gzip negotiated");
		check(gz.body.size() < app.big.size() && gunzip(gz.body) == app.big, "gzip body smaller + round-trips");
		auto raw = do_request(port, "/big", "");
		check(raw.status == 200 && raw.body == app.big, "no Accept-Encoding -> identity");
	}
	// [G] Error hook.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.start(port); wait_listen(port);
		auto r = do_request(port, "/throw");
		std::printf("  [G] error hook: /throw -> %d\n", r.status);
		check(r.status == 500 && r.body == "mapped: boom\n", "on_error mapped the exception to 500");
		auto nf = do_request(port, "/nope");
		check(nf.status == 404, "unknown path -> 404 from the router");
		auto who = do_request(port, "/remote");
		check(who.status == 200 && who.body == "127.0.0.1", "Request::remote is the client's address");
	}
	// [H] Streaming request-body intake: a multi-MB NDJSON body streams to the app's
	// sink chunk-by-chunk (never buffered) -- the gigabyte-safe path. The body is far
	// larger than one read buffer, so it must span many reads; the app reports the line
	// count and that Request::body stayed empty (proof it was not accumulated).
	{
		unsigned short port = free_port();
		StreamApp sapp;
		http::HttpAsioService svc(sapp, 1, 64);
		svc.start(port); wait_listen(port);
		const int N = 200000;
		std::string body;
		body.reserve(N * 12);
		for (int i = 0; i < N; ++i) { body += "{\"n\":"; body += std::to_string(i); body += "}\n"; }
		auto r = do_post(port, "/bulk", body);
		std::printf("  [H] streaming: POST /bulk %zu bytes -> %d, %s", body.size(), r.status, r.body.c_str());
		check(r.status == 200, "streamed bulk body answered 200");
		check(r.body.find(std::to_string(N)) != std::string::npos, "every line counted through the sink");
		check(r.body.find("streamed") != std::string::npos, "body streamed to the sink, never buffered");
		// A small body on a non-opted route still buffers into Request::body (dual mode):
		// the sink is only returned for /bulk, so here handle() sees a non-empty body.
		auto b = do_post(port, "/buffered", "one\ntwo\n");
		check(b.status == 200 && b.body.find("buffered") != std::string::npos, "a non-opted route buffers into Request::body (dual mode)");
	}
	// [I] Conditional GET: enable_conditional -> a first GET gets a weak ETag; a repeat
	// with If-None-Match is answered 304 (bodyless).
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.enable_conditional();
		svc.start(port); wait_listen(port);
		auto first = do_request(port, "/big");
		std::string etag = header_value(first.headers, "ETag");
		std::printf("  [I] conditional: /big -> %d, ETag=%s\n", first.status, etag.c_str());
		check(first.status == 200 && !etag.empty() && etag.rfind("W/\"", 0) == 0, "first GET returns a weak ETag");
		auto again = do_get_h(port, "/big", "If-None-Match: " + etag);
		check(again.status == 304 && again.body.empty(), "If-None-Match on the same ETag -> 304, no body");
		auto stale = do_get_h(port, "/big", "If-None-Match: W/\"deadbeef\"");
		check(stale.status == 200 && !stale.body.empty(), "a non-matching ETag -> full 200");
	}
	// [J] Range: enable_ranges -> a byte Range is served 206 with Content-Range; an
	// unsatisfiable range is 416.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.enable_ranges();
		svc.start(port); wait_listen(port);
		auto full = do_request(port, "/big");
		size_t total = full.body.size();
		auto part = do_get_h(port, "/big", "Range: bytes=0-9");
		std::printf("  [J] range: bytes=0-9 -> %d, %zu bytes, %s\n", part.status, part.body.size(),
			header_value(part.headers, "Content-Range").c_str());
		check(part.status == 206 && part.body.size() == 10 && part.body == full.body.substr(0, 10), "bytes=0-9 -> 206 with the first 10 bytes");
		check(header_value(part.headers, "Content-Range") == "bytes 0-9/" + std::to_string(total), "Content-Range reflects the slice + total");
		check(has_header(full.headers, "Accept-Ranges", "bytes"), "a rangeable 200 advertises Accept-Ranges: bytes");
		auto bad = do_get_h(port, "/big", "Range: bytes=" + std::to_string(total + 100) + "-");
		check(bad.status == 416, "an unsatisfiable range -> 416");
	}
	// [K] Streamed body read with co_await: a slow-consuming handler + a large body.
	// Asserts no data loss under back-pressure (count + checksum) AND that the reactor
	// stays free (a fast request on another connection is served mid-stream).
	{
		unsigned short port = free_port();
		ConcurrentStreamApp capp;
		http::HttpAsioService svc(capp, 1, 64);
		svc.start(port); wait_listen(port);

		const std::size_t N = 2u * 1024 * 1024;   // 2 MB, >> the 8x64KB channel window
		std::string body;
		body.reserve(N);
		std::uint64_t expect_sum = 0;
		for (std::size_t i = 0; i < N; ++i) { char c = static_cast<char>(i & 0xff); body += c; expect_sum += static_cast<unsigned char>(c); }

		Resp restore;
		std::thread t([&] { restore = do_post(port, "/restore", body); });

		std::this_thread::sleep_for(std::chrono::milliseconds(60));   // let /restore get going + back-pressure
		auto t0 = std::chrono::steady_clock::now();
		auto fast = do_request(port, "/fast");
		double fast_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
		t.join();

		std::printf("  [K] concurrent stream: /restore -> %d %s| /fast %.1f ms mid-stream\n",
			restore.status, restore.body.c_str(), fast_ms);
		check(restore.status == 200, "streamed restore answered 200");
		check(restore.body.find(std::to_string(N)) != std::string::npos, "all bytes delivered (no loss under back-pressure)");
		check(restore.body.find(std::to_string(expect_sum)) != std::string::npos, "checksum matches (data intact + ordered)");
		check(restore.body.find("empty") != std::string::npos, "body streamed, request.body never buffered");
		check(restore.body.find("end") != std::string::npos, "over-reading past the end returns false (never blocks)");
		check(fast.status == 200 && fast_ms < 250.0, "reactor stays free: a fast request is served while a slow stream is back-pressured");
		{
			std::lock_guard<std::mutex> lk(capp.mutex);
			check(capp.threads.size() == 1 && capp.all_inside_run, "every body read resumed on the one loop thread (no worker)");
		}

		// A tiny body arrives in the SAME read as the headers (completes during header
		// parsing) -- the stream must still deliver it, not drop it as already-complete.
		std::string tiny = "hello world";
		std::uint64_t tiny_sum = 0;
		for (unsigned char c : tiny) { tiny_sum += c; }
		auto sm = do_post(port, "/restore", tiny);
		std::printf("  [K] tiny streamed body -> %d %s", sm.status, sm.body.c_str());
		check(sm.status == 200 && sm.body.find(std::to_string(tiny.size())) != std::string::npos
			&& sm.body.find(std::to_string(tiny_sum)) != std::string::npos,
			"a body arriving with the headers is streamed, not dropped");
	}
	// [L] Shutdown while a stream is mid-flight must not hang: a client sends headers +
	// a partial body (less than Content-Length) and holds; stop() cancels the body read,
	// so the handler waiting in async_read() sees an error and completes.
	{
		unsigned short port = free_port();
		ConcurrentStreamApp capp;
		auto* svc = new http::HttpAsioService(capp, 1, 64);
		svc->start(port); wait_listen(port);
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
		std::string hdr = "POST /restore HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\npartial-body-then-hold";
		::send(fd, hdr.data(), hdr.size(), 0);
		std::this_thread::sleep_for(std::chrono::milliseconds(80));   // let the handler block in read()

		std::atomic<bool> stopped{false};
		std::thread stopper([svc, &stopped] { svc->stop(); stopped.store(true); });
		for (int i = 0; i < 500 && !stopped.load(); ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(10)); }
		bool ok = stopped.load();
		std::printf("  [L] stop() mid-stream -> %s\n", ok ? "returned" : "HUNG");
		check(ok, "stop() returns while a stream is mid-flight (no shutdown deadlock)");
		if (ok) { stopper.join(); delete svc; } else { stopper.detach(); }   // leak on hang so the suite continues
		::close(fd);
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
