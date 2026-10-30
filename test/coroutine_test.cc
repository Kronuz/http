// coroutine_test -- Phase 7c L3: handlers are coroutines on the connection's loop.
// Each section proves one claim of docs/mechanisms/coroutine-handlers.md over real TCP:
//   [A] a suspended handler does not stall the loop, and a throw after a suspension is mapped
//   [B] BodyStream semantics (order, eof, error, bound, cancel)
//   [C] a disconnect cancels reads but never writes; EOF cancels; a pipelined byte ends the watch
//   [D] the in-flight gate answers 503 + Retry-After and counts the refusals
//   [E] the buffered-body cap answers 413 (Content-Length and chunked), streams are not capped
//   [F] shutdown: cancel, the response is still written, frames outlive the cancel, bounded by the grace
//   [G] pipelined and keep-alive requests are unaffected by the per-request watch

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "http_asio.h"
#include "http_router.h"

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}
static double since_ms(clk::time_point t) { return std::chrono::duration<double, std::milli>(clk::now() - t).count(); }

// ---- a raw client ----
struct Resp { int status = 0; std::string head, body; bool complete = false; };
static std::string header_value(const std::string& head, const std::string& name) {
	std::string low = head, ln = name + ":";
	for (auto& c : low) { c = static_cast<char>(::tolower(c)); }
	for (auto& c : ln) { c = static_cast<char>(::tolower(c)); }
	auto p = low.find("\r\n" + ln);
	if (p == std::string::npos) { return ""; }
	p += 2 + ln.size();
	auto e = head.find("\r\n", p);
	std::string v = head.substr(p, e == std::string::npos ? std::string::npos : e - p);
	auto b = v.find_first_not_of(' ');
	return b == std::string::npos ? "" : v.substr(b);
}
struct Conn {
	int fd = -1;
	std::string rest;
	explicit Conn(unsigned short port, int timeout_s = 10) {
		fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		timeval tv{.tv_sec = timeout_s, .tv_usec = 0};
		::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); fd = -1; }
	}
	~Conn() { if (fd >= 0) { ::close(fd); } }
	Conn(const Conn&) = delete;
	void send(const std::string& s) { std::size_t off = 0; while (off < s.size()) { ssize_t n = ::send(fd, s.data() + off, s.size() - off, 0); if (n <= 0) { break; } off += static_cast<std::size_t>(n); } }
	void shut_wr() { ::shutdown(fd, SHUT_WR); }
	void close() { if (fd >= 0) { ::close(fd); fd = -1; } }
	bool fill() {
		char b[16384];
		ssize_t n = ::recv(fd, b, sizeof(b), 0);
		if (n <= 0) { return false; }
		rest.append(b, static_cast<std::size_t>(n));
		return true;
	}
	// One response: the head, then Content-Length bytes (or to EOF without one).
	Resp read_response() {
		Resp r;
		std::size_t end;
		while ((end = rest.find("\r\n\r\n")) == std::string::npos) { if (!fill()) { return r; } }
		r.head = rest.substr(0, end);
		if (r.head.rfind("HTTP/1.1 ", 0) == 0) { r.status = std::atoi(r.head.c_str() + 9); }
		std::string len = header_value(r.head, "content-length");
		rest.erase(0, end + 4);
		std::size_t want = len.empty() ? 0 : std::stoul(len);
		while (rest.size() < want) { if (!fill()) { return r; } }
		r.body = rest.substr(0, want);
		rest.erase(0, want);
		r.complete = true;
		return r;
	}
	bool closed_by_peer() { char b; return ::recv(fd, &b, 1, 0) == 0; }
};
static std::string get(const std::string& path, const std::string& extra = "") {
	return "GET " + path + " HTTP/1.1\r\nHost: x\r\n" + extra + "\r\n";
}
static Resp one(unsigned short port, const std::string& request) {
	Conn c(port);
	c.send(request);
	return c.read_response();
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
		Conn c(port, 1);
		if (c.fd >= 0) { return; }
		std::this_thread::sleep_for(5ms);
	}
}
template <class F> static bool until(F&& f, int ms = 3000) {
	for (int i = 0; i < ms / 5; ++i) { if (f()) { return true; } std::this_thread::sleep_for(5ms); }
	return f();
}

// ---- the application ----
struct Obs {
	std::atomic<bool> started{false}, finished{false};
	std::atomic<int> reason{-1};           // CancelReason once cancelled
	std::atomic<bool> frame_ok{true};
	std::atomic<double> cancelled_after_ms{-1};
	clk::time_point t0{};
};
struct App : http::HttpHandler {
	std::mutex mutex;
	std::map<std::string, std::shared_ptr<Obs>> obs;
	std::atomic<bool> inside_run_ok{true};
	std::shared_ptr<Obs> get_obs(const std::string& id) {
		std::lock_guard<std::mutex> lk(mutex);
		auto& o = obs[id];
		if (!o) { o = std::make_shared<Obs>(); }
		return o;
	}
	static std::string param(const std::string& query, const std::string& name) {
		auto p = query.find(name + "=");
		if (p == std::string::npos) { return ""; }
		p += name.size() + 1;
		return query.substr(p, query.find('&', p) - p);
	}
	static asio::awaitable<void> sleep_for(std::chrono::milliseconds d) {
		asio::steady_timer t(co_await asio::this_coro::executor, d);
		co_await t.async_wait(asio::use_awaitable);
	}
	bool wants_body_stream(const http::Request& r) override { return r.path == "/stream"; }
	std::unique_ptr<http::BodySink> on_request_body(http::Request& r) override {
		struct Null : http::BodySink { std::size_t n = 0; void write(std::string_view c) override { n += c.size(); } void end() override {} };
		return r.path == "/sink" ? std::make_unique<Null>() : nullptr;
	}
	void on_error(std::exception_ptr e, const http::Request&, http::ResponseWriter& w) override {
		try { std::rethrow_exception(e); } catch (const std::exception& x) { w.send(500, std::string("mapped: ") + x.what()); }
	}
	asio::awaitable<void> handle(http::Request& req, http::ResponseWriter& w) override {
		if (!reactor::inside_run()) { inside_run_ok = false; }
		const std::string id = param(req.query, "id");
		if (req.path == "/fast") { w.send(200, "fast"); co_return; }
		if (req.path == "/echo") { w.send(200, std::to_string(req.body.size())); co_return; }
		if (req.path == "/sink") { w.send(200, "sunk"); co_return; }
		if (req.path == "/slow") {   // suspends, then answers
			co_await sleep_for(300ms);
			w.send(200, "slow");
			co_return;
		}
		if (req.path == "/throw-later") {   // throws after a suspension: the exception crosses it
			co_await sleep_for(50ms);
			throw std::runtime_error("later");
		}
		if (req.path == "/hold") { co_await sleep_for(1000ms); w.send(200, "held"); co_return; }
		if (req.path == "/huge") { w.send(200, std::string(64u << 20, 'x')); co_return; }
		if (req.path == "/stream") {   // reads the body with co_await; answers with the byte count
			std::size_t total = 0;
			std::string chunk;
			for (;;) {
				asio::error_code ec;
				co_await req.body_stream->async_read(chunk, asio::redirect_error(asio::use_awaitable, ec));
				if (ec == asio::error::eof) { break; }
				if (ec) { w.send(400, "failed: " + ec.message()); co_return; }
				total += chunk.size();
			}
			w.send(200, "streamed " + std::to_string(total));
			co_return;
		}
		if (req.path == "/read" || req.path == "/write") {
			// /read stops at its next checkpoint once cancelled (a search); /write ignores the
			// cancellation and finishes (a PUT). Both then answer, and both touch the request
			// and the writer after the cancel: the frame must still be alive.
			auto o = get_obs(id);
			o->t0 = clk::now();
			req.on_cancel([o](http::CancelReason r) {
				o->reason = static_cast<int>(r);
				o->cancelled_after_ms = since_ms(o->t0);
			});
			o->started = true;
			const auto limit = std::chrono::milliseconds(std::atoi(param(req.query, "ms").c_str()));
			const auto until_t = clk::now() + limit;
			const bool reads = req.path == "/read";
			while (clk::now() < until_t && !(reads && req.cancelled())) { co_await sleep_for(10ms); }
			co_await sleep_for(100ms);
			o->frame_ok = !req.path.empty() && req.method == (reads ? "GET" : "PUT");
			w.send(200, std::string(reads ? "read:" : "write:") + (req.cancelled() ? "cancelled" : "completed"));
			o->finished = true;
			co_return;
		}
		w.send(404, "nope");
	}
};

// ---- [B] BodyStream, driven directly on a private loop ----
static void body_stream_tests() {
	asio::io_context io;
	auto run = [&](asio::awaitable<void> a) {
		std::exception_ptr failure;
		asio::co_spawn(io, std::move(a), [&](std::exception_ptr e) { failure = e; });
		io.restart();
		io.run();
		if (failure) { std::rethrow_exception(failure); }
	};
	using Ec = asio::error_code;
	{   // order, then a sticky eof
		http::BodyStream s(io.get_executor(), 4);
		std::vector<std::string> got; Ec last, again;
		run([&]() -> asio::awaitable<void> {
			asio::co_spawn(io, [&]() -> asio::awaitable<void> {
				co_await s.push("a"); co_await s.push("b"); co_await s.push("c"); s.finish();
			}, asio::detached);
			std::string c;
			for (;;) {
				Ec ec; co_await s.async_read(c, asio::redirect_error(asio::use_awaitable, ec));
				if (ec) { last = ec; break; }
				got.push_back(c);
			}
			co_await s.async_read(c, asio::redirect_error(asio::use_awaitable, again));
		}());
		check(got == std::vector<std::string>({"a", "b", "c"}), "[B] chunks arrive in order");
		check(last == asio::error::eof && again == asio::error::eof, "[B] the end is eof, and it is sticky");
	}
	{   // fail(): buffered chunks first, then the error
		http::BodyStream s(io.get_executor(), 4);
		std::string first; Ec e1, e2;
		run([&]() -> asio::awaitable<void> {
			co_await s.push("kept");
			s.fail(asio::error::connection_reset);
			std::string c;
			co_await s.async_read(c, asio::redirect_error(asio::use_awaitable, e1)); first = c;
			co_await s.async_read(c, asio::redirect_error(asio::use_awaitable, e2));
		}());
		check(!e1 && first == "kept" && e2 == asio::error::connection_reset, "[B] a failed body delivers what was buffered, then the error");
	}
	{   // bounded: a push waits while the channel is full, and abandon() releases it
		http::BodyStream s(io.get_executor(), 2);
		int pushed = 0; bool third = true;
		run([&]() -> asio::awaitable<void> {
			asio::co_spawn(io, [&]() -> asio::awaitable<void> {
				if (co_await s.push("1")) { ++pushed; }
				if (co_await s.push("2")) { ++pushed; }
				third = co_await s.push("3");   // blocks: capacity 2, nobody reads
			}, asio::detached);
			asio::steady_timer t(io, 50ms); co_await t.async_wait(asio::use_awaitable);
			if (pushed != 2) { throw std::runtime_error("did not fill"); }
			s.abandon();
			asio::steady_timer t2(io, 20ms); co_await t2.async_wait(asio::use_awaitable);
		}());
		check(pushed == 2 && !third, "[B] the channel is bounded (flow control), and abandon() releases a waiting push");
	}
	{   // a plain-lambda handler (what task::await_asio supplies) and cancellation
		http::BodyStream s(io.get_executor(), 2);
		Ec ec1 = asio::error::would_block, ec2 = asio::error::would_block;
		std::string chunk1, chunk2;
		s.async_read(chunk1, [&](Ec e) { ec1 = e; });
		io.restart(); io.poll();
		check(ec1 == asio::error::would_block, "[B] a read with nothing buffered stays pending");
		run([&]() -> asio::awaitable<void> { co_await s.push("x"); }());
		io.restart(); io.run();
		check(!ec1 && chunk1 == "x", "[B] a plain-lambda completion token works");
		asio::cancellation_signal cancel;
		s.async_read(chunk2, asio::bind_cancellation_slot(cancel.slot(), [&](Ec e) { ec2 = e; }));
		io.restart(); io.poll();
		cancel.emit(asio::cancellation_type::all);
		io.restart(); io.run();
		check(ec2 == asio::error::operation_aborted, "[B] a pending read is cancellable");
		run([&]() -> asio::awaitable<void> { co_await s.push("kept"); }());
		Ec ec3; std::string chunk3;
		s.async_read(chunk3, [&](Ec e) { ec3 = e; });
		io.restart(); io.run();
		check(!ec3 && chunk3 == "kept", "[B] a cancelled read loses nothing");
	}
	{   // Request::on_cancel runs a late registration at once, and only the first cancel counts
		http::Request r;
		int calls = 0; http::CancelReason seen = http::CancelReason::None;
		r.cancel(http::CancelReason::Disconnect);
		r.cancel(http::CancelReason::Shutdown);
		r.on_cancel([&](http::CancelReason why) { ++calls; seen = why; });
		check(calls == 1 && seen == http::CancelReason::Disconnect && r.cancel_reason() == http::CancelReason::Disconnect,
			"[B] a late on_cancel runs at once with the first reason");
		r.clear();
		check(!r.cancelled(), "[B] clear() resets cancellation");
	}
}

int main() {
	std::printf("== coroutine handler tests ==\n");
	App app;

	// [A] A coroutine handler that waits suspends; the loop serves others. One reactor.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 256);
		svc.start(port); wait_listen(port);
		Resp slow;
		std::thread st([&] { slow = one(port, get("/slow", "Connection: close\r\n")); });
		std::this_thread::sleep_for(60ms);
		std::vector<double> fast_ms;
		for (int i = 0; i < 20; ++i) {
			auto t0 = clk::now();
			auto r = one(port, get("/fast", "Connection: close\r\n"));
			if (r.status != 200) { fast_ms.push_back(1e9); } else { fast_ms.push_back(since_ms(t0)); }
		}
		st.join();
		const double worst = *std::max_element(fast_ms.begin(), fast_ms.end());
		std::printf("  [A] 20 fast under 1 suspended slow on one loop -> worst %.1f ms\n", worst);
		check(slow.status == 200 && slow.body == "slow", "[A] the suspended handler completes");
		check(worst < 150.0, "[A] the loop serves other connections while a handler is suspended");
		auto r = one(port, get("/throw-later", "Connection: close\r\n"));
		check(r.status == 500 && r.body == "mapped: later", "[A] a throw after a suspension reaches on_error");
		check(app.inside_run_ok.load(), "[A] handlers run inside the reactor's loop");
	}

	body_stream_tests();

	// [C] Disconnect cancels reads, not writes.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 256);
		svc.start(port); wait_listen(port);

		{   // a full close: the read is cancelled early; nobody is left to answer
			auto o = app.get_obs("close");
			Conn c(port);
			c.send(get("/read?id=close&ms=3000"));
			until([&] { return o->started.load(); });
			auto t0 = clk::now();
			c.close();
			until([&] { return o->finished.load(); }, 4000);
			std::printf("  [C] close: cancelled after %.0f ms, finished %.0f ms after the close\n", o->cancelled_after_ms.load(), since_ms(t0));
			check(o->reason == static_cast<int>(http::CancelReason::Disconnect), "[C] a client close cancels the request with Disconnect");
			check(o->finished && since_ms(t0) < 1500.0, "[C] the read stopped at its next checkpoint (not after its full 3 s)");
			check(o->frame_ok, "[C] the request frame was alive after the cancel");
		}
		{   // a half-close (EOF): reads are cancelled, the response is still written
			auto o = app.get_obs("half");
			Conn c(port);
			c.send(get("/read?id=half&ms=3000"));
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(o->reason == static_cast<int>(http::CancelReason::Disconnect), "[C] EOF cancels the request (the per-request rule)");
			check(r.status == 200 && r.body == "read:cancelled", "[C] a half-closed client still receives the response of a cancelled read");
		}
		{   // a write is never cancelled: it completes and its response is attempted
			auto o = app.get_obs("put");
			Conn c(port);
			c.send("PUT /write?id=put&ms=400 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(o->reason == static_cast<int>(http::CancelReason::Disconnect), "[C] the write was told about the half-close");
			check(r.status == 200 && r.body == "write:cancelled" && o->frame_ok, "[C] a write completes after the client half-closed, and its response arrives");
			auto o2 = app.get_obs("put2");
			Conn c2(port);
			c2.send("PUT /write?id=put2&ms=400 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
			until([&] { return o2->started.load(); });
			c2.close();
			until([&] { return o2->finished.load(); }, 3000);
			check(o2->finished.load(), "[C] a write runs to completion even when the client is gone");
		}
		{   // C17.2: a pipelined byte ends the watch for the rest of that request
			auto o = app.get_obs("pipe");
			Conn c(port);
			c.send(get("/read?id=pipe&ms=700"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);           // the watch is reading
			c.send("G");                                 // the next request's first byte arrives while it runs
			std::this_thread::sleep_for(50ms);           // and the watch consumes it
			c.close();
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->reason == -1, "[C] after a pipelined byte, a later close is not seen (the documented limit)");
		}
		{   // without the byte the same close is seen (the control for the case above)
			auto o = app.get_obs("nopipe");
			Conn c(port);
			c.send(get("/read?id=nopipe&ms=700"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.close();
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->reason == static_cast<int>(http::CancelReason::Disconnect), "[C] the same close without the byte is seen");
		}
	}

	// [D] The in-flight gate: a request over the cap is refused with 503 + Retry-After: 1.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 2);
		svc.start(port); wait_listen(port);
		std::vector<Resp> out(5);
		std::vector<std::thread> ts;
		for (int i = 0; i < 5; ++i) { ts.emplace_back([&, i] { out[static_cast<std::size_t>(i)] = one(port, get("/hold", "Connection: close\r\n")); }); }
		for (auto& t : ts) { t.join(); }
		int n200 = 0, n503 = 0; bool retry = true;
		for (auto& r : out) {
			if (r.status == 200) { ++n200; }
			if (r.status == 503) { ++n503; retry = retry && header_value(r.head, "retry-after") == "1"; }
		}
		std::printf("  [D] 5 simultaneous at a cap of 2 -> %d x 200, %d x 503, refused=%llu\n", n200, n503,
			static_cast<unsigned long long>(svc.reactor(0).gate().rejected.load()));
		check(n200 == 2 && n503 == 3, "[D] exactly the cap is admitted; the rest get 503");
		check(retry, "[D] every 503 carries Retry-After: 1");
		check(svc.reactor(0).gate().rejected == 3, "[D] the gate counts the refusals");
		check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[D] every lease is released once the responses settle");
		check(one(port, get("/fast", "Connection: close\r\n")).status == 200, "[D] admission resumes after the load drains");
	}

	// [E] The buffered-body cap.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.set_max_body(1024);
		svc.start(port); wait_listen(port);
		auto post = [&](const std::string& path, std::size_t advertised, const std::string& sent) {
			Conn c(port);
			c.send("POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(advertised) + "\r\nConnection: close\r\n\r\n" + sent);
			return c.read_response();
		};
		auto r = post("/echo", 5000, "only-ten-bytes");
		check(r.status == 413 && header_value(r.head, "connection") == "close", "[E] a Content-Length over the cap is refused at headers-complete (413), before the body is read");
		r = post("/echo", 1024, std::string(1024, 'a'));
		check(r.status == 200 && r.body == "1024", "[E] a body exactly at the cap is accepted");
		r = post("/echo", 1025, std::string(1025, 'a'));
		check(r.status == 413, "[E] one byte over is refused");
		{   // chunked: no Content-Length, so the cap applies as bytes arrive
			Conn c(port);
			std::string body;
			for (int i = 0; i < 3; ++i) { body += "200\r\n" + std::string(512, 'b') + "\r\n"; }
			c.send("POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" + body + "0\r\n\r\n");
			auto rc = c.read_response();
			check(rc.status == 413, "[E] a chunked body past the cap is refused as the bytes arrive");
		}
		{
			Conn c(port);
			c.send("POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n200\r\n" + std::string(512, 'c') + "\r\n0\r\n\r\n");
			auto rc = c.read_response();
			check(rc.status == 200 && rc.body == "512", "[E] a chunked body under the cap is accepted");
		}
		r = post("/stream", 100000, std::string(100000, 'z'));
		check(r.status == 200 && r.body == "streamed 100000", "[E] a streamed body is not capped");
		r = post("/sink", 100000, std::string(100000, 'z'));
		check(r.status == 200 && r.body == "sunk", "[E] a body pushed to a sink is not capped");
	}
	{   // the default cap is 16 MiB; 0 turns it off
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.start(port); wait_listen(port);
		Conn c(port);
		c.send("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string((16u << 20) + 1) + "\r\nConnection: close\r\n\r\n");
		check(c.read_response().status == 413, "[E] the default cap is 16 MiB");
	}

	// [F] Shutdown.
	{   // a running request is cancelled (Shutdown), its frame outlives the cancel, and its response is written
		unsigned short port = free_port();
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, 64);
		svc->start(port); wait_listen(port);
		auto o = app.get_obs("stop");
		Conn c(port);
		c.send(get("/read?id=stop&ms=5000"));
		until([&] { return o->started.load(); });
		auto t0 = clk::now();
		std::thread stopper([&] { svc->stop(); });
		auto r = c.read_response();
		stopper.join();
		const double ms = since_ms(t0);
		std::printf("  [F] stop() with a request in flight -> %.0f ms\n", ms);
		check(o->reason == static_cast<int>(http::CancelReason::Shutdown), "[F] shutdown cancels a running request with Shutdown");
		check(r.status == 200 && r.body == "read:cancelled" && o->frame_ok, "[F] the cancelled request's response is still written, from a live frame");
		check(ms < 2000.0, "[F] stop() returns promptly once the request completes");
	}
	{   // an idle keep-alive connection that already served a request does not hold stop()
		unsigned short port = free_port();
		auto svc = std::make_unique<http::HttpAsioService>(app, 2, 64);
		svc->start(port); wait_listen(port);
		std::vector<std::unique_ptr<Conn>> idle;
		for (int i = 0; i < 8; ++i) {
			idle.push_back(std::make_unique<Conn>(port));
			idle.back()->send(get("/fast"));
			if (idle.back()->read_response().status != 200) { check(false, "[F] warm-up request"); }
		}
		auto t0 = clk::now();
		svc->stop();
		const double ms = since_ms(t0);
		std::printf("  [F] stop() with 8 idle keep-alive connections -> %.0f ms\n", ms);
		check(ms < 1000.0, "[F] idle keep-alive connections (a pending watch read) are cancelled at once");
	}
	{   // a stalled writer is bounded by the grace
		unsigned short port = free_port();
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, 64);
		svc->set_shutdown_grace(300ms);
		svc->start(port); wait_listen(port);
		Conn c(port);
		int rcv = 4096; ::setsockopt(c.fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
		c.send(get("/huge"));
		std::this_thread::sleep_for(300ms);   // the 64 MiB response is stuck in the socket buffers: we never read
		auto t0 = clk::now();
		svc->stop();
		const double ms = since_ms(t0);
		std::printf("  [F] stop() with a stalled write, grace 300 ms -> %.0f ms\n", ms);
		check(ms >= 250.0, "[F] the stalled write is given the grace");
		check(ms < 3000.0, "[F] after the grace the socket is closed and stop() returns");
	}

	// [G] The per-request watch does not disturb pipelining or keep-alive.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		svc.start(port); wait_listen(port);
		Conn c(port);
		c.send(get("/fast") + get("/echo") + get("/fast", "Connection: close\r\n"));
		auto a = c.read_response(), b = c.read_response(), d = c.read_response();
		check(a.body == "fast" && b.body == "0" && d.body == "fast", "[G] three pipelined requests are answered in order");
		Conn k(port);
		int ok = 0;
		for (int i = 0; i < 200; ++i) {
			k.send(get("/fast"));
			if (k.read_response().body == "fast") { ++ok; }
		}
		check(ok == 200, "[G] 200 sequential keep-alive requests, each with the watch armed and released");
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
