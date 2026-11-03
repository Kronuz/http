// coroutine_test -- Phase 7c L3: handlers are coroutines on the connection's loop.
// Each section proves one claim of docs/mechanisms/coroutine-handlers.md over real TCP:
//   [A] a suspended handler does not stall the loop, and a throw after a suspension is mapped
//   [B] BodyStream semantics (order, eof, error, bound, cancel)
//   [C] a disconnect cancels reads but never writes; EOF cancels; a pipelined byte ends the watch
//   [D] the in-flight gate answers 503 + Retry-After and counts the refusals
//   [E] the buffered-body cap answers 413 (Content-Length and chunked), streams are not capped
//   [F] shutdown: cancel, the response is still written, frames outlive the cancel, bounded by the grace
//   [G] pipelined and keep-alive requests are unaffected by the per-request watch
//   [H] a body cut short (FIN, half-close, RST, chunked) fails the reader, never eof, with the request cancelled
//   [I] a throwing cancel callback is dropped: the handler's frames outlive it, streamed and buffered
//   [T] the task loop (design T4): (b) budget demotion, (c) affinity after a std::thread resume,
//       (d) stop() with suspended handlers leaves no task frame, (e) task::cancelled after a
//       cancellation is 503 + close, (f) no checkpoint passes after a hang-up, (g) an owned
//       stream abandoned by task::cancelled writes no terminal chunk, (h) writer affinity
//   [X] the teardown phases across the service and an application's own scheduler

#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
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
#include "test_loop.h"

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
	void rst() { linger l{1, 0}; ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); close(); }
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
	std::atomic<int> reason{-1};           // the task::cancel_reason once cancelled
	std::atomic<bool> frame_ok{true};
	std::atomic<double> cancelled_after_ms{-1};
	std::atomic<int> end_kind{0};          // how a body read ended: 1 eof, 2 operation_aborted, 3 another error
	std::atomic<bool> cancelled_at_end{false};
	std::atomic<bool> cancelled_late{false};
	std::atomic<bool> release{false};      // the test lets a held handler finish
	std::atomic<int> checkpoints_after{-1};   // /hangup: checkpoints passed after the request was cancelled
	std::atomic<bool> on_loop{false};      // /thread: resumed on the loop thread
	clk::time_point t0{};
};
// Busy work of about `d`, between checkpoints.
static void spin(std::chrono::microseconds d) { for (auto end = clk::now() + d; clk::now() < end;) {} }
struct App : http::HttpHandler {
	std::mutex mutex;
	std::map<std::string, std::shared_ptr<Obs>> obs;
	std::atomic<bool> inside_run_ok{true};
	std::atomic<std::size_t> demoted_seen{0};   // the most /demoted saw queued on reactor 0
	task::asio_scheduler* away = nullptr;       // another loop's scheduler, for /affine-*
	http::HttpAsioService* service = nullptr;   // the service running it: its handlers sleep on its loops
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
	static task::task<bool> sleep_for(const http::Request& req, std::chrono::milliseconds d) { return ::sleep_for(req.scheduler().executor(), d); }
	bool wants_body_stream(const http::Request& r) override { return r.path == "/stream" || r.path == "/tbody" || r.path == "/throwcb-stream"; }
	std::unique_ptr<http::BodySink> on_request_body(http::Request& r) override {
		struct Null : http::BodySink { std::size_t n = 0; void write(std::string_view c) override { n += c.size(); } void end() override {} };
		return r.path == "/sink" ? std::make_unique<Null>() : nullptr;
	}
	void on_error(std::exception_ptr e, const http::Request&, http::ResponseWriter& w) override {
		// task::cancelled is left unanswered: the transport maps it (503 when the request was cancelled).
		try { std::rethrow_exception(e); } catch (const task::cancelled&) {} catch (const std::exception& x) { w.send(500, std::string("mapped: ") + x.what()); }
	}
	task::task<void> handle(http::Request& req, http::ResponseWriter& w) override {
		if (!reactor::inside_run()) { inside_run_ok = false; }
		const std::string id = param(req.query, "id");
		if (req.path == "/fast") { w.send(200, "fast"); co_return; }
		if (req.path == "/echo") { w.send(200, std::to_string(req.body.size())); co_return; }
		if (req.path == "/sink") { w.send(200, "sunk"); co_return; }
		if (req.path == "/slow") {   // suspends, then answers
			co_await sleep_for(req, 300ms);
			w.send(200, "slow");
			co_return;
		}
		if (req.path == "/throw-later") {   // throws after a suspension: the exception crosses it
			co_await sleep_for(req, 50ms);
			throw std::runtime_error("later");
		}
		if (req.path == "/hold") { co_await sleep_for(req, 1000ms); w.send(200, "held"); co_return; }
		if (req.path == "/huge") { w.send(200, std::string(64u << 20, 'x')); co_return; }
		if (req.path == "/stream") {   // reads the body with co_await; answers with the byte count
			std::size_t total = 0;
			std::string chunk;
			for (;;) {
				asio::error_code ec = co_await read_chunk(*req.body_stream, chunk);
				if (ec == asio::error::eof) { break; }
				if (ec) { w.send(400, "failed: " + ec.message()); co_return; }
				total += chunk.size();
			}
			w.send(200, "streamed " + std::to_string(total));
			co_return;
		}
		if (req.path == "/tbody") {   // reads a streamed body to its end and records how it ended
			auto o = get_obs(id);
			std::string chunk;
			for (;;) {
				asio::error_code ec = co_await read_chunk(*req.body_stream, chunk);
				if (!ec) { continue; }
				o->cancelled_at_end = req.cancel_token().requested();
				o->end_kind = ec == asio::error::eof ? 1 : ec == asio::error::operation_aborted ? 2 : 3;
				break;
			}
			o->finished = true;
			w.send(200, "ended");
			co_return;
		}
		if (req.path == "/throwcb" || req.path == "/throwcb-stream") {   // a cancel callback that throws
			auto o = get_obs(id);
			task::cancel_callback throws(req.cancel_token(), [] { throw std::runtime_error("callback throws"); });
			o->started = true;
			// Held until the test releases it (5 s at most), so a slow host cannot finish it early.
			for (auto end = clk::now() + 5s; !o->release.load() && clk::now() < end;) { co_await sleep_for(req, 5ms); }
			o->cancelled_late = req.cancel_token().requested() && !req.path.empty();   // touches the request after the cancel
			o->finished = true;
			w.send(200, "survived");
			co_return;
		}
		if (req.path == "/read" || req.path == "/write") {
			// /read stops at its next checkpoint once cancelled (a search); /write ignores the
			// cancellation and finishes (a PUT). Both then answer, and both touch the request
			// and the writer after the cancel: the frame must still be alive.
			auto o = get_obs(id);
			o->t0 = clk::now();
			const task::cancel_token token = req.cancel_token();
			task::cancel_callback record(token, [o](task::cancel_reason r) {
				o->reason = static_cast<int>(r);
				o->cancelled_after_ms = since_ms(o->t0);
			});
			o->started = true;
			const auto limit = std::chrono::milliseconds(std::atoi(param(req.query, "ms").c_str()));
			const auto until_t = clk::now() + limit;
			const bool reads = req.path == "/read";
			while (clk::now() < until_t && !(reads && token.requested())) { co_await sleep_for(req, 10ms); }
			co_await sleep_for(req, 100ms);
			o->frame_ok = !req.path.empty() && req.method == (reads ? "GET" : "PUT");
			w.send(200, std::string(reads ? "read:" : "write:") + (token.requested() ? "cancelled" : "completed"));
			o->finished = true;
			co_return;
		}
		if (req.path == "/budget") {   // T4 (b): a long computation in 200 us slices, demoted between them
			task::budget slice(200us, req.cancel_token());
			for (auto end = clk::now() + std::chrono::milliseconds(std::atoi(param(req.query, "ms").c_str())); clk::now() < end;) {
				spin(20us);
				co_await slice.checkpoint();
			}
			w.send(200, "budget");
			co_return;
		}
		if (req.path == "/demoted") {   // T4 (b): runs while /budget waits in the demoted queue
			const std::size_t n = service->reactor(0).scheduler().demoted();
			for (std::size_t seen = demoted_seen.load(); n > seen && !demoted_seen.compare_exchange_weak(seen, n);) {}
			w.send(200, std::to_string(n));
			co_return;
		}
		if (req.path == "/thread") {   // T4 (c): resumed by an event a std::thread sets
			auto o = get_obs(id);
			const auto loop = std::this_thread::get_id();
			task::event done;
			std::thread setter([&done] { std::this_thread::sleep_for(20ms); done.set(); });
			co_await done;
			o->on_loop = std::this_thread::get_id() == loop;
			setter.join();
			w.send(200, o->on_loop ? "loop" : "away");
			co_return;
		}
		if (req.path == "/suspend-read") {   // T4 (d): suspended in a read of a pipe nobody writes; the request's token ends it
			auto o = get_obs(id);
			int fds[2];
			if (::pipe(fds) != 0) { w.send(500, "pipe"); co_return; }
			asio::posix::stream_descriptor pipe(req.scheduler().executor(), fds[0]);
			char b[16];
			o->started = true;
			const auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
				[&](auto h) { pipe.async_read_some(asio::buffer(b), std::move(h)); }, req.cancel_token());
			(void)n;
			::close(fds[1]);
			o->finished = true;
			w.send(200, error == asio::error::operation_aborted ? "read ended" : "read " + error.message());
			co_return;
		}
		if (req.path == "/suspend-event") {   // T4 (d): suspended on an event the request's cancellation sets
			auto o = get_obs(id);
			task::event wake;
			task::cancel_callback set(req.cancel_token(), [&wake] { wake.set(); });
			o->started = true;
			co_await wake;
			o->finished = true;
			w.send(200, "event ended");
			co_return;
		}
		if (req.path == "/scheduler") {   // Request::scheduler(): the connection's reactor scheduler serves timers and task::deadline
			const bool same = &req.scheduler() == &service->reactor(0).scheduler();
			const bool timer = co_await sleep_for(req, 5ms);
			task::deadline<task::asio_scheduler> limit(req.scheduler(), clk::now() + 20ms);
			asio::steady_timer long_timer(req.scheduler().executor(), 30s);
			const auto t0 = clk::now();
			const asio::error_code error = co_await task::await_asio<void(asio::error_code)>(
				[&](auto h) { long_timer.async_wait(std::move(h)); }, limit.token());
			w.send(200, std::string(same ? "same," : "other,") + (timer ? "timer," : "no-timer,") +
				(limit.expired() && error && since_ms(t0) < 5000 ? "deadline" : "no-deadline"));
			co_return;
		}
		if (req.path == "/ignore") {   // T4 (d): a handler with no token, suspended on a long timer: only the phases end it
			auto o = get_obs(id);
			o->started = true;
			co_await sleep_for(req, 1h);
			w.send(200, "never");
			co_return;
		}
		if (req.path == "/cb-write") {   // T4 (h): the writer touched from a cancel callback on the loop thread (no current task)
			auto o = get_obs(id);
			task::event wake;
			task::cancel_callback set(req.cancel_token(), [&w, &wake] { w.set_header("X-Cancelled", "1"); wake.set(); });
			o->started = true;
			co_await wake;
			o->finished = true;
			w.send(200, "callback wrote");
			co_return;
		}
		if (req.path == "/checkpoint") {   // T4 (d), (e): a budget loop until the request is cancelled; task::cancelled escapes
			auto o = get_obs(id);
			task::budget slice(200us, req.cancel_token());
			o->started = true;
			for (auto end = clk::now() + 5s; clk::now() < end;) {
				spin(20us);
				co_await slice.checkpoint();
			}
			w.send(200, "never cancelled");
			co_return;
		}
		if (req.path == "/hangup") {   // T4 (f): no checkpoint passes once the request is cancelled
			auto o = get_obs(id);
			const task::cancel_token token = req.cancel_token();
			bool seen = false;
			int after = 0;
			task::cancel_callback note(token, [&seen] { seen = true; });
			task::budget slice(200us, token);
			o->started = true;
			try {
				for (auto end = clk::now() + 5s; clk::now() < end;) {
					spin(20us);
					co_await slice.checkpoint();
					if (seen) { ++after; }
				}
			} catch (const task::cancelled&) {
				o->checkpoints_after = after;
				o->finished = true;
				throw;
			}
			w.send(200, "never cancelled");
			co_return;
		}
		if (req.path == "/own-cancel") {   // T4 (e): task::cancelled from the handler's own source is any exception
			task::cancel_source mine;
			mine.request();
			task::budget slice(200us, mine.token());
			co_await slice.checkpoint();
			w.send(200, "not thrown");
			co_return;
		}
		if (req.path == "/owned-cancel") {   // T4 (g): an owned stream started, then task::cancelled escapes
			http::StreamOptions options;
			options.reply.deadline = clk::now() + 5s;
			auto reply = w.stream(std::move(options));
			http::Response head;
			head.status = 200;
			reply->start_chunked(head, true);
			auto body = std::make_shared<std::string>("partial");
			reply->publish({body, *body});
			throw task::cancelled{};
		}
		if (req.path == "/affine-child") {   // T4 (h): a child that moves away comes home at its end
			co_await [](task::asio_scheduler& other) -> task::task<void> { co_await task::schedule_on{task::scheduler_ref::of(other)}; }(*away);
			w.send(200, "home");
			co_return;
		}
		if (req.path == "/affine-body") {   // T4 (h): the handler's own body moves away, then writes: Debug asserts
			co_await task::schedule_on{task::scheduler_ref::of(*away)};
			w.send(200, "away");
			co_return;
		}
		if (req.path == "/affine-thread") {   // T4 (h): a raw thread (no task, not the loop) touches the writer: Debug asserts
			std::thread([&w] { w.set_header("X-Thread", "1"); }).join();
			w.send(200, "thread");
			co_return;
		}
		w.send(404, "nope");
	}
};

// ---- [B] BodyStream, driven directly on a private loop ----
static void body_stream_tests() {
	asio::io_context io;
	task::asio_scheduler scheduler(io.get_executor());
	const auto on_loop = task::scheduler_ref::of(scheduler);
	auto run = [&](task::task<void> t) {
		std::exception_ptr failure;
		task::spawn(on_loop, std::move(t), [&](std::exception_ptr e) { failure = e; });
		io.restart();
		io.run();
		if (failure) { std::rethrow_exception(failure); }
	};
	auto wait = [&](std::chrono::milliseconds d) -> task::task<void> {
		asio::steady_timer t(io, d);
		co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
	};
	using Ec = asio::error_code;
	{   // order, then a sticky eof
		http::BodyStream s(io.get_executor(), 4);
		std::vector<std::string> got; Ec last, again;
		run([&]() -> task::task<void> {
			auto producer = [&]() -> task::task<void> {
				co_await s.push("a", {}); co_await s.push("b", {}); co_await s.push("c", {}); s.finish();
			};   // a named closure: it outlives the task
			task::spawn(on_loop, producer());
			std::string c;
			for (;;) {
				Ec ec = co_await read_chunk(s, c);
				if (ec) { last = ec; break; }
				got.push_back(c);
			}
			again = co_await read_chunk(s, c);
		}());
		check(got == std::vector<std::string>({"a", "b", "c"}), "[B] chunks arrive in order");
		check(last == asio::error::eof && again == asio::error::eof, "[B] the end is eof, and it is sticky");
	}
	{   // fail(): buffered chunks first, then the error
		http::BodyStream s(io.get_executor(), 4);
		std::string first; Ec e1, e2;
		run([&]() -> task::task<void> {
			co_await s.push("kept", {});
			s.fail(asio::error::connection_reset);
			std::string c;
			e1 = co_await read_chunk(s, c); first = c;
			e2 = co_await read_chunk(s, c);
		}());
		check(!e1 && first == "kept" && e2 == asio::error::connection_reset, "[B] a failed body delivers what was buffered, then the error");
	}
	{   // bounded: a push waits while the channel is full, and abandon() releases it
		http::BodyStream s(io.get_executor(), 2);
		int pushed = 0; bool third = true;
		run([&]() -> task::task<void> {
			auto producer = [&]() -> task::task<void> {
				if (co_await s.push("1", {})) { ++pushed; }
				if (co_await s.push("2", {})) { ++pushed; }
				third = co_await s.push("3", {});   // blocks: capacity 2, nobody reads
			};   // a named closure: it outlives the task
			task::spawn(on_loop, producer());
			co_await wait(50ms);
			if (pushed != 2) { throw std::runtime_error("did not fill"); }
			s.abandon();
			co_await wait(20ms);
		}());
		check(pushed == 2 && !third, "[B] the channel is bounded (flow control), and abandon() releases a waiting push");
	}
	{   // a push waiting for room ends when its token is requested
		http::BodyStream s(io.get_executor(), 1);
		task::cancel_source stop;
		bool second = true;
		run([&]() -> task::task<void> {
			auto producer = [&]() -> task::task<void> {
				co_await s.push("1", stop.token());
				second = co_await s.push("2", stop.token());   // blocks: capacity 1, nobody reads
			};   // a named closure: it outlives the task
			task::spawn(on_loop, producer());
			co_await wait(20ms);
			stop.request();
			co_await wait(20ms);
		}());
		check(!second, "[B] a requested token ends a waiting push with false");
	}
	{   // a plain-lambda handler (what task::await_asio supplies) and cancellation
		http::BodyStream s(io.get_executor(), 2);
		Ec ec1 = asio::error::would_block, ec2 = asio::error::would_block;
		std::string chunk1, chunk2;
		s.async_read(chunk1, [&](Ec e) { ec1 = e; });
		io.restart(); io.poll();
		check(ec1 == asio::error::would_block, "[B] a read with nothing buffered stays pending");
		run([&]() -> task::task<void> { co_await s.push("x", {}); }());
		io.restart(); io.run();
		check(!ec1 && chunk1 == "x", "[B] a plain-lambda completion token works");
		asio::cancellation_signal cancel;
		s.async_read(chunk2, asio::bind_cancellation_slot(cancel.slot(), [&](Ec e) { ec2 = e; }));
		io.restart(); io.poll();
		cancel.emit(asio::cancellation_type::all);
		io.restart(); io.run();
		check(ec2 == asio::error::operation_aborted, "[B] a pending read is cancellable");
		run([&]() -> task::task<void> { co_await s.push("kept", {}); }());
		Ec ec3; std::string chunk3;
		s.async_read(chunk3, [&](Ec e) { ec3 = e; });
		io.restart(); io.run();
		check(!ec3 && chunk3 == "kept", "[B] a cancelled read loses nothing");
	}
	{   // Request::cancel_token: a late callback runs at once, and only the first cancel counts
		http::Request r;
		int calls = 0; task::cancel_reason seen = task::cancel_reason::none;
		r.cancel(http::disconnected);
		r.cancel(task::cancel_reason::shutdown);
		task::cancel_callback late(r.cancel_token(), [&](task::cancel_reason why) { ++calls; seen = why; });
		check(calls == 1 && seen == http::disconnected && r.cancel_token().reason() == http::disconnected,
			"[B] a late callback on the request's token runs at once with the first reason");
		r.clear();
		check(!r.cancel_token().requested(), "[B] clear() resets cancellation");
	}
}

// T4 (h), Debug: a handler body that moved to another scheduler and then writes fails the writer's
// affinity assertion. Runs in a child forked before any thread exists; true when it held.
static bool affinity_death_test(const char* path) {
	const pid_t child = ::fork();
	if (child == 0) {
		App app;
		asio::io_context other_io;
		auto guard = asio::make_work_guard(other_io);
		task::asio_scheduler other(other_io.get_executor());
		std::thread other_thread([&] { other_io.run(); });
		app.away = &other;
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		auto r = one(port, get(path, "Connection: close\r\n"));
		std::fflush(stdout);
		::_exit(r.status == 200 ? 0 : 3);   // NDEBUG: the assertion is compiled out and the reply arrives
	}
	int status = 0;
	::waitpid(child, &status, 0);
#ifdef NDEBUG
	return WIFEXITED(status) && WEXITSTATUS(status) == 0;
#else
	return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
#endif
}

int main() {
	std::printf("== coroutine handler tests ==\n");
	check(affinity_death_test("/affine-body"), "[T] (h) a handler body that moved away and then writes fails the writer's affinity check (Debug)");
	check(affinity_death_test("/affine-thread"), "[T] (h) a raw thread (no task, off the loop) that writes fails the writer's affinity check (Debug)");
	App app;

	// [A] A coroutine handler that waits suspends; the loop serves others. One reactor.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 256);
		app.service = &svc;
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
		app.service = &svc;
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
			check(o->reason == static_cast<int>(http::disconnected), "[C] a client close cancels the request with Disconnect");
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
			check(o->reason == static_cast<int>(http::disconnected), "[C] EOF cancels the request (the per-request rule)");
			check(r.status == 200 && r.body == "read:cancelled", "[C] a half-closed client still receives the response of a cancelled read");
		}
		{   // a write is never cancelled: it completes and its response is attempted
			auto o = app.get_obs("put");
			Conn c(port);
			c.send("PUT /write?id=put&ms=400 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(o->reason == static_cast<int>(http::disconnected), "[C] the write was told about the half-close");
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
			check(o->finished && o->reason == static_cast<int>(http::disconnected), "[C] the same close without the byte is seen");
		}
	}

	// [D] The in-flight gate: a request over the cap is refused with 503 + Retry-After: 1.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 2);
		app.service = &svc;
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
		Conn keep(port);   // a connection that stays open: its lease must end with each response, not with the socket
		for (int i = 0; i < 3; ++i) { keep.send(get("/fast")); keep.read_response(); }
		check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[D] a keep-alive connection holds no lease between requests");
	}

	// [E] The buffered-body cap.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
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
		app.service = &svc;
		svc.start(port); wait_listen(port);
		Conn c(port);
		c.send("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string((16u << 20) + 1) + "\r\nConnection: close\r\n\r\n");
		check(c.read_response().status == 413, "[E] the default cap is 16 MiB");
	}

	// [F] Shutdown.
	{   // a running request is cancelled (Shutdown), its frame outlives the cancel, and its response is written
		unsigned short port = free_port();
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, 64);
		app.service = svc.get();
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
		check(o->reason == static_cast<int>(task::cancel_reason::shutdown), "[F] shutdown cancels a running request with Shutdown");
		check(r.status == 200 && r.body == "read:cancelled" && o->frame_ok, "[F] the cancelled request's response is still written, from a live frame");
		check(ms < 2000.0, "[F] stop() returns promptly once the request completes");
	}
	{   // an idle keep-alive connection that already served a request does not hold stop()
		unsigned short port = free_port();
		auto svc = std::make_unique<http::HttpAsioService>(app, 2, 64);
		app.service = svc.get();
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
		app.service = svc.get();
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
		app.service = &svc;
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

	// [H] A body cut short never reads as a clean end: the reader gets an error other than eof,
	// and the request is already cancelled when its read returns.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		const auto head = [](const std::string& id) {
			return "POST /tbody?id=" + id + " HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(1000, 'a');
		};
		const auto outcome = [&](const std::string& id, const char* what) {
			auto o = app.get_obs(id);
			until([&] { return o->finished.load(); }, 3000);
			std::printf("  [H] %s: end_kind=%d cancelled=%d\n", what, o->end_kind.load(), int(o->cancelled_at_end.load()));
			check(o->finished && o->end_kind == 3 && o->cancelled_at_end,
				std::string("[H] ") + what + ": the reader gets an error (not eof) and the request is cancelled");
		};
		{ Conn c(port); c.send(head("trunc-fin")); std::this_thread::sleep_for(80ms); c.close(); outcome("trunc-fin", "Content-Length body, client close"); }
		{ Conn c(port); c.send(head("trunc-rst")); std::this_thread::sleep_for(80ms); c.rst(); outcome("trunc-rst", "Content-Length body, client reset"); }
		{ Conn c(port); c.send(head("trunc-half")); std::this_thread::sleep_for(80ms); c.shut_wr(); outcome("trunc-half", "Content-Length body, half-close"); }
		{
			Conn c(port);
			c.send("POST /tbody?id=trunc-chunked HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n");
			std::this_thread::sleep_for(80ms); c.close();
			outcome("trunc-chunked", "chunked body cut after a chunk");
		}
		{   // a body that does complete still ends with eof
			Conn c(port);
			c.send("POST /stream HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello");
			auto r = c.read_response();
			check(r.status == 200 && r.body == "streamed 5", "[H] a complete body still ends cleanly");
		}
	}

	// [I] A cancel callback that throws is dropped; the handler's frames end by completing.
	{
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		{   // streamed: the connection frame must not unwind under the running handler
			auto o = app.get_obs("sx");
			Conn c(port);
			c.send("POST /throwcb-stream?id=sx HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n" + std::string(100, 'a'));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.close();
			std::this_thread::sleep_for(120ms);
			check(!o->finished && svc.reactor(0).gate().inflight.load() == 1,
				"[I] streamed: after the throwing callback the connection still holds its request (the handler runs)");
			o->release = true;
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->cancelled_late, "[I] streamed: the handler finishes and its request was intact");
			check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[I] streamed: the request is released afterwards");
		}
		{   // buffered: the throw must not leave io_context::run()
			auto o = app.get_obs("bx");
			Conn c(port);
			c.send(get("/throwcb?id=bx"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(50ms);
			c.close();
			std::this_thread::sleep_for(50ms);
			o->release = true;
			until([&] { return o->finished.load(); }, 3000);
			check(o->finished && o->cancelled_late, "[I] buffered: the handler finishes after a throwing callback");
			check(one(port, get("/fast", "Connection: close\r\n")).body == "fast", "[I] buffered: the server keeps serving");
		}
		{   // a late registration that throws
			http::Request r;
			r.cancel(http::disconnected);
			bool threw = false;
			try { task::cancel_callback late(r.cancel_token(), [] { throw std::runtime_error("late"); }); } catch (...) { threw = true; }
			check(!threw && r.cancel_token().requested(), "[I] a late cancel callback that throws is dropped");
		}
	}

	// [T] The task loop (design T4 (b)-(h); (a) and the never-asking watch are handler_count_test).
	{   // (b) a budget loop is demoted between slices, the loop serves others meanwhile, and nothing stays queued after stop()
		unsigned short port = free_port();
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, 64);
		app.service = svc.get();
		svc->start(port); wait_listen(port);
		app.demoted_seen = 0;
		Resp budget;
		std::thread long_one([&] { budget = one(port, get("/budget?ms=300", "Connection: close\r\n")); });
		std::this_thread::sleep_for(30ms);
		Conn c(port);
		int answered = 0;
		for (auto end = clk::now() + 150ms; clk::now() < end;) {
			c.send(get("/demoted"));
			if (c.read_response().status == 200) { ++answered; }
		}
		long_one.join();
		std::printf("  [T] (b) %d requests answered during a 300 ms budget loop; most demoted seen %zu\n", answered, app.demoted_seen.load());
		check(budget.status == 200 && budget.body == "budget", "[T] (b) the budget loop answers");
		check(answered > 10 && app.demoted_seen.load() > 0, "[T] (b) the loop ran other requests while the budget loop waited demoted");
		c.close();
		svc->stop();
		check(svc->reactor(0).scheduler().demoted() == 0, "[T] (b) nothing is left in the demoted queue after stop()");
	}
	{   // (c) a handler resumed by a std::thread finishes on the loop thread
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		auto r = one(port, get("/thread?id=thread", "Connection: close\r\n"));
		check(r.status == 200 && r.body == "loop" && app.get_obs("thread")->on_loop, "[T] (c) a handler resumed from a std::thread finishes on the loop thread");
	}
	{   // (d) stop() with handlers suspended in a read, an event and a checkpoint: every task frame is gone
		const auto baseline = task::detail::live_frames();
		{
			unsigned short port = free_port();
			auto svc = std::make_unique<http::HttpAsioService>(app, 2, 64);
			app.service = svc.get();
			svc->start(port); wait_listen(port);
			Conn a(port), b(port), k(port);
			a.send(get("/suspend-read?id=d-read"));
			b.send(get("/suspend-event?id=d-event"));
			k.send(get("/checkpoint?id=d-check"));
			check(until([&] { return app.get_obs("d-read")->started && app.get_obs("d-event")->started && app.get_obs("d-check")->started; }),
				"[T] (d) the three handlers are suspended");
			std::this_thread::sleep_for(20ms);
			check(task::detail::live_frames() > baseline, "[T] (d) live frames are counted while handlers run");
			const auto t0 = clk::now();
			svc->stop();
			std::printf("  [T] (d) stop() with three suspended handlers -> %.0f ms\n", since_ms(t0));
			auto ra = a.read_response(), rb = b.read_response(), rk = k.read_response();
			check(ra.body == "read ended" && rb.body == "event ended" && rk.status == 503,
				"[T] (d) each ended by the request's cancellation and answered (" + ra.body + ", " + rb.body + ", " + std::to_string(rk.status) + ")");
			svc->close();
			svc->destroy_roots();
		}
		check(task::detail::live_frames() == baseline, "[T] (d) no task frame outlives the service (" + std::to_string(task::detail::live_frames() - baseline) + " left)");
	}
	{   // Request::scheduler(): a handler starts a timer and a task::deadline on the connection's scheduler
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		auto r = one(port, get("/scheduler", "Connection: close\r\n"));
		check(r.status == 200 && r.body == "same,timer,deadline", "[T] Request::scheduler() is the reactor's scheduler and serves a timer and a deadline (" + r.body + ")");
		http::Request detached;
		bool threw = false;
		try { (void)detached.scheduler(); } catch (const std::logic_error&) { threw = true; }
		check(threw, "[T] a request no transport serves has no scheduler");
	}
	{   // (h) a cancel callback on the loop thread, outside any task, touches the writer: the affinity check passes
		unsigned short port = free_port();
		auto svc = std::make_unique<http::HttpAsioService>(app, 1, 64);
		app.service = svc.get();
		svc->start(port); wait_listen(port);
		{   // the client hangs up: the watch's read completion runs the callback
			auto o = app.get_obs("h-hangup");
			Conn c(port);
			c.send(get("/cb-write?id=h-hangup"));
			until([&] { return o->started.load(); });
			c.shut_wr();
			auto r = c.read_response();
			check(r.status == 200 && r.body == "callback wrote" && header_value(r.head, "x-cancelled") == "1",
				"[T] (h) a cancel callback on the hang-up writes through the writer");
		}
		{   // the service stops: the reactor's abort walk runs the callback
			auto o = app.get_obs("h-stop");
			Conn c(port);
			c.send(get("/cb-write?id=h-stop"));
			until([&] { return o->started.load(); });
			svc->stop();
			auto r = c.read_response();
			check(r.status == 200 && r.body == "callback wrote" && header_value(r.head, "x-cancelled") == "1",
				"[T] (h) a cancel callback on shutdown writes through the writer");
		}
		svc->close();
		svc->destroy_roots();
	}
	{   // (d) a handler that ignores cancellation: stop() returns within the grace, the phases leave no frame
		const auto baseline = task::detail::live_frames();
		{
			unsigned short port = free_port();
			auto svc = std::make_unique<http::HttpAsioService>(app, 1, 64);
			app.service = svc.get();
			svc->set_shutdown_grace(200ms);
			svc->start(port); wait_listen(port);
			Conn c(port);
			c.send(get("/ignore?id=d-ignore"));
			check(until([&] { return app.get_obs("d-ignore")->started.load(); }), "[T] (d) the handler that ignores cancellation is suspended");
			std::this_thread::sleep_for(20ms);
			const auto t0 = clk::now();
			svc->stop();
			const double took = since_ms(t0);
			std::printf("  [T] (d) stop() with a handler that ignores cancellation (grace 200 ms) -> %.0f ms\n", took);
			check(took < 2 * 200 + 300, "[T] (d) stop() returns within twice the grace plus 300 ms (CI runners add jitter)");
			svc->close();
			svc->destroy_roots();
		}
		check(task::detail::live_frames() == baseline, "[T] (d) no task frame outlives the service after an ignored cancellation (" + std::to_string(task::detail::live_frames() - baseline) + " left)");
	}
	{   // (e) task::cancelled after the request's cancellation is 503 + close; from the handler's own source, 500
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		{
			auto o = app.get_obs("e");
			Conn c(port);
			c.send(get("/checkpoint?id=e"));
			until([&] { return o->started.load(); });
			c.shut_wr();   // a hang-up: the request is cancelled, and the half-closed client still reads
			auto r = c.read_response();
			check(r.status == 503 && header_value(r.head, "connection") == "close" && c.closed_by_peer(),
				"[T] (e) task::cancelled after a disconnect is 503 with Connection: close, then the connection closes");
			check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[T] (e) the request is released");
		}
		auto r = one(port, get("/own-cancel"));
		check(r.status == 500, "[T] (e) task::cancelled from the handler's own source is a 500 (" + std::to_string(r.status) + ")");
	}
	{   // (f) a hang-up during a budget loop: no checkpoint passes once the request is cancelled
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		auto o = app.get_obs("f");
		Conn c(port);
		c.send(get("/hangup?id=f"));
		until([&] { return o->started.load(); });
		std::this_thread::sleep_for(20ms);
		const auto t0 = clk::now();
		c.shut_wr();
		auto r = c.read_response();
		std::printf("  [T] (f) a hang-up during a budget loop -> 503 after %.1f ms, %d checkpoints after the cancel\n", since_ms(t0), o->checkpoints_after.load());
		check(r.status == 503 && o->checkpoints_after == 0, "[T] (f) the first checkpoint after the hang-up throws task::cancelled");
	}
	{   // (g) an owned stream started, then task::cancelled escapes: no terminal chunk, the request released
		unsigned short port = free_port();
		http::HttpAsioService svc(app, 1, 64);
		app.service = &svc;
		svc.start(port); wait_listen(port);
		Conn c(port);
		c.send(get("/owned-cancel"));
		while (c.fill()) {}
		check(c.rest.find("0\r\n\r\n") == std::string::npos, "[T] (g) the abandoned owned stream sends no terminal chunk (" + std::to_string(c.rest.size()) + " bytes)");
		check(until([&] { return svc.reactor(0).gate().inflight.load() == 0; }), "[T] (g) the request is released");
	}
	{   // (h) a child that moves to another scheduler comes home; the handler then writes from its loop
		asio::io_context other_io;
		auto guard = asio::make_work_guard(other_io);
		task::asio_scheduler other(other_io.get_executor());
		std::thread other_thread([&] { other_io.run(); });
		app.away = &other;
		{
			unsigned short port = free_port();
			http::HttpAsioService svc(app, 1, 64);
			app.service = &svc;
			svc.start(port); wait_listen(port);
			auto r = one(port, get("/affine-child", "Connection: close\r\n"));
			check(r.status == 200 && r.body == "home", "[T] (h) a child that schedule_on()s away comes home: the writer passes its check");
		}
		guard.reset();
		other_io.stop();
		other_thread.join();
		other.close();
		other.destroy_roots();
		app.away = nullptr;
	}

	// [X] Teardown phases across the service and an application's own scheduler (W2 D15).
	{
		const auto baseline = task::detail::live_frames();
		{
			asio::io_context app_io;
			auto guard = asio::make_work_guard(app_io);
			task::asio_scheduler app_scheduler(app_io.get_executor());
			std::thread app_thread([&] { app_io.run(); });
			{
				unsigned short port = free_port();
				http::HttpAsioService svc(app, 1, 64);
				app.service = &svc;
				svc.start(port); wait_listen(port);
				// A root of the application's scheduler suspended on a timer of the service's loop, and a
				// root of the service's loop suspended on an event nobody sets.
				asio::steady_timer far(svc.reactor(0).io(), 1h);
				task::event never;
				std::atomic<int> suspended{0};
				auto on_app = [&]() -> task::task<void> {
					++suspended;
					co_await task::await_asio<void(asio::error_code)>([&](auto h) { far.async_wait(std::move(h)); });
				};
				auto on_service = [&]() -> task::task<void> { ++suspended; co_await never; };
				task::spawn(task::scheduler_ref::of(app_scheduler), on_app(), [](std::exception_ptr) {});
				task::spawn(task::scheduler_ref::of(svc.reactor(0).scheduler()), on_service(), [](std::exception_ptr) {});
				check(until([&] { return suspended.load() == 2; }), "[X] both roots are suspended");
				check(one(port, get("/fast", "Connection: close\r\n")).body == "fast", "[X] the service serves");
				svc.stop();                                     // phase 1, on every loop
				guard.reset(); app_io.stop(); app_thread.join();
				svc.close();                                    // phase 2, on every scheduler
				app_scheduler.close();
				bool ran = false;
				auto late = [&]() -> task::task<void> { ran = true; co_return; };
				task::spawn(task::scheduler_ref::of(svc.reactor(0).scheduler()), late(), [](std::exception_ptr) {});
				check(!ran, "[X] a task spawned on a closed service scheduler is destroyed unrun");
				svc.destroy_roots();                            // phase 3, with every io_context alive
				app_scheduler.destroy_roots();
				check(task::detail::live_frames() == baseline, "[X] after destroy_roots() on both, no task frame is left (" + std::to_string(task::detail::live_frames() - baseline) + ")");
			}                                                   // phase 4: the service's timer dies before its io_context
		}
		{   // any order is safe: destroy_roots() alone runs stop() and close() first
			unsigned short port = free_port();
			http::HttpAsioService svc(app, 1, 64);
			app.service = &svc;
			svc.start(port); wait_listen(port);
			task::event never;
			auto parked = [&]() -> task::task<void> { co_await never; };
			task::spawn(task::scheduler_ref::of(svc.reactor(0).scheduler()), parked(), [](std::exception_ptr) {});
			std::this_thread::sleep_for(20ms);
			svc.destroy_roots();
			check(task::detail::live_frames() == baseline, "[X] destroy_roots() alone stops, closes and destroys the parked root");
		}
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
