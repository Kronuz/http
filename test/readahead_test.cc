// readahead_test -- the connection's disconnect watch reads ahead (ConnectionInput) and the
// reply is written in slices that span its buffers (write_with_progress). Both change how bytes
// move between the kernel and the parser, so every semantic that rides on those bytes is pinned
// here over real TCP:
//   [U] ConnectionInput on its own: the watch holds at most kReadAhead bytes outside the kernel,
//       hands them back in order whatever the output size, arms nothing until they are consumed,
//       and keeps the disconnect rules (EOF with nothing read fires; data ends the watch)
//   [L] the lazy watch: it reads nothing until the handler asks (CancelWatch::engage), and the
//       first question decides what happened meanwhile as the read would have; the Request hook
//       engages it once, from the first cancel_token()
//   [Q] the same over real TCP: a hang-up before the first question is seen by it, pipelined input
//       before it ends the watch, a handler that never asks has its pipelined successors answered
//   [N] a buffered reply sent at once as far as the socket takes it: replies to 4 MiB arrive whole
//   [P] a pipelined request that arrives inside the read-ahead (while the handler of the one
//       before runs) is parsed and answered, at every alignment of the 4096-byte window against
//       the request line, the headers, the CRLF CRLF, the body and the next request
//   [F] a flood of pipelined requests is answered in order, none lost or repeated
//   [D] disconnect: the watch's data ends it for that request; a half-close after the pipelined
//       request is seen by that request's own watch
//   [B] a client that disconnects in the middle of a buffered body never reaches the handler,
//       and the request it held is released
//   [S] request smuggling: ambiguous framing and a request hidden in a body are never executed
//       as a request, as the first request and as one that arrives inside the read-ahead; trailer
//       fields never become headers; nothing after a CONNECT, or after an HTTP/1.0 message with
//       Transfer-Encoding, is read as a request
//   [W] the reply's inactivity slices: every byte arrives, at every alignment of head and body
//       against the 64 KiB slice
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "http_asio.h"
#include "test_loop.h"

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

// ---- helpers ----
static unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
}
template <class F> static bool until(F&& f, int ms = 3000) {
	for (int i = 0; i < ms / 2; ++i) { if (f()) { return true; } std::this_thread::sleep_for(2ms); }
	return f();
}
static std::string pattern(std::size_t n, unsigned seed = 0) {
	std::string s(n, '\0');
	for (std::size_t i = 0; i < n; ++i) { s[i] = static_cast<char>('a' + (i * 7 + seed) % 26); }
	return s;
}

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
		for (int i = 0; i < 400; ++i) {
			fd = ::socket(AF_INET, SOCK_STREAM, 0);
			sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
			::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
			timeval tv{.tv_sec = timeout_s, .tv_usec = 0};
			::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
			if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0) { return; }
			::close(fd); fd = -1;
			std::this_thread::sleep_for(5ms);
		}
	}
	~Conn() { if (fd >= 0) { ::close(fd); } }
	Conn(const Conn&) = delete;
	void send(const std::string& s) {
		std::size_t off = 0;
		while (off < s.size()) {
			ssize_t n = ::send(fd, s.data() + off, s.size() - off, 0);
			if (n <= 0) { break; }
			off += static_cast<std::size_t>(n);
		}
	}
	void shut_wr() { ::shutdown(fd, SHUT_WR); }
	void close() { if (fd >= 0) { ::close(fd); fd = -1; } }
	bool fill() {
		char b[16384];
		ssize_t n = ::recv(fd, b, sizeof(b), 0);
		if (n <= 0) { return false; }
		rest.append(b, static_cast<std::size_t>(n));
		return true;
	}
	// One response: the head, then Content-Length bytes.
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
	// True when the peer closes without sending anything more (after what was already read).
	bool ends_silently() { return rest.empty() && !fill(); }
};

static std::string get(const std::string& path, const std::string& extra = "") {
	return "GET " + path + " HTTP/1.1\r\nHost: x\r\n" + extra + "\r\n";
}
static std::string post(const std::string& path, const std::string& body, const std::string& extra = "") {
	return "POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(body.size()) + "\r\n" + extra + "\r\n" + body;
}

// ---- the application ----
struct Obs {
	std::atomic<bool> started{false}, finished{false}, release{false};
	std::atomic<int> reason{-1};   // the task::cancel_reason once cancelled
	std::atomic<bool> at_once{false};   // /lazy: the callback ran as it was registered
};
struct App : http::HttpHandler {
	std::mutex mutex;
	std::map<std::string, std::shared_ptr<Obs>> obs;
	std::map<std::string, int> hits;   // requests the handler ran, by path
	http::HttpAsioService* service = nullptr;   // the service running it: handlers wait on its loop
	std::shared_ptr<Obs> get_obs(const std::string& id) {
		std::lock_guard<std::mutex> lk(mutex);
		auto& o = obs[id];
		if (!o) { o = std::make_shared<Obs>(); }
		return o;
	}
	int hit_count(const std::string& path) { std::lock_guard<std::mutex> lk(mutex); return hits[path]; }
	static std::string param(const std::string& query, const std::string& name) {
		auto p = query.find(name + "=");
		if (p == std::string::npos) { return ""; }
		p += name.size() + 1;
		return query.substr(p, query.find('&', p) - p);
	}
	task::task<void> handle(http::Request& req, http::ResponseWriter& w) override {
		{ std::lock_guard<std::mutex> lk(mutex); ++hits[req.path]; }
		if (req.path == "/echo") { w.send(200, req.body); co_return; }
		if (req.path == "/fast") { w.send(200, "fast"); co_return; }
		if (req.path == "/who") {   // the identity headers the handler sees, and the trailers apart
			std::string out = "x-user=" + std::string(req.header("X-User")) + ";xff=" + std::string(req.header("X-Forwarded-For")) +
				";headers=" + std::to_string(req.headers.size()) + ";trailers=";
			for (const auto& [k, v] : req.trailers) { out += k + ":" + v + ","; }
			w.send(200, out);
			co_return;
		}
		if (req.path == "/body") {   // a body of the size the query names, from a known pattern
			w.send(200, pattern(std::stoul(param(req.query, "n")), 5));
			co_return;
		}
		if (req.path == "/lazy") {   // waits for the test without asking about cancellation, then asks (or never does)
			auto o = get_obs(param(req.query, "id"));
			o->started = true;
			const auto loop = req.scheduler().executor();
			for (auto end = clk::now() + 5s; !o->release.load() && clk::now() < end;) { co_await sleep_for(loop, 2ms); }
			const std::string ask = param(req.query, "ask");
			if (ask == "cb") {
				task::cancel_callback record(req.cancel_token(), [o](task::cancel_reason r) { o->reason = static_cast<int>(r); });
				o->at_once = o->reason.load() != -1;
			} else if (ask == "poll") {
				const task::cancel_token token = req.cancel_token();
				o->reason = token.requested() ? static_cast<int>(token.reason()) : 0;
			}
			o->finished = true;
			w.send(200, "lazy");
			co_return;
		}
		if (req.path == "/hold") {   // runs until the test releases it, noting how a cancel reached it
			auto o = get_obs(param(req.query, "id"));
			task::cancel_callback record(req.cancel_token(), [o](task::cancel_reason r) { o->reason = static_cast<int>(r); });
			o->started = true;
			const auto loop = req.scheduler().executor();
			for (auto end = clk::now() + 5s; !o->release.load() && clk::now() < end;) { co_await sleep_for(loop, 2ms); }
			o->finished = true;
			w.send(200, "held");
			co_return;
		}
		w.send(404, "nope");
	}
};

// ---- [U] ConnectionInput on its own ----
// (-DREADAHEAD_TEST_INTEGRATION_ONLY builds [P]..[W] without this section, so they also run
// against a header that has no read-ahead: they pin the behavior the watch must keep.)
#ifndef READAHEAD_TEST_INTEGRATION_ONLY
using Socket = asio::ip::tcp::socket;
using Input = http::detail::ConnectionInput<Socket>;
static int pending_bytes(int fd) { int n = 0; ::ioctl(fd, FIONREAD, &n); return n; }

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
static Read read_once(Pair& p, const std::shared_ptr<Input>& in, std::size_t max) {
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

// The lazy watch (a plain TCP socket): it reads nothing until engage(), and engage() decides
// what happened meanwhile as the read would have.
static void lazy_watch_tests() {
	static_assert(Input::kLazyWatch, "a plain TCP socket's watch waits for engage()");
	{   // nothing is read while nobody asks; asking ends the watch on data and leaves the data in the kernel
		Pair p;
		const std::string data = pattern(10000);
		p.send(data);
		check(p.arrived(10000), "[L] 10000 bytes reached the server's receive queue");
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, /*lazy=*/true);
		p.settle();
		check(pending_bytes(p.server.native_handle()) == 10000 && fired == 0, "[L] a lazy watch reads nothing before engage()");
		in->engage();
		p.settle();
		check(pending_bytes(p.server.native_handle()) == 10000 && fired == 0, "[L] engage() with data waiting ends the watch, reads nothing, fires nothing");
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0, "[L] and a later EOF does not fire the ended watch");
		auto a = read_once(p, in, 65536), b = read_once(p, in, 65536), c = read_once(p, in, 65536);
		check(a.data + b.data + c.data == data, "[L] the bytes are read from the socket, in order");
	}
	{   // EOF before anyone asked: engage() fires at once, before it returns
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(until([&] { char b; return ::recv(p.server.native_handle(), &b, 1, MSG_PEEK | MSG_DONTWAIT) == 0; }), "[L] the EOF reached the server");
		check(fired == 0, "[L] an EOF nobody asked about fires nothing");
		in->engage();
		check(fired == 1, "[L] engage() after the EOF fires the disconnect before it returns");
		in->engage();
		p.settle();
		check(fired == 1, "[L] once");
		auto r = read_once(p, in, 100);
		check(r.done && r.error == asio::error::eof, "[L] and the EOF is read afterwards");
	}
	{   // a reset before anyone asked is a disconnect too
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		linger l{1, 0}; ::setsockopt(p.client, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(p.client); p.client = -1;
		check(until([&] { char b; return ::recv(p.server.native_handle(), &b, 1, MSG_PEEK | MSG_DONTWAIT) < 0 && errno != EAGAIN && errno != EWOULDBLOCK; }), "[L] the reset reached the server");
		in->engage();
		check(fired == 1, "[L] engage() after a reset fires the disconnect");
	}
	{   // asked before anything happened: from then on it is the read-ahead watch
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		in->engage();
		p.settle();
		check(fired == 0, "[L] engage() with nothing waiting fires nothing");
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 1, "[L] and the read it armed sees the EOF that follows");
	}
	{
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		in->engage();
		p.settle();
		const std::string data = pattern(10000, 3);
		p.send(data);
		check(p.arrived(10000 - static_cast<int>(Input::kReadAhead)), "[L] 10000 bytes sent after engage()");
		p.settle();
		check(pending_bytes(p.server.native_handle()) == 10000 - static_cast<int>(Input::kReadAhead) && fired == 0,
			"[L] the engaged watch reads ahead kReadAhead bytes, and the data ends it");
		auto a = read_once(p, in, 65536), b = read_once(p, in, 65536);
		check(a.data + b.data == data, "[L] and the stream is intact");
	}
	{   // unwatched (the handler returned): engage() does nothing
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; }, true);
		in->unwatch();
		::shutdown(p.client, SHUT_WR);
		p.settle();
		in->engage();
		p.settle();
		check(fired == 0, "[L] engage() after unwatch() neither fires nor reads");
		auto r = read_once(p, in, 100);
		check(r.done && r.error == asio::error::eof, "[L] the EOF stays for the next read");
	}
	// Request: the first cancel_token() engages the watch, once.
	struct Probe : http::CancelWatch {
		http::Request* request = nullptr;
		bool gone = false;
		int engaged = 0;
		void engage() noexcept override { ++engaged; if (gone) { request->cancel(http::disconnected); } }
	};
	{
		http::Request r;
		Probe w; w.request = &r;
		r.set_cancel_watch(&w);
		check(w.engaged == 0, "[L] setting the watch engages nothing");
		check(!r.cancel_token().requested() && w.engaged == 1, "[L] cancel_token() engages the watch");
		(void)r.cancel_token(); (void)r.cancel_token();
		check(w.engaged == 1, "[L] once");
	}
	for (int how = 0; how < 3; ++how) {
		http::Request r;
		Probe w; w.request = &r; w.gone = true;
		r.set_cancel_watch(&w);
		int calls = 0;
		bool seen = false;
		if (how == 0) { seen = r.cancel_token().requested(); }
		if (how == 1) { seen = r.cancel_token().reason() == http::disconnected; }
		if (how == 2) { task::cancel_callback cb(r.cancel_token(), [&](task::cancel_reason why) { ++calls; seen = why == http::disconnected; }); }
		check(seen && w.engaged == 1 && calls == (how == 2 ? 1 : 0),
			std::string("[L] a hang-up engage() finds is seen by the token that engaged it: ") + (how == 0 ? "requested()" : how == 1 ? "reason()" : "a callback on it runs once"));
	}
	{
		http::Request r;
		Probe w; w.request = &r;
		r.set_cancel_watch(&w);
		r.cancel(task::cancel_reason::shutdown);
		check(r.cancel_token().requested() && w.engaged == 0, "[L] a request already cancelled engages nothing");
		http::Request q;
		q.set_cancel_watch(&w);
		q.clear();
		(void)q.cancel_token();
		check(w.engaged == 0, "[L] clear() drops the watch");
		q.set_cancel_watch(&w);
		q.set_cancel_watch(nullptr);
		(void)q.cancel_token();
		check(w.engaged == 0, "[L] and so does set_cancel_watch(nullptr)");
	}
}

static void input_tests() {
	// Bounded and ordered: 10000 bytes wait in the kernel; the watch takes 4096 of them and no more.
	{
		Pair p;
		const std::string data = pattern(10000);
		p.send(data);
		check(p.arrived(10000), "[U] the 10000 bytes reached the server's receive queue");
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; });
		p.settle();
		const int left = pending_bytes(p.server.native_handle());
		check(left == 10000 - static_cast<int>(Input::kReadAhead), "[U] the watch read ahead exactly kReadAhead bytes (" + std::to_string(left) + " left in the kernel)");
		check(fired == 0, "[U] data ends the watch without a disconnect");
		in->watch([&] { ++fired; });   // nothing more is armed while the bytes wait
		p.settle();
		check(pending_bytes(p.server.native_handle()) == left && fired == 0, "[U] no further read-ahead is armed until the bytes are consumed");
		auto a = read_once(p, in, 65536);
		check(a.done && !a.error && a.data == data.substr(0, 4096), "[U] read() returns the read-ahead bytes first, in order");
		auto b = read_once(p, in, 65536);
		check(b.done && !b.error && b.data == data.substr(4096), "[U] the next read() continues from the socket without a gap or a repeat");
	}
	// A small output takes the read-ahead in pieces, in order, and the watch reads nothing new meanwhile.
	{
		Pair p;
		const std::string first = pattern(3000, 1), more = pattern(500, 2);
		p.send(first);
		check(p.arrived(3000), "[U] 3000 bytes arrived");
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {});
		p.settle();
		p.send(more);
		check(p.arrived(500), "[U] 500 more bytes arrived behind the read-ahead");
		std::string got;
		bool whole = true;
		for (int i = 0; i < 4; ++i) {
			in->watch([] {});   // re-arming must not read while read-ahead bytes remain
			p.settle();
			if (i < 3) { whole = whole && pending_bytes(p.server.native_handle()) == 500; }
			auto r = read_once(p, in, 1000);
			if (!r.done || r.error) { whole = false; break; }
			got += r.data;
			whole = whole && r.data.size() == (i < 3 ? 1000u : 500u);
		}
		check(whole, "[U] three reads of 1000 take the 3000 read-ahead bytes while the 500 behind stay in the kernel; the fourth reads them");
		check(got == first + more, "[U] the pieces are the stream, in order (" + std::to_string(got.size()) + " bytes)");
	}
	// EOF with nothing read fires; data then EOF does not fire, and the EOF is read afterwards.
	{
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; });
		p.settle();
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 1, "[U] EOF with nothing read fires the disconnect once");
		auto r = read_once(p, in, 100);
		check(r.done && r.error == asio::error::eof, "[U] and the EOF is read afterwards");
	}
	{
		Pair p;
		auto in = std::make_shared<Input>(p.server);
		int fired = 0;
		in->watch([&] { ++fired; });
		p.settle();
		p.send("abc");
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0, "[U] data followed by EOF: the data ended the watch, so the EOF is not a disconnect");
		auto a = read_once(p, in, 100);
		check(a.done && !a.error && a.data == "abc", "[U] the data is read");
		auto b = read_once(p, in, 100);
		check(b.done && b.error == asio::error::eof, "[U] then the EOF");
	}
	// A watch set after the data arrived is ended by it at once.
	{
		Pair p;
		p.send("xyz");
		check(p.arrived(3), "[U] 3 bytes arrived");
		auto in = std::make_shared<Input>(p.server);
		in->watch([] {});
		p.settle();
		int fired = 0;
		in->watch([&] { ++fired; });   // ready_ with data: no disconnect, and no new read
		::shutdown(p.client, SHUT_WR);
		p.settle();
		check(fired == 0, "[U] a watch set while read-ahead bytes wait never fires");
		auto a = read_once(p, in, 100);
		check(a.done && a.data == "xyz", "[U] the bytes are still delivered");
	}
	// The reply monitor (a deferred or owned-stream reply waits) uses the same read-ahead: it never
	// reads over bytes that wait, a half-close is not a disconnect, and a reset is.
	{
		const auto make = [](Pair& p) {
			return http::detail::AsioDeferredControl::create(p.io.get_executor(), http::DeferredOptions{clk::now() + 30s, {}, {}, {}});
		};
		{
			Pair p;
			const std::string data = pattern(10000);
			p.send(data);
			check(p.arrived(10000), "[U] the monitor: 10000 bytes reached the server's receive queue");
			auto in = std::make_shared<Input>(p.server);
			in->watch([] {});
			p.settle();
			auto control = make(p);
			in->monitor(control);   // read-ahead bytes wait: nothing may be read over them
			p.settle();
			check(pending_bytes(p.server.native_handle()) == 10000 - static_cast<int>(Input::kReadAhead), "[U] the monitor arms nothing while read-ahead bytes wait");
			auto a = read_once(p, in, 65536), b = read_once(p, in, 65536);
			check(a.data + b.data == data, "[U] and the stream is intact");
			control->disable();
		}
		{   // the watch ended with the data: after it is consumed, a monitor read that sees EOF does not fire the old watch
			Pair p;
			auto in = std::make_shared<Input>(p.server);
			int fired = 0;
			in->watch([&] { ++fired; });
			p.settle();
			p.send("abc");
			p.settle();
			auto a = read_once(p, in, 100);
			auto control = make(p);
			in->monitor(control);
			p.settle();
			::shutdown(p.client, SHUT_WR);
			p.settle();
			check(a.data == "abc" && fired == 0, "[U] a watch that data ended stays ended: a later EOF does not call it");
			check(control->exchange->phase() == http::detail::DeferredExchange::Phase::Waiting, "[U] the monitor does not treat EOF as a disconnect (a half-close still expects its reply)");
			control->disable();
		}
		{   // a reset is one
			Pair p;
			auto in = std::make_shared<Input>(p.server);
			auto control = make(p);
			in->monitor(control);
			p.settle();
			linger l{1, 0}; ::setsockopt(p.client, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(p.client); p.client = -1;
			p.settle();
			check(control->exchange->phase() != http::detail::DeferredExchange::Phase::Waiting, "[U] the monitor treats a reset as a disconnect");
			control->disable();
		}
	}
	// However the sends, the watches and the reads interleave, the reads are the stream: a
	// seeded random walk over sends of 1..2000 bytes, watch calls and reads of 1..3000 bytes.
	{
		int bad = 0, total = 0;
		for (unsigned seed = 1; seed <= 40; ++seed) {
			std::uint32_t state = seed * 2654435761u;
			const auto rnd = [&](std::uint32_t n) { state = state * 1664525u + 1013904223u; return (state >> 8) % n; };
			Pair p;
			auto in = std::make_shared<Input>(p.server);
			std::string sent, got;
			for (int step = 0; step < 60; ++step) {
				switch (rnd(3)) {
					case 0: if (sent.size() - got.size() < 60000) { auto chunk = pattern(1 + rnd(2000), static_cast<unsigned>(sent.size())); p.send(chunk); sent += chunk; } break;
					case 1: in->watch([] {}); p.io.restart(); p.io.poll(); break;
					default:
						if (got.size() < sent.size()) { auto r = read_once(p, in, 1 + rnd(3000)); if (!r.done || r.error) { ++bad; } got += r.data; }
				}
			}
			while (got.size() < sent.size() && bad == 0) { auto r = read_once(p, in, 65536); if (!r.done || r.error) { ++bad; break; } got += r.data; }
			++total;
			if (got != sent) { ++bad; std::printf("    seed %u: %zu of %zu bytes, stream %s\n", seed, got.size(), sent.size(), got == sent.substr(0, got.size()) ? "a prefix" : "corrupt"); }
		}
		check(bad == 0, "[U] " + std::to_string(total) + " random walks of sends, watches and reads return the stream byte for byte");
	}
	lazy_watch_tests();
}
#endif  // READAHEAD_TEST_INTEGRATION_ONLY

// ---- the server for the integration sections ----
struct Server {
	App app;
	unsigned short port = free_port();
	http::HttpAsioService svc;
	explicit Server(std::size_t in_flight = 64) : svc(app, 1, in_flight) { app.service = &svc; }
	void start() { svc.start(port); Conn probe(port, 1); }
	std::size_t inflight() { return static_cast<std::size_t>(svc.reactor(0).gate().inflight.load()); }
};

// Opens a connection whose first request is held, and waits until its handler runs (so the
// watch is armed): what follows arrives while the watch reads.
struct Held {
	Conn c;
	std::shared_ptr<Obs> o;
	Held(Server& s, const std::string& id) : c(s.port), o(s.app.get_obs(id)) {
		c.send(get("/hold?id=" + id));
		until([&] { return o->started.load(); });
		std::this_thread::sleep_for(20ms);   // the watch is reading
	}
};

int main() {
	std::signal(SIGPIPE, SIG_IGN);
	std::printf("== read-ahead watch and sliced write tests ==\n");
#ifndef READAHEAD_TEST_INTEGRATION_ONLY
	input_tests();
#endif

	// [P] A pipelined request inside the read-ahead, at every alignment of the 4096-byte window.
	{
		Server s; s.start();
		int n = 0, ok = 0;
		const auto run = [&](const std::string& second, const std::string& expect_body, const std::string& what) {
			const std::string id = "p" + std::to_string(++n);
			Held h(s, id);
			const std::string third_body = pattern(40, 9);
			h.c.send(second + post("/echo", third_body, "Connection: close\r\n"));
			std::this_thread::sleep_for(30ms);   // the watch took the first 4096 bytes of it
			h.o->release = true;
			auto r1 = h.c.read_response(), r2 = h.c.read_response(), r3 = h.c.read_response();
			const bool good = r1.status == 200 && r1.body == "held" && r2.status == 200 && r2.body == expect_body &&
				r3.status == 200 && r3.body == third_body;
			if (good) { ++ok; } else { check(false, "[P] " + what + ": r1=" + std::to_string(r1.status) + " r2=" + std::to_string(r2.status) + "/" + std::to_string(r2.body.size()) + "B r3=" + std::to_string(r3.status)); }
		};
		const std::string body = pattern(300, 3);
		const std::string skeleton_a = "POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: \r\nContent-Length: 300\r\n\r\n";
		// The head's CRLF CRLF straddles the window: head lengths 4096 + d, d from -6 to +3.
		for (int d = -6; d <= 3; ++d) {
			const std::size_t pad = 4096 + d - skeleton_a.size();
			run("POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: " + std::string(pad, 'p') + "\r\nContent-Length: 300\r\n\r\n" + body, body, "head ends at window" + std::string(d >= 0 ? "+" : "") + std::to_string(d));
		}
		// The window ends inside the body.
		for (std::size_t cut : {1u, 2u, 17u, 150u, 299u}) {
			const std::size_t head = 4096 - cut;
			const std::string skeleton = "POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: \r\nContent-Length: 300\r\n\r\n";
			const std::size_t pad = head - skeleton.size();
			run("POST /echo HTTP/1.1\r\nHost: x\r\nX-Pad: " + std::string(pad, 'p') + "\r\nContent-Length: 300\r\n\r\n" + body, body, "window ends " + std::to_string(cut) + " bytes into the body");
		}
		// The window ends at, or one either side of, the end of the second request.
		for (int d = -2; d <= 2; ++d) {
			const std::string skeleton = "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 0000\r\n\r\n";
			const std::size_t blen = 4096 + d - skeleton.size();
			const std::string b = pattern(blen, 4);
			run("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(blen) + "\r\n\r\n" + b, b, "request ends at window" + std::string(d >= 0 ? "+" : "") + std::to_string(d));
		}
		check(ok == n, "[P] " + std::to_string(ok) + " of " + std::to_string(n) + " alignments: the pipelined request inside the read-ahead is answered in order, intact");
		check(s.app.hit_count("/echo") == 2 * n, "[P] the handler ran exactly two echoes per connection (no request repeated or dropped)");
	}

	// [F] A flood of pipelined requests, far more than the window, is answered in order.
	{
		Server s; s.start();
		Held h(s, "flood");
		constexpr int N = 3000;
		std::string burst;
		for (int i = 0; i < N; ++i) {
			burst += post("/echo", "n" + std::to_string(i), i == N - 1 ? "Connection: close\r\n" : "");
		}
		std::thread sender([&] { h.c.send(burst); });   // the kernel's buffers bound what the server holds
		std::this_thread::sleep_for(50ms);
		h.o->release = true;
		auto first = h.c.read_response();
		bool ordered = first.body == "held";
		int got = 0;
		for (int i = 0; i < N && ordered; ++i) {
			auto r = h.c.read_response();
			ordered = r.complete && r.status == 200 && r.body == "n" + std::to_string(i);
			if (ordered) { ++got; }
		}
		sender.join();
		check(ordered && got == N, "[F] " + std::to_string(N) + " pipelined requests (" + std::to_string(burst.size() / 1024) + " KiB) behind a held one are answered in order, none lost or repeated");
		check(s.app.hit_count("/echo") == N, "[F] each ran exactly once");
	}

	// [D] The watch's data ends it; a half-close after the pipelined request is that request's to see.
	{
		Server s; s.start();
		Held h(s, "d1");
		auto o2 = s.app.get_obs("d2");
		h.c.send(get("/hold?id=d2"));   // arrives inside the read-ahead
		std::this_thread::sleep_for(30ms);
		h.c.shut_wr();
		std::this_thread::sleep_for(30ms);
		check(h.o->reason == -1, "[D] the first request is not cancelled by a half-close that came after pipelined input");
		h.o->release = true;
		auto r1 = h.c.read_response();
		until([&] { return o2->started.load(); });
		until([&] { return o2->reason.load() != -1; }, 1000);
		check(o2->reason == static_cast<int>(http::disconnected), "[D] the pipelined request's own watch sees the half-close");
		o2->release = true;
		auto r2 = h.c.read_response();
		check(r1.body == "held" && r2.status == 200 && r2.body == "held", "[D] both are answered, the half-closed client included");
		check(h.c.ends_silently(), "[D] and then the connection closes");
	}
	{   // the control: without pipelined input the half-close is seen by the first request
		Server s; s.start();
		Held h(s, "d3");
		h.c.shut_wr();
		until([&] { return h.o->reason.load() != -1; }, 1000);
		check(h.o->reason == static_cast<int>(http::disconnected), "[D] a half-close with nothing pipelined cancels the request in flight");
		h.o->release = true;
		check(h.c.read_response().body == "held", "[D] and its response is still written");
	}

	// [B] A client that goes away in the middle of a buffered body never reaches the handler.
	{
		Server s; s.start();
		const auto head = [](const std::string& path) { return "POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n"; };
		{ Conn c(s.port); c.send(head("/echo") + std::string(1000, 'a')); std::this_thread::sleep_for(60ms); c.close(); }
		{ Conn c(s.port); c.send(head("/echo") + std::string(1000, 'a')); std::this_thread::sleep_for(60ms); c.shut_wr(); check(c.ends_silently(), "[B] a half-close mid-body ends the connection with no response"); }
		{ Conn c(s.port); c.send(head("/echo") + std::string(5000, 'a')); std::this_thread::sleep_for(60ms); linger l{1, 0}; ::setsockopt(c.fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); c.close(); }
		{   // chunked, cut after a chunk
			Conn c(s.port); c.send("POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n"); std::this_thread::sleep_for(60ms); c.close();
		}
		check(until([&] { return s.inflight() == 0; }), "[B] every cut-short request released its admission");
		check(s.app.hit_count("/echo") == 0, "[B] the handler never ran for a body that did not complete");
		check(Conn(s.port).fd >= 0 && [&] { Conn c(s.port); c.send(get("/fast", "Connection: close\r\n")); return c.read_response().body == "fast"; }(), "[B] the server keeps serving");
	}

	// [S] Request smuggling: ambiguous framing is never executed, and a request inside a body is data.
	{
		Server s; s.start();
		const std::string hidden = get("/smuggled");
		struct Case { const char* what; std::string request; };
		const std::vector<Case> bad = {
			{"Content-Length with Transfer-Encoding: chunked",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(5 + hidden.size()) + "\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n" + hidden},
			{"two different Content-Lengths",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: " + std::to_string(5 + hidden.size()) + "\r\n\r\nhello" + hidden},
			{"a space before the colon of Content-Length",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length : 5\r\n\r\nhello" + hidden},
			{"a signed Content-Length",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: +5\r\n\r\nhello" + hidden},
			{"a Content-Length with trailing junk",
				"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5 5\r\n\r\nhello" + hidden},
			{"a chunk size that is not hex",
				"POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nhello\r\n0\r\n\r\n" + hidden},
		};
		int rejected_first = 0, rejected_inside = 0;
		for (const auto& c : bad) {
			{   // as the first request on its connection
				Conn k(s.port);
				k.send(c.request);
				auto r = k.read_response();
				if (r.status != 200 && s.app.hit_count("/smuggled") == 0) { ++rejected_first; }
				else { check(false, std::string("[S] ") + c.what + " (first): status " + std::to_string(r.status) + ", /smuggled ran " + std::to_string(s.app.hit_count("/smuggled")) + "x"); }
			}
			{   // arriving inside the read-ahead, behind a request still running
				static int n = 0;
				Held h(s, "s" + std::to_string(++n));
				h.c.send(c.request);
				std::this_thread::sleep_for(30ms);
				h.o->release = true;
				auto r1 = h.c.read_response();
				auto r2 = h.c.read_response();
				const bool good = r1.body == "held" && !r2.complete && s.app.hit_count("/smuggled") == 0;
				if (good) { ++rejected_inside; }
				else { check(false, std::string("[S] ") + c.what + " (inside the read-ahead): second response " + std::to_string(r2.status) + ", /smuggled ran " + std::to_string(s.app.hit_count("/smuggled")) + "x"); }
			}
		}
		check(rejected_first == static_cast<int>(bad.size()), "[S] " + std::to_string(rejected_first) + " of " + std::to_string(bad.size()) + " ambiguous requests were refused as the first request, none ran /smuggled");
		check(rejected_inside == static_cast<int>(bad.size()), "[S] " + std::to_string(rejected_inside) + " of " + std::to_string(bad.size()) + " arriving inside the read-ahead were refused after the held request's response, none ran /smuggled");
		// A request in a body is body, whether the body fits the window or crosses it.
		for (std::size_t pad : {0u, 3000u, 6000u}) {
			const std::string inner = std::string(pad, 'x') + hidden + std::string(pad / 2, 'y');
			{
				Held h(s, "inner" + std::to_string(pad));
				h.c.send(post("/echo", inner));
				std::this_thread::sleep_for(30ms);
				h.o->release = true;
				auto r1 = h.c.read_response(), r2 = h.c.read_response();
				check(r1.body == "held" && r2.body == inner && s.app.hit_count("/smuggled") == 0,
					"[S] a request inside a Content-Length body of " + std::to_string(inner.size()) + " bytes is data, answered once, never run");
			}
			{
				const std::string chunked = "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n";
				char size[16]; std::snprintf(size, sizeof size, "%zx", inner.size());
				Held h(s, "chunk" + std::to_string(pad));
				h.c.send(chunked + size + "\r\n" + inner + "\r\n0\r\n\r\n");
				std::this_thread::sleep_for(30ms);
				h.o->release = true;
				auto r1 = h.c.read_response(), r2 = h.c.read_response();
				check(r1.body == "held" && r2.body == inner && s.app.hit_count("/smuggled") == 0,
					"[S] a request inside a chunk of " + std::to_string(inner.size()) + " bytes is data, answered once, never run");
			}
		}
	}

	// [S] The trailer section of a chunked body: a front end that sets identity headers in the
	// header section cannot be overridden from the trailers (RFC 9110 section 6.5.1).
	{
		Server s; s.start();
		const std::vector<std::pair<std::string, std::string>> cases = {
			{"X-User: admin\r\nX-Forwarded-For: 6.6.6.6\r\nX-Pad: 1\r\n", "X-User:admin,X-Forwarded-For:6.6.6.6,X-Pad:1,"},
			{"X-User: admin\r\nX-Forwarded-For: 6.6.6.6\r\n", "X-User:admin,X-Forwarded-For:6.6.6.6,"},
			{"X-User: admin\r\n", "X-User:admin,"},
		};
		for (const auto& [trailers, want] : cases) {
			Conn k(s.port);
			k.send("POST /who HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n" + trailers + "\r\n");
			auto r = k.read_response();
			check(r.status == 200 && r.body == "x-user=;xff=;headers=3;trailers=" + want, "[S] trailer fields are trailers, never headers: " + r.body);
		}
	}
	// [S] CONNECT opens no tunnel, and has no content (RFC 9110 section 9.3.6): it is answered,
	// the connection closes, and nothing after its header section runs as a request, whatever
	// framing it named. And after an HTTP/1.0 message with Transfer-Encoding the connection
	// closes too (RFC 9112 section 6.1).
	{
		Server s; s.start();
		const std::string hidden = get("/smuggled");
		const std::vector<std::pair<std::string, std::string>> cases = {
			{"CONNECT with Content-Length", "CONNECT x:443 HTTP/1.1\r\nHost: x:443\r\nContent-Length: " + std::to_string(hidden.size()) + "\r\n\r\n" + hidden},
			{"CONNECT with Transfer-Encoding", "CONNECT x:443 HTTP/1.1\r\nHost: x:443\r\nTransfer-Encoding: chunked\r\n\r\n" + hidden},
			{"CONNECT", "CONNECT x:443 HTTP/1.1\r\nHost: x:443\r\n\r\n" + hidden},
			{"HTTP/1.0 with Transfer-Encoding and keep-alive",
				"POST /echo HTTP/1.0\r\nHost: x\r\nConnection: keep-alive\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n" + hidden},
		};
		for (const auto& [what, request] : cases) {
			Conn k(s.port);
			k.send(request);
			auto r = k.read_response();
			const bool closed = k.ends_silently();
			check(r.complete && closed && s.app.hit_count("/smuggled") == 0,
				"[S] " + what + ": answered " + std::to_string(r.status) + ", then the connection " + (closed ? "closes" : "stays open") +
				", /smuggled ran " + std::to_string(s.app.hit_count("/smuggled")) + "x");
		}
	}

	// [Q] The watch starts when the handler first asks about cancellation, and the first question
	// sees what happened before it: a hang-up is a disconnect (polled or by callback, which runs
	// as it is registered), pipelined input ends the watch, and a handler that never asks still has
	// the requests pipelined behind it answered in order.
	{
		Server s;
		s.start();
		for (const std::string ask : {"poll", "cb"}) {
			Conn c(s.port);
			const std::string id = "q-" + ask;
			auto o = s.app.get_obs(id);
			c.send(get("/lazy?id=" + id + "&ask=" + ask));
			until([&] { return o->started.load(); });
			c.shut_wr();
			std::this_thread::sleep_for(30ms);
			o->release = true;
			until([&] { return o->finished.load(); });
			check(o->reason == static_cast<int>(http::disconnected), "[Q] " + ask + ": a half-close before the first question is a disconnect at that question");
			if (ask == "cb") { check(o->at_once.load(), "[Q] cb: and the callback ran as it was registered"); }
			check(c.read_response().body == "lazy" && c.ends_silently(), "[Q] " + ask + ": the response is still written, then the connection closes");
		}
		{
			Conn c(s.port);
			auto o = s.app.get_obs("q-pipe");
			c.send(get("/lazy?id=q-pipe&ask=poll"));
			until([&] { return o->started.load(); });
			c.send(get("/fast"));
			c.shut_wr();
			std::this_thread::sleep_for(30ms);
			o->release = true;
			until([&] { return o->finished.load(); });
			check(o->reason == 0, "[Q] input pipelined before the first question ends the watch: the half-close after it is not this request's");
			auto r1 = c.read_response(), r2 = c.read_response();
			check(r1.body == "lazy" && r2.body == "fast" && c.ends_silently(), "[Q] both are answered in order, then the connection closes");
		}
		{
			Conn c(s.port);
			auto o = s.app.get_obs("q-none");
			c.send(get("/lazy?id=q-none&ask=none") + get("/echo") + post("/echo", "abc"));
			until([&] { return o->started.load(); });
			std::this_thread::sleep_for(20ms);
			o->release = true;
			auto r1 = c.read_response(), r2 = c.read_response(), r3 = c.read_response();
			check(r1.body == "lazy" && r2.status == 200 && r2.body.empty() && r3.body == "abc", "[Q] a handler that never asks: the requests pipelined behind it are answered in order");
		}
	}

	// [N] A buffered reply leaves at once as far as the socket takes it, and the rest is written
	// after: with no timeouts, replies far larger than the send buffer reach a client that reads
	// late byte for byte, on one keep-alive connection.
	{
		Server s;
		s.start();
		Conn c(s.port);
		int ok = 0, n = 0;
		for (std::size_t size : {1u, 1000u, 65536u, 300000u, 1u << 20, 4u << 20, 1000u}) {
			c.send(get("/body?n=" + std::to_string(size)));
			std::this_thread::sleep_for(30ms);   // the send buffer fills before the client reads
			auto r = c.read_response();
			++n;
			if (r.complete && r.body == pattern(size, 5)) { ++ok; }
			else { check(false, "[N] reply of " + std::to_string(size) + " bytes arrived as " + std::to_string(r.body.size())); }
		}
		check(ok == n, "[N] " + std::to_string(ok) + " of " + std::to_string(n) + " replies from 1 byte to 4 MiB arrive byte for byte to a client that reads late");
	}

	// [W] The reply goes out in 64 KiB slices of head and body together; every byte arrives.
	{
		Server s;
		s.svc.set_inactivity_timeout(2000ms);
		s.start();
		std::size_t head = 0;
		{   // the head's length for a five-digit body
			Conn c(s.port);
			c.send(get("/body?n=65000", "Connection: close\r\n"));
			auto r = c.read_response();
			head = r.head.size() + 4;
			check(r.body == pattern(65000, 5), "[W] a 65000 byte reply arrives intact (head " + std::to_string(head) + " bytes)");
		}
		int ok = 0, n = 0;
		for (std::size_t total : {65536u - 1, 65536u, 65536u + 1, 131072u - 1, 131072u, 131072u + 1}) {
			for (int extra : {-1, 0, 1}) {   // the head and the body together land at, and either side of, a slice end
				const std::size_t size = total - head + extra;
				Conn c(s.port, 2);
				c.send(get("/body?n=" + std::to_string(size), "Connection: close\r\n"));
				auto r = c.read_response();
				++n;
				if (r.complete && r.body == pattern(size, 5)) { ++ok; }
				else { check(false, "[W] reply of " + std::to_string(size) + " bytes arrived as " + std::to_string(r.body.size())); }
			}
		}
		check(ok == n, "[W] " + std::to_string(ok) + " of " + std::to_string(n) + " replies around the slice ends arrive byte for byte");
		Conn k(s.port, 2);
		int keep = 0;
		for (int i = 0; i < 50 && keep == i; ++i) {   // and keep-alive carries on after sliced replies
			k.send(get("/body?n=70000"));
			if (k.read_response().body == pattern(70000, 5)) { ++keep; }
		}
		check(keep == 50, "[W] 50 sliced replies in a row on one keep-alive connection");
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
