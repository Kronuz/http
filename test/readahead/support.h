#pragma once

// The read-ahead test's shared pieces: the raw client, the application, the server and the held connection.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
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
#include "../test_loop.h"

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

namespace detail {

inline int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

// ---- helpers ----
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
	void rst() { linger l{1, 0}; ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l)); ::close(fd); fd = -1; }
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

// ---- the server for the integration sections ----
struct Server {
	App app;
	unsigned short port = 0;
	http::HttpAsioService svc;
	explicit Server(http::Limits limits = http::Limits{.max_in_flight = 64}) : svc(app, 1, limits) { app.service = &svc; }
	void start() { svc.start(0); port = svc.port(); Conn probe(port, 1); }
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

}  // namespace detail

using namespace detail;

// The [U] sections, run by main() unless READAHEAD_TEST_INTEGRATION_ONLY.
void read_ahead_tests();
void input_tests();
