#pragma once

// The coroutine test's shared pieces: the raw client, the observation record and the wrapper scheduler.

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
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "http_asio.h"
#include "http_router.h"
#include "../test_loop.h"

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

namespace detail {

inline int g_fail = 0;
inline void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}
inline double since_ms(clk::time_point t) { return std::chrono::duration<double, std::milli>(clk::now() - t).count(); }

// ---- a raw client ----
struct Resp { int status = 0; std::string head, body; bool complete = false; };
inline std::string header_value(const std::string& head, const std::string& name) {
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
inline std::string get(const std::string& path, const std::string& extra = "") {
	return "GET " + path + " HTTP/1.1\r\nHost: x\r\n" + extra + "\r\n";
}
inline std::string post(const std::string& path, const std::string& body, const std::string& extra = "") {
	return "POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(body.size()) + "\r\n" + extra + "\r\n" + body;
}
inline Resp one(unsigned short port, const std::string& request) {
	Conn c(port);
	c.send(request);
	return c.read_response();
}
inline void wait_listen(unsigned short port) {
	for (int i = 0; i < 300; ++i) {
		Conn c(port, 1);
		if (c.fd >= 0) { return; }
		std::this_thread::sleep_for(5ms);
	}
}
template <class F> bool until(F&& f, int ms = 3000) {
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
	std::atomic<double> finished_after_ms{-1};
	std::optional<task::cancel_token> token;   // the request's token, kept past handle(); set before `started`
	clk::time_point t0{};
};
// Busy work of about `d`, between checkpoints.
inline void spin(std::chrono::microseconds d) { for (auto end = clk::now() + d; clk::now() < end;) {} }
// A per-request wrapper scheduler: its resumes run on the reactor's loop thread under its own identity.
struct wrapper_scheduler {
	task::asio_scheduler& loop;
	void post(std::coroutine_handle<> h) {
		asio::post(loop.executor(), [this, h] {
			task::running_on scope(task::scheduler_ref::of(*this));
			h.resume();
		});
	}
	std::uint64_t now() const noexcept { return loop.now(); }
	bool runs_here() const noexcept { return loop.runs_here(); }

}  // namespace detail

using namespace detail;
