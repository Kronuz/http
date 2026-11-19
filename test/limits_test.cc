// http::Limits, close_idle() and the phased stop over real TCP, each section plain and over TLS:
// [B] every Limits field at its bound (max_clients, max_in_flight, the idle, header, inactivity and
// request timeouts, shutdown_grace, max_body; tls_handshake and tls_handshakes over TLS), and the
// constructor's refusals; [I] close_idle() closes an idle connection at once and a busy one after
// its reply; [S] after stop_accepting() a new connection is refused, a request on an open
// keep-alive connection is answered 503 and an in-flight reply says Connection: close, and
// wait_requests() counts a request whose headers are still arriving; [P] a held poll, its headers
// complete and its body unread, trips neither the header nor the inactivity timeout.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "http_asio_tls.h"
#include "test_loop.h"

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;
using ms = std::chrono::milliseconds;

static int failures = 0;
static void check(bool ok, const std::string& what) {
	std::printf("  %s: %s\n", ok ? "ok" : "FAILED", what.c_str());
	if (!ok) { ++failures; }
}
static double since(clk::time_point t0) { return std::chrono::duration<double>(clk::now() - t0).count(); }
template<class F> static bool until(F f, ms within = 3000ms) {
	for (const auto end = clk::now() + within; clk::now() < end; std::this_thread::sleep_for(2ms)) { if (f()) { return true; } }
	return f();
}

// ---- the application ----
// "/" answers ok; "/hold?id=" (and the streamed "/stream-hold?id=") runs until the test releases
// it, without reading its body or asking about cancellation, as a held poll does; "/size" answers
// its buffered body's size.
struct Hold {
	std::atomic<bool> started{false}, release{false};
};
struct App : http::HttpHandler {
	std::mutex mutex;
	std::map<std::string, std::shared_ptr<Hold>> holds;
	std::shared_ptr<Hold> hold(const std::string& id) {
		std::lock_guard lock(mutex);
		auto& h = holds[id];
		if (!h) { h = std::make_shared<Hold>(); }
		return h;
	}
	bool wants_body_stream(const http::Request& r) override { return r.path == "/stream-hold"; }
	task::task<void> handle(http::Request& req, http::ResponseWriter& w) override {
		if (req.path == "/hold" || req.path == "/stream-hold") {
			auto h = hold(req.query.substr(3));   // "id=<id>"
			h->started = true;
			const auto loop = req.scheduler().executor();
			for (auto end = clk::now() + 10s; !h->release.load() && clk::now() < end;) { co_await sleep_for(loop, 2ms); }
			w.send(200, "held");
			co_return;
		}
		if (req.path == "/size") { w.send(200, std::to_string(req.body.size())); co_return; }
		w.send(200, "ok");
	}
};

// ---- TLS: an in-memory self-signed certificate; clients do not verify it ----
static std::shared_ptr<asio::ssl::context> server_tls() {
	auto context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_server);
	EVP_PKEY* key = EVP_EC_gen("P-256");
	X509* x = X509_new();
	X509_set_version(x, 2);
	ASN1_INTEGER_set(X509_get_serialNumber(x), 1);
	X509_gmtime_adj(X509_getm_notBefore(x), 0);
	X509_gmtime_adj(X509_getm_notAfter(x), 3600);
	X509_set_pubkey(x, key);
	X509_NAME* name = X509_NAME_new();
	X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
	X509_set_subject_name(x, name);
	X509_set_issuer_name(x, name);
	X509_NAME_free(name);
	X509_sign(x, key, EVP_sha256());
	SSL_CTX_use_certificate(context->native_handle(), x);
	SSL_CTX_use_PrivateKey(context->native_handle(), key);
	X509_free(x);
	EVP_PKEY_free(key);
	return context;
}

// ---- a blocking client, plain or TLS, whose reads take a bound ----
static int dial(unsigned short port) {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
	::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
	return fd;
}
struct Client {
	int fd = -1;
	SSL* ssl = nullptr;
	std::string in;   // received, not yet taken
	// Connects, and over TLS (`tls` set) handshakes, unless `handshake` is false: a TCP connection
	// that never sends its ClientHello.
	Client(unsigned short port, SSL_CTX* tls, bool handshake = true) : fd(dial(port)) {
		if (fd < 0 || !tls || !handshake) { return; }
		ssl = SSL_new(tls);
		SSL_set_fd(ssl, fd);
		bound(3000ms);
		if (SSL_connect(ssl) != 1) { drop(); }
	}
	Client(const Client&) = delete;
	Client& operator=(const Client&) = delete;
	~Client() { drop(); }
	void drop() {
		if (ssl) { SSL_free(ssl); ssl = nullptr; }
		if (fd >= 0) { ::close(fd); fd = -1; }
	}
	bool ok() const { return fd >= 0; }
	void bound(ms within) {
		timeval tv{.tv_sec = static_cast<time_t>(within.count() / 1000), .tv_usec = static_cast<suseconds_t>(within.count() % 1000 * 1000)};
		::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	}
	bool send(const std::string& bytes) {
		if (fd < 0) { return false; }
		if (ssl) { return SSL_write(ssl, bytes.data(), static_cast<int>(bytes.size())) == static_cast<int>(bytes.size()); }
		return ::send(fd, bytes.data(), bytes.size(), 0) == static_cast<ssize_t>(bytes.size());
	}
	// One read within `within`: 1 with bytes appended to `in`, 0 once the peer closed, -1 at the bound.
	int read(ms within) {
		if (fd < 0) { return 0; }
		bound(within);
		char b[16384];
		if (ssl) {
			const int n = SSL_read(ssl, b, sizeof b);
			const int error = errno;
			if (n > 0) { in.append(b, static_cast<std::size_t>(n)); return 1; }
			const int e = SSL_get_error(ssl, n);
			ERR_clear_error();
			return e == SSL_ERROR_WANT_READ && (error == EAGAIN || error == EWOULDBLOCK) ? -1 : 0;
		}
		const ssize_t n = ::recv(fd, b, sizeof b, 0);
		if (n > 0) { in.append(b, static_cast<std::size_t>(n)); return 1; }
		return n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) ? -1 : 0;
	}
	// Reads until the peer closes or `within` passes: whether it closed, and the seconds it took.
	std::pair<bool, double> until_closed(ms within) {
		const auto t0 = clk::now();
		for (;;) {
			const auto left = std::chrono::duration_cast<ms>(t0 + within - clk::now());
			if (left <= 0ms) { return {false, since(t0)}; }
			const int r = read(left);
			if (r == 0) { return {true, since(t0)}; }
			if (r < 0) { return {false, since(t0)}; }
		}
	}
	// The next response, head and Content-Length body; "" when the connection ends or `within` passes first.
	std::string response(ms within = 3000ms) {
		const auto end = clk::now() + within;
		const auto more = [&] {
			const auto left = std::chrono::duration_cast<ms>(end - clk::now());
			return left > 0ms && read(left) > 0;
		};
		while (in.find("\r\n\r\n") == std::string::npos) { if (!more()) { return ""; } }
		const auto head = in.find("\r\n\r\n") + 4;
		const auto at = in.find("Content-Length: ");
		const std::size_t length = at < head ? std::stoul(in.substr(at + 16)) : 0;
		while (in.size() < head + length) { if (!more()) { return ""; } }
		std::string out = in.substr(0, head + length);
		in.erase(0, head + length);
		return out;
	}
};
static std::string get(const std::string& path, const std::string& extra = "") { return "GET " + path + " HTTP/1.1\r\nHost: x\r\n" + extra + "\r\n"; }
static bool status(const std::string& r, int code) { return r.rfind("HTTP/1.1 " + std::to_string(code), 0) == 0; }
static bool closes(const std::string& r) { return r.find("\r\nConnection: close\r\n") != std::string::npos; }
static std::string body(const std::string& r) { const auto at = r.find("\r\n\r\n"); return at == std::string::npos ? "" : r.substr(at + 4); }

// ---- one transport: plain, or TLS with its server context and client ----
struct Mode {
	const char* name;
	std::shared_ptr<asio::ssl::context> server;   // none: plain
	SSL_CTX* client = nullptr;
	std::atomic<int>* handshake_failures = nullptr;   // TlsFailure::handshakes seen
	std::atomic<int>* deadline_failures = nullptr;    // TlsFailure::deadline seen
	std::unique_ptr<http::HttpAsioService> service(App& app, const http::Limits& limits) const {
		if (!server) { return std::make_unique<http::HttpAsioService>(app, 1, limits); }
		auto* handshakes = handshake_failures;
		auto* deadlines = deadline_failures;
		return std::make_unique<http::HttpAsioService>(app, 1, limits, http::Tls{server, [handshakes, deadlines](http::TlsFailure why) {
			if (why == http::TlsFailure::handshakes) { ++*handshakes; }
			if (why == http::TlsFailure::deadline) { ++*deadlines; }
		}});
	}
	std::string label(const std::string& what) const { return std::string("[") + name + "] " + what; }
};

// A limits value with every timeout off but the ones a section sets.
static http::Limits quiet_limits() {
	http::Limits limits;
	limits.idle_timeout = limits.header_timeout = limits.inactivity_timeout = 0ms;
	return limits;
}

static void bounds(const Mode& m, App& app) {
	{  // max_clients: the second connection is served, a third is closed at the accept until one leaves
		auto limits = quiet_limits();
		limits.max_clients = 2;
		auto svc = m.service(app, limits);
		svc->start(0);
		const auto port = svc->port();
		Client a(port, m.client), b(port, m.client);
		a.send(get("/")); b.send(get("/"));
		check(status(a.response(), 200) && status(b.response(), 200), m.label("max_clients 2: two connections are served"));
		Client c(port, m.client);
		const bool refused = !c.ok() || (c.send(get("/")), c.until_closed(2000ms).first);
		check(refused && svc->connection_stats().rejected >= 1, m.label("a third is closed at the accept"));
		a.drop();
		until([&] { return svc->connection_stats().active == 1; });
		Client d(port, m.client);
		d.send(get("/"));
		check(status(d.response(), 200), m.label("and one is served once a connection leaves"));
		svc->stop();
	}
	{  // max_in_flight: a second request while one is held is answered 503 with Retry-After
		auto limits = quiet_limits();
		limits.max_in_flight = 1;
		auto svc = m.service(app, limits);
		svc->start(0);
		const auto port = svc->port();
		const std::string id = std::string(m.name) + "-inflight";
		Client held(port, m.client), other(port, m.client);
		held.send(get("/hold?id=" + id));
		until([&] { return app.hold(id)->started.load(); });
		other.send(get("/"));
		const auto refused = other.response();
		check(status(refused, 503) && refused.find("\r\nRetry-After: 1\r\n") != std::string::npos, m.label("max_in_flight 1: a second request is answered 503, Retry-After 1"));
		app.hold(id)->release = true;
		check(status(held.response(), 200), m.label("the held one completes"));
		Client after(port, m.client);
		after.send(get("/"));
		check(status(after.response(), 200), m.label("and the next request is admitted"));
		svc->stop();
	}
	{  // the idle, header, inactivity and request timeouts, each alone at 300 ms (request 400 ms)
		const auto with = [&](auto set) { auto limits = quiet_limits(); set(limits); return m.service(app, limits); };
		auto idle = with([](http::Limits& l) { l.idle_timeout = 300ms; });
		idle->start(0);
		{
			Client c(idle->port(), m.client);
			const auto [closed, s] = c.until_closed(3000ms);
			check(closed && s > 0.25 && s < 2.0, m.label("idle_timeout 300 ms: a silent connection closes at it (" + std::to_string(s) + " s)"));
		}
		{
			Client c(idle->port(), m.client);
			c.send(get("/"));
			const bool served = status(c.response(), 200);
			const auto [closed, s] = c.until_closed(3000ms);
			check(served && closed && s > 0.25 && s < 2.0, m.label("and after a reply"));
		}
		idle->stop();
		auto header = with([](http::Limits& l) { l.header_timeout = 300ms; });
		header->start(0);
		{
			Client c(header->port(), m.client);
			const std::string req = get("/", "X-Pad: " + std::string(40, 'a') + "\r\n");
			const auto t0 = clk::now();
			bool closed = false;
			for (std::size_t i = 0; i < req.size() && !closed; ++i) {   // a byte every 50 ms
				c.send(req.substr(i, 1));
				closed = c.read(50ms) == 0;
			}
			const double s = since(t0);
			check(closed && s > 0.25 && s < 2.0, m.label("header_timeout 300 ms: trickled headers close at it (" + std::to_string(s) + " s)"));
		}
		header->stop();
		auto inactive = with([](http::Limits& l) { l.inactivity_timeout = 300ms; });
		inactive->start(0);
		{
			Client c(inactive->port(), m.client);
			c.send("POST /size HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789");
			const auto [closed, s] = c.until_closed(3000ms);
			check(closed && s > 0.25 && s < 2.0 && c.in.empty(), m.label("inactivity_timeout 300 ms: a stalled body closes at it (" + std::to_string(s) + " s)"));
		}
		inactive->stop();
		auto request = with([](http::Limits& l) { l.request_timeout = 400ms; });
		request->start(0);
		{  // a byte every 50 ms keeps every other bound happy; the request's own bound ends it
			Client c(request->port(), m.client);
			c.send("POST /size HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n");
			const auto t0 = clk::now();
			bool closed = false;
			for (int i = 0; i < 100 && !closed; ++i) {
				c.send("x");
				closed = c.read(50ms) == 0;
			}
			const double s = since(t0);
			check(closed && s > 0.3 && s < 2.0 && c.in.empty(), m.label("request_timeout 400 ms: a body that keeps moving ends at it (" + std::to_string(s) + " s)"));
		}
		request->stop();
	}
	{  // shutdown_grace: stop() forces a request that ignores its cancellation after the grace, twice over
		auto limits = quiet_limits();
		limits.shutdown_grace = 300ms;
		auto svc = m.service(app, limits);
		svc->start(0);
		const std::string id = std::string(m.name) + "-grace";
		Client c(svc->port(), m.client);
		c.send(get("/hold?id=" + id));
		until([&] { return app.hold(id)->started.load(); });
		const auto t0 = clk::now();
		svc->stop();
		const double s = since(t0);
		check(s > 0.5 && s < 2.5, m.label("shutdown_grace 300 ms: stop() returns after the grace, twice (" + std::to_string(s) + " s)"));
		check(c.until_closed(1000ms).first, m.label("and the forced connection is closed"));
		app.hold(id)->release = true;
	}
	{  // max_body: a body at the cap is buffered, one byte over is answered 413
		auto limits = quiet_limits();
		limits.max_body = 1000;
		auto svc = m.service(app, limits);
		svc->start(0);
		Client c(svc->port(), m.client);
		c.send("POST /size HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n" + std::string(1000, 'x'));
		const auto at = c.response();
		check(status(at, 200) && body(at) == "1000", m.label("max_body 1000: a 1000-byte body is buffered"));
		Client over(svc->port(), m.client);
		over.send("POST /size HTTP/1.1\r\nHost: x\r\nContent-Length: 1001\r\n\r\n" + std::string(1001, 'x'));
		check(status(over.response(), 413), m.label("a 1001-byte body is answered 413"));
		svc->stop();
	}
	if (m.server) {
		{  // tls_handshake: a client that never sends a ClientHello is closed at the deadline
			auto limits = quiet_limits();
			limits.tls_handshake = 300ms;
			auto svc = m.service(app, limits);
			svc->start(0);
			const int before = m.deadline_failures->load();
			Client silent(svc->port(), m.client, /*handshake=*/false);
			const auto [closed, s] = silent.until_closed(3000ms);
			check(closed && s > 0.25 && s < 2.0, m.label("tls_handshake 300 ms: a silent client is closed at it (" + std::to_string(s) + " s)"));
			check(until([&] { return m.deadline_failures->load() == before + 1; }), m.label("and reported as deadline"));
			svc->stop();
		}
		{  // tls_handshakes: a second handshake while one is pending is refused
			auto limits = quiet_limits();
			limits.tls_handshake = 1000ms;
			limits.tls_handshakes = 1;
			auto svc = m.service(app, limits);
			svc->start(0);
			const int before = m.handshake_failures->load();
			Client pending(svc->port(), m.client, /*handshake=*/false);
			std::this_thread::sleep_for(100ms);   // its session holds the one slot
			Client second(svc->port(), m.client);
			check(!second.ok() && until([&] { return m.handshake_failures->load() == before + 1; }),
				m.label("tls_handshakes 1: a second handshake while one is pending is refused, reported as handshakes"));
			pending.drop();
			bool served = false;
			for (int i = 0; i < 50 && !served; ++i) {   // the slot comes back when the pending one ends
				Client next(svc->port(), m.client);
				served = next.ok() && next.send(get("/")) && status(next.response(), 200);
				if (!served) { std::this_thread::sleep_for(20ms); }
			}
			check(served, m.label("and one is served once the slot is free"));
			svc->stop();
		}
	}
}

static void refusals() {
	App app;
	const auto refused = [&](auto set, bool tls) {
		http::Limits limits;
		set(limits);
		try {
			if (tls) { http::HttpAsioService svc(app, 1, limits, http::Tls{server_tls(), {}}); }
			else { http::HttpAsioService svc(app, 1, limits); }
		} catch (const std::invalid_argument&) { return true; }
		return false;
	};
	check(refused([](http::Limits& l) { l.idle_timeout = -1ms; }, false), "a negative timeout is refused");
	check(refused([](http::Limits& l) { l.request_timeout = std::chrono::hours(25); }, false), "a timeout past 24 hours is refused");
	check(refused([](http::Limits& l) { l.max_in_flight = std::size_t(1) << 40; }, false), "a max_in_flight past int is refused");
	check(refused([](http::Limits& l) { l.tls_handshakes = 0; }, true), "TLS with no handshake cap is refused");
	check(refused([](http::Limits& l) { l.tls_handshake = 0ms; }, true), "TLS with no handshake deadline is refused");
	check(!refused([](http::Limits& l) { l.tls_handshakes = 0; }, false), "a plain service ignores the TLS fields");
}

static void close_idle(const Mode& m, App& app) {
	auto svc = m.service(app, http::Limits{});   // the defaults: no timeout fires within the section
	svc->start(0);
	const auto port = svc->port();
	const std::string id = std::string(m.name) + "-close-idle";
	Client idle(port, m.client), busy(port, m.client);
	idle.send(get("/"));
	check(status(idle.response(), 200), m.label("an idle-to-be connection is served"));
	busy.send(get("/hold?id=" + id));
	until([&] { return app.hold(id)->started.load(); });
	const auto t0 = clk::now();
	svc->close_idle();
	const auto [closed, s] = idle.until_closed(2000ms);
	check(closed && s < 0.5, m.label("close_idle() closes an idle connection at once (" + std::to_string(s) + " s)"));
	std::this_thread::sleep_for(100ms);
	check(busy.read(1ms) < 0, m.label("and leaves a busy one open"));
	app.hold(id)->release = true;
	const auto reply = busy.response();
	check(status(reply, 200) && body(reply) == "held" && closes(reply), m.label("whose reply says Connection: close"));
	check(busy.until_closed(1000ms).first, m.label("and which closes after it"));
	Client later(port, m.client);
	later.send(get("/"));
	const auto first = later.response();
	later.send(get("/"));
	const auto second = later.response();
	check(status(first, 200) && !closes(first) && status(second, 200), m.label("a connection opened after it is kept alive"));
	check(since(t0) < 5.0, m.label("well within the default timeouts"));
	svc->stop();
}

static void stop_accepting(const Mode& m, App& app) {
	{
		auto svc = m.service(app, http::Limits{});
		svc->start(0);
		const auto port = svc->port();
		const std::string id = std::string(m.name) + "-stop";
		Client kept(port, m.client), held(port, m.client);
		kept.send(get("/"));
		const auto first = kept.response();
		check(status(first, 200) && !closes(first), m.label("a keep-alive connection is served"));
		held.send(get("/hold?id=" + id));
		until([&] { return app.hold(id)->started.load(); });
		svc->stop_accepting();
		Client late(port, m.client);
		check(!late.ok() || (late.send(get("/")), late.until_closed(2000ms).first && late.in.empty()), m.label("after stop_accepting() a new connection is refused"));
		kept.send(get("/"));
		const auto refused = kept.response();
		check(status(refused, 503) && closes(refused) && refused.find("Retry-After") == std::string::npos,
			m.label("a request on the open keep-alive connection is answered 503, Connection: close"));
		check(kept.until_closed(1000ms).first, m.label("which then closes"));
		task::cancel_source give_up;
		auto waited = std::async(std::launch::async, [&] { return svc->wait_requests(give_up.token()); });
		check(waited.wait_for(200ms) == std::future_status::timeout, m.label("wait_requests() waits for the held request"));
		app.hold(id)->release = true;
		const auto reply = held.response();
		check(status(reply, 200) && body(reply) == "held" && closes(reply), m.label("whose reply says Connection: close"));
		check(waited.wait_for(2000ms) == std::future_status::ready && waited.get(), m.label("and wait_requests() returns true once it is written"));
		check(held.until_closed(1000ms).first, m.label("and its connection closes"));
		svc->stop();
	}
	{  // a request whose headers are still arriving at stop_accepting() is counted, served and closed
		auto svc = m.service(app, http::Limits{});
		svc->start(0);
		Client c(svc->port(), m.client);
		c.send("GET / HTTP/1.1\r\nHost: x\r\n");
		std::this_thread::sleep_for(100ms);   // the server reads the first bytes
		svc->stop_accepting();
		task::cancel_source give_up;
		auto waited = std::async(std::launch::async, [&] { return svc->wait_requests(give_up.token()); });
		check(waited.wait_for(300ms) == std::future_status::timeout, m.label("wait_requests() counts a request from its first byte"));
		c.send("\r\n");
		const auto reply = c.response();
		check(status(reply, 200) && body(reply) == "ok" && closes(reply), m.label("which is served, Connection: close"));
		check(waited.wait_for(2000ms) == std::future_status::ready && waited.get(), m.label("and then wait_requests() returns true"));
		svc->stop();
	}
	{  // wait_requests() ends at its token while a request runs
		auto svc = m.service(app, http::Limits{});
		svc->start(0);
		const std::string id = std::string(m.name) + "-token";
		Client c(svc->port(), m.client);
		c.send(get("/hold?id=" + id));
		until([&] { return app.hold(id)->started.load(); });
		svc->stop_accepting();
		task::cancel_source give_up;
		auto waited = std::async(std::launch::async, [&] { return svc->wait_requests(give_up.token()); });
		std::this_thread::sleep_for(100ms);
		give_up.request();
		check(waited.wait_for(2000ms) == std::future_status::ready && !waited.get(), m.label("wait_requests() returns false at its token"));
		app.hold(id)->release = true;
		svc->stop();
	}
}

static void held_poll(const Mode& m, App& app) {
	auto limits = http::Limits{};
	limits.idle_timeout = 300ms;
	limits.header_timeout = 300ms;
	limits.inactivity_timeout = 300ms;
	auto svc = m.service(app, limits);
	svc->start(0);
	for (const std::string path : {"/hold", "/stream-hold"}) {
		const std::string id = std::string(m.name) + "-poll" + path;
		Client c(svc->port(), m.client);
		// The headers complete in one send; the body, when there is one, arrives whole and is never read.
		c.send(path == "/hold" ? get("/hold?id=" + id) : "POST /stream-hold?id=" + id + " HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello");
		until([&] { return app.hold(id)->started.load(); });
		std::this_thread::sleep_for(1000ms);   // over three times either timeout
		check(c.read(1ms) < 0, m.label("a held poll (" + path + ") stays open past the header and inactivity timeouts"));
		app.hold(id)->release = true;
		const auto reply = c.response();
		check(status(reply, 200) && body(reply) == "held", m.label("and its reply arrives when it is released"));
	}
	svc->stop();
}

int main() {
	std::signal(SIGPIPE, SIG_IGN);
	App app;
	std::atomic<int> handshakes{0}, deadlines{0};
	SSL_CTX* client = SSL_CTX_new(TLS_client_method());
	SSL_CTX_set_verify(client, SSL_VERIFY_NONE, nullptr);
	const Mode modes[] = {{"plain", nullptr, nullptr, nullptr, nullptr}, {"tls", server_tls(), client, &handshakes, &deadlines}};
	for (const auto& m : modes) {
		std::printf("[B] bounds, %s\n", m.name);
		bounds(m, app);
		std::printf("[I] close_idle, %s\n", m.name);
		close_idle(m, app);
		std::printf("[S] stop_accepting, %s\n", m.name);
		stop_accepting(m, app);
		std::printf("[P] held poll, %s\n", m.name);
		held_poll(m, app);
	}
	std::printf("[B] refusals\n");
	refusals();
	SSL_CTX_free(client);
	std::printf("limits: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
