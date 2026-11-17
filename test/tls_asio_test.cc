// The TLS connection factory over real TCP: a client that trusts the server's certificate
// gets its reply; the failure hook names why other connections ended before serving: plain
// HTTP sent to the TLS port (protocol), a client that refuses the certificate
// (certificate), and a client that never sends a ClientHello (deadline). The stream itself:
// keep-alive across a long idle (OpenSSL's record buffers released and reacquired), pipelined
// requests, bodies far larger than a record in both directions under a slow reader, a peer
// that leaves with or without close_notify, and a stop with idle connections.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <openssl/crypto.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif

#include "http_asio_tls.h"
#include "reactor_tls.h"

using namespace std::chrono_literals;

static int failures = 0;
static void check(bool ok, const char* what) {
	std::printf("  %s: %s\n", ok ? "ok" : "FAILED", what);
	if (!ok) { ++failures; }
}

// "/" says hello; "/big" sends kBig patterned bytes; "/sum" answers its body's size and sum.
constexpr std::size_t kBig = 4 * 1024 * 1024;
static char pattern(std::size_t i) { return static_cast<char>('a' + i % 23); }
struct Hello : http::HttpHandler {
	task::task<void> handle(http::Request& r, http::ResponseWriter& w) override {
		if (r.path == "/big") {
			std::string body(kBig, '\0');
			for (std::size_t i = 0; i < body.size(); ++i) { body[i] = pattern(i); }
			w.send(200, body);
		} else if (r.path == "/sum") {
			std::uint64_t sum = 0;
			for (unsigned char c : r.body) { sum += c; }
			w.send(200, std::to_string(r.body.size()) + ":" + std::to_string(sum));
		} else {
			w.send(200, "hello");
		}
		co_return;
	}
};

// The server's OpenSSL live bytes, counted through OpenSSL's allocator hooks (set before any
// other OpenSSL call): every thread but the test's own, which runs the clients.
static std::atomic<long long> openssl_bytes{0};
static thread_local bool client_thread = false;
static std::size_t usable(void* p) {
#ifdef __APPLE__
	return p ? malloc_size(p) : 0;
#else
	return p ? malloc_usable_size(p) : 0;
#endif
}
static void* counted_malloc(std::size_t n, const char*, int) {
	void* p = std::malloc(n);
	if (!client_thread) { openssl_bytes += static_cast<long long>(usable(p)); }
	return p;
}
static void counted_free(void* p, const char*, int) {
	if (!client_thread) { openssl_bytes -= static_cast<long long>(usable(p)); }
	std::free(p);
}
static void* counted_realloc(void* p, std::size_t n, const char*, int) {
	const auto before = static_cast<long long>(usable(p));
	void* q = std::realloc(p, n);
	if (!client_thread && (q || !n)) { openssl_bytes += static_cast<long long>(usable(q)) - before; }
	return q;
}

// A blocking TLS client connection to the server.
struct Client {
	asio::io_context io;
	asio::ssl::stream<asio::ip::tcp::socket> s;
	Client(unsigned short port, asio::ssl::context& context) : s(io, context) {
		s.next_layer().connect({asio::ip::make_address("127.0.0.1"), port});
		reactor::tls::bind_hostname(s.native_handle(), "localhost");
		s.handshake(asio::ssl::stream_base::client);
	}
	void send(const std::string& bytes) { asio::write(s, asio::buffer(bytes)); }
	// One response: its head and exactly Content-Length body bytes; "" when the stream ends first.
	std::string response() {
		while (in.find("\r\n\r\n") == std::string::npos) { if (!more()) { return ""; } }
		const auto end = in.find("\r\n\r\n") + 4;
		const auto at = in.find("Content-Length: ");
		const std::size_t length = at < end ? std::stoul(in.substr(at + 16)) : 0;
		while (in.size() < end + length) { if (!more()) { return ""; } }
		std::string out = in.substr(0, end + length);
		in.erase(0, end + length);
		return out;
	}
	std::string get(const std::string& path) {
		send("GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n");
		return response();
	}
	bool more() {
		char b[16384];
		asio::error_code ec;
		const std::size_t n = s.read_some(asio::buffer(b), ec);
		if (ec) { return false; }
		in.append(b, n);
		return true;
	}
	std::string in;
};

static bool says_hello(const std::string& r) { return r.rfind("HTTP/1.1 200", 0) == 0 && r.size() >= 5 && r.compare(r.size() - 5, 5, "hello") == 0; }

// A self-signed certificate for localhost and 127.0.0.1, and its key, as PEM files.
static void self_signed(const std::string& cert_path, const std::string& key_path) {
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
	X509V3_CTX v3;
	X509V3_set_ctx_nodb(&v3);
	X509V3_set_ctx(&v3, x, x, nullptr, nullptr, 0);
	X509_EXTENSION* ext = X509V3_EXT_conf(nullptr, &v3, "subjectAltName", "DNS:localhost,IP:127.0.0.1");
	X509_add_ext(x, ext, -1);
	X509_EXTENSION_free(ext);
	X509_sign(x, key, EVP_sha256());
	FILE* f = std::fopen(cert_path.c_str(), "wb");
	PEM_write_X509(f, x);
	std::fclose(f);
	f = std::fopen(key_path.c_str(), "wb");
	PEM_write_PrivateKey(f, key, nullptr, nullptr, 0, nullptr, nullptr);
	std::fclose(f);
	X509_free(x);
	EVP_PKEY_free(key);
}

static int dial(unsigned short port) {
	for (int i = 0; i < 300; ++i) {
		int fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0) {
			timeval tv{.tv_sec = 5, .tv_usec = 0};
			::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
			return fd;
		}
		::close(fd);
		std::this_thread::sleep_for(5ms);
	}
	return -1;
}

// A TLS GET over a client context; the response, or "" when the handshake fails.
static std::string tls_get(unsigned short port, asio::ssl::context& client) {
	try {
		asio::io_context io;
		asio::ssl::stream<asio::ip::tcp::socket> s(io, client);
		s.next_layer().connect({asio::ip::make_address("127.0.0.1"), port});
		reactor::tls::bind_hostname(s.native_handle(), "localhost");
		s.handshake(asio::ssl::stream_base::client);
		const std::string req = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
		asio::write(s, asio::buffer(req));
		std::string out;
		char b[1024];
		asio::error_code ec;
		for (std::size_t n; (n = s.read_some(asio::buffer(b), ec)) > 0 && !ec;) { out.append(b, n); }
		return out;
	} catch (const std::exception&) {
		return "";
	}
}

int main() {
	if (!CRYPTO_set_mem_functions(counted_malloc, counted_realloc, counted_free)) { return EXIT_FAILURE; }
	client_thread = true;
	std::signal(SIGPIPE, SIG_IGN);
	std::printf("  OpenSSL headers %s, library %s\n", OPENSSL_VERSION_TEXT, OpenSSL_version(OPENSSL_VERSION));
	check(OPENSSL_version_major() == OPENSSL_VERSION_MAJOR && OPENSSL_version_minor() == OPENSSL_VERSION_MINOR,
		"the OpenSSL headers compiled against are the library that runs");
	char dir[] = "/tmp/http_tls_XXXXXX";
	if (!::mkdtemp(dir)) { return EXIT_FAILURE; }
	const std::string cert = std::string(dir) + "/cert.pem", key = std::string(dir) + "/key.pem";
	self_signed(cert, key);

	std::mutex mu;
	std::map<http::TlsFailure, int> seen;
	auto count = [&](http::TlsFailure why) {
		std::lock_guard lk(mu);
		++seen[why];
	};
	auto seen_of = [&](http::TlsFailure why) {
		std::lock_guard lk(mu);
		return seen[why];
	};
	auto server = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(*server, reactor::tls::Options{cert, key, ""}, reactor::tls::Role::Server);
	Hello hello;
	unsigned short port = 0;
	http::HttpAsioService svc(hello, 1, 64, http::tls_connection_factory(server, 300ms, 64, 1024, count), 1024);
	svc.start(0); port = svc.port();

	asio::ssl::context trusting(asio::ssl::context::tls), refusing(asio::ssl::context::tls);
	reactor::tls::configure(trusting, reactor::tls::Options{"", "", cert}, reactor::tls::Role::Client);
	reactor::tls::configure(refusing, reactor::tls::Options{"", "", "", true}, reactor::tls::Role::Client);  // system roots only

	const std::string ok = tls_get(port, trusting);
	check(ok.rfind("HTTP/1.1 200", 0) == 0 && ok.find("hello") != std::string::npos, "a client trusting the certificate gets its reply");

	{  // plain HTTP to the TLS port
		int fd = dial(port);
		const std::string req = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";
		::send(fd, req.data(), req.size(), 0);
		char b[256];
		while (::recv(fd, b, sizeof(b), 0) > 0) {}
		::close(fd);
	}
	check(tls_get(port, refusing).empty(), "a client on system roots refuses the self-signed certificate");
	{  // a connection that never sends a ClientHello
		int fd = dial(port);
		char b[16];
		while (::recv(fd, b, sizeof(b), 0) > 0) {}
		::close(fd);
	}
	for (int i = 0; i < 100 && (!seen_of(http::TlsFailure::protocol) || !seen_of(http::TlsFailure::certificate) || !seen_of(http::TlsFailure::deadline)); ++i) {
		std::this_thread::sleep_for(20ms);
	}
	check(seen_of(http::TlsFailure::protocol) == 1, "plain HTTP to the TLS port is reported as protocol");
	check(seen_of(http::TlsFailure::certificate) == 1, "a refused certificate is reported as certificate");
	check(seen_of(http::TlsFailure::deadline) == 1, "a silent client is reported at the handshake deadline");

	const auto active_is = [&](std::size_t n) {
		for (int i = 0; i < 200 && svc.connection_stats().active != n; ++i) { std::this_thread::sleep_for(10ms); }
		return svc.connection_stats().active == n;
	};
	{  // keep-alive across a long idle: the record buffers are released while idle and come back
		constexpr int kIdle = 8;
		std::vector<std::unique_ptr<Client>> idle;
		const long long before = openssl_bytes.load();
		bool served = true;
		for (int i = 0; i < kIdle; ++i) {
			idle.push_back(std::make_unique<Client>(port, trusting));
			served = served && says_hello(idle.back()->get("/"));
		}
		check(served, "each idle-to-be connection is served");
		std::this_thread::sleep_for(1500ms);
		// An asio::ssl::stream's BIO pair alone would hold two 17 KiB halves per connection.
		const long long per_connection = (openssl_bytes.load() - before) / kIdle;
		std::printf("  server OpenSSL bytes per idle connection: %lld\n", per_connection);
		check(per_connection < 48 * 1024, "an idle TLS connection holds no record or BIO buffers (OpenSSL under 48 KiB)");
		bool again = true;
		for (auto& c : idle) { again = again && says_hello(c->get("/")) && says_hello(c->get("/")); }
		check(again, "a request after a long idle is served on every connection, twice");
	}
	check(active_is(0), "every idle connection ends when its client leaves");
	{  // pipelined requests in one write, answered in order
		Client c(port, trusting);
		c.send("GET / HTTP/1.1\r\nHost: x\r\n\r\nGET /sum HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcGET / HTTP/1.1\r\nHost: x\r\n\r\n");
		const auto a = c.response(), b = c.response(), d = c.response();
		check(says_hello(a) && b.find("\r\n\r\n3:294") != std::string::npos && says_hello(d), "three pipelined requests are answered in order");
	}
	{  // a small reply's head and body arrive in one record (the server joins them, as asio::ssl::stream did)
		// Each record read reports its 5-byte header (set before the handshake, so the record layer
		// has it); an encrypted record's type byte is application data.
		static int records = 0;
		asio::ssl::context counting(asio::ssl::context::tls);
		reactor::tls::configure(counting, reactor::tls::Options{"", "", cert}, reactor::tls::Role::Client);
		::SSL_CTX_set_msg_callback(counting.native_handle(), [](int write, int, int type, const void* buf, std::size_t len, SSL*, void*) {
			if (!write && type == SSL3_RT_HEADER && len >= 1 && *static_cast<const unsigned char*>(buf) == SSL3_RT_APPLICATION_DATA) { ++records; }
		});
		Client c(port, counting);
		const bool first = says_hello(c.get("/"));   // the session tickets arrive around this one
		records = 0;
		const bool second = says_hello(c.get("/"));
		std::printf("  records in a small reply: %d\n", records);
		check(first && second && records == 1, "a small reply's head and body arrive in one TLS record");
	}
	{  // TlsStream: a write found done never completes inside its initiating call, nested or not
		asio::io_context io(1);
		asio::ip::tcp::acceptor acceptor(io, {asio::ip::make_address("127.0.0.1"), 0});
		std::string got;
		std::thread peer([&, p = acceptor.local_endpoint().port()] {
			try {
				asio::io_context client_io;
				asio::ssl::stream<asio::ip::tcp::socket> s(client_io, trusting);
				s.next_layer().connect({asio::ip::make_address("127.0.0.1"), p});
				reactor::tls::bind_hostname(s.native_handle(), "localhost");
				s.handshake(asio::ssl::stream_base::client);
				char b[16];
				std::size_t n = 0;
				while (n < 10) { n += s.read_some(asio::buffer(b + n, sizeof b - n)); }
				got.assign(b, n);
			} catch (const std::exception&) {}
		});
		http::detail::TlsStream tls(acceptor.accept(), *server);
		asio::error_code handshake = asio::error::would_block;
		tls.async_handshake(asio::ssl::stream_base::server, [&](asio::error_code e) { handshake = e; });
		io.run();
		io.restart();
		const std::array<asio::const_buffer, 2> one{asio::buffer("hel", 3), asio::buffer("lo", 2)}, two{asio::buffer("wor", 3), asio::buffer("ld", 2)};
		bool first = false, second = false, inside = false, second_inside = true;
		asio::async_write(tls, one, [&](asio::error_code e, std::size_t n) {
			first = !e && n == 5;
			inside = true;
			asio::async_write(tls, two, [&](asio::error_code e2, std::size_t n2) { second = !e2 && n2 == 5; second_inside = inside; });
			inside = false;
		});
		const bool in_call = first;
		io.run();
		peer.join();
		check(!handshake && !in_call && first, "a TLS write found done completes after its initiating call");
		check(second && !second_inside, "and so does one started inside that completion");
		check(got == "helloworld", "both writes arrive, in order");
	}
	{  // a body far larger than a record each way; the client reads the download slowly
		Client c(port, trusting);
		std::string body(kBig, '\0');
		std::uint64_t sum = 0;
		for (std::size_t i = 0; i < body.size(); ++i) { body[i] = pattern(i); sum += static_cast<unsigned char>(body[i]); }
		c.send("POST /sum HTTP/1.1\r\nHost: x\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body);
		const auto up = c.response();
		check(up.find("\r\n\r\n" + std::to_string(kBig) + ":" + std::to_string(sum)) != std::string::npos, "a 4 MiB upload arrives whole");
		c.send("GET /big HTTP/1.1\r\nHost: x\r\n\r\n");
		std::this_thread::sleep_for(300ms);   // the server's writes wait for the socket meanwhile
		const auto down = c.response();
		const auto at = down.find("\r\n\r\n") + 4;
		bool same = down.size() == at + kBig;
		for (std::size_t i = 0; same && i < kBig; ++i) { same = down[at + i] == pattern(i); }
		check(same, "a 4 MiB download arrives whole under a slow reader");
		check(says_hello(c.get("/")), "the connection serves on after both");
	}
	{  // a peer that leaves while idle, with and without close_notify
		auto polite = std::make_unique<Client>(port, trusting), abrupt = std::make_unique<Client>(port, trusting);
		check(says_hello(polite->get("/")) && says_hello(abrupt->get("/")), "two connections are served");
		check(active_is(2), "both are held idle");
		asio::error_code ignored;
		polite->s.shutdown(ignored);
		check(active_is(1), "a close_notify ends its idle connection");
		abrupt->s.next_layer().close();
		check(active_is(0), "a TCP close without close_notify ends its idle connection");
	}
	auto lingering = std::make_unique<Client>(port, trusting);
	check(says_hello(lingering->get("/")), "a connection left idle is served");
	const auto stop_started = std::chrono::steady_clock::now();
	svc.stop();
	check(std::chrono::steady_clock::now() - stop_started < 3s, "stop ends an idle TLS connection's wait promptly");
	check(lingering->get("/").empty(), "the stopped connection is closed");
	std::remove(cert.c_str());
	std::remove(key.c_str());
	::rmdir(dir);
	std::printf("tls_asio: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
