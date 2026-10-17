// The TLS connection factory over real TCP: a client that trusts the server's certificate
// gets its reply; the failure hook names why other connections ended before serving: plain
// HTTP sent to the TLS port (protocol), a client that refuses the certificate
// (certificate), and a client that never sends a ClientHello (deadline).
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include "http_asio_tls.h"
#include "reactor_tls.h"

using namespace std::chrono_literals;

static int failures = 0;
static void check(bool ok, const char* what) {
	std::printf("  %s: %s\n", ok ? "ok" : "FAILED", what);
	if (!ok) { ++failures; }
}

struct Hello : http::HttpHandler {
	void handle(const http::Request&, http::ResponseWriter& w) override { w.send(200, "hello"); }
};

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

static unsigned short free_port() {
	int fd = ::socket(AF_INET, SOCK_STREAM, 0);
	sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
	socklen_t len = sizeof(a); ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
	unsigned short p = ntohs(a.sin_port); ::close(fd); return p;
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
	std::signal(SIGPIPE, SIG_IGN);
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
	const unsigned short port = free_port();
	http::HttpAsioService svc(hello, 1, 0, 64, http::tls_connection_factory_timed(server, 300ms, 64, 1024, count), 1024);
	svc.start(port);

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
	svc.stop();
	std::remove(cert.c_str());
	std::remove(key.c_str());
	::rmdir(dir);
	std::printf("tls_asio: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
