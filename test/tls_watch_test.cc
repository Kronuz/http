// tls_watch_test -- a held read on a TLS connection's disconnect watch ends when its token is
// requested, also when the request lands between the lowest-layer read's wake and the TLS
// stream's own continuation. The window: a wait queued first on the socket runs first in the
// reactor pass that completes the watch's read with part of a TLS record, so its token request
// finds no operation to cancel; the stream's continuation then reads again. The client is
// OpenSSL over memory BIOs, so the test sends the record in two pieces.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "http_asio_tls.h"

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

using Stream = asio::ssl::stream<asio::ip::tcp::socket>;
using Input = http::detail::ConnectionInput<Stream>;

// An in-memory self-signed certificate; the client does not verify it.
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

// The server's TLS stream and a client whose TLS records the test sends itself.
struct Pair {
	asio::io_context io;
	std::shared_ptr<asio::ssl::context> context = server_tls();
	Stream server{io, *context};
	task::asio_scheduler scheduler{io.get_executor()};   // after io and server: destroyed first
	int fd = -1;
	SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
	SSL* ssl = SSL_new(ctx);
	BIO* in = BIO_new(BIO_s_mem());
	BIO* out = BIO_new(BIO_s_mem());
	Pair() {
		asio::ip::tcp::acceptor acceptor(io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
		fd = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(acceptor.local_endpoint().port());
		::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
		acceptor.accept(server.next_layer());
		SSL_set_bio(ssl, in, out);
		SSL_set_connect_state(ssl);
	}
	~Pair() { io.stop(); if (fd >= 0) { ::close(fd); } SSL_free(ssl); SSL_CTX_free(ctx); }
	// The client's pending TLS bytes.
	std::string take() { std::string s; char b[4096]; int n; while ((n = BIO_read(out, b, sizeof b)) > 0) { s.append(b, static_cast<std::size_t>(n)); } return s; }
	void send(const std::string& s) { std::size_t o = 0; while (o < s.size()) { const ssize_t n = ::send(fd, s.data() + o, s.size() - o, 0); if (n <= 0) { break; } o += static_cast<std::size_t>(n); } }
	// What the server sent so far goes to the client's input.
	void fill() { char b[4096]; const ssize_t n = ::recv(fd, b, sizeof b, MSG_DONTWAIT); if (n > 0) { BIO_write(in, b, static_cast<int>(n)); } }
	bool handshake() {
		bool done = false;
		server.async_handshake(asio::ssl::stream_base::server, [&](asio::error_code error) { done = !error; });
		bool client = false;
		for (auto end = clk::now() + 5s; !(done && client) && clk::now() < end;) {
			if (!client) { client = SSL_do_handshake(ssl) == 1; }
			send(take());
			fill();
			io.restart();
			io.poll();
			std::this_thread::sleep_for(1ms);
		}
		return done && client;
	}
	void settle() { io.restart(); io.run_for(150ms); }
};

struct Read { std::string data; asio::error_code error; bool done = false; };
static task::task<void> read_into(std::shared_ptr<Input> in, Read& r, task::cancel_token token) {
	std::string buf(65536, '\0');
	const http::detail::ReadResult got = co_await in->read(asio::buffer(buf), token);
	if (got.error) { r.error = got.error; } else { r.data.assign(buf, 0, got.bytes); }
	r.done = true;
}
static void run_until(Pair& p, const bool& done) {
	p.io.restart();
	for (auto end = clk::now() + 2s; !done && clk::now() < end;) { p.io.run_one_for(5ms); }
}

int main() {
	std::signal(SIGPIPE, SIG_IGN);
	Pair p;
	check(p.handshake(), "[T] the TLS handshake completes");
	auto in = std::make_shared<Input>(p.server);
	static_assert(!Input::kLazyWatch, "a TLS stream's watch reads from the start");

	// The record for "x", sent in two pieces.
	check(SSL_write(p.ssl, "x", 1) == 1, "[T] the client writes one byte");
	const std::string record = p.take();
	const std::size_t cut = 3;
	check(record.size() > cut, "[T] the record is longer than its first piece (" + std::to_string(record.size()) + " bytes)");

	task::cancel_source source;
	// A wait queued first on the socket runs first in the pass that completes the watch's read: it
	// requests the token while that read's completion is queued, so the socket cancel finds nothing.
	bool woke = false;
	p.server.lowest_layer().async_wait(asio::socket_base::wait_read, [&](asio::error_code error) {
		woke = !error;
		source.request();
	});
	in->watch([] {});   // the watch's read, queued behind the wait
	Read r;
	task::spawn(task::scheduler_ref::of(p.scheduler), read_into(in, r, source.token()));
	p.settle();
	check(!r.done, "[T] the reader waits on the pending read");
	p.send(record.substr(0, cut));
	run_until(p, r.done);
	check(woke, "[T] the first piece woke the wait queued before the watch's read");
	check(r.done && r.error == asio::error::operation_aborted, "[T] a cancel between the wake and the stream's continuation ends the read with operation_aborted");
	if (r.done) {
		p.send(record.substr(cut));
		Read next;
		task::spawn(task::scheduler_ref::of(p.scheduler), read_into(in, next, {}));
		run_until(p, next.done);
		check(next.done && !next.error && next.data == "x", "[T] and the next read returns the byte, the record completed");
	}
	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}
