#pragma once
#include "http_client_tls.h"
#include <openssl/x509v3.h>
#include <atomic>
#include <future>
#include <mutex>
#include <poll.h>
#include <sys/socket.h>
#include <thread>

namespace tls_fixture {
inline void require(bool value) {
	if (!value) throw std::runtime_error("TLS network fixture invariant failed");
}
struct Credentials {
	std::shared_ptr<asio::ssl::context> server, client;
	Credentials() {
		std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(EVP_EC_gen("P-256"), EVP_PKEY_free);
		std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), X509_free);
		require(key && certificate);
		require(X509_set_version(certificate.get(), 2) == 1);
		require(ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1) == 1);
		require(X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60) != nullptr);
		require(X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 3600) != nullptr);
		require(X509_set_pubkey(certificate.get(), key.get()) == 1);
		std::unique_ptr<X509_NAME, decltype(&X509_NAME_free)> name(X509_NAME_new(), X509_NAME_free);
		require(name != nullptr);
		require(X509_NAME_add_entry_by_txt(name.get(), "CN", MBSTRING_ASC,
			reinterpret_cast<const unsigned char *>("localhost"), -1, -1, 0) == 1);
		require(X509_set_subject_name(certificate.get(), name.get()) == 1);
		require(X509_set_issuer_name(certificate.get(), name.get()) == 1);
		X509V3_CTX extensions;
		X509V3_set_ctx_nodb(&extensions);
		X509V3_set_ctx(&extensions, certificate.get(), certificate.get(), nullptr, nullptr, 0);
		for (auto text : {"subjectAltName=DNS:localhost,DNS:elsewhere.localhost",
			"basicConstraints=critical,CA:TRUE", "keyUsage=critical,digitalSignature,keyCertSign"}) {
			const std::string specification(text);
			const auto equal = specification.find('=');
			std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
				X509V3_EXT_conf(nullptr, &extensions, specification.substr(0, equal).c_str(),
					specification.substr(equal + 1).c_str()), X509_EXTENSION_free);
			require(extension && X509_add_ext(certificate.get(), extension.get(), -1) == 1);
		}
		require(X509_sign(certificate.get(), key.get(), EVP_sha256()) > 0);
		server = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
		client = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
		require(SSL_CTX_use_certificate(server->native_handle(), certificate.get()) == 1);
		require(SSL_CTX_use_PrivateKey(server->native_handle(), key.get()) == 1);
		require(X509_STORE_add_cert(SSL_CTX_get_cert_store(client->native_handle()), certificate.get()) == 1);
		client->set_verify_mode(asio::ssl::verify_peer);
	}
};
struct LateReply {
	std::string bytes;
	std::promise<void> release, queued;
	std::future<void> permission = release.get_future();
	std::atomic<bool> sent = false;
};
class Server {
  public:
	explicit Server(std::shared_ptr<asio::ssl::context> context,
		std::string reply = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
		std::shared_ptr<LateReply> late = {}, std::size_t split = 0)
		: context_(std::move(context)), reply_(std::move(reply)), late_(std::move(late)),
		  split_(split), acceptor_(io_, {asio::ip::address_v4::loopback(), 0}) {
		require(!split_ || split_ < reply_.size());
		acceptor_.non_blocking(true);
		endpoint_ = acceptor_.local_endpoint();
		thread_ = std::thread([this] { serve(); });
	}
	~Server() {
		stop_ = true;
		{
			std::lock_guard lock(gate_);
			if (peer_ != -1) ::shutdown(peer_, SHUT_RDWR);
		}
		thread_.join();
	}
	asio::ip::tcp::endpoint endpoint() const { return endpoint_; }
	std::future<void> first_written() { return written_.get_future(); }
  private:
	std::shared_ptr<asio::ssl::context> context_;
	std::string reply_;
	std::shared_ptr<LateReply> late_;
	std::size_t split_ = 0;
	std::promise<void> written_;
	bool wrote_ = false;
	asio::io_context io_;
	asio::ip::tcp::acceptor acceptor_;
	asio::ip::tcp::endpoint endpoint_;
	std::atomic<bool> stop_ = false;
	std::mutex gate_;
	int peer_ = -1;
	std::thread thread_;
	void serve() {
		while (!stop_) {
			pollfd ready{acceptor_.native_handle(), POLLIN, 0};
			if (::poll(&ready, 1, 20) <= 0) continue;
			asio::ssl::stream<asio::ip::tcp::socket> stream(io_, *context_);
			asio::error_code error;
			acceptor_.accept(stream.next_layer(), error);
			if (error) continue;
			stream.next_layer().non_blocking(false, error);
			if (error) continue;
			{
				std::lock_guard lock(gate_);
				peer_ = stream.next_layer().native_handle();
				if (stop_) ::shutdown(peer_, SHUT_RDWR);
			}
			stream.handshake(asio::ssl::stream_base::server, error);
			asio::streambuf input(65536);
			while (!stop_ && !error) {
				const auto size = asio::read_until(stream, input, "\r\n\r\n", error);
				if (error) break;
				input.consume(size);
				if (split_) {
					asio::write(stream, asio::buffer(reply_.data(), split_), error);
					if (!error) asio::write(stream, asio::buffer(reply_.data() + split_, reply_.size() - split_), error);
				} else asio::write(stream, asio::buffer(reply_), error);
				if (!error && !wrote_) { wrote_ = true; written_.set_value(); }
				if (!error && late_ && !late_->sent.exchange(true)) {
					if (late_->permission.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
						late_->queued.set_exception(std::make_exception_ptr(std::runtime_error("late reply gate timeout")));
						break;
					}
					asio::write(stream, asio::buffer(late_->bytes), error);
					if (error) late_->queued.set_exception(std::make_exception_ptr(std::system_error(error)));
					else late_->queued.set_value();
				}
			}
			{
				std::lock_guard lock(gate_);
				peer_ = -1;
				stream.next_layer().close(error);
			}
		}
	}
};
} // namespace tls_fixture
