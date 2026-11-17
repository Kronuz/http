#pragma once

// Optional TLS adapter. Plain HTTP users retain the same API and dependencies.
#include "http_asio.h"
#include <asio/ssl.hpp>
#include <atomic>
#include <cstring>
#include <functional>
#include <algorithm>
#include <iterator>
#include <memory>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <system_error>
#include <utility>

namespace http {

// Why a TLS connection ended before serving: past the handshake deadline (queue time
// included), the connection or handshake cap, or the handshake's own failure, by kind.
enum class TlsFailure { deadline, connections, handshakes, version, cipher, certificate, protocol, other };
using TlsFailureHook = std::function<void(TlsFailure)>;

namespace detail {
// The kind of a failed server handshake, from OpenSSL's reason code.
inline TlsFailure classify(const asio::error_code& error) {
	if (error.category() != asio::error::get_ssl_category()) { return TlsFailure::other; }
	switch (ERR_GET_REASON(static_cast<unsigned long>(error.value()))) {
	case SSL_R_HTTP_REQUEST:
	case SSL_R_HTTPS_PROXY_REQUEST:
	case SSL_R_WRONG_VERSION_NUMBER:
	case SSL_R_PACKET_LENGTH_TOO_LONG:
		return TlsFailure::protocol;  // not TLS at all, such as plain HTTP
	case SSL_R_UNSUPPORTED_PROTOCOL:
	case SSL_R_VERSION_TOO_LOW:
	case SSL_R_TLSV1_ALERT_PROTOCOL_VERSION:
		return TlsFailure::version;
	case SSL_R_NO_SHARED_CIPHER:
	case SSL_R_SSLV3_ALERT_HANDSHAKE_FAILURE:
		return TlsFailure::cipher;
	case SSL_R_TLSV1_ALERT_UNKNOWN_CA:
	case SSL_R_SSLV3_ALERT_BAD_CERTIFICATE:
	case SSL_R_SSLV3_ALERT_CERTIFICATE_EXPIRED:
	case SSL_R_SSLV3_ALERT_CERTIFICATE_UNKNOWN:
	case SSL_R_TLSV1_ALERT_DECRYPT_ERROR:
		return TlsFailure::certificate;  // the client refused this server's certificate
	default:
		return TlsFailure::other;
	}
}
// A TLS connection that keeps no buffers of its own: OpenSSL reads and writes the socket
// through a BIO over it. A read that needs bytes receives them through Asio into the caller's
// buffer (speculative, so the reactor is not re-armed per wait), and the BIO serves them from a
// copy held only until OpenSSL takes it. Idle, it holds the SSL object and what OpenSSL keeps of
// the session, nothing of the records (SSL_MODE_RELEASE_BUFFERS). The handshake, writes and small
// reads read the socket directly and wait for readiness. One read and one write may be in flight.
class TlsStream {
public:
	using executor_type = asio::ip::tcp::socket::executor_type;
	using lowest_layer_type = asio::ip::tcp::socket;

	TlsStream(asio::ip::tcp::socket socket, asio::ssl::context& context) : socket_(std::move(socket)) {
		socket_.non_blocking(true);   // the BIO's reads and writes return instead of waiting
		ssl_ = ::SSL_new(context.native_handle());
		BIO* bio = ssl_ ? ::BIO_new(method()) : nullptr;
		if (!bio) {
			::SSL_free(ssl_);
			throw std::system_error(asio::error_code(static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category()), "TLS stream");
		}
		::BIO_set_data(bio, this);
		::SSL_set_bio(ssl_, bio, bio);
		::SSL_set_mode(ssl_, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER | SSL_MODE_RELEASE_BUFFERS);
	}
	// Only with no operation in flight: the BIO is pointed at the new stream.
	TlsStream(TlsStream&& other) noexcept
		: socket_(std::move(other.socket_)), ssl_(std::exchange(other.ssl_, nullptr)), input_(std::move(other.input_)),
		  input_begin_(other.input_begin_), input_end_(other.input_end_), socket_error_(other.socket_error_), eof_(other.eof_) {
		if (ssl_) { ::BIO_set_data(::SSL_get_rbio(ssl_), this); }
	}
	TlsStream& operator=(TlsStream&&) = delete;
	~TlsStream() { ::SSL_free(ssl_); }

	executor_type get_executor() noexcept { return socket_.get_executor(); }
	lowest_layer_type& lowest_layer() noexcept { return socket_; }
	lowest_layer_type& next_layer() noexcept { return socket_; }
	SSL* native_handle() noexcept { return ssl_; }

	template<class Token>
	auto async_handshake(asio::ssl::stream_base::handshake_type type, Token&& token) {
		if (type == asio::ssl::stream_base::server) { ::SSL_set_accept_state(ssl_); }
		else { ::SSL_set_connect_state(ssl_); }
		// After the state, which resets the record layer: a record's header and body in one recv.
		::SSL_set_read_ahead(ssl_, 1);
		return start<void(asio::error_code)>([](SSL* ssl, std::size_t& n) { n = 0; return ::SSL_do_handshake(ssl) == 1; }, true, {}, std::forward<Token>(token));
	}
	template<class Buffers, class Token>
	auto async_read_some(const Buffers& buffers, Token&& token) {
		const asio::mutable_buffer b = first(buffers);
		return start<void(asio::error_code, std::size_t)>(
			[b](SSL* ssl, std::size_t& n) { return ::SSL_read_ex(ssl, b.data(), b.size(), &n) == 1; }, b.size() > 0,
			b.size() >= kMinReceive ? b : asio::mutable_buffer(), std::forward<Token>(token));
	}
	// Small buffers are joined into one record (up to kJoin bytes), as asio::ssl::stream does: a
	// reply's head and body leave in one record and one send, not two.
	template<class Buffers, class Token>
	auto async_write_some(const Buffers& buffers, Token&& token) {
		asio::const_buffer b = first(buffers);
		std::unique_ptr<char[]> joined;
		if (b.size() < kJoin) {
			std::size_t total = 0;
			for (auto it = asio::buffer_sequence_begin(buffers); it != asio::buffer_sequence_end(buffers) && total < kJoin; ++it) {
				total += std::min(it->size(), kJoin - total);
			}
			if (total > b.size()) {
				joined.reset(new char[total]);
				asio::buffer_copy(asio::buffer(joined.get(), total), buffers);
				b = asio::const_buffer(joined.get(), total);
			}
		}
		return start<void(asio::error_code, std::size_t)>(
			[b, joined = std::move(joined)](SSL* ssl, std::size_t& n) { return ::SSL_write_ex(ssl, b.data(), b.size(), &n) == 1; },
			b.size() > 0, {}, std::forward<Token>(token));
	}

private:
	static constexpr std::size_t kJoin = 8192;        // asio::ssl::stream's linearisation size
	static constexpr std::size_t kMinReceive = 512;   // a smaller read waits and reads directly
	enum class Want { nothing, read, write };

	// One SSL call: done (error and bytes set), or the readiness it waits for.
	// With `receiving`, the BIO serves only received bytes and an empty input wants a read.
	template<class Call>
	Want step(Call& call, bool receiving, asio::error_code& error, std::size_t& bytes) {
		::ERR_clear_error();
		socket_error_ = {};
		std::size_t n = 0;
		receiving_ = receiving;
		const bool done = call(ssl_, n);
		receiving_ = false;
		if (done) { error = {}; bytes = n; return Want::nothing; }
		bytes = 0;
		switch (::SSL_get_error(ssl_, 0)) {
		case SSL_ERROR_WANT_READ: return Want::read;
		case SSL_ERROR_WANT_WRITE: return Want::write;
		case SSL_ERROR_ZERO_RETURN: error = asio::error::eof; break;   // the peer's close_notify
		case SSL_ERROR_SYSCALL:
			if (socket_error_) { error = socket_error_; break; }
			[[fallthrough]];
		case SSL_ERROR_SSL: {
			const unsigned long code = ::ERR_get_error();
			// A transport EOF without close_notify, as asio::ssl::stream reports it.
			if (eof_ && (!code || ERR_GET_REASON(code) == SSL_R_UNEXPECTED_EOF_WHILE_READING)) { error = asio::ssl::error::stream_truncated; }
			else if (code) { error = asio::error_code(static_cast<int>(code), asio::error::get_ssl_category()); }
			else { error = asio::ssl::error::unspecified_system_error; }
			break;
		}
		default: error = asio::ssl::error::unexpected_result; break;
		}
		return Want::nothing;
	}

	// The operation: SSL calls until one is done, receiving or waiting for readiness between them.
	// A result found without either is deferred, never delivered inside the initiating call: it
	// runs after the current handler, from the thread's own queue, so it neither locks nor wakes
	// the reactor. A read with `room` receives into it: the caller's buffer, unused until then.
	template<class Call>
	struct Operation {
		TlsStream* stream;
		Call call;
		bool any;                     // false: an empty buffer, done at once
		asio::mutable_buffer room;    // empty: wait for readiness instead of receiving
		enum class Phase { start, waiting, deferred } phase = Phase::start;
		asio::error_code error;
		std::size_t bytes = 0;
		template<class Self>
		void operator()(Self& self, asio::error_code received_error, std::size_t received) {
			if (received_error == asio::error::eof) { stream->eof_ = true; }   // OpenSSL reports it
			else if (received_error) { error = received_error; return finish(self); }
			else { stream->take(room, received); }
			(*this)(self);
		}
		template<class Self>
		void operator()(Self& self, asio::error_code wait_error = {}) {
			if (phase == Phase::deferred) { return finish(self); }
			if (phase == Phase::waiting && wait_error) { error = wait_error; return finish(self); }
			if (any) {
				switch (stream->step(call, room.size() > 0, error, bytes)) {
				case Want::read:
					if (room.size() > 0) {
						phase = Phase::waiting;
						return stream->socket_.async_receive(room, std::move(self));
					}
					return wait(self, asio::socket_base::wait_read);
				case Want::write: return wait(self, asio::socket_base::wait_write);
				case Want::nothing: break;
				}
			}
			if (phase == Phase::start) {
				phase = Phase::deferred;
				auto executor = self.get_io_executor();
				return asio::defer(executor, std::move(self));
			}
			finish(self);
		}
		template<class Self>
		void wait(Self& self, asio::socket_base::wait_type type) {
			phase = Phase::waiting;
			stream->socket_.async_wait(type, std::move(self));
		}
		template<class Self>
		void finish(Self& self) {
			if constexpr (requires { self.complete(error, bytes); }) { self.complete(error, bytes); }
			else { self.complete(error); }
		}
	};
	template<class Signature, class Call, class Token>
	auto start(Call call, bool any, asio::mutable_buffer room, Token&& token) {
		return asio::async_compose<Token, Signature>(Operation<Call>{this, std::move(call), any, room}, token, socket_);
	}
	// Keeps `size` received bytes for the BIO; the read's buffer is about to take the plaintext.
	void take(asio::mutable_buffer room, std::size_t size) {
		input_.reset(new char[size]);
		std::memcpy(input_.get(), room.data(), size);
		input_begin_ = 0;
		input_end_ = size;
	}

	template<class Buffers>
	static auto first(const Buffers& buffers) {
		for (auto it = asio::buffer_sequence_begin(buffers); it != asio::buffer_sequence_end(buffers); ++it) {
			if (it->size()) { return *it; }
		}
		return typename std::iterator_traits<decltype(asio::buffer_sequence_begin(buffers))>::value_type{};
	}

	// The BIO: the socket's own non-blocking reads and writes. A socket error is kept for
	// step(), and a read of EOF for BIO_eof.
	static int bio_write(BIO* bio, const char* data, std::size_t size, std::size_t* written) {
		auto* self = static_cast<TlsStream*>(::BIO_get_data(bio));
		::BIO_clear_retry_flags(bio);
		asio::error_code error;
		*written = self->socket_.write_some(asio::const_buffer(data, size), error);
		if (!error) { return 1; }
		if (error == asio::error::would_block || error == asio::error::try_again) { ::BIO_set_retry_write(bio); }
		else { self->socket_error_ = error; }
		return 0;
	}
	static int bio_read(BIO* bio, char* data, std::size_t size, std::size_t* read) {
		auto* self = static_cast<TlsStream*>(::BIO_get_data(bio));
		::BIO_clear_retry_flags(bio);
		if (self->input_begin_ < self->input_end_) {   // received bytes first; freed once taken
			*read = std::min(size, self->input_end_ - self->input_begin_);
			std::memcpy(data, self->input_.get() + self->input_begin_, *read);
			self->input_begin_ += *read;
			if (self->input_begin_ == self->input_end_) { self->input_.reset(); self->input_begin_ = self->input_end_ = 0; }
			return 1;
		}
		if (self->receiving_ && !self->eof_) { ::BIO_set_retry_read(bio); *read = 0; return 0; }
		if (self->eof_) { *read = 0; return 0; }
		asio::error_code error;
		*read = self->socket_.read_some(asio::mutable_buffer(data, size), error);
		if (!error) { return 1; }
		if (error == asio::error::would_block || error == asio::error::try_again) { ::BIO_set_retry_read(bio); }
		else if (error == asio::error::eof) { self->eof_ = true; }
		else { self->socket_error_ = error; }
		return 0;
	}
	static long bio_control(BIO* bio, int command, long, void*) {
		switch (command) {
		case BIO_CTRL_FLUSH: return 1;
		case BIO_CTRL_EOF: return static_cast<TlsStream*>(::BIO_get_data(bio))->eof_ ? 1 : 0;
		default: return 0;
		}
	}
	static BIO_METHOD* method() {
		static BIO_METHOD* const shared = [] {
			BIO_METHOD* m = ::BIO_meth_new(::BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "http tls socket");
			if (m) {
				::BIO_meth_set_write_ex(m, bio_write);
				::BIO_meth_set_read_ex(m, bio_read);
				::BIO_meth_set_ctrl(m, bio_control);
				::BIO_meth_set_create(m, [](BIO* bio) { ::BIO_set_init(bio, 1); return 1; });
			}
			return m;
		}();
		return shared;
	}

	asio::ip::tcp::socket socket_;
	SSL* ssl_ = nullptr;
	std::unique_ptr<char[]> input_;   // received bytes OpenSSL has not taken yet
	std::size_t input_begin_ = 0, input_end_ = 0;
	asio::error_code socket_error_;   // the last BIO call's socket error
	bool receiving_ = false;          // a read's SSL call: the BIO does not read the socket
	bool eof_ = false;                // the socket read EOF
};

struct TlsAdmission {
	std::atomic<std::size_t> pending{0};
	std::atomic<std::size_t> active{0};
	std::size_t maximum, connections;
	TlsAdmission(std::size_t bound, std::size_t connection_bound)
		: maximum(bound), connections(connection_bound) {}
};
// A TLS connection: admission, then the handshake under a deadline linked to the session's
// token (shutdown ends it too), then the HTTP connection over the stream.
inline task::task<void>
serve_tls_connection(asio::ip::tcp::socket socket, HttpHandler *handler, reactor::Reactor *reactor,
					 const AsioResponseOptions *options, std::shared_ptr<asio::ssl::context> context,
					 std::chrono::milliseconds deadline, std::shared_ptr<TlsAdmission> admission,
					 reactor::SessionContext accepted, TlsFailureHook failed = {}) {
	auto fail = [&](TlsFailure why) {
		if (failed) { failed(why); }
	};
	// Queue time consumes the same deadline, before any SSL allocation.
	auto now = std::chrono::steady_clock::now();
	if (now >= accepted.accepted_at && now - accepted.accepted_at >= deadline) { fail(TlsFailure::deadline); co_return; }
	// Admission precedes TLS allocation and is shared across reactor threads.
	if (admission->active.fetch_add(1) >= admission->connections) {
		admission->active.fetch_sub(1);
		fail(TlsFailure::connections);
		co_return;
	}
	struct Lease {
		std::shared_ptr<TlsAdmission> owner;
		bool handshake = false;
		~Lease() {
			if (handshake)
				owner->pending.fetch_sub(1);
			owner->active.fetch_sub(1);
		}
		void release() {
			owner->pending.fetch_sub(1);
			handshake = false;
		}
	} lease{admission};
	if (admission->pending.fetch_add(1) >= admission->maximum) {
		admission->pending.fetch_sub(1);
		fail(TlsFailure::handshakes);
		co_return;
	}
	lease.handshake = true;
	// Keep the context and stream alive through every original operation.
	TlsStream stream(std::move(socket), *context);
	// Subtract elapsed time instead of adding an unbounded duration to a clock.
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - accepted.accepted_at);
	if (elapsed >= deadline) { fail(TlsFailure::deadline); co_return; }
	asio::error_code handshake_error;
	{
		task::deadline<task::asio_scheduler> handshake(reactor->scheduler(),
			std::chrono::steady_clock::now() + (deadline - std::max(elapsed, std::chrono::milliseconds::zero())), accepted.stop);
		handshake_error = co_await task::await_asio<void(asio::error_code)>(
			[&](auto handler) { stream.async_handshake(asio::ssl::stream_base::server, std::move(handler)); }, handshake.token());
		if (handshake_error && handshake.expired()) { fail(TlsFailure::deadline); co_return; }
	}
	if (handshake_error) { fail(classify(handshake_error)); co_return; }
	lease.release();
	co_await serve_connection(std::move(stream), handler, reactor, options, std::move(accepted));
}
} // namespace detail
// Serves each connection over TLS with `context`: at most `maximum_handshakes` handshakes and
// `maximum_connections` TLS connections at once, each handshake within `handshake_deadline` of
// its accept (queue time included). `failed`, when set, hears every connection that ends
// before serving, and why.
inline HttpAsioService::ConnectionFactory
tls_connection_factory(std::shared_ptr<asio::ssl::context> context,
	std::chrono::milliseconds handshake_deadline = std::chrono::seconds(5),
	std::size_t maximum_handshakes = 64, std::size_t maximum_connections = 1024, TlsFailureHook failed = {}) {
	if (!context || handshake_deadline <= std::chrono::milliseconds::zero() || !maximum_handshakes || !maximum_connections) {
		throw std::invalid_argument("TLS connection configuration");
	}
	auto admission = std::make_shared<detail::TlsAdmission>(maximum_handshakes, maximum_connections);
	return [context = std::move(context), handshake_deadline, admission, failed = std::move(failed)](asio::ip::tcp::socket socket,
		HttpHandler* handler, reactor::Reactor* reactor, const AsioResponseOptions* options, reactor::SessionContext accepted) {
		return detail::serve_tls_connection(std::move(socket), handler, reactor, options, context, handshake_deadline, admission, std::move(accepted), failed);
	};
}

} // namespace http
