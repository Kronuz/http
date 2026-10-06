#pragma once

// Optional TLS adapter. Plain HTTP users retain the same API and dependencies.
#include "http_asio.h"
#include <asio/ssl.hpp>
#include <atomic>

namespace http {
namespace detail {
struct TlsAdmission {
	std::atomic<std::size_t> pending{0};
	std::size_t maximum;
	explicit TlsAdmission(std::size_t bound) : maximum(bound) {}
};
inline asio::awaitable<void>
serve_tls_connection(asio::ip::tcp::socket socket, HttpHandler *handler, reactor::Reactor *reactor,
					 const AsioResponseOptions *options, std::shared_ptr<asio::ssl::context> context,
					 std::chrono::milliseconds deadline, std::shared_ptr<TlsAdmission> admission) {
	// Admission precedes TLS allocation and is shared across reactor threads.
	if (admission->pending.fetch_add(1) >= admission->maximum) {
		admission->pending.fetch_sub(1);
		co_return;
	}
	struct Lease {
		std::shared_ptr<TlsAdmission> owner;
		~Lease() {
			if (owner)
				owner->pending.fetch_sub(1);
		}
		void release() {
			owner->pending.fetch_sub(1);
			owner.reset();
		}
	} lease{admission};
	// Keep the context and stream alive through every original operation.
	asio::ssl::stream<asio::ip::tcp::socket> stream(std::move(socket), *context);
	asio::steady_timer timer(stream.get_executor(), deadline);
	asio::error_code handshake_error, timer_error;
	using namespace asio::experimental::awaitable_operators;
	auto winner =
		co_await (stream.async_handshake(asio::ssl::stream_base::server,
										 asio::redirect_error(asio::use_awaitable, handshake_error)) ||
				  timer.async_wait(asio::redirect_error(asio::use_awaitable, timer_error)));
	if (winner.index() != 0 || handshake_error) {
		co_return;
	}
	lease.release();
	co_await serve_connection(std::move(stream), handler, reactor, options);
}
} // namespace detail
inline HttpAsioService::ConnectionFactory
tls_connection_factory(std::shared_ptr<asio::ssl::context> context,
					   std::chrono::milliseconds handshake_deadline = std::chrono::seconds(5),
					   std::size_t maximum_handshakes = 64) {
	if (!context || handshake_deadline <= std::chrono::milliseconds::zero() || !maximum_handshakes) {
		throw std::invalid_argument("TLS connection configuration");
	}
	auto admission = std::make_shared<detail::TlsAdmission>(maximum_handshakes);
	return [context = std::move(context), handshake_deadline,
			admission](asio::ip::tcp::socket socket, HttpHandler *handler, reactor::Reactor *reactor,
					   const AsioResponseOptions *options) {
		return detail::serve_tls_connection(std::move(socket), handler, reactor, options, context,
											handshake_deadline, admission);
	};
}
} // namespace http
