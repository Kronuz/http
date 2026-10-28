/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#include "http_client_tls.h"
#include <reactor_tls.h>

namespace http::client {
namespace {
bool input_idle(TlsCache::Stream &stream) noexcept {
	return !SSL_pending(stream.native_handle()) && !SSL_has_pending(stream.native_handle()) &&
		detail::transport_input_idle(stream);
}
class VerificationCategory final : public std::error_category {
  public:
	const char *name() const noexcept override { return "x509"; }
	std::string message(int code) const override { return X509_verify_cert_error_string(code); }
};
void validate(const TlsKey &key) {
	if (!key.context || !(SSL_CTX_get_verify_mode(key.context->native_handle()) & SSL_VERIFY_PEER) ||
		!key.endpoint.port() || key.hostname.empty() || key.hostname.size() > 253 ||
		!std::all_of(key.hostname.begin(), key.hostname.end(), [](unsigned char c) { return c > 32 && c < 127; }))
		throw Error(ErrorKind::Options, "invalid HTTP TLS connection key");
}
std::shared_ptr<TlsCache::Stream> make_stream(const TlsKey &key, const asio::any_io_executor &executor,
	const allocators::AllocationContext &context) {
	auto stream = std::allocate_shared<TlsCache::Stream>(allocators::OwnedAllocator<TlsCache::Stream>(context), executor, *key.context);
	reactor::tls::bind_hostname(stream->native_handle(), std::string(key.hostname));
	return stream;
}
} // namespace
TlsCache::TlsCache(asio::any_io_executor executor, std::size_t capacity, allocators::AllocationContext context)
	: context_(std::move(context)), executor_(std::move(executor)), entries_(context_.resource()) {
	if (!capacity || capacity > 65536) throw Error(ErrorKind::Options, "HTTP connection cache bound");
	try {
		entries_.reserve(capacity);
		for (std::size_t index = 0; index != capacity; ++index) entries_.emplace_back(context_.resource());
	} catch (const allocators::ResourcePressure &) { throw Error(ErrorKind::Pressure, "HTTP cache allocation pressure"); }
	catch (const std::bad_alloc &) { throw Error(ErrorKind::Pressure, "HTTP cache allocation failed"); }
}
TlsCache::~TlsCache() { for (auto &entry : entries_) retire(entry); }
std::shared_ptr<TlsCache> make_tls_cache(asio::any_io_executor executor, std::size_t capacity,
	allocators::AllocationContext context) {
	try {
		return std::allocate_shared<TlsCache>(allocators::OwnedAllocator<TlsCache>(context),
			std::move(executor), capacity, context);
	} catch (const allocators::ResourcePressure &) { throw Error(ErrorKind::Pressure, "HTTP cache control pressure"); }
	catch (const std::bad_alloc &) { throw Error(ErrorKind::Pressure, "HTTP cache control allocation failed"); }
}
void TlsCache::retire(Entry &entry) noexcept {
	if (!entry.stream) return;
	asio::error_code ignored;
	entry.stream->next_layer().close(ignored);
	entry.stream.reset();
	entry.tls.reset();
	entry.connected = false;
	--stats_.live;
	++stats_.retirements;
}
std::size_t TlsCache::acquire(const TlsKey &key, const asio::any_io_executor &executor) {
	if (executor != executor_) throw Error(ErrorKind::Options, "HTTP cache executor mismatch");
	std::optional<std::size_t> available;
	for (std::size_t index = 0; index != entries_.size(); ++index) {
		auto &entry = entries_[index];
		if (entry.leased) continue;
		if (entry.stream && entry.connected && entry.endpoint == key.endpoint && entry.hostname == key.hostname &&
			entry.tls.get() == key.context.get() && !entry.tls.owner_before(key.context) &&
			!key.context.owner_before(entry.tls) && entry.binding == key.binding) {
			if (input_idle(*entry.stream)) {
				entry.leased = entry.reused = true;
				++stats_.leased; ++stats_.reuse_attempts;
				return index;
			}
			retire(entry);
		}
		if (!available || !entry.stream) available = index;
	}
	if (!available) {
		++stats_.pressure;
		throw Error(ErrorKind::Pressure, "HTTP connection cache fully leased",
			std::make_error_code(std::errc::resource_unavailable_try_again));
	}
	auto &entry = entries_[*available];
	retire(entry);
	try {
		entry.hostname.assign(key.hostname);
		entry.stream = make_stream(key, executor_, context_);
	} catch (...) {
		entry.stream.reset();
		entry.hostname.clear();
		throw;
	}
	entry.endpoint = key.endpoint;
	entry.tls = key.context;
	entry.binding = key.binding;
	entry.leased = true; entry.reused = false;
	++stats_.leased; ++stats_.live;
	stats_.peak_live = std::max(stats_.peak_live, stats_.live);
	return *available;
}
void TlsCache::release(std::size_t index, bool reusable) noexcept {
	auto &entry = entries_[index];
	reusable = reusable && entry.connected && input_idle(*entry.stream);
	if (reusable && entry.reused) ++stats_.reuses;
	if (!reusable) retire(entry);
	entry.leased = false;
	--stats_.leased;
}
TlsLease::TlsLease(TlsKey key, asio::any_io_executor executor, std::shared_ptr<TlsCache> cache,
	allocators::AllocationContext context)
	: cache_(std::move(cache)), context_(key.context), endpoint_(key.endpoint) {
	validate(key);
	try {
		if (cache_) index_ = cache_->acquire(key, executor);
		else one_shot_ = make_stream(key, executor, context);
	} catch (const allocators::ResourcePressure &) { throw Error(ErrorKind::Pressure, "HTTP TLS allocation pressure"); }
	catch (const std::bad_alloc &) { throw Error(ErrorKind::Pressure, "HTTP TLS allocation failed"); }
}
TlsLease::~TlsLease() {
	if (cache_) cache_->release(index_, reusable_);
	else if (one_shot_) {
		asio::error_code ignored;
		one_shot_->next_layer().close(ignored);
	}
}
TlsCache::Stream &TlsLease::stream() { return cache_ ? *cache_->entries_[index_].stream : *one_shot_; }
bool TlsLease::connected() const noexcept {
	return cache_ ? cache_->entries_[index_].connected : one_shot_connected_;
}
void TlsLease::tcp_connected() noexcept { if (cache_) ++cache_->stats_.connects; }
void TlsLease::mark_connected() noexcept {
	if (cache_) { cache_->entries_[index_].connected = true; ++cache_->stats_.handshakes; }
	else one_shot_connected_ = true;
}
asio::awaitable<void> connect(TlsLease &lease) {
	if (lease.connected()) co_return;
	auto &stream = lease.stream();
	if (!(SSL_get_verify_mode(stream.native_handle()) & SSL_VERIFY_PEER))
		throw Error(ErrorKind::Trust, "HTTP TLS peer verification disabled");
	try {
		co_await stream.next_layer().async_connect(lease.endpoint(), asio::use_awaitable);
		lease.tcp_connected();
		asio::error_code error;
		co_await stream.async_handshake(asio::ssl::stream_base::client,
			asio::redirect_error(asio::use_awaitable, error));
		if (error == asio::error::operation_aborted) throw std::system_error(error);
		const auto verification = SSL_get_verify_result(stream.native_handle());
		if (verification != X509_V_OK) {
			static const VerificationCategory category;
			const std::error_code cause(static_cast<int>(verification), category);
			const auto message = std::string("HTTP TLS peer verification failed: ") + cause.message();
			throw Error(ErrorKind::Trust, message.c_str(), cause);
		}
		if (error) throw std::system_error(error);
		if (!SSL_get0_peer_certificate(stream.native_handle()))
			throw Error(ErrorKind::Trust, "HTTP TLS peer certificate missing");
		lease.mark_connected();
	} catch (const std::system_error &error) {
		throw Error(error.code() == asio::error::operation_aborted ? ErrorKind::Cancelled : ErrorKind::Transport,
			"HTTP TLS connection failed", error.code());
	}
}
namespace detail {
asio::awaitable<BufferedResponse> tls_exchange_io(TlsLease &lease, PreparedRequest request, Deadline deadline,
	Limits limits, ResponsePolicy policy) {
	lease.reusable(false);
	(void)deadline.remaining();
	if (lease.connected() && !input_idle(lease.stream()))
		throw Error(ErrorKind::Transport, "HTTP TLS connection has unconsumed input");
	co_await connect(lease);
	auto response = co_await exchange_io(lease.stream(), std::move(request), deadline, limits, policy);
	response.reusable = response.reusable && !SSL_pending(lease.stream().native_handle()) &&
		!SSL_has_pending(lease.stream().native_handle());
	co_return response;
}
} // namespace detail
asio::awaitable<BufferedResponse> exchange(TlsLease &lease, PreparedRequest request, Deadline deadline,
	Limits limits, ResponsePolicy policy) {
	return before(deadline, detail::tls_exchange_io(lease, std::move(request), deadline, limits, policy));
}
} // namespace http::client
