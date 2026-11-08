/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#pragma once
#include "http_client_asio.h"
#include <asio/ssl.hpp>
#include <array>

namespace http::client {
struct CacheStats {
	std::uint64_t connects = 0, handshakes = 0, reuse_attempts = 0, reuses = 0;
	std::uint64_t retirements = 0, pressure = 0;
	std::size_t live = 0, leased = 0, peak_live = 0;
};
struct TlsKey {
	asio::ip::tcp::endpoint endpoint;
	std::string_view hostname;
	std::shared_ptr<asio::ssl::context> context;
	std::array<unsigned char, 32> binding{};
};
class TlsLease;
// Contexts and trust policy are immutable while entries or leases retain them.
class TlsCache {
  public:
	using Stream = asio::ssl::stream<asio::ip::tcp::socket>;
	TlsCache(asio::any_io_executor executor, std::size_t capacity, allocators::AllocationContext context = {});
	~TlsCache();
	TlsCache(const TlsCache &) = delete;
	TlsCache &operator=(const TlsCache &) = delete;
	CacheStats stats() const noexcept { return stats_; }
  private:
	friend class TlsLease;
	struct Entry {
		asio::ip::tcp::endpoint endpoint;
		std::pmr::string hostname;
		std::shared_ptr<asio::ssl::context> tls;
		std::array<unsigned char, 32> binding{};
		std::shared_ptr<Stream> stream;
		bool leased = false, connected = false, reused = false;
		explicit Entry(std::pmr::memory_resource *resource) : hostname(resource) {}
	};
	allocators::AllocationContext context_;
	asio::any_io_executor executor_;
	std::pmr::vector<Entry> entries_;
	CacheStats stats_;
	std::size_t acquire(const TlsKey &, const asio::any_io_executor &);
	void release(std::size_t, bool reusable) noexcept;
	void retire(Entry &) noexcept;
};
std::shared_ptr<TlsCache> make_tls_cache(asio::any_io_executor executor, std::size_t capacity,
	allocators::AllocationContext context = {});
class TlsLease {
  public:
	TlsLease(TlsKey key, asio::any_io_executor executor, std::shared_ptr<TlsCache> cache = {},
		allocators::AllocationContext context = {});
	~TlsLease();
	TlsLease(const TlsLease &) = delete;
	TlsLease &operator=(const TlsLease &) = delete;
	TlsCache::Stream &stream();
	bool connected() const noexcept;
	void reusable(bool value) noexcept { reusable_ = value; }
	asio::ip::tcp::endpoint endpoint() const noexcept { return endpoint_; }
  private:
	friend asio::awaitable<void> connect(TlsLease &lease);
	void tcp_connected() noexcept;
	void mark_connected() noexcept;
	std::shared_ptr<TlsCache> cache_;
	std::shared_ptr<asio::ssl::context> context_;
	std::shared_ptr<TlsCache::Stream> one_shot_;
	asio::ip::tcp::endpoint endpoint_;
	std::size_t index_ = 0;
	bool reusable_ = false, one_shot_connected_ = false;
};
asio::awaitable<void> connect(TlsLease &lease);
asio::awaitable<BufferedResponse> exchange(TlsLease &lease, PreparedRequest request, Deadline deadline,
	Limits limits = {}, ResponsePolicy policy = {});
} // namespace http::client
