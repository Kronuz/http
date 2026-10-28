#include "http_client_tls.h"
#include <allocators/memory_resource.h>
#include <iostream>
#include <source_location>
#include <asio/use_future.hpp>
#include "support/response_boundary.h"
#if defined(__unix__) || defined(__APPLE__)
#include "tls_client_fixture.h"
#endif

namespace client = http::client;
void check(bool value, std::source_location location = std::source_location::current()) {
	if (!value) throw std::runtime_error("HTTP TLS client invariant at line " + std::to_string(location.line()));
}
#if defined(__unix__) || defined(__APPLE__)
asio::awaitable<void> queued_response(tls_fixture::Credentials &credentials) {
	const auto executor = co_await asio::this_coro::executor;
	const auto &forged = http_test::unsolicited;
	const auto first = http_test::response_at_boundary(client::ResponseParser::read_bytes);
	check(first.size() == client::ResponseParser::read_bytes);
	{
		tls_fixture::Server peer(credentials.server, first + forged);
		auto cache = client::make_tls_cache(executor, 1);
		client::TlsKey key{peer.endpoint(), "localhost", credentials.client};
		bool rejected = false;
		{
			client::TlsLease lease(key, executor, cache);
			try {
				auto response = co_await client::exchange(lease,
					client::PreparedRequest({"GET", "localhost", "/"}), client::Deadline(std::chrono::seconds(2)));
				check(response.head.status == 200 && std::string_view(response.body) == std::string_view(first).substr(first.find("\r\n\r\n") + 4));
				lease.reusable(response.reusable);
			} catch (const client::Error &error) {
				rejected = error.kind() == client::ErrorKind::Framing;
				check(rejected);
			}
		}
		check(cache->stats().leased == 0 && cache->stats().live == 0);
	}
	{
		auto late = std::make_shared<tls_fixture::LateReply>();
		late->bytes = forged;
		auto queued = late->queued.get_future();
		tls_fixture::Server peer(credentials.server,
			"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", late);
		auto cache = client::make_tls_cache(executor, 1);
		client::TlsKey key{peer.endpoint(), "localhost", credentials.client};
		{
			client::TlsLease lease(key, executor, cache);
			auto response = co_await client::exchange(lease,
				client::PreparedRequest({"GET", "localhost", "/"}), client::Deadline(std::chrono::seconds(2)));
			check(response.body == "ok");
			lease.reusable(response.reusable);
		}
		check(cache->stats().live == 1);
		late->release.set_value();
		check(queued.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
		queued.get();
		{
			client::TlsLease next(key, executor, cache);
			check(!next.connected() && cache->stats().connects == 1);
		}
		check(cache->stats().live == 0 && cache->stats().leased == 0);
	}
}
asio::awaitable<void> network(tls_fixture::Credentials &credentials, tls_fixture::Server &one,
	tls_fixture::Server &two) {
	const auto executor = co_await asio::this_coro::executor;
	auto cache = client::make_tls_cache(executor, 1);
	client::TlsKey key{one.endpoint(), "localhost", credentials.client};
	const auto request = [&](client::TlsKey value) -> asio::awaitable<void> {
		const client::Deadline deadline(std::chrono::milliseconds(2000));
		client::TlsLease lease(value, executor, cache);
		client::PreparedRequest wire({"GET", value.hostname, "/"});
		auto response = co_await client::exchange(lease, std::move(wire), deadline);
		check(response.head.status == 200 && response.body == "ok");
		lease.reusable(response.reusable);
	};
	co_await request(key);
	co_await request(key);
	check(cache->stats().connects == 1 && cache->stats().handshakes == 1 && cache->stats().reuses == 1);
	key.binding[0] = 1;
	co_await request(key);
	key.hostname = "elsewhere.localhost";
	co_await request(key);
	auto owner = key.context;
	key.context = std::shared_ptr<asio::ssl::context>(owner.get(), [](auto *) {});
	co_await request(key);
	key.context = owner;
	co_await request(key);
	key.endpoint = two.endpoint();
	co_await request(key);
	check(cache->stats().connects == 6 && cache->stats().handshakes == 6 && cache->stats().retirements == 5);
	for (bool wrong_name : {true, false}) {
		auto bad = key;
		if (wrong_name) bad.hostname = "untrusted-name.localhost";
		else {
			bad.context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
			bad.context->set_verify_mode(asio::ssl::verify_peer);
		}
		bool refused = false;
		try { co_await request(bad); }
		catch (const client::Error &error) { refused = error.kind() == client::ErrorKind::Trust; }
		check(refused && cache->stats().live == 0 && cache->stats().leased == 0);
	}
	{
		client::TlsLease leased(key, executor, cache);
		const client::Deadline deadline(std::chrono::milliseconds(2000));
		auto response = co_await client::exchange(leased,
			client::PreparedRequest({"GET", key.hostname, "/"}), deadline);
		leased.reusable(response.reusable);
		asio::error_code ignored;
		leased.stream().next_layer().close(ignored);
		bool failed = false;
		try {
			(void)co_await client::exchange(leased,
				client::PreparedRequest({"GET", key.hostname, "/"}), deadline);
		} catch (const client::Error &error) { failed = error.kind() == client::ErrorKind::Transport; }
		check(failed);
	}
	check(cache->stats().live == 0 && cache->stats().leased == 0);
	{
		client::TlsLease isolated(key, executor);
		const client::Deadline deadline(std::chrono::milliseconds(2000));
		check(!isolated.connected());
		co_await client::before(deadline, client::connect(isolated));
		check(isolated.connected());
		co_await client::before(deadline, client::connect(isolated));
		auto response = co_await client::exchange(isolated,
			client::PreparedRequest({"GET", key.hostname, "/"}), deadline);
		check(response.head.status == 200 && response.body == "ok" && isolated.connected());
	}
}
#endif
int main() {
	asio::io_context io, other;
	auto context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	context->set_verify_mode(asio::ssl::verify_peer);
	const client::TlsKey key{{asio::ip::address_v4::loopback(), 1234}, "localhost", context};
	auto cache = client::make_tls_cache(io.get_executor(), 1);
	{
		client::TlsLease leased(key, io.get_executor(), cache);
		check(cache->stats().live == 1 && cache->stats().leased == 1 && !leased.connected());
		bool pressure = false, mismatch = false;
		try { client::TlsLease excess(key, io.get_executor(), cache); }
		catch (const client::Error &error) { pressure = error.kind() == client::ErrorKind::Pressure; }
		try { client::TlsLease wrong(key, other.get_executor(), cache); }
		catch (const client::Error &error) { mismatch = error.kind() == client::ErrorKind::Options; }
		check(pressure && mismatch && cache->stats().pressure == 1);
		leased.reusable(true);
	}
	check(cache->stats().live == 0 && cache->stats().leased == 0 && cache->stats().retirements == 1);
	auto insecure = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	bool rejected = false;
	try { client::TlsLease invalid({key.endpoint, key.hostname, insecure}, io.get_executor(), cache); }
	catch (const client::Error &error) { rejected = error.kind() == client::ErrorKind::Options; }
	check(rejected && cache->stats().live == 0);
	auto account = std::make_shared<allocators::ByteAccount>(65536);
	std::weak_ptr<std::pmr::memory_resource> weak_resource;
	std::weak_ptr<client::TlsCache> weak_cache;
	std::optional<client::TlsLease> escaped;
	{
		auto resource = allocators::make_charged_resource(account);
		weak_resource = resource;
		auto owned = client::make_tls_cache(io.get_executor(), 1, allocators::AllocationContext(resource));
		weak_cache = owned;
		escaped.emplace(key, io.get_executor(), owned);
	}
	check(!weak_cache.expired() && !weak_resource.expired() && account->used() > 0);
	check(escaped->stream().native_handle() != nullptr);
	escaped.reset();
	check(weak_cache.expired() && !weak_resource.expired());
	weak_cache.reset();
	check(weak_resource.expired() && account->used() > 0);
	weak_resource.reset();
	check(account->used() == 0);
#if defined(__unix__) || defined(__APPLE__)
	{
		tls_fixture::Credentials credentials;
		tls_fixture::Server one(credentials.server), two(credentials.server);
		asio::io_context network_io;
		auto result = asio::co_spawn(network_io, network(credentials, one, two), asio::use_future);
		network_io.run();
		result.get();
		network_io.restart();
		auto poisoned = asio::co_spawn(network_io, queued_response(credentials), asio::use_future);
		network_io.run();
		poisoned.get();
	}
#endif
	std::cout << "HTTP TLS capacity, executor, verification and escaped resource ownership passed\n";
}
