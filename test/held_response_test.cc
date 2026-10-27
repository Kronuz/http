#include "http_asio.h"
#include "http_client_asio.h"
#include "support/held_write_stream.h"
#include <allocators/memory_resource.h>
#include <asio/use_future.hpp>
#include <iostream>
#include <source_location>

namespace {
void check(bool value, std::source_location location = std::source_location::current()) {
	if (!value) throw std::runtime_error("held response invariant at " + std::to_string(location.line()));
}
struct Retention {
	std::pmr::string bytes;
	unsigned *destroyed;
	Retention(std::pmr::memory_resource *resource, unsigned *count) : bytes(4096, 'r', resource), destroyed(count) {}
	~Retention() { ++*destroyed; }
};
class Handler final : public http::HttpHandler {
  public:
	bool chunked;
	std::shared_ptr<std::pmr::memory_resource> resource;
	unsigned calls = 0, settlements = 0, destroyed = 0;
	bool alive_at_settlement = false;
	http::ReplySettlement outcome = http::ReplySettlement::Failed;
	std::weak_ptr<Retention> retention;
	http::OwnedStreamCapability producer;
	Handler(bool framing, std::shared_ptr<std::pmr::memory_resource> owner)
		: chunked(framing), resource(std::move(owner)) {}
	void handle(const http::Request &, http::ResponseWriter &writer) override {
		if (++calls != 1) { writer.send(200, "second"); return; }
		http::StreamOptions options;
		options.resource = resource;
		options.reply.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
		auto lease = std::allocate_shared<Retention>(
			allocators::OwnedAllocator<Retention>(allocators::AllocationContext(resource)), resource.get(), &destroyed);
		retention = lease;
		options.reply.request_retention = std::move(lease);
		options.reply.on_settled = [this](http::ReplySettlement reason) {
			++settlements; outcome = reason; alive_at_settlement = !retention.expired();
		};
		producer = writer.stream(std::move(options));
		http::Response metadata;
		metadata.content_type = "application/octet-stream";
		check((chunked ? producer->start_chunked(metadata, true) : producer->start(metadata, 5, true)) == http::StreamSubmit::Accepted);
		auto body = std::make_shared<const std::string>("first");
		check(producer->publish({body, *body}) == http::StreamSubmit::Accepted);
		check(producer->finish() == http::StreamSubmit::Accepted);
	}
	bool should_offload(const http::Request &) const override { return false; }
};
asio::awaitable<void> run(reactor::Reactor &reactor, std::shared_ptr<allocators::ByteAccount> account,
	bool chunked, http::ReplySettlement terminal) {
	const auto executor = co_await asio::this_coro::executor;
	auto resource = allocators::make_charged_resource(account);
	const auto fixed = account->used();
	Handler handler(chunked, resource);
	asio::ip::tcp::acceptor acceptor(executor, {asio::ip::address_v4::loopback(), 0});
	asio::ip::tcp::socket peer(executor), server(executor);
	co_await peer.async_connect(acceptor.local_endpoint(), asio::use_awaitable);
	co_await acceptor.async_accept(server, asio::use_awaitable);
	acceptor.close();
	auto gate = std::make_shared<http::test::WriteGate>(executor);
	gate->arm();
	http::AsioResponseOptions options;
	bool serving = true;
	asio::co_spawn(executor,
		http::detail::serve_connection(http::test::HeldWriteStream(std::move(server), gate), &handler, &reactor, &options),
		[&](std::exception_ptr) { serving = false; });
	std::optional<http::client::BufferedResponse> second;
	std::exception_ptr second_error, failure;
	bool waiting_second = false;
	try {
		auto first = co_await http::client::exchange(peer, http::client::PreparedRequest({"GET", "localhost", "/first"}),
			http::client::Deadline(std::chrono::seconds(2)));
		check(first.body == "first");
		co_await gate->wait_held();
		check(gate->observed() == gate->delivered() + 1 && handler.calls == 1 && handler.settlements == 0);
		check(!handler.retention.expired() && handler.destroyed == 0 && account->used() >= fixed + 4096);
		waiting_second = true;
		asio::co_spawn(executor,
			http::client::exchange(peer, http::client::PreparedRequest({"GET", "localhost", "/second"}),
				http::client::Deadline(std::chrono::seconds(2))),
			[&](std::exception_ptr error, http::client::BufferedResponse response) {
				second_error = error;
				if (!error) second.emplace(std::move(response));
				waiting_second = false;
			});
		co_await gate->wait_read();
		co_await asio::post(executor, asio::use_awaitable);
		check(handler.calls == 1 && handler.settlements == 0 && !handler.retention.expired());
		if (terminal == http::ReplySettlement::Abandoned) handler.producer->abandon();
		if (terminal == http::ReplySettlement::Shutdown) reactor.abort_all();
		co_await asio::post(executor, asio::use_awaitable);
		check(handler.calls == 1 && handler.settlements == 0 && handler.destroyed == 0);
		check(gate->release() && !gate->release());
		while (waiting_second || !handler.settlements) co_await asio::post(executor, asio::use_awaitable);
		if (terminal == http::ReplySettlement::Written)
			check(!second_error && second && second->body == "second" && handler.calls == 2);
		else check(second_error && handler.calls == 1);
		check(handler.settlements == 1 && handler.outcome == terminal && handler.alive_at_settlement);
		check(handler.retention.expired() && handler.destroyed == 1 && gate->observed() == gate->delivered());
	} catch (...) { failure = std::current_exception(); }
	co_await asio::this_coro::throw_if_cancelled(false);
	reactor.abort_all();
	gate->release();
	asio::error_code ignored;
	peer.close(ignored);
	while (serving || waiting_second) co_await asio::post(executor, asio::use_awaitable);
	handler.producer.reset();
	handler.retention.reset();
	check(account->used() == fixed);
	if (failure) std::rethrow_exception(failure);
}
} // namespace
int main() {
	try {
		for (bool chunked : {false, true}) {
			for (auto terminal : {http::ReplySettlement::Written, http::ReplySettlement::Abandoned, http::ReplySettlement::Shutdown}) {
				reactor::Reactor reactor(0, 1);
				auto account = std::make_shared<allocators::ByteAccount>(1024 * 1024);
				auto result = asio::co_spawn(reactor.io(), run(reactor, account, chunked, terminal), asio::use_future);
				reactor.io().run(); result.get();
				check(account->used() == 0);
			}
		}
		std::cout << "authentic held final writes, next-request exclusion, cancellation and conservation passed\n";
	} catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
