#include "http_owned_stream.h"
#include <iostream>
#include <source_location>
using namespace http;
namespace {
void check(bool value, std::source_location where = std::source_location::current()) {
	if (!value)
		throw std::runtime_error("owned stream invariant at " + std::to_string(where.line()));
}
class CountingResource final : public std::pmr::memory_resource {
  public:
	unsigned allocations = 0, releases = 0;
  private:
	void *do_allocate(std::size_t size, std::size_t alignment) override {
		auto *result = std::pmr::new_delete_resource()->allocate(size, alignment);
		++allocations;
		return result;
	}
	void do_deallocate(void *pointer, std::size_t size, std::size_t alignment) override {
		++releases;
		std::pmr::new_delete_resource()->deallocate(pointer, size, alignment);
	}
	bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override { return this == &other; }
};
void default_resource_capture() {
	CountingResource first, second;
	struct Restore {
		std::pmr::memory_resource *previous;
		~Restore() { std::pmr::set_default_resource(previous); }
	} restore{std::pmr::set_default_resource(&first)};
	auto value = std::allocate_shared<int>(detail::StreamAllocator<int>{}, 42);
	std::weak_ptr<int> weak = value;
	std::pmr::set_default_resource(&second);
	value.reset();
	check(first.allocations == 1 && first.releases == 0);
	weak.reset();
	check(first.releases == 1 && second.releases == 0);
	std::pmr::set_default_resource(&first);
	struct ChangesDefault {
		explicit ChangesDefault(std::pmr::memory_resource *next) { std::pmr::set_default_resource(next); }
	};
	auto unique = detail::make_resource_unique<ChangesDefault>({}, &second);
	unique.reset();
	check(first.allocations == 2 && first.releases == 2 && second.allocations == 0 && second.releases == 0);
}
StreamOptions options() {
	StreamOptions result;
	result.reply.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	return result;
}
void held_original() {
	unsigned settled = 0, writable = 0;
	auto request = std::make_shared<char>();
	std::weak_ptr<char> request_alive = request;
	auto body = std::make_shared<const std::string>(65536, 'x');
	std::weak_ptr<const std::string> body_alive = body;
	auto policy = options();
	policy.reply.request_retention = request;
	policy.reply.on_settled = [&](ReplySettlement reason) {
		++settled;
		check(reason == ReplySettlement::TimedOut && !request_alive.expired());
	};
	policy.on_writable = [&] { ++writable; };
	auto exchange = std::make_shared<detail::OwnedStreamExchange>(std::move(policy), [] {});
	OwnedStreamReply producer(exchange);
	request.reset();
	check(producer.start({}, 65536) == StreamSubmit::Accepted);
	check(producer.publish({body, *body}) == StreamSubmit::Accepted);
	body.reset();
	auto original = exchange->take_write();
	check(original && original->head.find("Content-Length: 65536") != std::string_view::npos);
	check(original->chunk.bytes.size() == 65536);
	check(producer.publish({}) == StreamSubmit::Busy);
	check(exchange->close(ReplySettlement::TimedOut));
	check(!exchange->settle() && settled == 0 && !body_alive.expired());
	check(producer.finish() == StreamSubmit::Closed && !exchange->take_write());
	check(!exchange->complete_write(original->serial + 1, ReplySettlement::Written));
	check(exchange->complete_write(original->serial, ReplySettlement::Written));
	check(!exchange->complete_write(original->serial, ReplySettlement::Written));
	check(writable == 0 && !body_alive.expired());
	original.reset();
	check(body_alive.expired());
	check(exchange->settle() && !exchange->settle() && settled == 1 && request_alive.expired());
}
void publication() {
	unsigned writable = 0, settled = 0;
	auto policy = options();
	policy.on_writable = [&] { ++writable; };
	policy.reply.on_settled = [&](ReplySettlement reason) {
		check(reason == ReplySettlement::Written);
		++settled;
	};
	auto exchange = std::make_shared<detail::OwnedStreamExchange>(std::move(policy), [] {});
	{
		OwnedStreamReply producer(exchange);
		check(producer.publish({}) == StreamSubmit::Busy);
		Response metadata;
		metadata.headers.emplace_back("Content-Length", "4");
		check(producer.start(metadata, 4) == StreamSubmit::Invalid);
		metadata.headers.clear();
		metadata.headers.emplace_back("X-Test", "invalid\r\nvalue");
		check(producer.start(metadata, 4) == StreamSubmit::Invalid);
		metadata.headers.clear();
		check(producer.start(metadata, 4, true) == StreamSubmit::Accepted);
		check(producer.finish() == StreamSubmit::Invalid);
		auto head = exchange->take_write();
		check(head && head->chunk.bytes.empty());
		check(exchange->complete_write(head->serial, ReplySettlement::Written) && writable == 1);
		auto body = std::make_shared<const std::string>("data");
		check(producer.publish({body, *body}) == StreamSubmit::Accepted);
		check(producer.publish({body, *body}) == StreamSubmit::Busy);
		check(producer.finish() == StreamSubmit::Accepted);
	}
	check(exchange->phase() == detail::OwnedStreamExchange::Phase::Streaming);
	check(!exchange->settle());
	auto chunk = exchange->take_write();
	check(chunk && chunk->head.empty() && chunk->chunk.bytes == "data");
	check(exchange->complete_write(chunk->serial, ReplySettlement::Written));
	chunk.reset();
	check(exchange->settle() && settled == 1 && writable == 1);
}
void peer_disconnect() {
	for (bool chunked : {false, true}) {
		for (auto reason :
			 {ReplySettlement::Written, ReplySettlement::Disconnected, ReplySettlement::TimedOut,
			  ReplySettlement::Abandoned, ReplySettlement::Shutdown}) {
			unsigned settled = 0;
			auto policy = options();
			policy.reply.on_settled = [&](ReplySettlement actual) {
				check(actual == reason);
				++settled;
			};
			auto exchange = std::make_shared<detail::OwnedStreamExchange>(std::move(policy), [] {});
			OwnedStreamReply producer(exchange);
			check((chunked ? producer.start_chunked({}) : producer.start({}, 4)) == StreamSubmit::Accepted);
			auto body = std::make_shared<const std::string>("data");
			std::weak_ptr<const std::string> alive = body;
			check(producer.publish({body, *body}) == StreamSubmit::Accepted);
			body.reset();
			check(producer.finish() == StreamSubmit::Accepted);
			auto original = exchange->take_write();
			check(original.has_value());
			if (chunked) {
				// The body alone is not a complete chunked response.
				check(exchange->complete_write(original->serial, ReplySettlement::Written));
				original.reset();
				original = exchange->take_write();
				check(original && original->terminal);
			}
			check(!exchange->observe_peer_disconnect());
			check(!exchange->settle() && settled == 0);
			if (!chunked)
				check(!alive.expired());
			if (reason != ReplySettlement::Written && reason != ReplySettlement::Disconnected)
				check(exchange->close(reason));
			check(exchange->complete_write(original->serial, reason == ReplySettlement::Disconnected
																 ? reason
																 : ReplySettlement::Written));
			original.reset();
			check(alive.expired());
			check(exchange->settle() && settled == 1 && !exchange->settle());
		}
	}
	{
		auto exchange = std::make_shared<detail::OwnedStreamExchange>(options(), [] {});
		OwnedStreamReply producer(exchange);
		check(producer.start({}, 4) == StreamSubmit::Accepted);
		auto body = std::make_shared<const std::string>("data");
		check(producer.publish({body, *body}) == StreamSubmit::Accepted);
		auto original = exchange->take_write();
		check(original && !exchange->observe_peer_disconnect() && !exchange->settle());
		check(exchange->complete_write(original->serial, ReplySettlement::Written));
		original.reset();
		check(!exchange->settle());
		check(producer.finish() == StreamSubmit::Accepted);
		check(exchange->settle() && exchange->closing_reason() == ReplySettlement::Written);
	}
	for (bool chunked : {false, true}) {
		auto exchange = std::make_shared<detail::OwnedStreamExchange>(options(), [] {});
		OwnedStreamReply producer(exchange);
		check((chunked ? producer.start_chunked({}) : producer.start({}, 4)) == StreamSubmit::Accepted);
		auto body = std::make_shared<const std::string>(chunked ? "data" : "da");
		check(producer.publish({body, *body}) == StreamSubmit::Accepted);
		if (chunked)
			check(producer.finish() == StreamSubmit::Accepted);
		auto original = exchange->take_write();
		check(original && exchange->observe_peer_disconnect() && !exchange->settle());
		check(exchange->complete_write(original->serial, ReplySettlement::Written));
		original.reset();
		check(exchange->settle() && exchange->closing_reason() == ReplySettlement::Disconnected);
	}
}
void peer_completion_boundaries() {
	for (auto bytes : {0u, 4u}) {
		for (bool finish : {false, true}) {
			auto exchange = std::make_shared<detail::OwnedStreamExchange>(options(), [] {});
			OwnedStreamReply producer(exchange);
			check(producer.start({}, bytes) == StreamSubmit::Accepted);
			if (bytes) {
				auto body = std::make_shared<const std::string>(bytes, 'x');
				check(producer.publish({body, *body}) == StreamSubmit::Accepted);
			}
			auto original = exchange->take_write();
			check(original && exchange->complete_write(original->serial, ReplySettlement::Written));
			original.reset();
			check(!exchange->observe_peer_disconnect() && !exchange->settle());
			if (finish) {
				check(producer.finish() == StreamSubmit::Accepted);
				check(exchange->settle() && exchange->closing_reason() == ReplySettlement::Written);
			} else {
				check(exchange->close(ReplySettlement::TimedOut));
				check(exchange->settle() && exchange->closing_reason() == ReplySettlement::TimedOut);
			}
		}
	}
}
void limits() {
	auto policy = options();
	policy.maximum_bytes = 3;
	policy.chunk_bytes = 2;
	auto exchange = std::make_shared<detail::OwnedStreamExchange>(std::move(policy), [] {});
	OwnedStreamReply producer(exchange);
	check(producer.start({}, 4) == StreamSubmit::TooLarge);
	check(producer.start({}, 3) == StreamSubmit::Accepted);
	auto body = std::make_shared<const std::string>("abc");
	check(producer.publish({body, *body}) == StreamSubmit::TooLarge);
	check(producer.publish({{}, "ab"}) == StreamSubmit::Invalid);
	producer.abandon();
	check(exchange->settle());
	policy = options();
	policy.reply.deadline = std::chrono::steady_clock::now();
	auto expired = std::make_shared<detail::OwnedStreamExchange>(std::move(policy), [] {});
	OwnedStreamReply late(expired);
	check(late.start({}, 0) == StreamSubmit::Closed);
	check(expired->settle() && expired->closing_reason() == ReplySettlement::TimedOut);
}
void chunked() {
	auto exchange = std::make_shared<detail::OwnedStreamExchange>(options(), [] {});
	OwnedStreamReply producer(exchange);
	check(producer.start_chunked({}) == StreamSubmit::Accepted);
	auto header = exchange->take_write();
	check(header && header->head.find("Transfer-Encoding: chunked") != std::string_view::npos);
	check(header->head.find("Content-Length") == std::string_view::npos);
	check(exchange->complete_write(header->serial, ReplySettlement::Written));
	header.reset();
	auto body = std::make_shared<const std::string>("data");
	check(producer.publish({body, *body}) == StreamSubmit::Accepted);
	check(producer.finish() == StreamSubmit::Accepted);
	auto chunk = exchange->take_write();
	check(chunk && chunk->chunked && !chunk->terminal);
	check(exchange->complete_write(chunk->serial, ReplySettlement::Written));
	chunk.reset();
	check(!exchange->settle());
	auto tail = exchange->take_write();
	check(tail && tail->terminal && tail->chunk.bytes.empty());
	check(!exchange->settle() && !exchange->take_write());
	check(exchange->complete_write(tail->serial, ReplySettlement::Written));
	tail.reset();
	check(exchange->settle() && !exchange->take_write());
}
} // namespace
int main() {
	try {
		default_resource_capture();
		held_original();
		peer_disconnect();
		peer_completion_boundaries();
		publication();
		limits();
		chunked();
		std::cout << "owned original, cancellation, publication, header validation and bounds passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
