#include "http_owned_stream.h"
#include <iostream>
#include <source_location>
using namespace http;
namespace {
void check(bool value, std::source_location where = std::source_location::current()) {
	if (!value)
		throw std::runtime_error("owned stream invariant at " + std::to_string(where.line()));
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
		held_original();
		publication();
		limits();
		chunked();
		std::cout << "owned original, cancellation, publication, header validation and bounds passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
