#include "http_deferred.h"
#include <atomic>
#include <iostream>
#include <thread>

namespace {
int failures = 0;
void check(bool value, const char *message) {
	if (!value) {
		++failures;
		std::cerr << message << '\n';
	}
}
http::DeferredOptions options() {
	http::DeferredOptions o;
	o.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	return o;
}
void held_write() {
	using namespace http;
	unsigned settlements = 0, wakeups = 0;
	auto request = std::make_shared<char>();
	std::weak_ptr<char> request_alive = request;
	auto reply = std::make_shared<char>();
	std::weak_ptr<char> reply_alive = reply;
	auto o = options();
	o.request_retention = request;
	o.on_settled = [&](ReplySettlement reason) {
		++settlements;
		check(reason == ReplySettlement::TimedOut && !request_alive.expired() && !reply_alive.expired(),
			  "settlement hook retains both leases after original write reap");
	};
	auto exchange = std::make_shared<detail::DeferredExchange>(std::move(o), [&] { ++wakeups; });
	DeferredReply capability(exchange);
	request.reset();
	Response response;
	response.body.assign(65536, 'r');
	auto address = response.body.data();
	check(capability.complete(std::move(response), reply) == ReplySubmit::Accepted,
		  "owned reply accepts cross-thread-ready payload");
	reply.reset();
	capability.abandon();
	check(exchange->phase() == detail::DeferredExchange::Phase::Ready,
		  "capability retirement cannot overwrite accepted reply");
	auto writing = exchange->take_response();
	check(writing && writing->body.data() == address && wakeups == 1,
		  "transport acquires original owned buffer once without copying");
	check(capability.complete({}) == ReplySubmit::AlreadyCompleted && !exchange->take_response(),
		  "duplicate reply cannot overwrite original write");
	exchange->close(ReplySettlement::TimedOut);
	check(!request_alive.expired() && !reply_alive.expired() && settlements == 0 &&
			  writing->body.size() == 65536,
		  "timeout retains accepted buffers and admission until write completion");
	check(capability.complete({}) == ReplySubmit::Closed, "late reply cannot restart a closing write");
	writing.reset();
	check(exchange->settle(ReplySettlement::Written) && !exchange->settle(ReplySettlement::Written),
		  "original write reaping settles one first-winner outcome");
	check(settlements == 1 && request_alive.expired() && reply_alive.expired(),
		  "terminal settlement releases both independent leases once");
	check(capability.complete({}) == ReplySubmit::Closed && wakeups == 2,
		  "escaped capability performs no wakeup after settlement");
}
void races() {
	using namespace http;
	for (unsigned iteration = 0; iteration < 500; ++iteration) {
		std::atomic<unsigned> wakeups = 0, settlements = 0;
		auto o = options();
		o.on_settled = [&](ReplySettlement) { ++settlements; };
		auto exchange = std::make_shared<detail::DeferredExchange>(std::move(o), [&] { ++wakeups; });
		DeferredReply capability(exchange);
		ReplySubmit submitted = ReplySubmit::Invalid;
		std::atomic<bool> go = false;
		std::thread publisher([&] {
			while (!go.load(std::memory_order_acquire)) {
				std::this_thread::yield();
			}
			Response response;
			response.body = "original";
			submitted = capability.complete(std::move(response));
		});
		std::thread closer([&] {
			while (!go.load(std::memory_order_acquire)) {
				std::this_thread::yield();
			}
			exchange->close(ReplySettlement::Disconnected);
		});
		go.store(true, std::memory_order_release);
		publisher.join();
		closer.join();
		check(submitted == ReplySubmit::Accepted || submitted == ReplySubmit::Closed,
			  "racing publisher/disconnect has one accepted or closed outcome");
		check(exchange->settle(ReplySettlement::Disconnected) && settlements == 1 && wakeups <= 2,
			  "cross-thread completion/close races settle exactly once");
		check(capability.complete({}) == ReplySubmit::Closed,
			  "raced retired capability never accesses a transport callback");
	}
}
void limits_and_abandonment() {
	using namespace http;
	unsigned wakeups = 0;
	auto o = options();
	o.limits.body_bytes = 4;
	auto exchange = std::make_shared<detail::DeferredExchange>(std::move(o), [&] { ++wakeups; });
	{
		DeferredReply capability(exchange);
		Response response;
		response.body = "large";
		check(capability.complete(std::move(response)) == ReplySubmit::TooLarge && wakeups == 0,
			  "oversized reply cannot consume response authority or wake transport");
		response = {};
		response.headers.emplace_back("X-Test", "bad\r\nvalue");
		check(capability.complete(std::move(response)) == ReplySubmit::Invalid,
			  "invalid buffered framing is rejected before acceptance");
		for (const char *name : {"bad name", "bad\x01", "Content-Length", "Transfer-Encoding", "Connection",
								 "Content-Type", "Trailer", "Upgrade"}) {
			response = {};
			response.headers.emplace_back(name, "value");
			check(capability.complete(std::move(response)) == ReplySubmit::Invalid,
				  "invalid or transport-owned header cannot consume response authority");
		}
	}
	check(exchange->phase() == detail::DeferredExchange::Phase::Closing &&
			  exchange->closing_reason() == ReplySettlement::Abandoned && wakeups == 1,
		  "abandoned response authority wakes its waiting transport once");
	exchange->settle(ReplySettlement::Abandoned);
	unsigned called = 0;
	o = options();
	o.deadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
	auto expired = std::make_shared<detail::DeferredExchange>(std::move(o), [&] { ++called; });
	DeferredReply late(expired);
	check(late.complete({}) == ReplySubmit::Closed &&
			  expired->closing_reason() == ReplySettlement::TimedOut && called == 1,
		  "late publication cannot escape its finite transport deadline");
	expired->settle(ReplySettlement::TimedOut);
}
} // namespace
int main() {
	held_write();
	races();
	limits_and_abandonment();
	std::cout << "deferred ownership failures: " << failures << '\n';
	return failures ? 1 : 0;
}
