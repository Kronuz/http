#include "http_client_core.h"
#include "http_client_request.h"
#include "http_client_response.h"
#include <allocators/memory_resource.h>
#include <iostream>
#include <source_location>

namespace client = http::client;
void check(bool value, std::source_location location = std::source_location::current()) {
	if (!value) throw std::runtime_error("HTTP client core invariant at " + std::to_string(location.line()));
}
struct Result {
	std::string body;
	unsigned status;
	bool reusable;
};
Result parse(std::string_view wire, std::size_t split, client::ResponsePolicy policy = {},
			 client::Limits limits = {}) {
	client::ResponseParser parser(limits, policy);
	Result result{};
	const auto consume = [&](std::string_view part) {
		auto input = std::span<const char>(part.data(), part.size());
		std::size_t turns = 0;
		while (!input.empty()) {
			check(++turns < wire.size() * 4 + 20);
			parser.feed(input);
			if (parser.head_ready()) parser.accept_head();
			const auto body = parser.body().first(std::min<std::size_t>(3, parser.body().size()));
			result.body.append(body.data(), body.size());
			parser.consume_body(body.size());
		}
	};
	consume(wire.substr(0, split));
	consume(wire.substr(split));
	while (!parser.body().empty()) {
		const auto bytes = parser.body();
		result.body.append(bytes.data(), bytes.size());
		parser.consume_body(bytes.size());
	}
	if (!parser.done()) parser.eof();
	check(parser.done());
	result.status = parser.head().status;
	result.reusable = parser.reusable();
	return result;
}
void valid(std::string_view wire, std::string_view body, bool reusable = true,
		   client::ResponsePolicy policy = {}, client::Limits limits = {}) {
	for (std::size_t split = 0; split <= wire.size(); ++split) {
		const auto result = parse(wire, split, policy, limits);
		check(result.body == body && result.reusable == reusable);
	}
}
void invalid(std::string_view wire, client::ResponsePolicy policy = {}, client::Limits limits = {}) {
	for (std::size_t split = 0; split <= wire.size(); ++split) {
		bool rejected = false;
		try { (void)parse(wire, split, policy, limits); }
		catch (const client::Error &) { rejected = true; }
		check(rejected);
	}
}
int main() {
	{
		auto account = std::make_shared<allocators::ByteAccount>(256 * 1024);
		auto resource = allocators::make_charged_resource(account);
		const auto fixed = account->used();
		client::ResponseParser parser({}, {}, allocators::AllocationContext(resource));
		std::string wire = "HTTP/1.1 200 OK\r\n";
		for (unsigned index = 0; index != 64; ++index)
			wire += "X-" + std::string(60, 'k') + std::to_string(index) + ": " + std::string(500, 'v') + "\r\n";
		wire += "Content-Length: 0\r\n\r\n";
		auto input = std::span<const char>(wire.data(), wire.size());
		while (!input.empty()) {
			parser.feed(input);
			if (parser.head_ready()) parser.accept_head();
		}
		check(parser.done() && account->used() > fixed);
		std::size_t bytes = 0, tokens = 0;
		check(!parser.discard(bytes, tokens) && !parser.reusable());
		std::size_t turns = 0;
		for (;;) {
			check(++turns < 100000);
			bytes = tokens = 1;
			const auto used = account->used();
			const auto done = parser.discard(bytes, tokens);
			check(account->used() <= used);
			if (done) break;
		}
		check(account->used() == fixed);
		bytes = tokens = 0;
		check(parser.discard(bytes, tokens));
	}
	for (bool malformed : {false, true}) {
		auto account = std::make_shared<allocators::ByteAccount>(65536);
		auto resource = allocators::make_charged_resource(account);
		const auto fixed = account->used();
		client::ResponseParser partial({}, {}, allocators::AllocationContext(resource));
		std::string wire = "HTTP/1.1 200 OK\r\nX-First: " + std::string(1000, 'a') +
			"\r\nX-Partial: " + std::string(1000, 'b');
		auto input = std::span<const char>(wire.data(), wire.size());
		bool rejected = false;
		try {
			while (!input.empty()) partial.feed(input);
			if (malformed) {
				const std::string suffix = "\r\n folded";
				auto tail = std::span<const char>(suffix.data(), suffix.size());
				partial.feed(tail);
			}
			partial.eof();
		} catch (const client::Error &error) {
			rejected = error.kind() == client::ErrorKind::Framing;
		}
		check(rejected && account->used() > fixed && !partial.reusable());
		std::size_t turns = 0;
		for (;;) {
			check(++turns < 100000);
			std::size_t bytes = 1, tokens = 1;
			const auto used = account->used();
			const auto done = partial.discard(bytes, tokens);
			check(account->used() <= used);
			if (done) break;
		}
		check(account->used() == fixed);
	}
	{
		auto account = std::make_shared<allocators::ByteAccount>(65536);
		std::optional<client::BufferedResponse> escaped;
		std::weak_ptr<std::pmr::memory_resource> weak;
		{
			auto resource = allocators::make_charged_resource(account);
			weak = resource;
			client::BufferedReceiver receiver({}, {}, allocators::AllocationContext(resource));
			resource.reset();
			const std::string wire = "HTTP/1.1 200 OK\r\nX-Owner: " + std::string(1000, 'o') + "\r\nContent-Length: 3000\r\n\r\n" + std::string(3000, 'b');
			for (std::size_t offset = 0; offset < wire.size();) {
				auto input = std::span<const char>(wire.data() + offset, std::min<std::size_t>(17, wire.size() - offset));
				const auto offered = input.size();
				while (!input.empty()) receiver.feed(input);
				offset += offered;
			}
			check(receiver.done());
			escaped.emplace(receiver.take_response());
		}
		check(!weak.expired() && account->used() > 0 && escaped->reusable);
		check(escaped->body.size() == 3000 && std::all_of(escaped->body.begin(), escaped->body.end(), [](char c) { return c == 'b'; }));
		check(escaped->head.get("x-owner")->size() == 1000);
		escaped.reset();
		check(weak.expired() && account->used() > 0);
		weak.reset();
		check(account->used() == 0);
	}
	{
		std::string target = "/knob", body = "abc", token = "Bearer example";
		const std::array headers{client::RequestField{"Authorization", token}};
		client::PreparedRequest request({"GET", "[::1]:1234", target, headers, body});
		target.assign("changed"); body.assign("changed"); token.assign("changed");
		auto moved = std::move(request);
		check(moved.wire() == "GET /knob HTTP/1.1\r\nHost: [::1]:1234\r\nAuthorization: Bearer example\r\nContent-Length: 3\r\nConnection: keep-alive\r\n\r\nabc");
		const auto reject = [](client::RequestView value, client::Limits limits = {}) {
			bool rejected = false;
			try { client::PreparedRequest prepared(value, limits); }
			catch (const client::Error &) { rejected = true; }
			check(rejected);
		};
		reject({"GE T", "localhost", "/"});
		reject({"CONNECT", "localhost", "/"});
		reject({"GET", "localhost\r\nInjected: yes", "/"});
		reject({"GET", "user@localhost", "/"});
		reject({"GET", "localhost", "/path#fragment"});
		for (auto name : {"Host", "Content-Length", "Transfer-Encoding", "Connection", "Trailer", "Upgrade", "Bad Name"}) {
			const std::array reserved{client::RequestField{name, "value"}};
			reject({"GET", "localhost", "/", reserved});
		}
		const std::array injected{client::RequestField{"Authorization", "secret\r\nOther: value"}};
		reject({"GET", "localhost", "/", injected});
		client::Limits limits; limits.header_bytes = 1;
		reject({"GET", "localhost", "/"}, limits);
		limits = {}; limits.body_bytes = 0;
		reject({"POST", "localhost", "/", {}, std::span<const char>("x", 1)}, limits);
		limits = {}; limits.header_fields = 2;
		reject({"GET", "localhost", "/"}, limits);
		const client::Deadline deadline(std::chrono::milliseconds(1000));
		const auto shared = deadline;
		check(shared.expires() == deadline.expires() && deadline.poll_milliseconds() > 0 && deadline.poll_milliseconds() <= 1000);
		for (auto timeout : {std::chrono::milliseconds(0), std::chrono::milliseconds(-1), std::chrono::milliseconds::max()}) {
			bool rejected = false;
			try { client::Deadline invalid(timeout); }
			catch (const client::Error &error) { rejected = error.kind() == client::ErrorKind::Options; }
			check(rejected);
		}
	}
	{
		client::ResponseParser parser;
		const std::string wire = "HTTP/1.1 200 OK\r\nX-Detent-Profile: 1 \t\r\nContent-Length: 0\r\n\r\n";
		auto input = std::span<const char>(wire.data(), wire.size());
		parser.feed(input);
		check(parser.head_ready() && parser.head().get("x-detent-profile") == "1 \t");
		parser.accept_head(); parser.feed(input);
		check(input.empty() && parser.done());
	}
	valid("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", "abc");
	valid("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n2\r\nbc\r\n0\r\n\r\n", "abc");
	valid("HTTP/1.1 200 OK\r\nConnection: close, foo\r\nContent-Length: 3\r\n\r\nabc", "abc", false);
	valid("HTTP/1.0 200 OK\r\nContent-Length: 3\r\n\r\nabc", "abc", false);
	valid("HTTP/1.1 204 No Content\r\nContent-Length: 1000\r\n\r\n", "");
	valid("HTTP/1.1 304 Not Modified\r\nContent-Length: 1000\r\n\r\n", "");
	client::ResponsePolicy head; head.head_request = true;
	valid("HTTP/1.1 200 OK\r\nContent-Length: 1000000000\r\n\r\n", "", true, head);
	valid("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", "abc");
	client::ResponsePolicy eof; eof.eof_delimited = true;
	valid("HTTP/1.1 200 OK\r\n\r\nabc", "abc", false, eof);
	client::ResponsePolicy equal; equal.equal_content_lengths = true;
	valid("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 003\r\n\r\nabc", "abc", true, equal);
	invalid("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\nabc");
	equal.unique_headers = true;
	invalid("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\nabc", equal);
	for (const auto* wire : {
		"HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n",
		"HTTP/1.1 0200 OK\r\nContent-Length: 0\r\n\r\n",
		"HTTP/1.1 20 OK\r\nContent-Length: 0\r\n\r\n",
		"HTTP/1.1 200 OK\r\n\r\nabc",
		"HTTP/1.1 200 OK\nContent-Length: 3\n\nabc",
		"\r\nHTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabcEXCESS",
		"HTTP/1.1 200 OK\r\nContent-Length: \r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nContent-Length: 1 0\r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nContent-Length: 18446744073709551616\r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nX-Field: first\r\n folded\r\nContent-Length: 3\r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\nabc",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Trailer: no\r\n\r\n",
		"HTTP/1.1 200 OK\r\nConnection: close;other\r\nContent-Length: 3\r\n\r\nabc",
		"HTTP/1.1 101 Switching Protocols\r\n\r\n",
		"HTTP/1.2 200 OK\r\nContent-Length: 3\r\n\r\nabc"}) invalid(wire);
	client::Limits limits; limits.body_bytes = 0;
	valid("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", "", true, {}, limits);
	invalid("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", {}, limits);
	const std::string empty = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
	limits = {}; limits.header_bytes = empty.size(); limits.wire_bytes = empty.size();
	valid(empty, "", true, {}, limits);
	--limits.header_bytes; invalid(empty, {}, limits);
	limits = {}; limits.informational_responses = 0;
	invalid("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", {}, limits);
	client::ResponseParser parser;
	std::string wire = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc";
	auto input = std::span<const char>(wire.data(), wire.size());
	parser.feed(input, 0); check(input.size() == wire.size());
	parser.feed(input); check(parser.head_ready() && parser.body().empty());
	parser.accept_head(); parser.feed(input); check(input.empty());
	std::fill(wire.begin(), wire.end(), 'X');
	check(std::string_view(parser.body().data(), parser.body().size()) == "abc");
	check(!parser.reusable()); parser.consume_body(3); check(parser.reusable());
	{
		auto account = std::make_shared<allocators::ByteAccount>(4096);
		auto resource = allocators::make_charged_resource(account);
		client::ResponseParser owned({}, {}, allocators::AllocationContext(resource));
		resource.reset();
		const auto available = account->limit() - account->used();
		auto occupied = account->admit(available - 100);
		std::string large = "HTTP/1.1 200 OK\r\nX-Large: " + std::string(1000, 'a') + "\r\nContent-Length: 0\r\n\r\n";
		auto bytes = std::span<const char>(large.data(), large.size());
		bool pressure = false;
		try { owned.feed(bytes); }
		catch (const client::Error &error) { pressure = error.kind() == client::ErrorKind::Pressure; }
		check(pressure && !owned.reusable());
	}
	std::cout << "HTTP client framing, bounds, split input and owning pressure checks passed\n";
}
