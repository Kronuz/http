// validate_reply: the framing rules an owned stream's metadata passes before it takes the wire.
#include "http_deferred.h"
#include <iostream>
#include <string>

namespace {
int failures = 0;
void check(bool value, const char *message) {
	if (!value) {
		++failures;
		std::cerr << message << '\n';
	}
}
void limits() {
	using namespace http;
	ReplyLimits limits;
	limits.body_bytes = 4;
	limits.headers = 2;
	limits.header_bytes = 64;
	Response response;
	response.body = "four";
	check(detail::validate_reply(response, limits) == ReplySubmit::Accepted, "a body at the cap is accepted");
	response.body = "large";
	check(detail::validate_reply(response, limits) == ReplySubmit::TooLarge, "a body past the cap is too large");
	response = {};
	response.headers = {{"A", "1"}, {"B", "2"}, {"C", "3"}};
	check(detail::validate_reply(response, limits) == ReplySubmit::TooLarge, "a field past the count is too large");
	response = {};
	response.headers = {{"X-Big", std::string(64, 'v')}};
	check(detail::validate_reply(response, limits) == ReplySubmit::TooLarge, "fields past the byte cap are too large");
}
void framing() {
	using namespace http;
	const ReplyLimits limits;
	Response response;
	response.headers.emplace_back("X-Test", "bad\r\nvalue");
	check(detail::validate_reply(response, limits) == ReplySubmit::Invalid, "a control byte in a value is invalid");
	response = {};
	response.content_type = "text/plain\n";
	check(detail::validate_reply(response, limits) == ReplySubmit::Invalid, "a control byte in the content type is invalid");
	for (const char *name : {"", "bad name", "bad\x01", "Content-Length", "Transfer-Encoding", "Connection",
							 "Content-Type", "Trailer", "Upgrade"}) {
		response = {};
		response.headers.emplace_back(name, "value");
		check(detail::validate_reply(response, limits) == ReplySubmit::Invalid,
			  "an invalid or transport-owned field name is invalid");
	}
	for (int status : {199, 600}) {
		response = {};
		response.status = status;
		check(detail::validate_reply(response, limits) == ReplySubmit::Invalid, "a status outside 200-599 is invalid");
	}
	for (int status : {204, 304}) {
		response = {};
		response.status = status;
		response.body = "x";
		check(detail::validate_reply(response, limits) == ReplySubmit::Invalid, "a 204 or 304 with a body is invalid");
	}
	response = {};
	response.headers.emplace_back("X-Tab", "a\tb");
	check(detail::validate_reply(response, limits) == ReplySubmit::Accepted, "a tab in a value is accepted");
}
} // namespace
int main() {
	limits();
	framing();
	std::cout << "reply validation failures: " << failures << '\n';
	return failures ? 1 : 0;
}
