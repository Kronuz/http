// Fuzzes RequestParser (http_request_parser.h): arbitrary bytes, split at arbitrary points,
// with a body cap and each of the three body modes. Invariants: it never consumes more than
// it is given, a capped buffered body never exceeds the cap, a request that exceeded the cap
// has stopped the parse, and a completed request can be taken and the parser reset.
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>

#include "http_request_parser.h"

namespace {
struct CountingSink : http::BodySink {
	std::size_t* total;
	explicit CountingSink(std::size_t* t) : total(t) {}
	void write(std::string_view chunk) override { *total += chunk.size(); }
	void end() override {}
};
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
	if (size < 3) { return 0; }
	const unsigned mode = data[0] % 3;                       // 0 buffered, 1 sink, 2 stream
	const std::size_t limit = data[1] % 4 == 0 ? 0 : (std::size_t{1} << (data[1] % 12));   // 0 (none) or 2^k
	const std::size_t step = 1 + data[2] % 64;               // bytes per feed
	data += 3; size -= 3;

	std::size_t sunk = 0;
	http::RequestParser parser;
	parser.set_body_limit(limit);
	parser.set_headers_callback([&](http::Request&) {
		http::RequestParser::BodyIntake intake;
		if (mode == 1) { intake.sink = std::make_unique<CountingSink>(&sunk); }
		if (mode == 2) { intake.stream = true; }
		return intake;
	});

	std::size_t at = 0;
	while (at < size) {
		const std::size_t n = std::min(step, size - at);
		const std::size_t used = parser.feed(reinterpret_cast<const char*>(data) + at, n);
		if (used > n) { std::abort(); }
		at += n;
		if (parser.errored()) {
			if (parser.body_limit_exceeded() && (mode != 0 || limit == 0)) { std::abort(); }
			return 0;
		}
		if (parser.complete()) {
			http::Request request = parser.take();
			if (mode == 0 && limit != 0 && request.body.size() > limit) { std::abort(); }
			parser.resume();
			parser.reset();
			continue;
		}
		if (parser.headers_done() && mode == 2) { (void)parser.take_pending_body(); }
	}
	return 0;
}
