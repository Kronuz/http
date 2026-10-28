#pragma once
#include <cstddef>
#include <stdexcept>
#include <string>
#include <array>
#include <charconv>

namespace http_test {
struct BoundaryResponse { std::string wire, body; };
inline BoundaryResponse boundary_response(std::size_t bytes, bool chunked = false) {
	for (std::size_t size = bytes; size; --size) {
		std::array<char, 32> encoded{};
		const auto [end, error] = std::to_chars(encoded.data(), encoded.data() + encoded.size(), size, 16);
		if (error != std::errc{}) throw std::runtime_error("chunk size encoding failed");
		const std::string head = chunked
			? "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + std::string(encoded.data(), end) + "\r\n"
			: "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(size) + "\r\n\r\n";
		const std::string tail = chunked ? "\r\n0\r\n\r\n" : "";
		if (head.size() + size + tail.size() == bytes) {
			std::string body(size, 'a');
			return {head + body + tail, std::move(body)};
		}
	}
	throw std::runtime_error("response boundary is too small");
}
inline std::string response_at_boundary(std::size_t bytes) { return boundary_response(bytes).wire; }
inline const std::string unsolicited = "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nforged";
}
