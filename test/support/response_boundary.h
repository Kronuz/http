#pragma once
#include <cstddef>
#include <stdexcept>
#include <string>

namespace http_test {
inline std::string response_at_boundary(std::size_t bytes) {
	for (std::size_t size = bytes; size; --size) {
		const auto head = "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(size) + "\r\n\r\n";
		if (head.size() + size == bytes) return head + std::string(size, 'a');
	}
	throw std::runtime_error("response boundary is too small");
}
inline const std::string unsolicited = "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nforged";
}
