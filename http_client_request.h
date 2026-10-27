/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#pragma once
#include "http_client_core.h"
#include <charconv>
#include <chrono>

namespace http::client {
class Deadline {
  public:
	using Clock = std::chrono::steady_clock;
	explicit Deadline(std::chrono::milliseconds timeout) {
		if (timeout.count() <= 0 || timeout > std::chrono::duration_cast<std::chrono::milliseconds>(Clock::duration::max()))
			throw Error(ErrorKind::Options, "invalid HTTP timeout");
		const auto duration = std::chrono::duration_cast<Clock::duration>(timeout);
		const auto now = Clock::now();
		if (now > Clock::time_point::max() - duration)
			throw Error(ErrorKind::Options, "HTTP deadline overflow");
		expires_ = now + duration;
	}
	Clock::time_point expires() const noexcept { return expires_; }
	Clock::duration remaining() const {
		const auto now = Clock::now();
		if (now >= expires_) throw Error(ErrorKind::Deadline, "HTTP deadline exceeded");
		return expires_ - now;
	}
	int poll_milliseconds() const {
		const auto duration = remaining();
		auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
		if (milliseconds < duration) ++milliseconds;
		return milliseconds.count() > std::numeric_limits<int>::max()
			? std::numeric_limits<int>::max() : static_cast<int>(milliseconds.count());
	}
  private:
	Clock::time_point expires_;
};
struct RequestField { std::string_view name, value; };
struct RequestView {
	std::string_view method, authority, target;
	std::span<const RequestField> headers;
	std::span<const char> body;
	bool close = false;
};
// Preparation copies borrowed request data before a lazy coroutine can suspend.
class PreparedRequest {
  public:
	explicit PreparedRequest(RequestView request, Limits limits = {}, allocators::AllocationContext context = {})
		: context_(std::move(context)), wire_(context_.resource()), head_(request.method == "HEAD") {
		try { prepare(request, limits); }
		catch (const allocators::ResourcePressure &) { throw Error(ErrorKind::Pressure, "HTTP request allocation pressure"); }
		catch (const std::bad_alloc &) { throw Error(ErrorKind::Pressure, "HTTP request allocation failed"); }
	}
	PreparedRequest(const PreparedRequest &) = delete;
	PreparedRequest &operator=(const PreparedRequest &) = delete;
	PreparedRequest(PreparedRequest &&) noexcept = default;
	PreparedRequest &operator=(PreparedRequest &&) = delete;
	std::string_view wire() const noexcept { return wire_; }
	bool head_request() const noexcept { return head_; }
	const allocators::AllocationContext &context() const noexcept { return context_; }
  private:
	allocators::AllocationContext context_;
	std::pmr::string wire_;
	bool head_;
	static bool text(std::string_view value, bool spaces) noexcept {
		for (unsigned char character : value)
			if (spaces ? ((character < 32 && character != '\t') || character == 127)
				: (character < 33 || character >= 127)) return false;
		return true;
	}
	void append(std::string_view value, std::uint64_t bound) {
		if (wire_.size() > bound || value.size() > bound - wire_.size())
			throw Error(ErrorKind::Limit, "HTTP request byte bound");
		wire_.append(value);
	}
	void prepare(RequestView request, Limits limits) {
		if (!detail::is_token(request.method) || request.method == "CONNECT" || request.authority.empty() || !text(request.authority, false) ||
			request.authority.find_first_of("/@?#\\") != std::string_view::npos ||
			request.target.empty() || !text(request.target, false) || request.target.find('#') != std::string_view::npos ||
			(request.target.front() != '/' && request.target != "*"))
			throw Error(ErrorKind::Options, "invalid HTTP request line or authority");
		if (request.body.size() > limits.body_bytes || limits.header_fields < 3 ||
			request.headers.size() > limits.header_fields - 3)
			throw Error(ErrorKind::Limit, "HTTP request field or body bound");
		const auto header_bound = std::min<std::uint64_t>(limits.header_bytes, limits.wire_bytes);
		const auto add = [&](std::string_view value) { append(value, header_bound); };
		add(request.method); add(" "); add(request.target); add(" HTTP/1.1\r\nHost: "); add(request.authority); add("\r\n");
		for (const auto &header : request.headers) {
			if (!detail::is_token(header.name) || !text(header.value, true) ||
				http::iequal(header.name, "host") || http::iequal(header.name, "content-length") ||
				http::iequal(header.name, "transfer-encoding") || http::iequal(header.name, "connection") ||
				http::iequal(header.name, "trailer") || http::iequal(header.name, "upgrade"))
				throw Error(ErrorKind::Options, "invalid or reserved HTTP request header");
			add(header.name); add(": "); add(header.value); add("\r\n");
		}
		std::array<char, std::numeric_limits<std::size_t>::digits10 + 1> length;
		const auto converted = std::to_chars(length.data(), length.data() + length.size(), request.body.size());
		if (converted.ec != std::errc{}) throw Error(ErrorKind::Options, "HTTP request length conversion");
		add("Content-Length: "); add({length.data(), static_cast<std::size_t>(converted.ptr - length.data())});
		add(request.close ? "\r\nConnection: close\r\n\r\n" : "\r\nConnection: keep-alive\r\n\r\n");
		if (!request.body.empty()) append({request.body.data(), request.body.size()}, limits.wire_bytes);
	}
};
} // namespace http::client
