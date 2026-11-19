/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include "http_message.h"
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>

namespace http {

// The reply options and outcomes an owned stream (http_owned_stream.h) shares with the transport.
enum class ReplySubmit { Accepted, TooLarge, Invalid };
enum class ReplySettlement { Written, Abandoned, TimedOut, Disconnected, Shutdown, Failed };
struct ReplyLimits {
	std::size_t headers = 128, header_bytes = 65536, body_bytes = 16 * 1024 * 1024;
};
struct DeferredOptions {
	std::chrono::steady_clock::time_point deadline;
	ReplyLimits limits;
	std::shared_ptr<void> request_retention;
	std::function<void(ReplySettlement)> on_settled;
};
namespace detail {
inline ReplySubmit validate_reply(const Response &response, const ReplyLimits &limits) {
	if (response.status < 200 || response.status > 599 ||
		((response.status == 204 || response.status == 304) && !response.body.empty())) {
		return ReplySubmit::Invalid;
	}
	if (response.headers.size() > limits.headers || response.body.size() > limits.body_bytes) {
		return ReplySubmit::TooLarge;
	}
	std::size_t bytes = 0;
	auto add = [&](std::size_t count) {
		if (count > limits.header_bytes - bytes) {
			return false;
		}
		bytes += count;
		return true;
	};
	auto value_ok = [](std::string_view value) {
		for (unsigned char byte : value) {
			if ((byte < 32 && byte != 9) || byte == 127) {
				return false;
			}
		}
		return true;
	};
	if (!value_ok(response.content_type)) {
		return ReplySubmit::Invalid;
	}
	if (!add(response.content_type.size())) {
		return ReplySubmit::TooLarge;
	}
	for (const auto &[name, value] : response.headers) {
		bool token = !name.empty();
		for (unsigned char byte : name) {
			token = token && ((byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
							  (byte >= '0' && byte <= '9') ||
							  std::string_view("!#$%&'*+-.^_`|~").find(static_cast<char>(byte)) !=
								  std::string_view::npos);
		}
		if (!token || !value_ok(value) || iequal(name, "Content-Length") ||
			iequal(name, "Transfer-Encoding") || iequal(name, "Connection") || iequal(name, "Trailer") ||
			iequal(name, "Upgrade") || iequal(name, "Content-Type")) {
			return ReplySubmit::Invalid;
		}
		if (!add(name.size()) || !add(value.size()) || !add(4)) {
			return ReplySubmit::TooLarge;
		}
	}
	return ReplySubmit::Accepted;
}
} // namespace detail
} // namespace http
