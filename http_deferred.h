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

enum class ReplySubmit { Accepted, AlreadyCompleted, Closed, TooLarge, Invalid };
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
// The transport supplies a lifetime-safe wakeup and settles only after its
// original writes have completed or been canceled and reaped. No stack writer,
// request, socket or executor is retained by an escaped application capability.
class DeferredExchange {
  public:
	enum class Phase { Waiting, Ready, Writing, Closing, Settled };
	DeferredExchange(DeferredOptions options, std::function<void()> wake)
		: options_(std::move(options)), wake_(std::move(wake)) {
		if (options_.deadline == std::chrono::steady_clock::time_point::max()) {
			throw std::invalid_argument("deferred reply requires a finite deadline");
		}
	}
	ReplySubmit complete(Response response, std::shared_ptr<void> retention) {
		std::function<void()> wake;
		ReplySubmit result;
		{
			std::lock_guard lock(mutex_);
			if (phase_ == Phase::Closing || phase_ == Phase::Settled) {
				return ReplySubmit::Closed;
			}
			if (phase_ != Phase::Waiting) {
				return ReplySubmit::AlreadyCompleted;
			}
			if (std::chrono::steady_clock::now() >= options_.deadline) {
				phase_ = Phase::Closing;
				reason_ = ReplySettlement::TimedOut;
				result = ReplySubmit::Closed;
			} else {
				result = validate(response);
				if (result != ReplySubmit::Accepted) {
					return result;
				}
				response_.emplace(std::move(response));
				reply_retention_ = std::move(retention);
				phase_ = Phase::Ready;
			}
			wake = wake_;
		}
		notify(wake);
		return result;
	}
	bool observe_peer_disconnect() {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (phase_ == Phase::Writing || phase_ == Phase::Closing || phase_ == Phase::Settled)
				return false;
			phase_ = Phase::Closing;
			reason_ = ReplySettlement::Disconnected;
			wake = wake_;
		}
		notify(wake);
		return true;
	}
	bool close(ReplySettlement reason) {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (phase_ == Phase::Closing || phase_ == Phase::Settled) {
				return false;
			}
			phase_ = Phase::Closing;
			reason_ = reason;
			wake = wake_;
		}
		notify(wake);
		return true;
	}
	bool abandon() {
		std::function<void()> wake;
		{
			std::lock_guard lock(mutex_);
			if (phase_ != Phase::Waiting) {
				return false;
			}
			phase_ = Phase::Closing;
			reason_ = ReplySettlement::Abandoned;
			wake = wake_;
		}
		notify(wake);
		return true;
	}
	std::optional<Response> take_response() {
		std::lock_guard lock(mutex_);
		if (phase_ != Phase::Ready) {
			return std::nullopt;
		}
		phase_ = Phase::Writing;
		auto response = std::move(response_);
		response_.reset();
		return response;
	}
	Phase phase() const {
		std::lock_guard lock(mutex_);
		return phase_;
	}
	auto deadline() const noexcept { return options_.deadline; }
	ReplySettlement closing_reason() const {
		std::lock_guard lock(mutex_);
		return reason_;
	}
	bool settle(ReplySettlement reason) {
		std::function<void(ReplySettlement)> hook;
		std::function<void()> retired_wake;
		std::shared_ptr<void> request, reply;
		std::optional<Response> response;
		{
			std::lock_guard lock(mutex_);
			if (phase_ == Phase::Settled) {
				return false;
			}
			if (phase_ == Phase::Closing) {
				reason = reason_;
			}
			if (reason == ReplySettlement::Written && phase_ != Phase::Writing) {
				throw std::logic_error("unwritten deferred response cannot settle as written");
			}
			phase_ = Phase::Settled;
			reason_ = reason;
			hook = std::move(options_.on_settled);
			retired_wake = std::move(wake_);
			request = std::move(options_.request_retention);
			reply = std::move(reply_retention_);
			response = std::move(response_);
			response_.reset();
		}
		// Application hooks and lease destructors run outside the exchange lock.
		if (hook) {
			try {
				hook(reason);
			} catch (...) {
			}
		}
		return true;
	}

  private:
	static void notify(const std::function<void()> &wake) noexcept {
		if (wake) {
			try {
				wake();
			} catch (...) {
			}
		}
	}
	ReplySubmit validate(const Response &response) const { return validate_reply(response, options_.limits); }
	mutable std::mutex mutex_;
	DeferredOptions options_;
	std::function<void()> wake_;
	std::optional<Response> response_;
	std::shared_ptr<void> reply_retention_;
	Phase phase_ = Phase::Waiting;
	ReplySettlement reason_ = ReplySettlement::Failed;
};
} // namespace detail
class DeferredReply {
  public:
	explicit DeferredReply(std::shared_ptr<detail::DeferredExchange> exchange)
		: exchange_(std::move(exchange)) {
		if (!exchange_) {
			throw std::invalid_argument("null deferred exchange");
		}
	}
	DeferredReply(const DeferredReply &) = delete;
	DeferredReply &operator=(const DeferredReply &) = delete;
	DeferredReply(DeferredReply &&) noexcept = default;
	DeferredReply &operator=(DeferredReply &&other) noexcept {
		if (this != &other) {
			abandon();
			exchange_ = std::move(other.exchange_);
		}
		return *this;
	}
	~DeferredReply() { abandon(); }
	ReplySubmit complete(Response response, std::shared_ptr<void> retention = {}) {
		return exchange_ ? exchange_->complete(std::move(response), std::move(retention))
						 : ReplySubmit::Closed;
	}
	void abandon() noexcept {
		if (exchange_) {
			try {
				exchange_->abandon();
			} catch (...) {
			}
		}
	}

  private:
	std::shared_ptr<detail::DeferredExchange> exchange_;
};
} // namespace http
