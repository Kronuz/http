/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#pragma once
#include "http_client_core.h"

namespace http::client {
class BufferedResponse {
  private:
	allocators::AllocationContext context_;
  public:
	ResponseHead head;
	std::pmr::string body;
	bool reusable = false;
	explicit BufferedResponse(allocators::AllocationContext context = {})
		: context_(std::move(context)), head(context_.resource()), body(context_.resource()) {}
	BufferedResponse(const BufferedResponse &) = delete;
	BufferedResponse &operator=(const BufferedResponse &) = delete;
	BufferedResponse(BufferedResponse &&) noexcept = default;
	BufferedResponse &operator=(BufferedResponse &&) = delete;
};
class BufferedReceiver {
  public:
	explicit BufferedReceiver(Limits limits = {}, ResponsePolicy policy = {}, allocators::AllocationContext context = {})
		: response_(context), parser_(limits, policy, std::move(context)) {}
	void feed(std::span<const char> &bytes, std::size_t budget = ResponseParser::read_bytes) {
		try {
			parser_.feed(bytes, budget);
			if (parser_.head_ready()) parser_.accept_head();
			const auto body = parser_.body();
			if (!body.empty()) response_.body.append(body.data(), body.size());
			parser_.consume_body(body.size());
			finish();
		} catch (const allocators::ResourcePressure &) {
			failed_ = true; throw Error(ErrorKind::Pressure, "HTTP response allocation pressure");
		} catch (const std::bad_alloc &) {
			failed_ = true; throw Error(ErrorKind::Pressure, "HTTP response allocation failed");
		} catch (...) { failed_ = true; throw; }
	}
	void eof() {
		try { parser_.eof(); finish(); }
		catch (...) { failed_ = true; throw; }
	}
	bool done() const noexcept { return complete_ && !failed_; }
	BufferedResponse take_response() {
		if (!done() || taken_) throw Error(ErrorKind::Options, "HTTP buffered response unavailable");
		taken_ = true;
		return std::move(response_);
	}
  private:
	BufferedResponse response_;
	ResponseParser parser_;
	bool complete_ = false, failed_ = false, taken_ = false;
	void finish() {
		if (!complete_ && parser_.done()) {
			response_.reusable = parser_.reusable();
			response_.head = parser_.take_head();
			complete_ = true;
		}
	}
};
} // namespace http::client
