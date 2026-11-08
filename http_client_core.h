/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 * SPDX-License-Identifier: MIT
 */
#pragma once
#include "http_message.h"
#include <allocators/allocation.h>
#include <allocators/resources.h>
#include <http_parser.h>
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <memory_resource>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace http::client {
namespace detail {
inline bool is_token(std::string_view value) noexcept {
	if (value.empty()) return false;
	for (unsigned char character : value)
		if (!((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			std::string_view("!#$%&'*+-.^_`|~").find(character) != std::string_view::npos)) return false;
	return true;
}
} // namespace detail
enum class ErrorKind {
	Options, Framing, Limit, Pressure, Resolve, Connect, Transport, Deadline, Cancelled, Trust
};
class Error : public std::runtime_error {
  public:
	Error(ErrorKind kind, const char *message, std::error_code cause = {})
		: std::runtime_error(message), kind_(kind), cause_(cause) {}
	ErrorKind kind() const noexcept { return kind_; }
	std::error_code cause() const noexcept { return cause_; }
  private:
	ErrorKind kind_;
	std::error_code cause_;
};
struct Limits {
	std::size_t header_bytes = 65536;
	std::size_t header_fields = 128;
	std::uint64_t body_bytes = 64 * 1024 * 1024;
	std::uint64_t wire_bytes = 128 * 1024 * 1024;
	unsigned informational_responses = 8;
};
struct ResponsePolicy {
	bool head_request = false;
	bool require_http11 = false;
	bool unique_headers = false;
	bool equal_content_lengths = false;
	bool eof_delimited = false;
	bool informational = true;
};
struct Header {
	std::pmr::string name, value;
	Header(std::string_view key, std::string_view text, std::pmr::memory_resource *resource)
		: name(key, resource), value(text, resource) {}
};
struct ResponseHead {
	unsigned status = 0, major = 0, minor = 0;
	std::pmr::vector<Header> headers;
	std::optional<std::uint64_t> content_length;
	bool chunked = false, keep_alive = false;
	explicit ResponseHead(std::pmr::memory_resource *resource) : headers(resource) {}
	std::optional<std::string_view> get(std::string_view name) const noexcept {
		for (const auto &header : headers)
			if (http::iequal(header.name, name)) return std::string_view(header.value);
		return std::nullopt;
	}
	bool contains(std::string_view name) const noexcept { return get(name).has_value(); }
};
// One bounded staging block preserves feed() ownership after input is consumed.
class ResponseParser {
  public:
	static constexpr std::size_t read_bytes = 4096;
	explicit ResponseParser(Limits limits = {}, ResponsePolicy policy = {},
							allocators::AllocationContext context = {})
		: context_(std::move(context)), limits_(limits), policy_(policy),
		  head_(context_.resource()), field_(context_.resource()), value_(context_.resource()) {
		if (!limits_.header_bytes || !limits_.header_fields || !limits_.wire_bytes)
			throw Error(ErrorKind::Options, "invalid HTTP response bounds");
		http_parser_init(&parser_, HTTP_RESPONSE);
		http_parser_set_allow_equal_content_length(&parser_, policy_.equal_content_lengths);
		parser_.data = this;
		settings_.on_header_field = [](http_parser *parser, const char *bytes, std::size_t count) {
			return callback(parser, [&](ResponseParser &self) {
				if (self.headers_accepted_) fail(ErrorKind::Framing, "HTTP trailers unsupported");
				if (self.last_value_) self.commit_header();
				self.field_.append(bytes, count);
			});
		};
		settings_.on_header_value = [](http_parser *parser, const char *bytes, std::size_t count) {
			return callback(parser, [&](ResponseParser &self) {
				self.last_value_ = true;
				self.value_.append(bytes, count);
			});
		};
		settings_.on_headers_complete = [](http_parser *parser) {
			const auto result = callback(parser, [](ResponseParser &self) { self.finish_head(); });
			if (result) return -1;
			auto &self = *static_cast<ResponseParser *>(parser->data);
			return self.policy_.head_request || self.head_.status < 200 ||
				self.head_.status == 204 || self.head_.status == 304 ? 1 : 0;
		};
		settings_.on_body = [](http_parser *parser, const char *bytes, std::size_t count) {
			return callback(parser, [&](ResponseParser &self) {
				if (count > self.limits_.body_bytes - self.body_bytes_)
					fail(ErrorKind::Limit, "HTTP response body bound");
				if (count > self.body_.size() - self.body_size_)
					fail(ErrorKind::Limit, "HTTP response intake bound");
				std::memcpy(self.body_.data() + self.body_size_, bytes, count);
				self.body_size_ += count;
				self.body_bytes_ += count;
			});
		};
		settings_.on_message_complete = [](http_parser *parser) {
			return callback(parser, [](ResponseParser &self) {
				if (self.head_.status < 200) {
					self.head_.headers.clear();
					self.head_.content_length.reset();
					self.header_bytes_ = 0;
					self.status_position_ = 0;
					self.raw_head_done_ = self.seen_line_ = self.pending_cr_ = false;
					self.line_empty_ = self.line_start_ = true;
				} else {
					self.complete_ = true;
					http_parser_pause(&self.parser_, 1);
				}
			});
		};
	}
	ResponseParser(const ResponseParser &) = delete;
	ResponseParser &operator=(const ResponseParser &) = delete;
	bool head_ready() const noexcept { return head_waiting_; }
	const ResponseHead &head() const noexcept { return head_; }
	void accept_head() {
		if (failed_ || !head_waiting_) fail(ErrorKind::Options, "HTTP response head unavailable");
		head_waiting_ = false;
		headers_accepted_ = true;
		http_parser_pause(&parser_, 0);
	}
	std::span<const char> body() const noexcept {
		return {body_.data() + body_position_, body_size_ - body_position_};
	}
	void consume_body(std::size_t count) {
		if (count > body_size_ - body_position_) fail(ErrorKind::Options, "HTTP body consumption bound");
		body_position_ += count;
		if (body_position_ == body_size_) body_position_ = body_size_ = 0;
	}
	bool done() const noexcept { return complete_; }
	bool reusable() const noexcept {
		return reusable_head() && head_.keep_alive;
	}
	std::uint64_t received_bytes() const noexcept { return wire_bytes_; }
	void feed(std::span<const char> &input, std::size_t budget = read_bytes) {
		try {
			if (failed_) fail(ErrorKind::Framing, "HTTP response parser unavailable");
			if (complete_) {
				if (!input.empty()) fail(ErrorKind::Framing, "trailing HTTP response bytes");
				return;
			}
			if (head_waiting_ || !body().empty() || input.empty() || !budget) return;
			auto count = std::min({input.size(), budget, read_bytes});
			if (!raw_head_done_) count = scan_head(input.first(count));
			if (count > limits_.wire_bytes - wire_bytes_)
				fail(ErrorKind::Limit, "HTTP response wire bound");
			const auto used = http_parser_execute(&parser_, &settings_, input.data(), count);
			input = input.subspan(used);
			wire_bytes_ += used;
			check_parser();
			if (used != count && !head_waiting_ && !complete_)
				fail(ErrorKind::Framing, "invalid HTTP response framing");
			if (complete_ && !input.empty()) fail(ErrorKind::Framing, "trailing HTTP response bytes");
		} catch (...) {
			failed_ = true;
			throw;
		}
	}
	void eof() {
		try {
			if (failed_ || head_waiting_ || !body().empty())
				fail(ErrorKind::Framing, "HTTP EOF before consumption");
			if (complete_) return;
			http_parser_execute(&parser_, &settings_, nullptr, 0);
			check_parser();
			if (!complete_) fail(ErrorKind::Framing, "truncated HTTP response");
		} catch (...) { failed_ = true; throw; }
	}
	bool discard(std::size_t &bytes, std::size_t &tokens) noexcept {
		failed_ = true;
		if (disposal_stage_ == 5) return true;
		if (!bytes || !tokens) return false;
		const auto pay = [&](std::size_t amount) {
			if (!tokens) return false;
			if (!disposal_remaining_) disposal_remaining_ = amount;
			const auto used = std::min(bytes, disposal_remaining_);
			bytes -= used; disposal_remaining_ -= used;
			if (disposal_remaining_) return false;
			--tokens;
			return true;
		};
		while (!head_.headers.empty()) {
			if (disposal_stage_ == 0) {
				if (!pay(head_.headers.back().value.capacity())) return false;
				disposal_stage_ = 1;
			}
			if (!pay(head_.headers.back().name.capacity())) return false;
			head_.headers.pop_back();
			disposal_stage_ = 0;
		}
		if (disposal_stage_ < 2) disposal_stage_ = 2;
		if (disposal_stage_ == 2) {
			if (!pay(head_.headers.capacity() * sizeof(Header))) return false;
			std::pmr::vector<Header> empty(context_.resource());
			head_.headers.swap(empty);
			disposal_stage_ = 3;
		}
		if (disposal_stage_ == 3) {
			if (!pay(field_.capacity())) return false;
			std::pmr::string empty(context_.resource());
			field_.swap(empty);
			disposal_stage_ = 4;
		}
		if (!pay(value_.capacity())) return false;
		std::pmr::string empty(context_.resource());
		value_.swap(empty);
		error_ = {};
		body_position_ = body_size_ = 0;
		disposal_stage_ = 5;
		return true;
	}
  private:
	friend class BufferedReceiver;
	ResponseHead take_head() {
		if (!reusable_head()) fail(ErrorKind::Options, "HTTP response head cannot transfer");
		head_taken_ = true;
		return std::move(head_);
	}
	bool reusable_head() const noexcept {
		return !failed_ && !head_taken_ && complete_ && headers_accepted_ && body().empty();
	}
	[[noreturn]] static void fail(ErrorKind kind, const char *message) { throw Error(kind, message); }
	template <class F> static int callback(http_parser *parser, F &&function) noexcept {
		auto &self = *static_cast<ResponseParser *>(parser->data);
		try { function(self); return 0; }
		catch (...) { if (!self.error_) self.error_ = std::current_exception(); return 1; }
	}
	void check_parser() {
		if (error_) {
			try { std::rethrow_exception(error_); }
			catch (const allocators::ResourcePressure &) { fail(ErrorKind::Pressure, "HTTP allocation pressure"); }
			catch (const std::bad_alloc &) { fail(ErrorKind::Pressure, "HTTP allocation failed"); }
		}
		const auto error = HTTP_PARSER_ERRNO(&parser_);
		if (parser_.upgrade || (error != HPE_OK && error != HPE_PAUSED))
			fail(ErrorKind::Framing, "invalid HTTP response framing");
	}
	static std::string_view trim(std::string_view value) noexcept {
		while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) value.remove_prefix(1);
		while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) value.remove_suffix(1);
		return value;
	}
	void commit_header() {
		if (field_.empty() || head_.headers.size() >= limits_.header_fields)
			fail(ErrorKind::Limit, "HTTP response header field bound");
		for (auto &character : field_)
			if (character >= 'A' && character <= 'Z') character += 'a' - 'A';
		if (!detail::is_token(field_)) fail(ErrorKind::Framing, "invalid HTTP header field");
		const auto text = trim(value_);
		if (head_.contains(field_) && (policy_.unique_headers || field_ == "transfer-encoding"))
			fail(ErrorKind::Framing, "duplicate HTTP response header");
		if (field_ == "content-length" && (text.empty() ||
			!std::all_of(text.begin(), text.end(), [](unsigned char c) { return c >= '0' && c <= '9'; })))
			fail(ErrorKind::Framing, "invalid HTTP Content-Length");
		head_.headers.emplace_back(field_, value_, context_.resource());
		field_.clear(); value_.clear(); last_value_ = false;
	}
	void finish_head() {
		if (!field_.empty()) commit_header();
		head_.status = parser_.status_code;
		head_.major = parser_.http_major; head_.minor = parser_.http_minor;
		if (head_.major != 1 || head_.minor > 1 || (policy_.require_http11 && head_.minor != 1) ||
			head_.status < 100 || head_.status > 599 || head_.status == 101 || head_.contains("upgrade"))
			fail(ErrorKind::Framing, "unsupported HTTP response");
		if (head_.contains("trailer")) fail(ErrorKind::Framing, "HTTP trailers unsupported");
		head_.chunked = head_.contains("transfer-encoding");
		if (head_.chunked && (!http::iequal(trim(*head_.get("transfer-encoding")), "chunked") ||
			head_.contains("content-length"))) fail(ErrorKind::Framing, "HTTP framing conflict");
		if (head_.contains("content-length")) head_.content_length = parser_.content_length;
		const bool bodyless = policy_.head_request || head_.status < 200 || head_.status == 204 || head_.status == 304;
		if (!bodyless && head_.content_length && *head_.content_length > limits_.body_bytes)
			fail(ErrorKind::Limit, "HTTP response body bound");
		if (!bodyless && !head_.chunked && !head_.content_length && !policy_.eof_delimited)
			fail(ErrorKind::Framing, "HTTP response requires explicit framing");
		head_.keep_alive = http_should_keep_alive(&parser_) &&
			(bodyless || head_.chunked || head_.content_length.has_value());
		for (const auto &header : head_.headers) if (header.name == "connection") {
			auto tokens = std::string_view(header.value);
			for (;;) {
				const auto comma = tokens.find(',');
				const auto token = trim(tokens.substr(0, comma));
				if (!detail::is_token(token))
					fail(ErrorKind::Framing, "invalid HTTP Connection token");
				if (http::iequal(token, "upgrade")) fail(ErrorKind::Framing, "HTTP upgrade unsupported");
				if (http::iequal(token, "close")) head_.keep_alive = false;
				if (comma == std::string_view::npos) break;
				tokens.remove_prefix(comma + 1);
			}
		}
		if (head_.status < 200) {
			if (!policy_.informational || informational_ >= limits_.informational_responses)
				fail(ErrorKind::Limit, "HTTP informational response bound");
			++informational_;
			return;
		}
		head_waiting_ = true;
		http_parser_pause(&parser_, 1);
	}
	std::size_t scan_head(std::span<const char> input) {
		for (std::size_t i = 0; i < input.size(); ++i) {
			if (header_bytes_ == limits_.header_bytes) fail(ErrorKind::Limit, "HTTP response header byte bound");
			++header_bytes_;
			const auto character = static_cast<unsigned char>(input[i]);
			if (pending_cr_) {
				if (character != '\n') fail(ErrorKind::Framing, "invalid HTTP line ending");
				pending_cr_ = false;
				if (line_empty_ && !seen_line_) fail(ErrorKind::Framing, "empty HTTP status line");
				if (line_empty_ && seen_line_) { raw_head_done_ = true; return i + 1; }
				seen_line_ = true; line_empty_ = line_start_ = true;
			} else if (character == '\r') {
				if (!seen_line_ && status_position_ < 13)
					fail(ErrorKind::Framing, "invalid HTTP status line");
				pending_cr_ = true;
			} else {
				if (character == '\n' || character == 127 || (character < 32 && character != '\t') ||
					(line_start_ && seen_line_ && (character == ' ' || character == '\t')))
					fail(ErrorKind::Framing, "invalid HTTP header syntax");
				if (!seen_line_) {
					const auto position = status_position_++;
					if ((position < 7 && character != static_cast<unsigned char>("HTTP/1."[position])) ||
						(position == 7 && character != '0' && character != '1') ||
						((position == 8 || position == 12) && character != ' ') ||
						(position >= 9 && position <= 11 && (character < '0' || character > '9')))
						fail(ErrorKind::Framing, "invalid HTTP status line");
				}
				line_empty_ = line_start_ = false;
			}
		}
		return input.size();
	}
	allocators::AllocationContext context_;
	Limits limits_;
	ResponsePolicy policy_;
	ResponseHead head_;
	std::pmr::string field_, value_;
	http_parser parser_{};
	http_parser_settings settings_{};
	std::array<char, read_bytes> body_{};
	std::size_t body_position_ = 0, body_size_ = 0, header_bytes_ = 0, status_position_ = 0;
	std::uint64_t body_bytes_ = 0, wire_bytes_ = 0;
	unsigned informational_ = 0;
	unsigned disposal_stage_ = 0;
	std::size_t disposal_remaining_ = 0;
	bool last_value_ = false, head_waiting_ = false, headers_accepted_ = false, complete_ = false, head_taken_ = false,
		failed_ = false, raw_head_done_ = false, seen_line_ = false, pending_cr_ = false,
		line_empty_ = true, line_start_ = true;
	std::exception_ptr error_;
};
} // namespace http::client
