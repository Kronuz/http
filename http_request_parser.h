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

// RequestParser -- a transport-agnostic wrapper around Kronuz/http-parser that turns a
// stream of received bytes into completed http::Request values. It owns the parser and
// the partial header/body state and knows nothing of the reactor: any transport (the
// libev connection, an Asio coroutine, a test) feeds it bytes and drains completed
// requests. This is the seam that lets the HTTP protocol ride on any runtime -- the
// parser callbacks (which used to live inside the libev HttpConnection) move here so
// the Asio connection reuses them verbatim. Buffered body only (the streaming BodySink
// intake stays a connection concern); pipelined requests are yielded one at a time.

#pragma once

#include <charconv>
#include <cstddef>
#include <string>
#include <utility>

#include <http_parser.h>

#include "http_message.h"

namespace http {

class RequestParser {
public:
	RequestParser() {
		http_parser_settings_init(&settings_);
		settings_.on_url = &on_url_cb;
		settings_.on_header_field = &on_header_field_cb;
		settings_.on_header_value = &on_header_value_cb;
		settings_.on_headers_complete = &on_headers_complete_cb;
		settings_.on_body = &on_body_cb;
		settings_.on_message_complete = &on_message_complete_cb;
		http_parser_init(&parser_, HTTP_REQUEST);
		parser_.data = this;
	}

	// Feed `len` received bytes. Returns the number consumed; a short count means a
	// parse error (see errored()). When a full request has been parsed, complete()
	// becomes true and the request is available via take() -- feed the remainder (a
	// pipelined next request) after take() resets the parser.
	std::size_t feed(const char* data, std::size_t len) {
		return http_parser_execute(&parser_, &settings_, data, len);
	}

	bool complete() const { return complete_; }

	// Unpause the parser after take(), so the next pipelined request on the same
	// connection can be parsed. (on_message_complete pauses so exactly one request is
	// produced per feed, leaving pipelined bytes buffered by the caller.)
	void resume() { http_parser_pause(&parser_, 0); }

	bool errored() const {
		auto e = HTTP_PARSER_ERRNO(&parser_);
		return e != HPE_OK && e != HPE_PAUSED;
	}

	// Move out the completed request and reset for the next one on the same connection.
	Request take() {
		Request out = std::move(request_);
		request_.clear();
		cur_field_.clear();
		cur_value_.clear();
		reading_value_ = false;
		complete_ = false;
		return out;
	}

	// The Content-Length the last completed request advertised (0 if none) -- lets the
	// connection reserve its body buffer.
	std::size_t content_length() const {
		auto cl = request_.header("Content-Length");
		std::size_t n = 0;
		if (!cl.empty()) { std::from_chars(cl.data(), cl.data() + cl.size(), n); }
		return n;
	}

private:
	http_parser parser_;
	http_parser_settings settings_;
	Request request_;
	std::string cur_field_;
	std::string cur_value_;
	bool reading_value_ = false;
	bool complete_ = false;

	void finalize() {
		request_.method = http_method_str(static_cast<enum http_method>(parser_.method));
		request_.http_major = parser_.http_major;
		request_.http_minor = parser_.http_minor;
		request_.keep_alive = http_should_keep_alive(&parser_) != 0;
		auto qpos = request_.target.find('?');
		if (qpos == std::string::npos) {
			request_.path = request_.target;
		} else {
			request_.path = request_.target.substr(0, qpos);
			request_.query = request_.target.substr(qpos + 1);
		}
	}

	static RequestParser* self(http_parser* p) { return static_cast<RequestParser*>(p->data); }

	static int on_url_cb(http_parser* p, const char* at, std::size_t len) {
		self(p)->request_.target.append(at, len);
		return 0;
	}
	static int on_header_field_cb(http_parser* p, const char* at, std::size_t len) {
		auto* c = self(p);
		if (c->reading_value_) {
			c->request_.headers.emplace_back(std::move(c->cur_field_), std::move(c->cur_value_));
			c->cur_field_.clear();
			c->cur_value_.clear();
			c->reading_value_ = false;
		}
		c->cur_field_.append(at, len);
		return 0;
	}
	static int on_header_value_cb(http_parser* p, const char* at, std::size_t len) {
		auto* c = self(p);
		c->reading_value_ = true;
		c->cur_value_.append(at, len);
		return 0;
	}
	static int on_headers_complete_cb(http_parser* p) {
		auto* c = self(p);
		if (!c->cur_field_.empty()) {
			c->request_.headers.emplace_back(std::move(c->cur_field_), std::move(c->cur_value_));
			c->cur_field_.clear();
			c->cur_value_.clear();
			c->reading_value_ = false;
		}
		c->finalize();
		auto cl = c->content_length();
		if (cl != 0) { c->request_.body.reserve(cl); }
		return 0;
	}
	static int on_body_cb(http_parser* p, const char* at, std::size_t len) {
		self(p)->request_.body.append(at, len);
		return 0;
	}
	static int on_message_complete_cb(http_parser* p) {
		auto* c = self(p);
		c->complete_ = true;
		http_parser_pause(p, 1);   // stop after this message so pipelined bytes wait for take()
		return 0;
	}
};

}  // namespace http
