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

// HttpConnection: a generic HTTP/1.1 connection. It is the only place that knows
// about the parser and the transport (it is a Kronuz/server BaseClient); it
// turns received bytes into a Request, hands the Request to the application's
// HttpHandler, and writes the Response back. Swapping the parser is contained
// here; swapping the reactor lives entirely under BaseClient; and the single
// handler call site below is the one that would become `co_await`.
//
// The parser is Kronuz/http-parser (the Joyent http_parser fork), chosen over
// stricter parsers because it accepts arbitrary request methods -- Xapiand's
// REST API uses custom verbs (COUNT, INFO, DUMP, RESTORE, ...) that a method-
// validating parser rejects outright. Because parsing only *produces* a Request,
// this choice is invisible above the seam: the handler and the Request/Response
// structs are parser-agnostic.

#pragma once

#include <memory>
#include <string>
#include <utility>

#include <http_parser.h>

#include "base_client.h"     // Kronuz/server: BaseClient<ClientImpl>
#include "worker.h"          // Kronuz/server: Worker

#include "http_handler.h"
#include "http_message.h"

namespace http {

class HttpConnection : public BaseClient<HttpConnection> {
	friend BaseClient<HttpConnection>;

	HttpHandler& handler_;

	http_parser parser_;
	http_parser_settings settings_;

	Request request_;
	std::string cur_field_;
	std::string cur_value_;
	bool reading_value_ = false;   // tracks the header field->value->field transition

public:
	HttpConnection(const std::shared_ptr<Worker>& parent, ev::loop_ref* loop, unsigned int flags, HttpHandler& handler)
		: BaseClient<HttpConnection>(parent, loop, flags),
		  handler_(handler) {
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

private:
	// ---- Kronuz/server BaseClient<ClientImpl> interface -------------------
	ssize_t on_read(const char* buf, ssize_t received) {
		if (received <= 0) {
			return received;
		}
		http_parser_execute(&parser_, &settings_, buf, static_cast<size_t>(received));
		if (HTTP_PARSER_ERRNO(&parser_) != HPE_OK) {
			Response resp;
			resp.set(400, std::string("Bad Request: ") + http_errno_name(HTTP_PARSER_ERRNO(&parser_)) + "\n");
			resp.close = true;
			write(resp.serialize(false));
			close();
		}
		return received;
	}

	// The file-transfer path (replication) is not part of HTTP.
	void on_read_file(const char* /*buf*/, ssize_t /*received*/) {}
	void on_read_file_done() {}

	bool is_idle() { return request_.method.empty() && request_.body.empty(); }

	// ---- the request is complete: hand it to the application --------------
	void dispatch() {
		// Finish building the Request from the parser's accumulated state. For a
		// custom method, http_method_str returns the verb the fork added.
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

		// THE SEAM. Value-semantic, so this is the one line that becomes
		// `co_await handler_.handle(request_, response)` when the engine moves to
		// coroutine handlers; the application's logic is unchanged.
		Response response;
		handler_.handle(request_, response);

		bool keep_alive = request_.keep_alive && !response.close;
		write(response.serialize(keep_alive));
		if (keep_alive) {
			request_.clear();
			cur_field_.clear();
			cur_value_.clear();
			reading_value_ = false;
		} else {
			close();
		}
	}

	// ---- http_parser callbacks (reach the instance via parser->data) ------
	static HttpConnection* self(http_parser* p) { return static_cast<HttpConnection*>(p->data); }

	static int on_url_cb(http_parser* p, const char* at, size_t len) {
		self(p)->request_.target.append(at, len);
		return 0;
	}
	static int on_header_field_cb(http_parser* p, const char* at, size_t len) {
		auto* c = self(p);
		if (c->reading_value_) {   // a new field begins: the previous pair is complete
			c->request_.headers.emplace_back(std::move(c->cur_field_), std::move(c->cur_value_));
			c->cur_field_.clear();
			c->cur_value_.clear();
			c->reading_value_ = false;
		}
		c->cur_field_.append(at, len);
		return 0;
	}
	static int on_header_value_cb(http_parser* p, const char* at, size_t len) {
		auto* c = self(p);
		c->reading_value_ = true;
		c->cur_value_.append(at, len);
		return 0;
	}
	static int on_headers_complete_cb(http_parser* p) {
		auto* c = self(p);
		if (!c->cur_field_.empty()) {   // flush the final pending header
			c->request_.headers.emplace_back(std::move(c->cur_field_), std::move(c->cur_value_));
			c->cur_field_.clear();
			c->cur_value_.clear();
			c->reading_value_ = false;
		}
		return 0;
	}
	static int on_body_cb(http_parser* p, const char* at, size_t len) {
		self(p)->request_.body.append(at, len);
		return 0;
	}
	static int on_message_complete_cb(http_parser* p) {
		self(p)->dispatch();
		return 0;
	}
};

}  // namespace http
