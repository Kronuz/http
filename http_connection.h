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
// about the parser (llhttp) and the transport (it is a Kronuz/server BaseClient);
// it turns received bytes into a Request, hands the Request to the application's
// HttpHandler, and writes the Response back. Swapping llhttp for another parser
// is contained here; swapping the reactor lives entirely under BaseClient; and
// the single handler call site below is the one that would become `co_await`.

#pragma once

#include <memory>
#include <string>

#include <llhttp.h>

#include "base_client.h"     // Kronuz/server: BaseClient<ClientImpl>
#include "worker.h"          // Kronuz/server: Worker

#include "http_handler.h"
#include "http_message.h"

namespace http {

class HttpConnection : public BaseClient<HttpConnection> {
	friend BaseClient<HttpConnection>;

	HttpHandler& handler_;

	llhttp_t parser_;
	llhttp_settings_t settings_;

	Request request_;
	std::string cur_field_;
	std::string cur_value_;

public:
	HttpConnection(const std::shared_ptr<Worker>& parent, ev::loop_ref* loop, unsigned int flags, HttpHandler& handler)
		: BaseClient<HttpConnection>(parent, loop, flags),
		  handler_(handler) {
		llhttp_settings_init(&settings_);
		settings_.on_url = &on_url_cb;
		settings_.on_header_field = &on_header_field_cb;
		settings_.on_header_value = &on_header_value_cb;
		settings_.on_header_value_complete = &on_header_value_complete_cb;
		settings_.on_body = &on_body_cb;
		settings_.on_message_complete = &on_message_complete_cb;
		llhttp_init(&parser_, HTTP_REQUEST, &settings_);
		parser_.data = this;
	}

private:
	// ---- Kronuz/server BaseClient<ClientImpl> interface -------------------
	ssize_t on_read(const char* buf, ssize_t received) {
		if (received <= 0) {
			return received;
		}
		llhttp_errno_t err = llhttp_execute(&parser_, buf, static_cast<size_t>(received));
		if (err != HPE_OK && err != HPE_PAUSED) {
			Response resp;
			resp.set(400, std::string("Bad Request: ") + llhttp_errno_name(err) + "\n");
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
		// Finish building the Request from the parser's accumulated state.
		request_.method = llhttp_method_name(static_cast<llhttp_method_t>(llhttp_get_method(&parser_)));
		request_.http_major = llhttp_get_http_major(&parser_);
		request_.http_minor = llhttp_get_http_minor(&parser_);
		request_.keep_alive = llhttp_should_keep_alive(&parser_) != 0;
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
		} else {
			close();
		}
	}

	// ---- llhttp callbacks (reach the instance via parser->data) -----------
	static HttpConnection* self(llhttp_t* p) { return static_cast<HttpConnection*>(p->data); }

	static int on_url_cb(llhttp_t* p, const char* at, size_t len) {
		self(p)->request_.target.append(at, len);
		return 0;
	}
	static int on_header_field_cb(llhttp_t* p, const char* at, size_t len) {
		self(p)->cur_field_.append(at, len);
		return 0;
	}
	static int on_header_value_cb(llhttp_t* p, const char* at, size_t len) {
		self(p)->cur_value_.append(at, len);
		return 0;
	}
	static int on_header_value_complete_cb(llhttp_t* p) {
		auto* c = self(p);
		c->request_.headers.emplace_back(std::move(c->cur_field_), std::move(c->cur_value_));
		c->cur_field_.clear();
		c->cur_value_.clear();
		return 0;
	}
	static int on_body_cb(llhttp_t* p, const char* at, size_t len) {
		self(p)->request_.body.append(at, len);
		return 0;
	}
	static int on_message_complete_cb(llhttp_t* p) {
		self(p)->dispatch();
		return 0;
	}
};

}  // namespace http
