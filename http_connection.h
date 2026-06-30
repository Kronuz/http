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
// this choice is invisible above the seam.
//
// The connection supplies the concrete ResponseWriter (the nested Writer): it
// frames the handler's output as HTTP/1.1 -- Content-Length for a buffered body,
// chunked transfer encoding once the response streams -- and treats end() as the
// signal to finish the request (flush, then keep-alive reset or close). Because
// completion is driven by end() rather than by handle() returning, a handler is
// free to finish the response later (off a worker thread, or as a coroutine)
// without any change here.

#pragma once

#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <http_parser.h>

#include "base_client.h"     // Kronuz/server: BaseClient<ClientImpl>
#include "worker.h"          // Kronuz/server: Worker

#include "http_handler.h"
#include "http_message.h"

namespace http {

class HttpConnection : public BaseClient<HttpConnection> {
	friend BaseClient<HttpConnection>;

	// The connection-backed ResponseWriter. Buffers status + headers until the
	// first body byte, then frames: a single write (or none) becomes a
	// Content-Length response; a second write before end() switches to chunked.
	class Writer : public ResponseWriter {
		HttpConnection* conn_;
		int status_ = 200;
		std::vector<std::pair<std::string, std::string>> headers_;
		std::string pending_;        // first/whole body, held until framing is known
		bool has_pending_ = false;
		bool headers_sent_ = false;
		bool chunked_ = false;
		bool close_ = false;
		bool ended_ = false;

	public:
		explicit Writer(HttpConnection* conn) : conn_(conn) {}

		void reset() {
			status_ = 200;
			headers_.clear();
			pending_.clear();
			has_pending_ = false;
			headers_sent_ = false;
			chunked_ = false;
			close_ = false;
			ended_ = false;
		}

		bool ended() const { return ended_; }

		void status(int code) override { status_ = code; }

		void set_header(std::string_view name, std::string_view value) override {
			headers_.emplace_back(std::string(name), std::string(value));
		}

		void set_close() override { close_ = true; }

		void write(std::string_view chunk) override {
			if (!headers_sent_ && !has_pending_) {
				pending_.assign(chunk);          // might be the whole body; decide at end()
				has_pending_ = true;
				return;
			}
			if (!headers_sent_) {                // a second chunk: commit to streaming
				send_headers(true, 0);
				send_chunk(pending_);
				pending_.clear();
				has_pending_ = false;
			}
			send_chunk(chunk);
		}

		void end() override {
			if (ended_) { return; }
			ended_ = true;
			if (!headers_sent_) {                // buffered: the length is known
				send_headers(false, has_pending_ ? pending_.size() : 0);
				if (has_pending_) { conn_->emit(std::move(pending_)); }
			} else {                             // streaming: terminate the chunked body
				conn_->emit("0\r\n\r\n");
			}
			conn_->complete_response(!close_);
		}

	private:
		bool has_header(std::string_view name) const {
			for (const auto& [k, v] : headers_) {
				if (iequal(k, name)) { return true; }
			}
			return false;
		}

		void send_headers(bool chunked, size_t content_length) {
			bool keep_alive = conn_->request_.keep_alive && !close_;
			std::string out;
			out.reserve(128);
			out += "HTTP/1.1 ";
			out += std::to_string(status_);
			out += ' ';
			out += reason_phrase(status_);
			out += "\r\n";
			for (const auto& [k, v] : headers_) {
				out += k; out += ": "; out += v; out += "\r\n";
			}
			if (chunked) {
				if (!has_header("Transfer-Encoding")) { out += "Transfer-Encoding: chunked\r\n"; }
			} else if (!has_header("Content-Length")) {
				out += "Content-Length: ";
				out += std::to_string(content_length);
				out += "\r\n";
			}
			if (!has_header("Connection")) {
				out += keep_alive ? "Connection: keep-alive\r\n" : "Connection: close\r\n";
			}
			out += "\r\n";
			conn_->emit(std::move(out));
			headers_sent_ = true;
			chunked_ = chunked;
		}

		void send_chunk(std::string_view chunk) {
			if (chunk.empty()) { return; }   // an empty chunk would read as the terminator
			char hex[24];
			int n = std::snprintf(hex, sizeof(hex), "%zx\r\n", chunk.size());
			std::string out;
			out.reserve(static_cast<size_t>(n) + chunk.size() + 2);
			out.append(hex, static_cast<size_t>(n));
			out.append(chunk.data(), chunk.size());
			out += "\r\n";
			conn_->emit(std::move(out));
		}
	};

	HttpHandler& handler_;

	http_parser parser_;
	http_parser_settings settings_;

	Request request_;
	std::string cur_field_;
	std::string cur_value_;
	bool reading_value_ = false;   // tracks the header field->value->field transition
	Writer writer_{this};

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

	// ---- helpers the Writer drives ----------------------------------------
	void emit(std::string bytes) { write(std::move(bytes)); }

	void complete_response(bool keep_alive) {
		bool ka = request_.keep_alive && keep_alive;
		if (ka) {
			request_.clear();
			cur_field_.clear();
			cur_value_.clear();
			reading_value_ = false;
		} else {
			close();
		}
	}

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

		writer_.reset();

		// THE SEAM. end() (not "handle() returned") completes the response, so a
		// handler may answer later off a worker thread or as a coroutine; this is
		// the one call site that becomes `co_await handler_.handle(...)`.
		try {
			handler_.handle(request_, writer_);
		} catch (...) {
			// Backstop: an app should map its own exceptions, but never leave a
			// synchronous handler's connection hung on an unhandled throw.
			if (!writer_.ended()) { writer_.send(500, "Internal Server Error\n"); }
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
