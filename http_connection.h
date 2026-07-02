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
//
// The un-stallable model. When the connection is given a Dispatcher (one per
// reactor, shared-nothing across cores), handle() runs on a worker thread so a
// slow or blocking handler never stalls the event loop -- the reactor keeps
// accepting, reading, and serving other connections. The contract:
//   * One in-flight handler per connection. Reading is paused (the parser is
//     paused at message-complete and the read io is stopped) while the handler
//     runs, so the worker has sole, race-free access to the request, responses
//     stay ordered, and a stopped read provides TCP backpressure. Any pipelined
//     bytes already received are stashed and re-fed when the handler completes.
//   * The worker drives the response through the thread-safe write path and never
//     touches reactor-owned connection state. Completion (end()) is handed back to
//     the reactor via an ev::async; the reactor runs complete_response + resumes
//     reading, because that step touches the ev watchers / close and must run on
//     the loop. The task captures share_this so the connection outlives the work.
//   * submit() returning false (the bounded queue is full) is the backpressure
//     signal: the reactor answers 503 inline instead of growing an unbounded
//     backlog. With no Dispatcher, handle() runs inline on the reactor (the cheap
//     fast path), exactly as before.
// Offloaded handlers run concurrently across connections, so an application that
// opts into a Dispatcher must make its HttpHandler thread-safe.

#pragma once

#include <atomic>
#include <charconv>
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <http_parser.h>

#include "base_client.h"     // Kronuz/server: BaseClient<ClientImpl>
#include "worker.h"          // Kronuz/server: Worker

#include "http_compression.h"
#include "http_conditional.h"
#include "http_dispatcher.h"
#include "http_handler.h"
#include "http_message.h"
#include "http_range.h"

namespace http {

// Service-level response transforms the connection applies after a handler finishes
// (configured once on HttpService; a connection sees a pointer, null => none). They
// are transparent: the application just writes its bytes.
struct ResponseOptions {
	bool compress = false;             // negotiate Accept-Encoding -> compress the body
	CompressionOptions compression{};  // ...with these tunables
	bool conditional = false;          // derive an ETag + answer 304 on If-None-Match
	bool ranges = false;               // serve a byte Range as 206 Partial Content
};

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
				if (has_pending_) {
					maybe_conditional();             // sets ETag; may flip to a bodyless 304
					if (status_ != 304) { maybe_range(); }   // may flip to 206 (a slice) or 416
					maybe_compress();                // self-skips 204/206/304 and tiny bodies
				}
				send_headers(false, has_pending_ ? pending_.size() : 0);
				if (has_pending_) { conn_->emit(std::move(pending_)); }
			} else {                             // streaming: terminate the chunked body
				conn_->emit("0\r\n\r\n");
			}
			conn_->response_finished(!close_);
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
			} else if (!has_header("Content-Length") && status_ != 304) {   // 304 is defined bodyless
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

		// Transparent response compression (buffered path only). If enabled and the
		// client advertised a codec we produce, swap the body for its compressed form
		// and set Content-Encoding + Vary. Runs wherever end() runs -- on a worker for
		// offloaded handlers, which is where the CPU should be. A streamed/chunked
		// response is left as-is (the first chunk already committed the framing).
		void maybe_compress() {
			if (conn_->response_ == nullptr || !conn_->response_->compress) { return; }
			const CompressionOptions& opt = conn_->response_->compression;
			if (pending_.size() < opt.min_size) { return; }              // too small to bother
			if (has_header("Content-Encoding")) { return; }              // handler already encoded
			if (status_ == 204 || status_ == 304 || status_ == 206 || (status_ >= 100 && status_ < 200)) { return; }  // bodyless / a partial slice
			std::string_view coding = negotiate_encoding(conn_->request_.header("Accept-Encoding"));
			if (coding.empty()) { return; }                              // client accepts nothing we offer
			std::string compressed = encode(coding, pending_, opt.zstd_level);
			if (compressed.empty() || compressed.size() >= pending_.size()) { return; }   // didn't help
			pending_ = std::move(compressed);
			headers_.emplace_back("Content-Encoding", std::string(coding));
			if (!has_header("Vary")) { headers_.emplace_back("Vary", "Accept-Encoding"); }
		}

		// Conditional GET: derive a weak ETag from the (uncompressed) body and, if the
		// client's If-None-Match already holds it, drop the body and answer 304. Runs
		// before maybe_compress so the ETag covers the resource, not the wire bytes,
		// and a 304 skips compression entirely. Buffered GET/HEAD 200s only.
		void maybe_conditional() {
			if (conn_->response_ == nullptr || !conn_->response_->conditional) { return; }
			if (status_ != 200) { return; }
			const std::string& method = conn_->request_.method;
			if (method != "GET" && method != "HEAD") { return; }
			if (has_header("ETag")) { return; }                          // handler set its own validator
			std::string etag = weak_etag(pending_);
			std::string_view inm = conn_->request_.header("If-None-Match");
			if (!inm.empty() && if_none_match(inm, etag)) {
				status_ = 304;                                           // not modified: no body
				pending_.clear();
				has_pending_ = false;
			}
			headers_.emplace_back("ETag", std::move(etag));
		}

		// Range request: serve a single byte range of the buffered body as 206 Partial
		// Content (Content-Range), or 416 if it can't be satisfied. Advertises
		// Accept-Ranges: bytes. Runs after conditional, before compression; a 206 is
		// served uncompressed (range + content-coding is a tar pit, and ranges are for
		// media, which is already compressed). Buffered GET 200s only; a multi-range or
		// unrecognized header is left as the full 200.
		void maybe_range() {
			if (conn_->response_ == nullptr || !conn_->response_->ranges) { return; }
			if (status_ != 200 || conn_->request_.method != "GET") { return; }
			if (!has_header("Accept-Ranges")) { headers_.emplace_back("Accept-Ranges", "bytes"); }
			std::string_view spec = conn_->request_.header("Range");
			if (spec.empty()) { return; }
			const std::size_t total = pending_.size();
			ByteRange r = parse_byte_range(spec, total);
			if (!r.recognized) { return; }                              // multi/other: serve the full 200
			if (!r.satisfiable) {
				status_ = 416;                                          // Range Not Satisfiable
				pending_.clear();
				has_pending_ = false;
				headers_.emplace_back("Content-Range", "bytes */" + std::to_string(total));
				return;
			}
			headers_.emplace_back("Content-Range", "bytes " + std::to_string(r.start) + "-" + std::to_string(r.end) + "/" + std::to_string(total));
			pending_ = pending_.substr(r.start, r.end - r.start + 1);
			status_ = 206;
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
	Dispatcher* dispatcher_;       // per-reactor worker pool; null => run inline
	const ResponseOptions* response_;   // response transforms (compress / conditional); null => none

	http_parser parser_;
	http_parser_settings settings_;

	Request request_;
	std::string cur_field_;
	std::string cur_value_;
	bool reading_value_ = false;   // tracks the header field->value->field transition
	std::unique_ptr<BodySink> body_sink_;   // non-null => the app is streaming the body in
	Writer writer_{this};

	// Off-reactor dispatch coordination (the un-stallable model). complete_async_
	// is signaled by the worker and runs complete_async_cb on the reactor;
	// pending_input_ stashes any pipelined bytes captured while reading is paused
	// for the in-flight handler; offloaded_ marks that a worker owns request_.
	ev::async complete_async_;
	std::string pending_input_;
	std::atomic<bool> offloaded_{false};
	std::atomic<bool> complete_keep_alive_{true};
	// The worker signals completion (end()) while it still holds writer_, but the
	// reactor must not reset writer_ for the next request until the worker has fully
	// released it. So on the offload path end() records the completion here rather
	// than waking the reactor directly; run_offloaded() fires the wake as its last
	// act, once it is done touching writer_. in_offloaded_task_ marks that window so
	// a handler that instead completes *after* run_offloaded() returns (a future
	// async handler) still wakes the reactor immediately.
	std::atomic<bool> completion_pending_{false};
	std::atomic<bool> in_offloaded_task_{false};

public:
	HttpConnection(const std::shared_ptr<Worker>& parent, ev::loop_ref* loop, unsigned int flags, HttpHandler& handler, Dispatcher* dispatcher = nullptr, const ResponseOptions* response = nullptr)
		: BaseClient<HttpConnection>(parent, loop, flags),
		  handler_(handler),
		  dispatcher_(dispatcher),
		  response_(response),
		  complete_async_(*ev_loop) {
		http_parser_settings_init(&settings_);
		settings_.on_url = &on_url_cb;
		settings_.on_header_field = &on_header_field_cb;
		settings_.on_header_value = &on_header_value_cb;
		settings_.on_headers_complete = &on_headers_complete_cb;
		settings_.on_body = &on_body_cb;
		settings_.on_message_complete = &on_message_complete_cb;
		http_parser_init(&parser_, HTTP_REQUEST);
		parser_.data = this;
		complete_async_.set<HttpConnection, &HttpConnection::complete_async_cb>(this);
	}

private:
	// The completion async is live only while the connection is started.
	void start_impl() override {
		BaseClient<HttpConnection>::start_impl();
		complete_async_.start();
	}
	void stop_impl() override {
		complete_async_.stop();
		BaseClient<HttpConnection>::stop_impl();
	}

	// ---- Kronuz/server BaseClient<ClientImpl> interface -------------------
	ssize_t on_read(const char* buf, ssize_t received) {
		if (received <= 0) {
			return received;
		}
		parse(buf, received);
		return received;   // fully consumed; a pipelined remainder is stashed, not left behind
	}

	// Feed bytes to the parser. On a parse error, answer 400 and close. When the
	// handler is offloaded, on_request_complete() pauses the parser at message
	// completion; the unparsed remainder (a pipelined request) is stashed and
	// re-fed once the in-flight handler finishes -- one in-flight handler/conn.
	void parse(const char* buf, ssize_t len) {
		size_t nparsed = http_parser_execute(&parser_, &settings_, buf, static_cast<size_t>(len));
		auto err = HTTP_PARSER_ERRNO(&parser_);
		if (err == HPE_PAUSED) {
			if (static_cast<ssize_t>(nparsed) < len) {
				pending_input_.append(buf + nparsed, static_cast<size_t>(len) - nparsed);
			}
			return;
		}
		if (err != HPE_OK) {
			Response resp;
			resp.set(400, std::string("Bad Request: ") + http_errno_name(err) + "\n");
			resp.close = true;
			write(resp.serialize(false));
			close();
		}
	}

	// The file-transfer path (replication) is not part of HTTP.
	void on_read_file(const char* /*buf*/, ssize_t /*received*/) {}
	void on_read_file_done() {}

	bool is_idle() { return request_.method.empty() && request_.body.empty(); }

	// ---- helpers the Writer drives ----------------------------------------
	void emit(std::string bytes) { write(std::move(bytes)); }

	// The response is finished (end() was called). On the inline path this runs on
	// the reactor and completes immediately. On the offload path it runs on a
	// worker and hands completion back to the reactor via complete_async_ -- the
	// reactor owns the connection lifecycle (the ev watchers / close), so that step
	// must not run on a worker.
	void response_finished(bool keep_alive) {
		if (offloaded_.load(std::memory_order_acquire)) {
			complete_keep_alive_.store(keep_alive, std::memory_order_relaxed);
			// Defer the wake until run_offloaded() has released writer_ (its tail), so
			// the reactor can't reset writer_ for the next request while the worker is
			// still in run_offloaded(). If the handler completed after run_offloaded()
			// already returned (a future async handler), the worker is gone -- wake now.
			if (in_offloaded_task_.load(std::memory_order_acquire)) {
				completion_pending_.store(true, std::memory_order_release);
			} else {
				complete_async_.send();   // thread-safe; wakes the reactor
			}
			return;
		}
		complete_response(keep_alive);
	}

	// Reset per-request state for keep-alive reuse, or close. Returns whether the
	// connection was kept alive. Reactor-only (touches close()).
	bool complete_response(bool keep_alive) {
		bool ka = request_.keep_alive && keep_alive;
		if (ka) {
			request_.clear();
			cur_field_.clear();
			cur_value_.clear();
			reading_value_ = false;
			body_sink_.reset();
			return true;
		}
		close();
		return false;
	}

	// ---- headers are complete: finalize the request and let the app decide
	// whether to stream the body in. Done here (not at message-complete) so the
	// streaming decision can use the method/path/headers before any body byte. ----
	void finalize_request() {
		// For a custom method, http_method_str returns the verb the fork added.
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

		// The app's typed per-request extension (if any), built from the finalized
		// request line + headers, before the streaming decision and offload check so
		// both -- and handle() -- can use it.
		request_.extension = handler_.create_extension(request_);

		// Opt-in streaming intake: if the app returns a sink, body chunks go to it
		// instead of being buffered. Otherwise reserve the buffer up front from
		// Content-Length to avoid reallocations as the body arrives.
		body_sink_ = handler_.on_request_body(request_);
		if (!body_sink_) {
			auto cl = request_.header("Content-Length");
			long n = 0;
			if (!cl.empty() && std::from_chars(cl.data(), cl.data() + cl.size(), n).ec == std::errc() && n > 0) {
				request_.body.reserve(static_cast<size_t>(n));
			}
		}
	}

	// ---- the request is complete: route it to the handler -----------------
	// Inline (no Dispatcher, or a route the handler classifies cheap) or offloaded
	// to a worker (the un-stallable model).
	void on_request_complete() {
		if (dispatcher_ == nullptr || !handler_.should_offload(request_)) {
			dispatch();   // run on the reactor (the cheap fast path)
			return;
		}
		// Offload so a slow/blocking handler never stalls the reactor. One in-flight
		// handler per connection: pause reading so the worker has sole, race-free
		// access to request_/writer_ and responses stay ordered, and a stopped read
		// applies TCP backpressure.
		writer_.reset();
		offloaded_.store(true, std::memory_order_release);
		auto keep_alive_ref = share_this<HttpConnection>();   // keep the conn alive across the handoff
		bool accepted = dispatcher_->submit([this, keep_alive_ref] { run_offloaded(); });
		if (!accepted) {
			// The bounded queue is full: shed load with 503 rather than block the
			// reactor or grow an unbounded backlog. Answered inline, on the reactor.
			offloaded_.store(false, std::memory_order_release);
			writer_.send(503, "Service Unavailable\n");
			return;
		}
		http_parser_pause(&parser_, 1);   // stop after this message (one in-flight)
		io_read.stop();                   // backpressure: no more reads until the handler completes
	}

	// Inline handler invocation (on the reactor). THE SEAM: completion is end(),
	// not "handle() returned", so the same call becomes `co_await` for coroutines.
	void dispatch() {
		writer_.reset();
		try {
			handler_.handle(request_, writer_);
		} catch (...) {
			// Backstop: an app should map its own exceptions, but never leave a
			// synchronous handler's connection hung on an unhandled throw.
			if (!writer_.ended()) { writer_.send(500, "Internal Server Error\n"); }
		}
	}

	// Offloaded handler invocation (on a worker thread). The worker reads request_
	// and drives writer_ (whose write path is thread-safe); it must NOT touch the
	// reactor-owned connection state -- end() routes completion back via the async.
	void run_offloaded() {
		in_offloaded_task_.store(true, std::memory_order_release);
		try {
			handler_.handle(request_, writer_);
		} catch (...) {
			if (!writer_.ended()) { writer_.send(500, "Internal Server Error\n"); }
		}
		if (!writer_.ended()) { writer_.end(); }   // a handler that neither ended nor threw still completes
		// The worker is done touching writer_. Now it is safe for the reactor to
		// complete + reuse the connection: fire the wake end() deferred above (or, if
		// a future async handler ended after we returned here, response_finished()
		// already woke the reactor directly).
		in_offloaded_task_.store(false, std::memory_order_release);
		if (completion_pending_.exchange(false, std::memory_order_acq_rel)) {
			complete_async_.send();
		}
	}

	// Runs on the reactor when a worker signals completion: finish the response and
	// resume reading. complete_response touches ev/close, so it must run here.
	void complete_async_cb(ev::async&, int) {
		if (!offloaded_.load(std::memory_order_acquire)) { return; }
		offloaded_.store(false, std::memory_order_release);
		bool keep_alive = complete_response(complete_keep_alive_.load(std::memory_order_relaxed));
		resume_reading(keep_alive);
	}

	// Undo the offload pause and re-enable reads. Even when closing we re-enable
	// io_read so the existing teardown path (io_cb sees `closed`) detaches the
	// connection, mirroring the inline path which never stops reading.
	void resume_reading(bool keep_alive) {
		if (HTTP_PARSER_ERRNO(&parser_) == HPE_PAUSED) {
			http_parser_pause(&parser_, 0);
		}
		if (!io_read.is_active()) { io_read.start(); }
		if (keep_alive && !closed && !pending_input_.empty()) {
			std::string buffered;
			buffered.swap(pending_input_);
			parse(buffered.data(), static_cast<ssize_t>(buffered.size()));
		} else {
			pending_input_.clear();
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
		c->finalize_request();
		return 0;
	}
	static int on_body_cb(http_parser* p, const char* at, size_t len) {
		auto* c = self(p);
		if (c->body_sink_) {            // streaming: hand the chunk straight to the app
			c->body_sink_->write(std::string_view(at, len));
		} else {                        // buffered: accumulate (reserved from Content-Length)
			c->request_.body.append(at, len);
		}
		return 0;
	}
	static int on_message_complete_cb(http_parser* p) {
		auto* c = self(p);
		if (c->body_sink_) { c->body_sink_->end(); }
		c->on_request_complete();
		return 0;
	}
};

}  // namespace http
