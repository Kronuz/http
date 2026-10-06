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

// The Asio connection + HTTP service for Kronuz/http -- the transport layer. The generic
// server runtime (the shared-nothing reactor pool, the accept/bind strategies, the
// offload gate, and the graceful abort->stop->join shutdown) lives in Kronuz/reactor;
// this file is what makes that runtime speak HTTP. Everything above it is unchanged: the
// HttpHandler seam, the radix router, http_message, http-parser (via RequestParser), the
// codings.
//
// The un-stallable model is native here: a connection is a coroutine (the reactor::Session
// detail::serve_connection), and offloading a blocking handler is a single
// `co_await co_spawn(reactor.pool(), ...)` -- the work runs on a pool thread while the
// coroutine suspends, the reactor is free, and resumption lands back on the connection's
// own io_context. No pause/resume/pending-input bookkeeping, and no cross-thread
// completion handoff to get wrong: the coroutine is the single owner of its
// request/response, so the ordering is structural.
//
// Scaling is the runtime's shared-nothing / thread-per-core model: N io_contexts on N
// threads, one port (SO_REUSEPORT or a portable shared acceptor), plus a bounded
// thread_pool per reactor for offload. HttpAsioService is a thin adapter over
// reactor::TcpServer. Response compression / conditional / range transforms are applied
// on the buffered response (the http_* helpers). Request-body intake is dual-mode (the
// app chooses per endpoint via wants_body_stream / on_request_body): buffered for small
// bodies, or TRUE INCREMENTAL streaming for large/unbounded ones -- a multi-gigabyte
// RESTORE/bulk load flows through in O(read-buffer) memory, never held whole, with the
// ChannelBodyReader registered as a reactor::Abortable so shutdown can unwedge it.

#pragma once

#include <asio.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/experimental/awaitable_operators.hpp>

#include <array>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "http_compression.h"
#include "http_conditional.h"
#include "http_handler.h"
#include "http_message.h"
#include "http_range.h"
#include "http_request_parser.h"
#include "reactor.h"

namespace http {

// The reactor pool, accept plumbing, offload gate, and graceful shutdown live in the
// generic Kronuz/reactor runtime now; http rides on it. The bind options are the
// runtime's -- alias kept so existing callers (Xapiand, the tests, the bench) are
// unchanged. reuse_port is what lets the N reactors share one port.
using AsioBindOptions = reactor::BindOptions;

// Response transforms applied on the buffered response before it goes on the wire, in
// order: conditional (weak ETag / If-None-Match -> 304), then range (a byte Range ->
// 206 / 416), then compression (Accept-Encoding -> zstd/gzip). Same shape and policy as
// the libev writer had.
struct AsioResponseOptions {
	bool compress = false;
	bool conditional = false;   // derive a weak ETag + answer 304 on If-None-Match
	bool ranges = false;        // serve a byte Range as 206 Partial Content
	CompressionOptions compression{};
};

namespace detail {
// Owner-only timers/cancellation are detached before runtime destruction. Escaped
// producers retain only the protocol exchange; their wakeup holds a weak control.
class AsioDeferredControl : public reactor::Abortable, public std::enable_shared_from_this<AsioDeferredControl> {
public:
	static std::shared_ptr<AsioDeferredControl> create(asio::any_io_executor executor, DeferredOptions options) {
		auto control = std::shared_ptr<AsioDeferredControl>(new AsioDeferredControl(std::move(executor)));
		std::weak_ptr<AsioDeferredControl> weak = control;
		control->exchange = std::make_shared<DeferredExchange>(std::move(options), [weak] { if (auto c = weak.lock()) { c->notify(); } });
		return control;
	}
	void abort() override { exchange->close(ReplySettlement::Shutdown); }
	void disable() noexcept {
		std::lock_guard lock(gate_); active_ = false; executor_.reset(); timer_.reset(); cancellation_.reset();
	}
	asio::awaitable<void> wait() {
		while (exchange->phase() == DeferredExchange::Phase::Waiting) {
			timer_->expires_at(exchange->deadline());
			asio::error_code error; co_await timer_->async_wait(asio::redirect_error(asio::use_awaitable, error));
			if (!error) { exchange->close(ReplySettlement::TimedOut); }
			else if (error != asio::error::operation_aborted) { exchange->close(ReplySettlement::Failed); }
			else { auto cancellation = co_await asio::this_coro::cancellation_state; if (cancellation.cancelled() != asio::cancellation_type::none) { co_return; } }
		}
	}
	asio::cancellation_slot write_cancellation() { return cancellation_->slot(); }
	std::shared_ptr<DeferredExchange> exchange;
private:
	explicit AsioDeferredControl(asio::any_io_executor executor)
		: executor_(executor), timer_(std::make_unique<asio::steady_timer>(executor)), cancellation_(std::make_unique<asio::cancellation_signal>()) {}
	void notify() {
		std::lock_guard lock(gate_);
		if (!active_) { return; }
		std::weak_ptr<AsioDeferredControl> weak = shared_from_this();
		asio::post(*executor_, [weak] {
			if (auto control = weak.lock()) {
				{ std::lock_guard lock(control->gate_); if (!control->active_) { return; } }
				control->timer_->cancel();
				if (control->exchange->phase() == DeferredExchange::Phase::Closing) { control->cancellation_->emit(asio::cancellation_type::all); }
			}
		});
	}
	std::mutex gate_; bool active_ = true;
	std::optional<asio::any_io_executor> executor_;
	std::unique_ptr<asio::steady_timer> timer_;
	std::unique_ptr<asio::cancellation_signal> cancellation_;
};
} // namespace detail

// The buffered ResponseWriter: the handler sets status/headers and writes the body;
// on serialize() the transforms run and the HTTP/1.1 bytes are framed (Content-Length
// + Connection are the transport's, everything else is the app's).
class AsioResponseWriter : public ResponseWriter, public BufferedResponse {
public:
	AsioResponseWriter(const Request& request, const AsioResponseOptions* options)
		: request_(request), options_(options) {}
	AsioResponseWriter(const Request& request, const AsioResponseOptions* options, const asio::any_io_executor* executor, reactor::Reactor* reactor)
		: request_(request), options_(options), executor_(executor), reactor_(reactor) {}
	~AsioResponseWriter() override {
		if (deferred_) { try { deferred_->exchange->close(ReplySettlement::Shutdown); } catch (...) {} deferred_->disable(); try { deferred_->exchange->settle(ReplySettlement::Shutdown); } catch (...) {} }
	}
	std::unique_ptr<DeferredReply> defer(DeferredOptions options) override {
		if (!executor_) { return {}; }
		if (started_ || ended_ || !headers_.empty() || close_ || deferred_) { throw std::logic_error("response authority already used"); }
		auto control = detail::AsioDeferredControl::create(*executor_, std::move(options));
		if (reactor_) { reactor_->track(control); }
		auto capability = std::make_unique<DeferredReply>(control->exchange); deferred_ = std::move(control); return capability;
	}
	const std::shared_ptr<detail::AsioDeferredControl>& deferred() const noexcept { return deferred_; }
	void adopt_deferred(Response response) {
		if (!deferred_ || deferred_->exchange->phase() != detail::DeferredExchange::Phase::Writing) { throw std::logic_error("deferred response unavailable"); }
		status_ = response.status; headers_ = std::move(response.headers); headers_.emplace_back("Content-Type", std::move(response.content_type)); body_ = std::move(response.body); close_ = response.close; started_ = ended_ = true;
	}

	void status(int code) override { synchronous(); status_ = code; started_ = true; }
	void set_header(std::string_view name, std::string_view value) override { synchronous(); headers_.emplace_back(name, value); }
	void write(std::string_view chunk) override { synchronous(); started_ = true; body_.append(chunk); }
	void end() override { synchronous(); ended_ = true; }
	void set_close() override { synchronous(); close_ = true; }

	bool ended() const { return ended_; }
	bool wants_close() const { return close_; }

	// BufferedResponse: read the app-level response back by reference (the transforms
	// in serialize_head() run later, so this is the pre-transform view a logger wants).
	int response_code() const override { return status_; }
	bool response_started() const override { return started_; }
	const Headers& response_headers() const override { return headers_; }
	std::string_view response_body() const override { return body_; }
	std::string_view response_content_type() const override {
		for (const auto& [k, v] : headers_) { if (iequal(k, "Content-Type")) { return v; } }
		return "text/plain; charset=utf-8";
	}

	// Apply the response transforms and frame the HTTP/1.1 response bytes. Transforms
	// run in order: conditional (may flip to a bodyless 304) -> range (may flip to a
	// 206 slice or 416) -> compression (self-skips 204/206/304 and tiny bodies).
	// Builds only the status line + headers; the body is written separately (scatter
	// -gather) so it is never copied into this string. See has_body()/body().
	std::string serialize_head(bool keep_alive) {
		if (!deferred_) { maybe_conditional(); if (status_ != 304) { maybe_range(); } maybe_compress(); }
		std::string out = "HTTP/1.1 ";
		out += std::to_string(status_);
		out += ' ';
		out += reason_phrase(status_);
		out += "\r\n";
		for (const auto& [k, v] : headers_) {
			out += k; out += ": "; out += v; out += "\r\n";
		}
		if (status_ != 304 && !has_header("Content-Length")) {   // 304 is defined bodyless
			out += "Content-Length: ";
			out += std::to_string(body_.size());
			out += "\r\n";
		}
		out += (keep_alive && !close_) ? "Connection: keep-alive\r\n" : "Connection: close\r\n";
		out += "\r\n";
		return out;
	}

	// The finalised body, valid after serialize_head(). Written as its own buffer.
	bool has_body() const { return status_ != 304 && !body_.empty(); }
	const std::string& body() const { return body_; }

private:
	void synchronous() const { if (deferred_) { throw std::logic_error("response authority was deferred"); } }
	const asio::any_io_executor* executor_ = nullptr;
	reactor::Reactor* reactor_ = nullptr;
	std::shared_ptr<detail::AsioDeferredControl> deferred_;
	const Request& request_;
	const AsioResponseOptions* options_;
	int status_ = 200;
	std::vector<std::pair<std::string, std::string>> headers_;
	std::string body_;
	bool ended_ = false;
	bool close_ = false;
	bool started_ = false;

	bool has_header(std::string_view name) const {
		for (const auto& [k, v] : headers_) { if (iequal(k, name)) { return true; } }
		return false;
	}

	// Conditional GET: derive a weak ETag from the (uncompressed) body and, if the
	// client's If-None-Match already holds it, drop the body and answer 304. Runs before
	// range/compression so the ETag covers the resource, not the wire bytes. Buffered
	// GET/HEAD 200s only; a handler that set its own ETag is left alone.
	void maybe_conditional() {
		if (options_ == nullptr || !options_->conditional) { return; }
		if (status_ != 200) { return; }
		const std::string& method = request_.method;
		if (method != "GET" && method != "HEAD") { return; }
		if (has_header("ETag")) { return; }
		std::string etag = weak_etag(body_);
		std::string_view inm = request_.header("If-None-Match");
		if (!inm.empty() && if_none_match(inm, etag)) {
			status_ = 304;
			body_.clear();
		}
		headers_.emplace_back("ETag", std::move(etag));
	}

	// Range request: serve a single byte range of the buffered body as 206 Partial
	// Content (Content-Range), or 416 if unsatisfiable; advertise Accept-Ranges: bytes.
	// Buffered GET 200s only; a multi-range or unrecognized header serves the full 200.
	// A 206 is left uncompressed (range + content-coding is a tar pit; ranges are for
	// already-compressed media).
	void maybe_range() {
		if (options_ == nullptr || !options_->ranges) { return; }
		if (status_ != 200 || request_.method != "GET") { return; }
		if (!has_header("Accept-Ranges")) { headers_.emplace_back("Accept-Ranges", "bytes"); }
		std::string_view spec = request_.header("Range");
		if (spec.empty()) { return; }
		const std::size_t total = body_.size();
		ByteRange r = parse_byte_range(spec, total);
		if (!r.recognized) { return; }
		if (!r.satisfiable) {
			status_ = 416;
			body_.clear();
			headers_.emplace_back("Content-Range", "bytes */" + std::to_string(total));
			return;
		}
		headers_.emplace_back("Content-Range", "bytes " + std::to_string(r.start) + "-" + std::to_string(r.end) + "/" + std::to_string(total));
		body_ = body_.substr(r.start, r.end - r.start + 1);
		status_ = 206;
	}

	// Transparent response compression -- identical policy to the libev Writer.
	void maybe_compress() {
		if (options_ == nullptr || !options_->compress) { return; }
		const CompressionOptions& opt = options_->compression;
		if (body_.size() < opt.min_size) { return; }
		if (has_header("Content-Encoding")) { return; }
		if (status_ == 204 || status_ == 304 || status_ == 206 || (status_ >= 100 && status_ < 200)) { return; }
		std::string coding = negotiate_encoding(request_.header("Accept-Encoding"), opt);
		if (coding.empty()) { return; }
		std::string compressed = encode(coding, body_, opt);
		if (compressed.empty() || compressed.size() >= body_.size()) { return; }
		body_ = std::move(compressed);
		headers_.emplace_back("Content-Encoding", std::move(coding));
		if (!has_header("Vary")) { headers_.emplace_back("Vary", "Accept-Encoding"); }
	}
};

namespace detail {

// The Asio-backed BodyReader: a bounded queue of raw body chunks bridging the reactor
// (producer) and the handler's worker (consumer). The consumer blocks on a condition
// variable -- NOT on the io_context -- so abort() (shutdown) can wake it directly, even
// after the reactor's io_context has stopped. The producer (reactor coroutine) stays
// non-blocking: when the queue is full it yields via a short timer and retries, so a
// slow consumer applies back-pressure (the socket read pauses) without ever blocking the
// reactor thread. An empty chunk from finish() is the end-of-body marker.
//
// It is a reactor::Abortable as well as a BodyReader: the reactor tracks it via
// Reactor::track() so TcpServer shutdown aborts it (the single abort() below satisfies
// both interfaces) before io.stop(), waking a consumer even after the loop has stopped.
class ChannelBodyReader : public BodyReader, public reactor::Abortable {
public:
	ChannelBodyReader(asio::any_io_executor ex, std::size_t capacity)
		: ex_(std::move(ex)), cap_(capacity != 0 ? capacity : 1) {}

	// consumer (handler worker): blocking pull. false at end-of-body or on abort; sticky
	// at end, so an over-read (a parser flushing a trailing item then looping) never blocks.
	bool read(std::string& chunk) override {
		std::unique_lock<std::mutex> lk(m_);
		cv_.wait(lk, [this] { return !q_.empty() || closed_ || aborted_; });
		if (!q_.empty()) {
			chunk = std::move(q_.front());
			q_.pop_front();
			cv_.notify_all();   // wake the producer if it is waiting for space
			return true;
		}
		return false;           // closed + drained, or aborted
	}

	bool aborted() const {
		std::lock_guard<std::mutex> lk(m_);
		return aborted_;
	}

	// producer (reactor coroutine): enqueue a chunk, yielding the reactor while the queue
	// is full (flow control). Returns early if aborted.
	asio::awaitable<void> push(std::string chunk) {
		for (;;) {
			{
				std::lock_guard<std::mutex> lk(m_);
				if (aborted_) { co_return; }
				if (q_.size() < cap_) {
					q_.push_back(std::move(chunk));
					cv_.notify_all();
					co_return;
				}
			}
			asio::steady_timer t(ex_, std::chrono::milliseconds(1));
			co_await t.async_wait(asio::use_awaitable);   // reactor stays free; retry
		}
	}
	asio::awaitable<void> finish() {
		{
			std::lock_guard<std::mutex> lk(m_);
			closed_ = true;
		}
		cv_.notify_all();
		co_return;
	}
	void abort() override {
		{
			std::lock_guard<std::mutex> lk(m_);
			aborted_ = true;
		}
		cv_.notify_all();
	}

private:
	asio::any_io_executor ex_;
	std::size_t cap_;
	mutable std::mutex m_;
	std::condition_variable cv_;
	std::deque<std::string> q_;
	bool closed_ = false;    // producer sent finish()
	bool aborted_ = false;   // shutdown / transport abort
};

// Serve one connection: keep-alive loop of parse -> handler seam -> write. A normal
// request runs handle() inline on the reactor, or (pool + should_offload) on the pool
// with the reactor free -- the un-stallable path. A request the handler marked
// wants_body_stream runs CONCURRENTLY: the handler on the pool pulls body chunks from a
// flow-controlled BodyReader that the reactor feeds -- O(buffer) memory for any size.
template<class Stream>
inline asio::awaitable<ReplySettlement> write_before_deadline(Stream& socket,
	std::array<asio::const_buffer, 2> buffers, std::chrono::steady_clock::time_point deadline) {
	using namespace asio::experimental::awaitable_operators;
	auto executor = co_await asio::this_coro::executor; asio::steady_timer timer(executor); timer.expires_at(deadline);
	asio::error_code write_error, timer_error;
	auto winner = co_await (asio::async_write(socket, buffers, asio::redirect_error(asio::use_awaitable, write_error)) || timer.async_wait(asio::redirect_error(asio::use_awaitable, timer_error)));
	if (winner.index() == 0) { co_return write_error ? ReplySettlement::Disconnected : ReplySettlement::Written; }
	co_return timer_error ? ReplySettlement::Failed : ReplySettlement::TimedOut;
}
// A deferred response keeps one owned plaintext read alive across its write.
// Consumed pipelined input is returned to the parser, never discarded by a race.
template<class Stream>
class ConnectionInput : public std::enable_shared_from_this<ConnectionInput<Stream>> {
public:
	explicit ConnectionInput(Stream& stream) : stream_(stream), done_(stream.get_executor()) {}
	void monitor(const std::shared_ptr<AsioDeferredControl>& control) {
		observer_ = control;
		if (ready_ && error_ && error_ != asio::error::eof) {
			control->exchange->close(ReplySettlement::Disconnected); return;
		}
		if (pending_ || ready_) { return; }
		pending_ = true;
		auto owner = this->shared_from_this();
		try {
			stream_.async_read_some(asio::buffer(&byte_, 1), [owner](asio::error_code error, std::size_t size) {
				owner->pending_ = false; owner->ready_ = true; owner->error_ = error; owner->size_ = size;
				owner->done_.cancel();
				if (error && error != asio::error::eof) {
					if (auto c = owner->observer_.lock()) { c->exchange->close(ReplySettlement::Disconnected); }
				}
			});
		} catch (...) { pending_ = false; throw; }
	}
	asio::awaitable<std::size_t> read(asio::mutable_buffer output) {
		co_await drain();
		if (ready_) {
			ready_ = false;
			if (size_) { *static_cast<char*>(output.data()) = byte_; co_return 1; }
			if (error_) { throw asio::system_error(error_); }
			co_return 0;
		}
		co_return co_await stream_.async_read_some(output, asio::use_awaitable);
	}
	asio::awaitable<void> drain() {
		while (pending_) {
			done_.expires_at(std::chrono::steady_clock::time_point::max());
			asio::error_code error; co_await done_.async_wait(asio::redirect_error(asio::use_awaitable, error));
		}
	}
private:
	Stream& stream_; asio::steady_timer done_;
	char byte_ = 0; std::size_t size_ = 0; asio::error_code error_;
	bool pending_ = false, ready_ = false;
	std::weak_ptr<AsioDeferredControl> observer_;
};
template<class Stream>
inline asio::awaitable<bool> write_response(Stream& socket, AsioResponseWriter& writer, bool keep_alive,
	const std::shared_ptr<ConnectionInput<Stream>>& input) {
	auto control = writer.deferred();
	if (control) {
		if (control->exchange->phase() == DeferredExchange::Phase::Waiting) {
			input->monitor(control);
			co_await control->wait();
		}
		if (std::chrono::steady_clock::now() >= control->exchange->deadline()) { control->exchange->close(ReplySettlement::TimedOut); }
		auto response = control->exchange->take_response();
		if (!response) { control->exchange->settle(control->exchange->closing_reason()); co_return false; }
		writer.adopt_deferred(std::move(*response));
	}
	std::string head = writer.serialize_head(keep_alive);
	std::array<asio::const_buffer, 2> buffers{asio::buffer(head), writer.has_body() ? asio::buffer(writer.body()) : asio::const_buffer{}};
	if (!control) { co_await asio::async_write(socket, buffers, asio::use_awaitable); co_return true; }
	auto executor = co_await asio::this_coro::executor;
	ReplySettlement outcome;
	try {
		outcome = co_await asio::co_spawn(executor, write_before_deadline(socket, buffers, control->exchange->deadline()), asio::bind_cancellation_slot(control->write_cancellation(), asio::use_awaitable));
	} catch (...) { control->exchange->close(ReplySettlement::Failed); control->exchange->settle(ReplySettlement::Failed); throw; }
	if (outcome != ReplySettlement::Written) { control->exchange->close(outcome); }
	control->exchange->settle(outcome);
	co_return outcome == ReplySettlement::Written && control->exchange->closing_reason() == ReplySettlement::Written;
}
inline asio::awaitable<void> invoke_handler(HttpHandler* handler, const Request& request, ResponseWriter& writer) {
	handler->handle(request, writer); co_return;
}
template<class Stream>
inline asio::awaitable<void> serve_connection_body(Stream& socket, const std::shared_ptr<ConnectionInput<Stream>>& input, HttpHandler* handler,
                                              reactor::Reactor* reactor, const AsioResponseOptions* options) {
	using asio::use_awaitable;
	try {
		asio::error_code opt_ec;
		socket.lowest_layer().set_option(asio::ip::tcp::no_delay(true), opt_ec);
		auto ex = co_await asio::this_coro::executor;
		RequestParser parser;
		// The headers-complete hook: build the app's per-request extension, then choose
		// how to take the body -- concurrent pull streaming (needs a worker), push to an
		// on_request_body sink, or buffer into Request::body.
		parser.set_headers_callback([handler, reactor](Request& req) -> RequestParser::BodyIntake {
			req.extension = handler->create_extension(req);
			RequestParser::BodyIntake intake;
			if (reactor->pool() != nullptr && handler->wants_body_stream(req)) {
				intake.stream = true;
			} else {
				intake.sink = handler->on_request_body(req);
			}
			return intake;
		});
		std::string buffered;
		char tmp[64 * 1024];

		for (;;) {
			// Phase 1: parse the request line + headers.
			while (!parser.headers_done()) {
				if (buffered.empty()) {
					std::size_t n = co_await input->read(asio::buffer(tmp));
					if (n == 0) { co_return; }
					buffered.append(tmp, n);
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { co_return; }
			}

			if (parser.streaming()) {
				// ---- CONCURRENT PULL STREAMING ----
				Request request = parser.take_headers();
				bool keep_alive = request.keep_alive;
				auto reader = std::make_shared<ChannelBodyReader>(ex, /*capacity=*/8);
				request.body_reader = reader;
				reactor->track(reader);   // so shutdown can unblock a mid-stream read()
				AsioResponseWriter writer(request, options, &ex, reactor);
				asio::any_io_executor hexec(reactor->pool()->get_executor());

				// Feed the body (reactor, flow-controlled) and run the handler (pool)
				// concurrently, then join. feed never throws -- a socket/parse error
				// aborts the reader so the handler's read() returns false and unblocks.
				auto feed = [&]() -> asio::awaitable<void> {
					asio::error_code fec;
					for (;;) {
						// Drain whatever body has been parsed so far -- including bytes that
						// arrived in the same read as the headers (a small body completes
						// during phase 1, so this must run before the complete() check).
						std::string chunk = parser.take_pending_body();
						if (!chunk.empty()) { co_await reader->push(std::move(chunk)); }
						if (parser.complete()) { break; }
						if (buffered.empty()) {
							std::size_t n = 0;
							try { n = co_await input->read(asio::buffer(tmp)); } catch (const asio::system_error& e) { fec = e.code(); }
							if (fec || n == 0) { reader->abort(); co_return; }
							buffered.append(tmp, n);
						}
						std::size_t used = parser.feed(buffered.data(), buffered.size());
						buffered.erase(0, used);
						if (parser.errored()) { reader->abort(); co_return; }
					}
					co_await reader->finish();   // end-of-body marker
				};
				// Run the handler on the POOL (a blocking read() pull), and feed the body
				// on the reactor -- concurrently. The handler is spawned with an explicit
				// pool executor + a completion handler that signals a done-channel; the
				// reactor then feeds and co_awaits done. (co_spawn's executor argument
				// guarantees the handler runs on the pool -- the awaitable_operators && did
				// not, which ran the blocking handler on the reactor and deadlocked it.)
				// feed never throws -- a socket/parse error aborts the reader so the
				// handler's read() returns false and unblocks.
				auto done = std::make_shared<asio::experimental::concurrent_channel<void(asio::error_code)>>(ex, 1);
				asio::co_spawn(hexec,
					[handler, &request, &writer]() -> asio::awaitable<void> {
						try { handler->handle(request, writer); }
						catch (...) { if (writer.deferred()) { writer.deferred()->exchange->abandon(); } else if (!writer.ended()) { handler->on_error(std::current_exception(), request, writer); } }
						co_return;
					},
					[done](std::exception_ptr) { (void)done->try_send(asio::error_code{}); });

				std::exception_ptr feed_error;
				try { co_await feed(); } catch (...) {
					feed_error = std::current_exception(); reader->abort();
					asio::error_code ignored; socket.lowest_layer().close(ignored);
				}
				auto cancellation = co_await asio::this_coro::cancellation_state;
				const bool canceled = cancellation.cancelled() != asio::cancellation_type::none;
				// The pool handler owns references to request/writer. Cancellation
				// cannot release either until that original handler has returned.
				co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
				co_await done->async_receive(use_awaitable);
				if (feed_error) { std::rethrow_exception(feed_error); }
				if (canceled || reactor->stopping()) { co_return; }
				co_await asio::this_coro::reset_cancellation_state(asio::enable_terminal_cancellation());

				if (!writer.deferred() && !writer.ended()) {
					writer.status(500);
					writer.write("Internal Server Error\n");
					writer.end();
				}
				if (writer.deferred()) {
				if (!co_await write_response(socket, writer, keep_alive, input)) { co_return; }
			} else {
				std::string head = writer.serialize_head(keep_alive);
				std::array<asio::const_buffer, 2> buffers{asio::buffer(head), writer.has_body() ? asio::buffer(writer.body()) : asio::const_buffer{}};
				co_await asio::async_write(socket, buffers, use_awaitable);
			}
				if (!keep_alive || writer.wants_close()) { break; }
				parser.reset();
				continue;
			}

			// ---- BUFFERED / PUSH path ----
			while (!parser.complete()) {
				if (buffered.empty()) {
					std::size_t n = co_await input->read(asio::buffer(tmp));
					buffered.append(tmp, n);
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { co_return; }
			}

			Request request = parser.take();
			parser.resume();
			bool keep_alive = request.keep_alive;
			// The extension and the body (streamed to the app's sink, or buffered into
			// request.body) were already handled at headers-complete by the parser hook.

			AsioResponseWriter writer(request, options, &ex, reactor);
			bool offload_active = false;
			try {
				if (reactor->pool() && handler->should_offload(request)) {
					if (!reactor->gate().try_enter()) {
						writer.status(503);
						writer.write("Service Unavailable\n");
						writer.end();
					} else {
						// co_spawn selects the worker executor for the coroutine body;
						// awaiting post() alone preserves the parent executor.
						offload_active = true;
						co_await asio::co_spawn(reactor->pool()->get_executor(), invoke_handler(handler, request, writer), use_awaitable);
						reactor->gate().leave();
						offload_active = false;
					}
				} else {
					handler->handle(request, writer);   // inline on the reactor
				}
			} catch (...) {
				if (offload_active) { reactor->gate().leave(); }
				if (writer.deferred()) { writer.deferred()->exchange->abandon(); } else if (!writer.ended()) { handler->on_error(std::current_exception(), request, writer); }
				if (!writer.deferred() && !writer.ended()) {
					writer.status(500);
					writer.write("Internal Server Error\n");
					writer.end();
				}
			}

			if (writer.deferred()) {
				if (!co_await write_response(socket, writer, keep_alive, input)) { co_return; }
			} else {
				std::string head = writer.serialize_head(keep_alive);
				std::array<asio::const_buffer, 2> buffers{asio::buffer(head), writer.has_body() ? asio::buffer(writer.body()) : asio::const_buffer{}};
				co_await asio::async_write(socket, buffers, use_awaitable);
			}
			if (!keep_alive || writer.wants_close()) { break; }
		}
	} catch (const std::exception&) {
		// client hangup / read-write error -- drop the connection
	}
}

template<class Stream>
inline asio::awaitable<void> serve_connection(Stream socket, HttpHandler* handler,
    reactor::Reactor* reactor, const AsioResponseOptions* options) {
	auto input = std::make_shared<ConnectionInput<Stream>>(socket);
	try { co_await serve_connection_body(socket, input, handler, reactor, options); } catch (...) {}
	asio::error_code error; socket.lowest_layer().close(error);
	// The socket and input slot outlive completion of the original read.
	co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
	co_await input->drain();
}

}  // namespace detail

// The HTTP service: an application HttpHandler served over a reactor::TcpServer. The
// server owns the runtime (N shared-nothing reactors on N threads, one port via
// SO_REUSEPORT or a portable shared acceptor, per-reactor offload pool, graceful
// shutdown); this class is the thin HTTP-shaped adapter -- it turns the handler + the
// response options into the per-connection Session (detail::serve_connection) and
// exposes the same surface callers had before the runtime was extracted.
class HttpAsioService {
public:
	using ConnectionFactory = std::function<asio::awaitable<void>(asio::ip::tcp::socket,
		HttpHandler*, reactor::Reactor*, const AsioResponseOptions*)>;
	// `reactors` runtimes; each with `workers` offload threads (0 => inline) and an
	// offload admission window of `queue_limit`.
	HttpAsioService(HttpHandler& handler, std::size_t reactors, std::size_t workers, std::size_t queue_limit,
		ConnectionFactory connection = {})
		: handler_(handler), connection_(std::move(connection)),
		  server_(reactors, workers, static_cast<int>(queue_limit),
		          [this](asio::ip::tcp::socket socket, reactor::Reactor& r) -> asio::awaitable<void> {
			          // Not a coroutine itself: it returns serve_connection's awaitable,
			          // so there is no closure-lifetime pitfall (the Session captures only
			          // this, a stable pointer; the coroutine frame owns the socket).
			          if (connection_) { return connection_(std::move(socket), &handler_, &r, &response_); }
			          return detail::serve_connection(std::move(socket), &handler_, &r, &response_);
		          }) {}

	HttpAsioService(const HttpAsioService&) = delete;
	HttpAsioService& operator=(const HttpAsioService&) = delete;

	void set_bind_options(const AsioBindOptions& options) { server_.set_bind_options(options); }
	void enable_compression(CompressionOptions options = {}) {
		response_.compress = true;
		response_.compression = std::move(options);
	}
	// A repeat GET of an unchanged resource costs a hash + a header instead of the body
	// (weak ETag / If-None-Match -> 304).
	void enable_conditional() { response_.conditional = true; }
	// A buffered GET 200 advertises Accept-Ranges and serves a single byte Range as 206
	// (or 416) -- what media players and resumable downloads use to seek.
	void enable_ranges() { response_.ranges = true; }

	// Bind + run all reactors (each on its own thread). Returns once the threads are
	// launched; the caller keeps its thread and stops with stop().
	void start(unsigned short port) { server_.start(port); }

	// Abort in-flight streams, stop the loops, join. Also runs from the destructor
	// (server_'s own dtor), so a service going out of scope tears down cleanly.
	void stop() { server_.stop(); }

	std::size_t reactors() const { return server_.reactors(); }

private:
	// Declared before server_: the Session captures &handler_/&response_, and member
	// destruction is reverse-declaration, so server_ (its threads joined in ~TcpServer)
	// is torn down before the handler and options it referenced.
	HttpHandler& handler_;
	AsioResponseOptions response_{};
	ConnectionFactory connection_;
	reactor::TcpServer server_;
};

}  // namespace http
