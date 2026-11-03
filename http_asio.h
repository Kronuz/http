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
// admission gate, and the graceful abort->stop->join shutdown) lives in Kronuz/reactor;
// this file is what makes that runtime speak HTTP. Everything above it is unchanged: the
// HttpHandler seam, the radix router, http_message, http-parser (via RequestParser), the
// codings.
//
// A connection is a task::task (the reactor::Session detail::serve_connection) and so is a
// handler: handle() is awaited on the connection's loop as a child task, nothing runs off it,
// and a handler that must wait suspends. The connection is the single owner of its
// request/response, so the ordering is structural. While a handler runs, the connection
// watches the socket for a hang-up and tracks the request for shutdown; both cancel it
// (Request::cancel) without destroying its frame. The session's token (SessionContext::stop)
// reaches only the waits shutdown should end: the reads of the next request and the TLS
// handshake, never the handler or a reply's write.
//
// Scaling is the runtime's shared-nothing / thread-per-core model: N io_contexts on N
// threads, one port (SO_REUSEPORT or a portable shared acceptor), and a bounded in-flight
// admission gate per reactor. HttpAsioService is a thin adapter over reactor::TcpServer.
// Response compression / conditional / range transforms are applied on the buffered
// response (the http_* helpers). Request-body intake has three modes, chosen per endpoint:
// buffered into Request::body up to a cap (then 413), pushed to a BodySink, or streamed to
// a BodyStream the handler reads with co_await -- a multi-gigabyte RESTORE/bulk load flows
// through in O(read-buffer) memory, never held whole.

#pragma once

#include <asio.hpp>
#include <asio/experimental/channel.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <type_traits>
#include <charconv>

#include "http_compression.h"
#include "http_conditional.h"
#include "http_handler.h"
#include "http_message.h"
#include "http_range.h"
#include "http_request_parser.h"
#include "reactor.h"
#include "http_owned_stream_asio.h"
#include "task_asio.hh"

namespace http {

// The reactor pool, accept plumbing, admission gate, and graceful shutdown live in the
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
	// Disabled for existing consumers. Authority opts into an absolute request
	// network deadline so trickled headers/body bytes cannot renew admission.
	std::chrono::milliseconds request_timeout{0};
	// Each 0 = off. idle: from a connection's start or a reply's end to the next request's
	// first byte. header: from that first byte to the end of the headers. inactivity: per
	// body read and per 64 KiB of a buffered reply, so a slow transfer that moves completes.
	std::chrono::milliseconds idle_timeout{0}, header_timeout{0}, inactivity_timeout{0};
	// The largest body buffered into Request::body (0 = no cap); a larger request gets 413.
	// A route that streams its body (wants_body_stream / on_request_body) is not capped.
	std::size_t max_body = 16u * 1024 * 1024;
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
		std::lock_guard lock(gate_); active_ = false; executor_.reset(); timer_.reset();
	}
	// Until the reply leaves the waiting phase: notify() cancels the timer to wake it, and
	// shutdown reaches it through abort() closing the exchange.
	task::task<void> wait() {
		while (exchange->phase() == DeferredExchange::Phase::Waiting) {
			timer_->expires_at(exchange->deadline());
			const asio::error_code error = co_await task::await_asio<void(asio::error_code)>(
				[this](auto handler) { timer_->async_wait(std::move(handler)); });
			if (!error) { exchange->close(ReplySettlement::TimedOut); }
			else if (error != asio::error::operation_aborted) { exchange->close(ReplySettlement::Failed); }
		}
	}
	// Requested, on the loop, once the exchange is closing: a write in flight is cancelled.
	task::cancel_token write_token() const { return writes_.token(); }
	std::shared_ptr<DeferredExchange> exchange;
private:
	explicit AsioDeferredControl(asio::any_io_executor executor)
		: executor_(executor), timer_(std::make_unique<asio::steady_timer>(executor)) {}
	void notify() {
		std::lock_guard lock(gate_);
		if (!active_) { return; }
		std::weak_ptr<AsioDeferredControl> weak = shared_from_this();
		asio::post(*executor_, [weak] {
			if (auto control = weak.lock()) {
				{ std::lock_guard lock(control->gate_); if (!control->active_) { return; } }
				control->timer_->cancel();
				if (control->exchange->phase() == DeferredExchange::Phase::Closing) { control->writes_.request(); }
			}
		});
	}
	std::mutex gate_; bool active_ = true;
	std::optional<asio::any_io_executor> executor_;
	std::unique_ptr<asio::steady_timer> timer_;
	task::cancel_source writes_;
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
		if (reactor_) {
			if (stream_) { reactor_->untrack(*stream_); }
			if (deferred_) { reactor_->untrack(*deferred_); }
		}
		if (stream_) { try { stream_->exchange->close(ReplySettlement::Shutdown); } catch (...) {} stream_->disable(); try { stream_->exchange->settle(); } catch (...) {} }
		if (deferred_) { try { deferred_->exchange->close(ReplySettlement::Shutdown); } catch (...) {} deferred_->disable(); try { deferred_->exchange->settle(ReplySettlement::Shutdown); } catch (...) {} }
	}
	std::unique_ptr<DeferredReply> defer(DeferredOptions options) override {
		affine();
		if (!executor_) { return {}; }
		if (started_ || ended_ || !headers_.empty() || close_ || deferred_ || stream_) { throw std::logic_error("response authority already used"); }
		auto control = detail::AsioDeferredControl::create(*executor_, std::move(options));
		if (reactor_) { reactor_->track(*control); }
		auto capability = std::make_unique<DeferredReply>(control->exchange); deferred_ = std::move(control); return capability;
	}
	const std::shared_ptr<detail::AsioDeferredControl>& deferred() const noexcept { return deferred_; }
	OwnedStreamCapability stream(StreamOptions options) override {
		affine();
		if (!executor_)
			return {};
		if (started_ || ended_ || !headers_.empty() || close_ || deferred_ || stream_)
			throw std::logic_error("response authority already used");
		options.allow_keep_alive = options.allow_keep_alive && request_.keep_alive;
		auto resource = options.resource;
		auto control = detail::AsioOwnedStreamControl::create(*executor_, std::move(options));
		if (reactor_)
			reactor_->track(*control);
		auto capability = detail::make_resource_unique<OwnedStreamReply>(resource, control->exchange);
		stream_ = std::move(control);
		return capability;
	}
	const std::shared_ptr<detail::AsioOwnedStreamControl> &owned_stream() const noexcept { return stream_; }
	bool is_deferred() const noexcept { return deferred_ || stream_; }
	void abandon_deferred() {
		if (deferred_)
			deferred_->exchange->abandon();
		if (stream_)
			stream_->exchange->close(ReplySettlement::Abandoned);
	}

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
	bool wants_close() const { return close_ || (stream_ && stream_->exchange->close_after()); }

	// BufferedResponse: read the app-level response back by reference (the transforms
	// in serialize_head() run later, so this is the pre-transform view a logger wants).
	int response_code() const override { return status_; }
	bool response_started() const override { return started_; }
	bool response_ended() const override { return ended_; }
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
		std::string out;
		serialize_head(out, keep_alive);
		return out;
	}
	// The same, appended to `out` (a connection reuses one string for every reply's head).
	void serialize_head(std::string& out, bool keep_alive) {
		if (!deferred_) { maybe_conditional(); if (status_ != 304) { maybe_range(); } maybe_compress(); }
		const char* reason = reason_phrase(status_);
		// The status line, the fields, Content-Length (at most 20 digits), Connection, the blank line.
		std::size_t size = 80 + std::strlen(reason);
		for (const auto& [k, v] : headers_) { size += k.size() + 2 + v.size() + 2; }
		out.reserve(out.size() + size);
		out += "HTTP/1.1 ";
		out += std::to_string(status_);
		out += ' ';
		out += reason;
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
	}

	// The finalised body, valid after serialize_head(). Written as its own buffer.
	bool has_body() const { return status_ != 304 && !body_.empty(); }
	const std::string& body() const { return body_; }

private:
	void synchronous() const { affine(); if (is_deferred()) { throw std::logic_error("response authority was deferred"); } }
	// Debug: the writer is used on its connection's loop thread, whatever scheduler wraps the task
	// running there (a handler body on another thread must come back first).
	void affine() const noexcept { assert(!reactor_ || reactor_->on_loop()); }
	const asio::any_io_executor* executor_ = nullptr;
	reactor::Reactor* reactor_ = nullptr;
	std::shared_ptr<detail::AsioDeferredControl> deferred_;
	std::shared_ptr<detail::AsioOwnedStreamControl> stream_;
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

// Writes buffers before `at`: the write takes a deadline linked to `stop` (a reply control's
// token, or none), so a deadline or a request of `stop` cancels it.
template<class Stream, std::size_t N>
inline task::task<ReplySettlement> write_before_deadline(Stream& socket, std::array<asio::const_buffer, N> buffers,
	std::chrono::steady_clock::time_point at, task::asio_scheduler& scheduler, task::cancel_token stop = {}) {
	task::deadline<task::asio_scheduler> deadline(scheduler, at, stop);
	const auto [error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto handler) { asio::async_write(socket, buffers, std::move(handler)); }, deadline.token());
	(void)written;
	if (!error) { co_return ReplySettlement::Written; }
	if (deadline.expired()) { co_return ReplySettlement::TimedOut; }
	co_return stop.requested() ? ReplySettlement::Failed : ReplySettlement::Disconnected;
}
// Writes buffers before `deadline`, and with an inactivity bound, 64 KiB of the buffers
// taken together at a time (a slice spans the buffers it covers, so a head and a small body
// leave in one send), each slice before `inactivity` from its start (whichever deadline
// comes first). No token: a reply's write finishes, or a deadline or the peer ends it.
template<class Stream, std::size_t N>
inline task::task<ReplySettlement> write_with_progress(Stream& socket, std::array<asio::const_buffer, N> buffers,
	std::chrono::milliseconds inactivity, std::chrono::steady_clock::time_point deadline, task::asio_scheduler& scheduler) {
	if (inactivity.count() <= 0) {
		if (deadline == std::chrono::steady_clock::time_point::max()) {
			const auto [error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
				[&](auto handler) { asio::async_write(socket, buffers, std::move(handler)); });
			(void)written;
			co_return error ? ReplySettlement::Disconnected : ReplySettlement::Written;
		}
		co_return co_await write_before_deadline(socket, buffers, deadline, scheduler);
	}
	constexpr std::size_t slice = 64 * 1024;
	std::size_t total = 0;
	for (const auto& b : buffers) { total += b.size(); }
	for (std::size_t at = 0; at < total; at += slice) {
		// The window [at, at + slice) of the buffers laid end to end: a slice of each buffer it touches.
		std::array<asio::const_buffer, N> window{};
		std::size_t skip = at, room = slice;
		for (std::size_t i = 0; i < N; ++i) {
			if (skip >= buffers[i].size()) { skip -= buffers[i].size(); continue; }
			const std::size_t n = std::min(buffers[i].size() - skip, room);
			window[i] = asio::const_buffer(static_cast<const char*>(buffers[i].data()) + skip, n);
			room -= n;
			skip = 0;
		}
		const auto until = std::min(deadline, std::chrono::steady_clock::now() + inactivity);
		const auto r = co_await write_before_deadline(socket, window, until, scheduler);
		if (r != ReplySettlement::Written) { co_return r; }
	}
	co_return ReplySettlement::Written;
}

// What a read of the connection returned: an error (eof, a socket error, or
// operation_aborted when its token was requested), or the bytes.
struct ReadResult {
	asio::error_code error;
	std::size_t bytes = 0;
};

// One owned read keeps watching the socket while a handler runs or a deferred response
// waits. It reads ahead, up to kReadAhead bytes into the input's own buffer, so the request
// a quick keep-alive or pipelining client sends meanwhile arrives with that one read: the
// pending read is the next request's read, and a request costs one recv, not a one-byte
// recv and a second one for the rest. What the watch took is returned to the parser by
// read(), first and in order, never discarded by a race, and no further read-ahead is
// armed until it is all consumed, so at most kReadAhead bytes are held outside the kernel
// however much the client sends. Two rules read it, and they differ on purpose:
//   * a deferred or owned-stream reply (monitor): only a read error other than EOF is a
//     disconnect, because the client may half-close after sending and still expect the reply;
//   * a request in flight (watch): EOF and any read error but our own cancellation are a
//     disconnect, because a client that closed has nobody to read what a search computes.
//     Pipelined input (the client sent the next request early) ends the watch for the
//     rest of that request: a later disconnect is not seen until the next read.
// On a plain TCP socket a request's watch is lazy: it reads nothing until the handler first
// asks for its cancel token (engage(), through Request's CancelWatch hook), and then a peek at
// the socket decides what happened meanwhile by the same rules. A handler that never asks
// costs no read, so its request's next read is the socket's own, after the reply.
template<class Stream>
class ConnectionInput : public std::enable_shared_from_this<ConnectionInput<Stream>>, public CancelWatch {
public:
	// The most the watch reads ahead of the parser.
	static constexpr std::size_t kReadAhead = 4096;
	// Whether watch(..., lazy) can wait for engage(): a plain TCP socket can be asked, at once
	// and without reading it, what the peer did while nobody watched (recv with MSG_PEEK and
	// MSG_DONTWAIT). A TLS stream cannot (its records must be decrypted to tell a close_notify
	// from data), so its watch reads from the start.
#if defined(MSG_PEEK) && defined(MSG_DONTWAIT)
	static constexpr bool kLazyWatch = std::is_same_v<Stream, asio::ip::tcp::socket>;
#else
	static constexpr bool kLazyWatch = false;
#endif
	explicit ConnectionInput(Stream& stream) : stream_(stream), done_(stream.get_executor(), 1) {}
	template<class Control>
	void monitor(const std::shared_ptr<Control>& control) {
		if constexpr (std::is_same_v<Control, AsioDeferredControl>) observer_ = control;
		else { static_assert(std::is_same_v<Control, AsioOwnedStreamControl>); owned_observer_ = control; }
		if (ready_ && error_ && error_ != asio::error::eof) {
			control->exchange->observe_peer_disconnect(); return;
		}
		arm();
	}
	// Calls `on_disconnect` once, on the loop, when the peer closes while the watch is set.
	// A lazy watch (kLazyWatch streams) reads nothing until engage(): until the handler asks
	// about cancellation, the request costs no read and no wake-up.
	void watch(std::function<void()> on_disconnect, bool lazy = false) {
		on_disconnect_ = std::move(on_disconnect);
		if (ready_) {
			if (error_ && error_ != asio::error::operation_aborted) { fire(); }
			else if (size_) { on_disconnect_ = nullptr; }
			return;
		}
		if (lazy && kLazyWatch) { return; }
		arm();
	}
	void unwatch() noexcept { on_disconnect_ = nullptr; }
	// CancelWatch: the handler asked about cancellation; starts a lazy watch. What the peer did
	// while nobody watched decides as the read would have: data waiting (a pipelined request)
	// ends the watch, EOF or a socket error is a disconnect, and nothing yet arms the read.
	void engage() noexcept override {
		if (!on_disconnect_ || pending_ || ready_) { return; }
#if defined(MSG_PEEK) && defined(MSG_DONTWAIT)
		if constexpr (kLazyWatch) {
			char byte;
			const auto n = ::recv(stream_.native_handle(), &byte, 1, MSG_PEEK | MSG_DONTWAIT);
			if (n > 0) { on_disconnect_ = nullptr; return; }
			if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) { fire(); return; }
		}
#endif
		try { arm(); } catch (...) {}   // no memory for the read: the request goes unwatched
	}

	// Waits for the pending read to complete; nothing cancels the wait. The channel holds one
	// completion that nobody may be waiting for; a stale one only makes a later wait return
	// once more, and the loop asks pending_ again.
	task::task<void> drain() {
		while (pending_) {
			co_await task::await_asio<void(asio::error_code)>([this](auto handler) { done_.async_receive(std::move(handler)); });
		}
	}

	// The awaitable read() returns (see read()).
	class Read {
	public:
		Read(ConnectionInput& input, asio::mutable_buffer output, task::cancel_token token)
			: input_(input), output_(output), token_(std::move(token)) {}
		bool await_ready() {
			if (!input_.pending_ && !input_.ready_) {
				direct_.emplace(SocketRead{&input_.stream_, output_}, std::move(token_));
				return direct_->await_ready();
			}
			held_ = input_.read_held(output_, std::move(token_));
			held_awaiter_.emplace(std::move(held_).operator co_await());
			return false;
		}
		std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
			if (direct_) { return direct_->await_suspend(h) ? std::noop_coroutine() : h; }
			return held_awaiter_->await_suspend(h);
		}
		ReadResult await_resume() {
			if (direct_) {
				const auto [error, bytes] = direct_->await_resume();
				return {error, bytes};
			}
			return held_awaiter_->await_resume();
		}
	private:
		struct SocketRead {
			Stream* stream;
			asio::mutable_buffer output;
			template<class Handler> void operator()(Handler&& handler) const { stream->async_read_some(output, std::forward<Handler>(handler)); }
		};
		using Direct = decltype(task::await_asio<void(asio::error_code, std::size_t)>(std::declval<SocketRead>(), std::declval<task::cancel_token>()));
		using HeldAwaiter = decltype(std::declval<task::task<ReadResult>>().operator co_await());
		ConnectionInput& input_;
		asio::mutable_buffer output_;
		task::cancel_token token_;
		std::optional<Direct> direct_;
		task::task<ReadResult> held_;
		std::optional<HeldAwaiter> held_awaiter_;
	};
	// The next bytes: what the watch read ahead first, else a read of the socket that `token`
	// cancels (a deadline's, the session's, or none). With nothing held it is the socket's own
	// operation, awaited in place: no coroutine frame. A requested token ends a watch read the
	// wait drains, by cancelling it on the socket.
	Read read(asio::mutable_buffer output, task::cancel_token token) { return Read(*this, output, std::move(token)); }
private:
	task::task<ReadResult> read_held(asio::mutable_buffer output, task::cancel_token token) {
		if (pending_) {
			task::cancel_callback cancel(token, [this] { asio::error_code ignored; stream_.lowest_layer().cancel(ignored); });
			co_await drain();
		}
		if (ready_) {
			if (size_) {
				// What the watch read ahead, oldest first; the rest stays for the next read.
				const std::size_t n = (std::min)(output.size(), size_ - taken_);
				std::memcpy(output.data(), buffer_ + taken_, n);
				taken_ += n;
				if (taken_ == size_) { size_ = taken_ = 0; ready_ = false; }
				co_return ReadResult{{}, n};
			}
			ready_ = false;
			co_return ReadResult{error_ ? error_ : asio::error::eof, 0};
		}
		const auto [error, bytes] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { stream_.async_read_some(output, std::move(handler)); }, std::move(token));
		co_return ReadResult{error, bytes};
	}
	void arm() {
		if (pending_ || ready_) { return; }
		pending_ = true;
		auto owner = this->shared_from_this();
		try {
			stream_.async_read_some(asio::buffer(buffer_, sizeof buffer_), [owner](asio::error_code error, std::size_t size) {
				owner->completed(error, size);
			});
		} catch (...) { pending_ = false; throw; }
	}
	void completed(asio::error_code error, std::size_t size) {
		pending_ = false; ready_ = true; error_ = error; size_ = size; taken_ = 0;
		done_.try_send(asio::error_code{});
		if (error && error != asio::error::eof) {
			if (auto c = observer_.lock()) { c->exchange->observe_peer_disconnect(); }
			if (auto c = owned_observer_.lock()) { c->exchange->observe_peer_disconnect(); }
		}
		if (!on_disconnect_) { return; }
		if (!error) { on_disconnect_ = nullptr; }
		else if (error != asio::error::operation_aborted) { fire(); }
	}
	void fire() {
		auto callback = std::move(on_disconnect_);
		on_disconnect_ = nullptr;
		if (callback) { callback(); }
	}
	Stream& stream_;
	asio::experimental::channel<void(asio::error_code)> done_;   // a completed read wakes drain()
	char buffer_[kReadAhead];
	std::size_t size_ = 0, taken_ = 0;   // bytes the last read returned, and how many read() has handed out
	asio::error_code error_;
	bool pending_ = false, ready_ = false;
	std::function<void()> on_disconnect_;
	std::weak_ptr<AsioDeferredControl> observer_;
	std::weak_ptr<AsioOwnedStreamControl> owned_observer_;
};
template <class Stream>
inline task::task<bool> write_owned_response(Stream &socket, const std::shared_ptr<AsioOwnedStreamControl> &control,
											  const std::shared_ptr<ConnectionInput<Stream>> &input, task::asio_scheduler &scheduler) {
	input->monitor(control);
	while (true) {
		if (std::chrono::steady_clock::now() >= control->exchange->deadline())
			control->exchange->close(ReplySettlement::TimedOut);
		if (control->exchange->settle())
			co_return control->exchange->closing_reason() == ReplySettlement::Written;
		auto original = control->exchange->take_write();
		if (!original) {
			co_await control->wait();
			continue;
		}
		const auto serial = original->serial;
		std::array<char, 32> chunk_head;
		std::string_view prefix, suffix;
		if (original->terminal)
			prefix = "0\r\n\r\n";
		else if (original->chunked && !original->chunk.bytes.empty()) {
			auto number = std::to_chars(chunk_head.data(), chunk_head.data() + chunk_head.size() - 2,
										original->chunk.bytes.size(), 16);
			if (number.ec != std::errc{})
				throw std::logic_error("HTTP chunk length overflow");
			*number.ptr++ = '\r';
			*number.ptr++ = '\n';
			prefix = {chunk_head.data(), static_cast<std::size_t>(number.ptr - chunk_head.data())};
			suffix = "\r\n";
		}
		std::array<asio::const_buffer, 4> buffers{asio::buffer(original->head), asio::buffer(prefix),
												  asio::buffer(original->chunk.bytes), asio::buffer(suffix)};
		const ReplySettlement outcome =
			co_await write_before_deadline(socket, buffers, control->exchange->deadline(), scheduler, control->write_token());
		// The write has completed, so the socket no longer reads the original's bytes. Only now
		// may its immutable body owner retire and wake the next producer turn.
		original.reset();
		control->exchange->complete_write(serial, outcome);
	}
}
template <class Stream>
inline task::task<bool> write_response(Stream &socket, AsioResponseWriter &writer, bool keep_alive,
										const std::shared_ptr<ConnectionInput<Stream>> &input, task::asio_scheduler &scheduler) {
	if (auto stream = writer.owned_stream()) { co_return co_await write_owned_response(socket, stream, input, scheduler); }
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
	if (!control) {
		const auto [error, written] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto handler) { asio::async_write(socket, buffers, std::move(handler)); });
		(void)written;
		co_return !error;
	}
	const ReplySettlement outcome = co_await write_before_deadline(socket, buffers, control->exchange->deadline(), scheduler, control->write_token());
	if (outcome != ReplySettlement::Written) { control->exchange->close(outcome); }
	control->exchange->settle(outcome);
	co_return outcome == ReplySettlement::Written && control->exchange->closing_reason() == ReplySettlement::Written;
}

// The transport's reads of a request's cancellation, which engage no watch and make no source.
struct RequestAccess {
	static bool cancelled(const Request& request) noexcept { return request.cancel_ && request.cancel_->requested(); }
	// Whether the request has its source: the handler asked for its token, or it was cancelled.
	static bool asked(const Request& request) noexcept { return request.cancel_.has_value(); }
};

// Maps an exception that escaped handle() to the response: a deferred or owned-stream reply
// is abandoned; otherwise the app's on_error first, then for whatever it left unanswered a
// 503 that closes the connection when the exception is task::cancelled from the request's own
// cancellation (a hang-up or shutdown), else a generic 500.
inline void map_handler_error(HttpHandler* handler, Request& request, AsioResponseWriter& writer, std::exception_ptr error) {
	if (writer.is_deferred()) { writer.abandon_deferred(); return; }
	if (!writer.ended()) { handler->on_error(error, request, writer); }
	if (writer.is_deferred() || writer.ended()) { return; }
	bool cancelled = false;
	try { std::rethrow_exception(error); } catch (const task::cancelled&) { cancelled = true; } catch (...) {}
	if (cancelled && RequestAccess::cancelled(request)) {
		writer.status(503);
		writer.set_close();
		writer.write("Service Unavailable\n");
	} else {
		writer.status(500);
		writer.write("Internal Server Error\n");
	}
	writer.end();
}

// Shutdown's hook into one running request: abort() cancels it (its reads stop at their next
// checkpoint, its writes finish), and force() -- after the shutdown grace -- closes the
// socket a stalled peer holds. Tracked for the request's whole run, on the reactor's list.
template<class Stream>
struct RequestAbort final : reactor::Abortable {
	RequestAbort(reactor::Reactor& owner, Request& req, Stream& stream) : reactor(owner), request(req), socket(stream) { reactor.track(*this); }
	~RequestAbort() override { reactor.untrack(*this); }
	void abort() override { request.cancel(task::cancel_reason::shutdown); }
	void force() override { asio::error_code ignored; socket.lowest_layer().close(ignored); }
	reactor::Reactor& reactor;
	Request& request;
	Stream& socket;
};

// Clears the connection's disconnect watch on every exit, so a late read cannot reach a
// request that is gone, and the request's handle on it.
template<class Stream>
struct WatchGuard {
	explicit WatchGuard(ConnectionInput<Stream>& in, Request* req = nullptr) : input(in), request(req) {}
	~WatchGuard() { input.unwatch(); if (request) { request->set_cancel_watch(nullptr); } }
	ConnectionInput<Stream>& input;
	Request* request;
};

// Sends what the socket takes at once, without waiting, and advances `buffers` past it: a
// reply that fits the send buffer leaves with no asynchronous write, whose completion costs a
// trip through the loop's queue. False when the connection failed. A plain TCP socket in
// non-blocking mode only.
template<std::size_t N>
inline bool send_now(asio::ip::tcp::socket& socket, std::array<asio::const_buffer, N>& buffers) {
	asio::error_code error;
	std::size_t sent = socket.write_some(buffers, error);
	if (error && error != asio::error::would_block && error != asio::error::try_again) { return false; }
	for (auto& buffer : buffers) {
		const std::size_t n = (std::min)(sent, buffer.size());
		buffer += n;
		sent -= n;
	}
	return true;
}
template<std::size_t N>
inline std::size_t bytes_left(const std::array<asio::const_buffer, N>& buffers) {
	std::size_t n = 0;
	for (const auto& buffer : buffers) { n += buffer.size(); }
	return n;
}

// Serve one connection: a keep-alive loop of parse -> handler -> write, all tasks on the
// connection's loop. handle() is awaited as a child task, so a handler that waits suspends and
// the loop serves other connections. A request is admitted at headers-complete (reactor gate,
// else 503), and from then until its response is written it is tracked for shutdown and
// watched for a client hang-up; both cancel it (Request::cancel) without destroying its frame.
// `stop` (the session's token) ends only the reads of a request, each through a deadline
// linked to it when a timeout bounds the read; the handler and the reply's writes never see
// it. A handler that opted into wants_body_stream runs beside the feed of its BodyStream.
template<class Stream>
inline task::task<void> serve_connection_body(Stream& socket, const std::shared_ptr<ConnectionInput<Stream>>& input, HttpHandler* handler,
	reactor::Reactor* reactor, const AsioResponseOptions* options, const task::cancel_token& stop) {
	std::optional<RequestRejected> rejected;
	task::asio_scheduler& scheduler = reactor->scheduler();
	try {
		asio::error_code opt_ec;
		socket.lowest_layer().set_option(asio::ip::tcp::no_delay(true), opt_ec);
		std::string remote;  // the peer's address, for every request on this connection
		if (auto peer = socket.lowest_layer().remote_endpoint(opt_ec); !opt_ec) { remote = peer.address().to_string(); }
		const asio::any_io_executor ex = socket.get_executor();
		RequestParser parser;
		parser.set_body_limit(options ? options->max_body : 0);
		reactor::AdmissionGate::Lease admitted;   // held from headers-complete to the settled response
		// The headers-complete hook: admit the request, build the app's per-request extension,
		// then choose how to take the body -- a BodyStream, an on_request_body sink, or the
		// buffer in Request::body.
		parser.set_headers_callback([handler, reactor, &remote, &admitted](Request& req) -> RequestParser::BodyIntake {
			admitted = reactor->gate().enter();
			if (!admitted) { throw RequestRejected(503, "Service Unavailable\n", RejectionContent::Text, 1); }
			req.remote = remote;
			req.extension = handler->create_extension(req);
			RequestParser::BodyIntake intake;
			if (handler->wants_body_stream(req)) {
				intake.stream = true;
			} else {
				intake.sink = handler->on_request_body(req);
			}
			return intake;
		});
		std::string buffered;
		char tmp[64 * 1024];
		std::string head;                          // a buffered reply's head; the storage is reused
		std::array<asio::const_buffer, 2> reply;   // what of the reply is still to be written
		// A plain TCP socket sends a buffered reply at once (send_now), so it is put in
		// non-blocking mode: a full send buffer then returns instead of waiting.
		bool send_first = false;
		if constexpr (std::is_same_v<Stream, asio::ip::tcp::socket>) {
			asio::error_code mode_ec;
			socket.non_blocking(true, mode_ec);
			send_first = !mode_ec;
		}

		using clock = std::chrono::steady_clock;
		const auto ms = [](std::chrono::milliseconds t) { return t.count() > 0; };
		const auto idle = options ? options->idle_timeout : std::chrono::milliseconds{0};
		const auto header = options ? options->header_timeout : std::chrono::milliseconds{0};
		const auto inactivity = options ? options->inactivity_timeout : std::chrono::milliseconds{0};
		// The next body read's deadline: the request's, or sooner when nothing arrives.
		const auto body_deadline = [&](clock::time_point request_deadline) {
			return ms(inactivity) ? std::min(request_deadline, clock::now() + inactivity) : request_deadline;
		};
		// A read's timeout: a deadline linked to `parent` when `until` bounds the read, else
		// `parent` itself. One read at a time holds it; the caller resets it after the read.
		std::optional<task::deadline<task::asio_scheduler>> timeout;
		const auto bounded = [&](clock::time_point until, const task::cancel_token& parent) -> task::cancel_token {
			if (until == clock::time_point::max()) { return parent; }
			timeout.emplace(scheduler, until, parent);
			return timeout->token();
		};
		// Ends the read's timeout; a read it cut short reports timed_out.
		const auto settle_read = [&](ReadResult& r) {
			if (timeout) {
				if (r.error == asio::error::operation_aborted && timeout->expired()) { r.error = asio::error::timed_out; }
				timeout.reset();
			}
		};
		// A parse that stopped for the body cap is a 413; any other parse error drops the connection.
		const auto parse_failed = [&] {
			if (parser.body_limit_exceeded()) { throw RequestRejected(413, "Payload Too Large\n"); }
		};
		// Writes the response; false when the connection cannot carry on. A buffered reply has
		// been framed by send_reply(), and this writes what it left.
		const auto respond = [&](AsioResponseWriter& writer, bool keep_alive, clock::time_point request_deadline) -> task::task<bool> {
			if (writer.is_deferred()) { co_return co_await write_response(socket, writer, keep_alive, input, scheduler); }
			co_return co_await write_with_progress(socket, reply, inactivity, request_deadline, scheduler) == ReplySettlement::Written;
		};
		// Frames a buffered reply into `reply` and, on plain TCP, sends what the socket takes at
		// once (send_now). 1: all of it left; 0: the connection failed; -1: respond() writes
		// the rest, or the reply is deferred.
		const auto send_reply = [&](AsioResponseWriter& writer, bool keep_alive) -> int {
			if (writer.is_deferred()) { return -1; }
			head.clear();
			writer.serialize_head(head, keep_alive);
			reply = {asio::buffer(head), writer.has_body() ? asio::buffer(writer.body()) : asio::const_buffer{}};
			if constexpr (std::is_same_v<Stream, asio::ip::tcp::socket>) {
				if (send_first) {
					if (!send_now(socket, reply)) { return 0; }
					if (bytes_left(reply) == 0) { return 1; }
				}
			}
			return -1;
		};
		for (;;) {
			// A shutdown that arrived while the last request ran: its next read would not start.
			if (stop.requested()) { co_return; }
			const auto request_deadline = options && options->request_timeout.count() > 0
				? clock::now() + options->request_timeout
				: clock::time_point::max();
			// Phase 1: parse the request line + headers. Until a byte arrives the idle
			// timeout bounds the wait; from the first byte, the header timeout.
			auto header_deadline = clock::time_point::max();
			if (!buffered.empty() && ms(header)) { header_deadline = clock::now() + header; }
			while (!parser.headers_done()) {
				if (buffered.empty()) {
					const bool waiting = header_deadline == clock::time_point::max();
					const auto until = std::min(request_deadline, waiting ? (ms(idle) ? clock::now() + idle : clock::time_point::max()) : header_deadline);
					ReadResult r = co_await input->read(asio::buffer(tmp), bounded(until, stop));
					settle_read(r);
					if (r.error || r.bytes == 0) { co_return; }
					if (waiting && ms(header)) { header_deadline = clock::now() + header; }
					buffered.append(tmp, r.bytes);
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { parse_failed(); co_return; }
			}

			if (parser.streaming()) {
				// ---- STREAMED BODY: the handler runs while the feed reads the body ----
				Request request = parser.take_headers();
				request.remote = remote;
				request.set_scheduler(&scheduler);
				reactor->lag().note_activity();
				bool keep_alive = request.keep_alive;
				auto stream = std::make_shared<BodyStream>(scheduler, /*capacity=*/8);
				request.body_stream = stream;
				AsioResponseWriter writer(request, options, &ex, reactor);
				RequestAbort<Stream> tracked(*reactor, request, socket);
				WatchGuard<Stream> watching(*input, &request);
				// Stops the feed when the handler completes; linked, so shutdown stops it too.
				task::cancel_source feed_stop{stop};
				bool handler_done = false;

				enum class Fed { Complete, Failed, Abandoned };
				// The body failed: cancel the request first, so a reader woken by the error sees it cancelled.
				const auto fail_body = [&](asio::error_code error) {
					request.cancel(stop.requested() ? task::cancel_reason::shutdown : disconnected);
					stream->fail(error);
				};
				// Feeds the body until it ends, or until the handler completes first (it answered
				// without reading it all) and requests `token`.
				const auto feed = [&](task::cancel_token token) -> task::task<Fed> {
					try {
					for (;;) {
						// Drain whatever body has been parsed so far -- including bytes that
						// arrived in the same read as the headers (a small body completes
						// during phase 1, so this must run before the complete() check).
						std::string chunk = parser.take_pending_body();
						if (!chunk.empty() && !co_await stream->push(std::move(chunk), token)) {
							if (handler_done || !stop.requested()) { co_return Fed::Abandoned; }
							fail_body(asio::error::operation_aborted);
							co_return Fed::Failed;
						}
						if (parser.complete()) {
							stream->finish();
							// The handler still runs: a hang-up from here cancels the request. As for a
							// buffered body, the watch waits for the handler to ask, unless it already did.
							if (!handler_done) {
								const bool lazy = !RequestAccess::asked(request);
								input->watch([&request] { request.cancel(disconnected); }, lazy);
								if (lazy) { request.set_cancel_watch(input.get()); }
							}
							co_return Fed::Complete;
						}
						if (buffered.empty()) {
							ReadResult r = co_await input->read(asio::buffer(tmp), bounded(body_deadline(request_deadline), token));
							settle_read(r);
							if (r.error == asio::error::operation_aborted && handler_done) { co_return Fed::Abandoned; }
							if (r.error || r.bytes == 0) {
								// The peer ended (or the read timed out, or the service stops) before the body
								// was complete: the reader sees a failed body, never eof, and the request is
								// already cancelled.
								asio::error_code failure = r.error;
								if (!failure || failure == asio::error::eof) { failure = asio::error::connection_reset; }
								fail_body(failure);
								co_return Fed::Failed;
							}
							buffered.append(tmp, r.bytes);
						}
						std::size_t used = parser.feed(buffered.data(), buffered.size());
						buffered.erase(0, used);
						if (parser.errored()) { fail_body(asio::error::invalid_argument); co_return Fed::Failed; }
					}
					} catch (...) {
						// The reader sees a failed body, never a hung read; when_all rethrows this.
						if (!stream->closed()) { fail_body(asio::error::operation_aborted); }
						throw;
					}
				};
				// The handler, with its exception mapped; its completion stops the feed.
				const auto run_handler = [&]() -> task::task<void> {
					try { co_await handler->handle(request, writer); }
					catch (...) {
						try { map_handler_error(handler, request, writer, std::current_exception()); } catch (...) {}
					}
					handler_done = true;
					stream->abandon();
					feed_stop.request();
				};
				// Both are children of this frame: when_all returns once both completed, so the
				// handler and the request and writer it references end only by completing.
				auto [fed, ignored] = co_await task::when_all(feed(feed_stop.token()), run_handler());
				(void)ignored;
				input->unwatch();
				request.set_cancel_watch(nullptr);
				if (fed != Fed::Complete) { keep_alive = false; }

				if (!writer.is_deferred() && !writer.ended()) {
					writer.status(500);
					writer.write("Internal Server Error\n");
					writer.end();
				}
				const int sent = send_reply(writer, keep_alive);
				if (sent == 0) { co_return; }
				if (sent < 0 && !co_await respond(writer, keep_alive, request_deadline)) { co_return; }
				admitted.reset();
				if (!keep_alive || writer.wants_close()) { break; }
				parser.reset();
				continue;
			}

			// ---- BUFFERED / PUSH path ----
			while (!parser.complete()) {
				if (buffered.empty()) {
					ReadResult r = co_await input->read(asio::buffer(tmp), bounded(body_deadline(request_deadline), stop));
					settle_read(r);
					if (r.error || r.bytes == 0) { co_return; }
					buffered.append(tmp, r.bytes);
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { parse_failed(); co_return; }
			}

			if (request_deadline != clock::time_point::max() && clock::now() >= request_deadline) { co_return; }
			Request request = parser.take();
			request.remote = remote;
			request.set_scheduler(&scheduler);
			reactor->lag().note_activity();
			parser.resume();
			bool keep_alive = request.keep_alive;
			// The extension and the body (pushed to the app's sink, or buffered into
			// request.body) were already handled at headers-complete by the parser hook.

			AsioResponseWriter writer(request, options, &ex, reactor);
			RequestAbort<Stream> tracked(*reactor, request, socket);
			{
				WatchGuard<Stream> watching(*input, &request);
				// The watch starts when the handler first asks for its cancel token (CancelWatch).
				input->watch([&request] { request.cancel(disconnected); }, /*lazy=*/true);
				request.set_cancel_watch(input.get());
				// A child task: declared after the request and writer, so a teardown that destroys
				// this frame destroys the handler's first; otherwise it ends only by completing.
				try {
					co_await handler->handle(request, writer);
				} catch (...) {
					map_handler_error(handler, request, writer, std::current_exception());
				}
			}
			const int sent = send_reply(writer, keep_alive);
			if (sent == 0) { co_return; }
			if (sent < 0 && !co_await respond(writer, keep_alive, request_deadline)) { co_return; }
			admitted.reset();
			if (!keep_alive || writer.wants_close()) { break; }
		}
	} catch (const RequestRejected& error) {
		rejected.emplace(error);
	} catch (const std::exception&) {
		// a read-write or parse failure -- drop the connection
	}
	if (rejected) {
		// Parser/body ownership has already unwound. The bounded error body
		// consumes no publisher reservation and cannot wait on a slow reader.
		std::string text(rejected->what());
		std::string head = "HTTP/1.1 " + std::to_string(rejected->status()) + " " + reason_phrase(rejected->status()) +
			"\r\nContent-Type: " + std::string(rejected->content_type()) + "\r\nContent-Length: " + std::to_string(text.size()) +
			(rejected->retry_after() ? "\r\nRetry-After: " + std::to_string(rejected->retry_after()) : std::string()) +
			"\r\nConnection: close\r\n\r\n";
		std::array<asio::const_buffer, 2> buffers{asio::buffer(head), asio::buffer(text)};
		co_await write_before_deadline(socket, buffers, std::chrono::steady_clock::now() + std::chrono::seconds(1), scheduler);
	}
}

// The reactor::Session body of a connection: serve it, close the socket, then wait for the
// watch's read to complete, so the socket and the input outlive it.
template<class Stream>
inline task::task<void> serve_connection(Stream socket, HttpHandler* handler,
    reactor::Reactor* reactor, const AsioResponseOptions* options, reactor::SessionContext context) {
	auto input = std::make_shared<ConnectionInput<Stream>>(socket);
	try { co_await serve_connection_body(socket, input, handler, reactor, options, context.stop); } catch (...) {}
	asio::error_code error; socket.lowest_layer().close(error);
	co_await input->drain();
}

}  // namespace detail

// The HTTP service: an application HttpHandler served over a reactor::TcpServer. The
// server owns the runtime (N shared-nothing reactors on N threads, one port via
// SO_REUSEPORT or a portable shared acceptor, per-reactor admission gate, graceful
// shutdown); this class is the thin HTTP-shaped adapter -- it turns the handler + the
// response options into the per-connection Session (detail::serve_connection) and
// exposes the same surface callers had before the runtime was extracted.
class HttpAsioService {
public:
	// A connection's session: serve `socket` with `handler` (detail::serve_connection, the
	// default, or the TLS factory's) and honor `context.stop` (shutdown) in its waits.
	using ConnectionFactory = std::function<task::task<void>(asio::ip::tcp::socket, HttpHandler*, reactor::Reactor*,
		const AsioResponseOptions*, reactor::SessionContext)>;
	// `reactors` loops, each admitting at most `in_flight_limit` requests at a time (0 =
	// no cap); a request over the cap is refused with a 503 and `Retry-After: 1`. At most
	// `maximum_connections` connections live at once (0 = no bound).
	HttpAsioService(HttpHandler& handler, std::size_t reactors, std::size_t in_flight_limit,
		ConnectionFactory connection = {}, std::size_t maximum_connections = 0)
		: handler_(handler), connection_(std::move(connection)),
		  server_(reactors, static_cast<int>(in_flight_limit), reactor::Session{
			[this](asio::ip::tcp::socket socket, reactor::Reactor& reactor, reactor::SessionContext context) -> task::task<void> {
				if (connection_) { return connection_(std::move(socket), &handler_, &reactor, &response_, std::move(context)); }
				return detail::serve_connection(std::move(socket), &handler_, &reactor, &response_, std::move(context));
			}}, maximum_connections) {}

	HttpAsioService(const HttpAsioService&) = delete;
	HttpAsioService& operator=(const HttpAsioService&) = delete;

	void set_bind_options(const AsioBindOptions& options) { server_.set_bind_options(options); }
	void set_connection_limit(std::size_t maximum) { server_.set_connection_limit(maximum); }
	reactor::ConnectionStats connection_stats() const noexcept { return server_.connection_stats(); }
	std::shared_ptr<const reactor::ConnectionAdmission> connection_observer() const noexcept { return server_.connection_observer(); }
	std::chrono::milliseconds checked_timeout(std::chrono::milliseconds timeout) const {
		if (started_) { throw std::logic_error("network timeouts must be set before startup"); }
		if (timeout < std::chrono::milliseconds::zero() || timeout > std::chrono::hours(24)) {
			throw std::invalid_argument("network timeouts must be between zero and 24 hours");
		}
		return timeout;
	}
	void set_request_timeout(std::chrono::milliseconds timeout) {
		if (started_) { throw std::logic_error("request network timeout must be set before startup"); }
		if (timeout < std::chrono::milliseconds::zero() || timeout > std::chrono::hours(24)) {
			throw std::invalid_argument("request network timeout must be between zero and 24 hours");
		}
		response_.request_timeout = timeout;
	}
	// Each off by default; set before startup, between zero and 24 hours (AsioResponseOptions).
	void set_idle_timeout(std::chrono::milliseconds timeout) { response_.idle_timeout = checked_timeout(timeout); }
	void set_header_timeout(std::chrono::milliseconds timeout) { response_.header_timeout = checked_timeout(timeout); }
	void set_inactivity_timeout(std::chrono::milliseconds timeout) { response_.inactivity_timeout = checked_timeout(timeout); }
	// The largest body buffered into Request::body (16 MiB by default; 0 = no cap). A larger
	// request is answered 413 and the connection closes. Streamed and sunk bodies are not capped.
	void set_max_body(std::size_t bytes) {
		if (started_) { throw std::logic_error("body cap must be set before startup"); }
		response_.max_body = bytes;
	}
	// How long stop() lets in-flight requests finish after asking them to stop (5 s by
	// default) before it closes the sockets a stalled peer still holds; see reactor::TcpServer.
	void set_shutdown_grace(std::chrono::milliseconds grace) { server_.set_shutdown_grace(grace); }
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
	// listening; the caller keeps its thread and stops with stop().
	void start(unsigned short port) { started_ = true; server_.start(port); }

	// Teardown is task's four phases, each a call forwarded to reactor::TcpServer, so an
	// application with its own schedulers runs each phase on all of them before the next:
	//   stop()           cancel in-flight requests (their responses are still written), end the
	//                    connections' reads, force the sockets still busy after the grace, stop
	//                    the loops and join;
	//   close()          the reactors' schedulers drop what is posted and destroy what is spawned;
	//   destroy_roots()  destroy the tasks still suspended on them, with every io_context alive;
	//   destruction      runs every phase not yet run (server_'s destructor), so a service going
	//                    out of scope alone tears down cleanly.
	// Each phase first runs any earlier one not yet run, so every call order is safe.
	void stop() { server_.stop(); }
	void close() { server_.stop(); server_.close(); }
	void destroy_roots() { close(); server_.destroy_roots(); }

	std::size_t reactors() const { return server_.reactors(); }
	// Reactor `i`, for its lag probe (reactor_lag.h): `reactor(i).lag().snapshot()`.
	reactor::Reactor& reactor(std::size_t i) { return server_.reactor(i); }
	// Run a lag probe on every reactor; before start().
	void enable_lag_probe() { server_.enable_lag_probe(); }

private:
	// Declared before server_: the Session captures &handler_/&response_, and member
	// destruction is reverse-declaration, so server_ (its threads joined in ~TcpServer)
	// is torn down before the handler and options it referenced.
	HttpHandler& handler_;
	AsioResponseOptions response_{};
	bool started_ = false;
	ConnectionFactory connection_;
	reactor::TcpServer server_;
};

}  // namespace http
