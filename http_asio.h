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
// A connection is a coroutine (the reactor::Session detail::serve_connection) and so is a
// handler: handle() is awaited on the connection's loop, nothing runs off it, and a handler
// that must wait suspends. The coroutine is the single owner of its request/response, so
// the ordering is structural. While a handler runs, the connection watches the socket for a
// hang-up and tracks the request for shutdown; both cancel it (Request::cancel) without
// destroying its frame.
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
#include <asio/experimental/awaitable_operators.hpp>

#include <array>
#include <cstddef>
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
		if (reactor_) {
			if (stream_) { reactor_->untrack(*stream_); }
			if (deferred_) { reactor_->untrack(*deferred_); }
		}
		if (stream_) { try { stream_->exchange->close(ReplySettlement::Shutdown); } catch (...) {} stream_->disable(); try { stream_->exchange->settle(); } catch (...) {} }
		if (deferred_) { try { deferred_->exchange->close(ReplySettlement::Shutdown); } catch (...) {} deferred_->disable(); try { deferred_->exchange->settle(ReplySettlement::Shutdown); } catch (...) {} }
	}
	std::unique_ptr<DeferredReply> defer(DeferredOptions options) override {
		if (!executor_) { return {}; }
		if (started_ || ended_ || !headers_.empty() || close_ || deferred_ || stream_) { throw std::logic_error("response authority already used"); }
		auto control = detail::AsioDeferredControl::create(*executor_, std::move(options));
		if (reactor_) { reactor_->track(*control); }
		auto capability = std::make_unique<DeferredReply>(control->exchange); deferred_ = std::move(control); return capability;
	}
	const std::shared_ptr<detail::AsioDeferredControl>& deferred() const noexcept { return deferred_; }
	OwnedStreamCapability stream(StreamOptions options) override {
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
	void synchronous() const { if (is_deferred()) { throw std::logic_error("response authority was deferred"); } }
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

template<class Stream, std::size_t N>
inline asio::awaitable<ReplySettlement> write_before_deadline(Stream& socket,
	std::array<asio::const_buffer, N> buffers, std::chrono::steady_clock::time_point deadline) {
	using namespace asio::experimental::awaitable_operators;
	auto executor = co_await asio::this_coro::executor; asio::steady_timer timer(executor); timer.expires_at(deadline);
	asio::error_code write_error, timer_error;
	auto winner = co_await (asio::async_write(socket, buffers, asio::redirect_error(asio::use_awaitable, write_error)) || timer.async_wait(asio::redirect_error(asio::use_awaitable, timer_error)));
	if (winner.index() == 0) { co_return write_error ? ReplySettlement::Disconnected : ReplySettlement::Written; }
	co_return timer_error ? ReplySettlement::Failed : ReplySettlement::TimedOut;
}
// Writes buffers before `deadline`, and with an inactivity bound, 64 KiB at a time, each
// slice before `inactivity` from its start (whichever deadline comes first).
template<class Stream, std::size_t N>
inline asio::awaitable<ReplySettlement> write_with_progress(Stream& socket, std::array<asio::const_buffer, N> buffers,
	std::chrono::milliseconds inactivity, std::chrono::steady_clock::time_point deadline) {
	if (inactivity.count() <= 0) {
		if (deadline == std::chrono::steady_clock::time_point::max()) {
			asio::error_code ec;
			co_await asio::async_write(socket, buffers, asio::redirect_error(asio::use_awaitable, ec));
			co_return ec ? ReplySettlement::Disconnected : ReplySettlement::Written;
		}
		co_return co_await write_before_deadline(socket, buffers, deadline);
	}
	constexpr std::size_t slice = 64 * 1024;
	for (const auto& b : buffers) {
		for (std::size_t at = 0; at < b.size(); at += slice) {
			const auto* data = static_cast<const char*>(b.data()) + at;
			const std::size_t n = std::min(slice, b.size() - at);
			const auto until = std::min(deadline, std::chrono::steady_clock::now() + inactivity);
			const auto r = co_await write_before_deadline(socket, std::array<asio::const_buffer, 1>{asio::buffer(data, n)}, until);
			if (r != ReplySettlement::Written) { co_return r; }
		}
	}
	co_return ReplySettlement::Written;
}

// One owned one-byte read keeps watching the socket while a handler runs or a deferred
// response waits, and consumed pipelined input is returned to the parser, never discarded
// by a race. Two rules read it, and they differ on purpose:
//   * a deferred or owned-stream reply (monitor): only a read error other than EOF is a
//     disconnect, because the client may half-close after sending and still expect the reply;
//   * a request in flight (watch): EOF and any read error but our own cancellation are a
//     disconnect, because a client that closed has nobody to read what a search computes.
//     A pipelined byte (the client sent the next request early) ends the watch for the
//     rest of that request: a later disconnect is not seen until the next read.
template<class Stream>
class ConnectionInput : public std::enable_shared_from_this<ConnectionInput<Stream>> {
public:
	explicit ConnectionInput(Stream& stream) : stream_(stream), done_(stream.get_executor()) {}
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
	void watch(std::function<void()> on_disconnect) {
		on_disconnect_ = std::move(on_disconnect);
		if (ready_) {
			if (error_ && error_ != asio::error::operation_aborted) { fire(); }
			else if (size_) { on_disconnect_ = nullptr; }
			return;
		}
		arm();
	}
	void unwatch() noexcept { on_disconnect_ = nullptr; }
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
	asio::awaitable<std::size_t> read(asio::mutable_buffer output, std::chrono::steady_clock::time_point deadline) {
		if (deadline == std::chrono::steady_clock::time_point::max()) { co_return co_await read(output); }
		using namespace asio::experimental::awaitable_operators;
		auto winner = co_await (read_completed(output) || expire_read(deadline));
		if (winner.index() == 0) {
			auto result = std::get<0>(winner);
			if (result.failure) { std::rethrow_exception(result.failure); }
			co_return result.bytes;
		}
		auto error = std::get<1>(winner);
		throw asio::system_error(error ? error : asio::error::timed_out);
	}
	// Waits for the pending one-byte read. A cancelled waiter cancels that read too, so the
	// wait ends: the caller then sees the read fail with operation_aborted.
	asio::awaitable<void> drain() {
		while (pending_) {
			done_.expires_at(std::chrono::steady_clock::time_point::max());
			asio::error_code error; co_await done_.async_wait(asio::redirect_error(asio::use_awaitable, error));
			if (pending_ && (co_await asio::this_coro::cancellation_state).cancelled() != asio::cancellation_type::none) {
				asio::error_code ignored; stream_.lowest_layer().cancel(ignored);
			}
		}
	}
private:
	struct ReadCompletion { std::size_t bytes; std::exception_ptr failure; };
	void arm() {
		if (pending_ || ready_) { return; }
		pending_ = true;
		auto owner = this->shared_from_this();
		try {
			stream_.async_read_some(asio::buffer(&byte_, 1), [owner](asio::error_code error, std::size_t size) {
				owner->completed(error, size);
			});
		} catch (...) { pending_ = false; throw; }
	}
	void completed(asio::error_code error, std::size_t size) {
		pending_ = false; ready_ = true; error_ = error; size_ = size;
		done_.cancel();
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
	asio::awaitable<ReadCompletion> read_completed(asio::mutable_buffer output) {
		try { co_return ReadCompletion{co_await read(output), {}}; }
		catch (...) { co_return ReadCompletion{0, std::current_exception()}; }
	}
	asio::awaitable<asio::error_code> expire_read(std::chrono::steady_clock::time_point deadline) {
		asio::steady_timer timer(stream_.get_executor()); timer.expires_at(deadline);
		asio::error_code error; co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, error));
		if (!error) {
			// The monitor read is a callback-based original read outside this
			// coroutine's cancellation slot. Cancel that socket operation too,
			// then the OR join and connection drain retain its storage until reap.
			asio::error_code ignored; stream_.lowest_layer().cancel(ignored);
		}
		co_return error;
	}
	Stream& stream_; asio::steady_timer done_;
	char byte_ = 0; std::size_t size_ = 0; asio::error_code error_;
	bool pending_ = false, ready_ = false;
	std::function<void()> on_disconnect_;
	std::weak_ptr<AsioDeferredControl> observer_;
	std::weak_ptr<AsioOwnedStreamControl> owned_observer_;
};
template <class Stream>
inline asio::awaitable<bool> write_owned_response(Stream &socket,
												  const std::shared_ptr<AsioOwnedStreamControl> &control,
												  const std::shared_ptr<ConnectionInput<Stream>> &input) {
	input->monitor(control);
	auto executor = co_await asio::this_coro::executor;
	while (true) {
		if (std::chrono::steady_clock::now() >= control->exchange->deadline())
			control->exchange->close(ReplySettlement::TimedOut);
		if (control->exchange->settle())
			co_return control->exchange->closing_reason() == ReplySettlement::Written;
		auto original = control->exchange->take_write();
		if (!original) {
			co_await control->wait();
			auto cancellation = co_await asio::this_coro::cancellation_state;
			if (cancellation.cancelled() != asio::cancellation_type::none)
				control->exchange->close(ReplySettlement::Shutdown);
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
		ReplySettlement outcome;
		try {
			outcome = co_await asio::co_spawn(
				executor, write_before_deadline(socket, buffers, control->exchange->deadline()),
				asio::bind_cancellation_slot(control->write_cancellation(), asio::use_awaitable));
		} catch (...) {
			original.reset();
			control->exchange->complete_write(serial, ReplySettlement::Failed);
			control->exchange->close(ReplySettlement::Failed);
			control->exchange->settle();
			throw;
		}
		// The OR join has reaped the original socket operation. Only now may
		// its immutable body owner retire and wake the next producer turn.
		original.reset();
		control->exchange->complete_write(serial, outcome);
	}
}
template <class Stream>
inline asio::awaitable<bool> write_response(Stream &socket, AsioResponseWriter &writer, bool keep_alive,
											const std::shared_ptr<ConnectionInput<Stream>> &input) {
	if (auto stream = writer.owned_stream()) { co_return co_await write_owned_response(socket, stream, input); }
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
// Maps an exception that escaped handle() to the response: the app's on_error first, then
// a generic 500 for whatever it left unanswered.
inline void map_handler_error(HttpHandler* handler, Request& request, AsioResponseWriter& writer, std::exception_ptr error) {
	if (writer.is_deferred()) { writer.abandon_deferred(); } else if (!writer.ended()) { handler->on_error(error, request, writer); }
	if (!writer.is_deferred() && !writer.ended()) {
		writer.status(500);
		writer.write("Internal Server Error\n");
		writer.end();
	}
}

// A streamed request runs its handler as its own coroutine on the connection's loop,
// next to the connection coroutine that feeds the body; this is the join between them.
struct HandlerDone {
	explicit HandlerDone(asio::any_io_executor executor) : timer(std::move(executor), std::chrono::steady_clock::time_point::max()) {}
	void finish() { done = true; timer.cancel(); }
	// Returns once the handler finished, or at once when the waiter is cancelled.
	asio::awaitable<void> wait() {
		while (!done) {
			asio::error_code error;
			co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, error));
			if ((co_await asio::this_coro::cancellation_state).cancelled() != asio::cancellation_type::none) { co_return; }
		}
	}
	asio::steady_timer timer;
	bool done = false;
};
inline asio::awaitable<void> run_streamed_handler(HttpHandler* handler, Request& request, AsioResponseWriter& writer) {
	try { co_await handler->handle(request, writer); }
	catch (...) { map_handler_error(handler, request, writer, std::current_exception()); }
}

// Shutdown's hook into one running request: abort() cancels it (its reads stop at their next
// checkpoint, its writes finish), and force() -- after the shutdown grace -- closes the
// socket a stalled peer holds. Tracked for the request's whole run, on the reactor's list.
template<class Stream>
struct RequestAbort final : reactor::Abortable {
	RequestAbort(reactor::Reactor& owner, Request& req, Stream& stream) : reactor(owner), request(req), socket(stream) { reactor.track(*this); }
	~RequestAbort() override { reactor.untrack(*this); }
	void abort() override { request.cancel(CancelReason::Shutdown); }
	void force() override { asio::error_code ignored; socket.lowest_layer().close(ignored); }
	reactor::Reactor& reactor;
	Request& request;
	Stream& socket;
};

// Clears the connection's disconnect watch on every exit, so a late read cannot reach a
// request that is gone.
template<class Stream>
struct WatchGuard {
	explicit WatchGuard(ConnectionInput<Stream>& in) : input(in) {}
	~WatchGuard() { input.unwatch(); }
	ConnectionInput<Stream>& input;
};

// Serve one connection: a keep-alive loop of parse -> handler -> write, all coroutines on
// the connection's loop. handle() is awaited, so a handler that waits suspends and the loop
// serves other connections. A request is admitted at headers-complete (reactor gate, else
// 503), and from then until its response is written it is tracked for shutdown and watched
// for a client hang-up; both cancel it (Request::cancel) without destroying its frame. A
// handler that opted into wants_body_stream runs while the body is still arriving, reading
// it from a flow-controlled BodyStream that this coroutine feeds.
template<class Stream>
inline asio::awaitable<void> serve_connection_body(Stream& socket, const std::shared_ptr<ConnectionInput<Stream>>& input, HttpHandler* handler,
                                              reactor::Reactor* reactor, const AsioResponseOptions* options) {
	using asio::use_awaitable;
	std::optional<RequestRejected> rejected;
	try {
		asio::error_code opt_ec;
		socket.lowest_layer().set_option(asio::ip::tcp::no_delay(true), opt_ec);
		std::string remote;  // the peer's address, for every request on this connection
		if (auto peer = socket.lowest_layer().remote_endpoint(opt_ec); !opt_ec) { remote = peer.address().to_string(); }
		auto ex = co_await asio::this_coro::executor;
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

		using clock = std::chrono::steady_clock;
		const auto ms = [](std::chrono::milliseconds t) { return t.count() > 0; };
		const auto idle = options ? options->idle_timeout : std::chrono::milliseconds{0};
		const auto header = options ? options->header_timeout : std::chrono::milliseconds{0};
		const auto inactivity = options ? options->inactivity_timeout : std::chrono::milliseconds{0};
		// The next body read's deadline: the request's, or sooner when nothing arrives.
		const auto body_deadline = [&](clock::time_point request_deadline) {
			return ms(inactivity) ? std::min(request_deadline, clock::now() + inactivity) : request_deadline;
		};
		// A parse that stopped for the body cap is a 413; any other parse error drops the connection.
		const auto parse_failed = [&] {
			if (parser.body_limit_exceeded()) { throw RequestRejected(413, "Payload Too Large\n"); }
		};
		// Writes the response; false when the connection cannot carry on.
		const auto respond = [&](AsioResponseWriter& writer, bool keep_alive, clock::time_point request_deadline) -> asio::awaitable<bool> {
			if (writer.is_deferred()) { co_return co_await write_response(socket, writer, keep_alive, input); }
			std::string head = writer.serialize_head(keep_alive);
			std::array<asio::const_buffer, 2> buffers{asio::buffer(head), writer.has_body() ? asio::buffer(writer.body()) : asio::const_buffer{}};
			co_return co_await write_with_progress(socket, buffers, inactivity, request_deadline) == ReplySettlement::Written;
		};
		for (;;) {
			// A shutdown that arrived while the last request ran found cancellation disabled.
			if (reactor->stopping()) { co_return; }
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
					std::size_t n = co_await input->read(asio::buffer(tmp), until);
					if (n == 0) { co_return; }
					if (waiting && ms(header)) { header_deadline = clock::now() + header; }
					buffered.append(tmp, n);
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { parse_failed(); co_return; }
			}

			if (parser.streaming()) {
				// ---- STREAMED BODY: the handler runs while this coroutine feeds the body ----
				Request request = parser.take_headers();
				request.remote = remote;
				reactor->lag().note_activity();
				bool keep_alive = request.keep_alive;
				auto stream = std::make_shared<BodyStream>(ex, /*capacity=*/8);
				request.body_stream = stream;
				AsioResponseWriter writer(request, options, &ex, reactor);
				RequestAbort<Stream> tracked(*reactor, request, socket);
				WatchGuard<Stream> watching(*input);
				auto handler_done = std::make_shared<HandlerDone>(ex);

				asio::co_spawn(ex, run_streamed_handler(handler, request, writer),
					[handler_done, stream](std::exception_ptr) { handler_done->finish(); stream->abandon(); });

				enum class Fed { Complete, Failed, Abandoned };
				const auto feed = [&]() -> asio::awaitable<Fed> {
					for (;;) {
						// Drain whatever body has been parsed so far -- including bytes that
						// arrived in the same read as the headers (a small body completes
						// during phase 1, so this must run before the complete() check).
						std::string chunk = parser.take_pending_body();
						if (!chunk.empty() && !co_await stream->push(std::move(chunk))) { co_return Fed::Abandoned; }
						if (parser.complete()) { stream->finish(); co_return Fed::Complete; }
						if (buffered.empty()) {
							std::size_t n = 0;
							asio::error_code failure;
							try { n = co_await input->read(asio::buffer(tmp), body_deadline(request_deadline)); }
							catch (const asio::system_error& e) { failure = e.code(); }
							if (failure || n == 0) { stream->fail(failure ? failure : asio::error::connection_reset); co_return Fed::Failed; }
							buffered.append(tmp, n);
						}
						std::size_t used = parser.feed(buffered.data(), buffered.size());
						buffered.erase(0, used);
						if (parser.errored()) { stream->fail(asio::error::invalid_argument); co_return Fed::Failed; }
					}
				};
				// The body is fed until it ends, or until the handler finishes first (it
				// answered without reading it all): then the feed is cancelled.
				Fed fed;
				{
					using namespace asio::experimental::awaitable_operators;
					auto first = co_await (feed() || handler_done->wait());
					fed = first.index() == 0 ? std::get<0>(first) : Fed::Abandoned;
				}
				// A feed that stopped early leaves the reader a failed body, never a hung read.
				if (!stream->closed()) { stream->fail(asio::error::operation_aborted); }
				// From here the handler and the request/writer it references end only by
				// completing, never by a frame being destroyed under them. A cancellation that
				// would have reached this coroutine arrives as Request::cancel through `tracked`.
				co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
				if (fed == Fed::Failed) { request.cancel(reactor->stopping() ? CancelReason::Shutdown : CancelReason::Disconnect); }
				else if (fed == Fed::Complete && !handler_done->done) {
					input->watch([&request] { request.cancel(CancelReason::Disconnect); });
				}
				co_await handler_done->wait();
				input->unwatch();
				if (fed != Fed::Complete) { keep_alive = false; }

				if (!writer.is_deferred() && !writer.ended()) {
					writer.status(500);
					writer.write("Internal Server Error\n");
					writer.end();
				}
				if (!co_await respond(writer, keep_alive, request_deadline)) { co_return; }
				admitted.reset();
				co_await asio::this_coro::reset_cancellation_state(asio::enable_terminal_cancellation());
				if (!keep_alive || writer.wants_close()) { break; }
				parser.reset();
				continue;
			}

			// ---- BUFFERED / PUSH path ----
			while (!parser.complete()) {
				if (buffered.empty()) {
					std::size_t n = co_await input->read(asio::buffer(tmp), body_deadline(request_deadline));
					buffered.append(tmp, n);
				}
				std::size_t used = parser.feed(buffered.data(), buffered.size());
				buffered.erase(0, used);
				if (parser.errored()) { parse_failed(); co_return; }
			}

			if (std::chrono::steady_clock::now() >= request_deadline) { co_return; }
			Request request = parser.take();
			request.remote = remote;
			reactor->lag().note_activity();
			parser.resume();
			bool keep_alive = request.keep_alive;
			// The extension and the body (pushed to the app's sink, or buffered into
			// request.body) were already handled at headers-complete by the parser hook.

			AsioResponseWriter writer(request, options, &ex, reactor);
			RequestAbort<Stream> tracked(*reactor, request, socket);
			{
				WatchGuard<Stream> watching(*input);
				// The handler's frame ends only by completing; see the streamed path.
				co_await asio::this_coro::reset_cancellation_state(asio::disable_cancellation());
				input->watch([&request] { request.cancel(CancelReason::Disconnect); });
				try {
					co_await handler->handle(request, writer);
				} catch (...) {
					map_handler_error(handler, request, writer, std::current_exception());
				}
			}
			if (!co_await respond(writer, keep_alive, request_deadline)) { co_return; }
			admitted.reset();
			co_await asio::this_coro::reset_cancellation_state(asio::enable_terminal_cancellation());
			if (!keep_alive || writer.wants_close()) { break; }
		}
	} catch (const RequestRejected& error) {
		rejected.emplace(error);
	} catch (const std::exception&) {
		// client hangup / read-write error -- drop the connection
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
		co_await write_before_deadline(socket, buffers, std::chrono::steady_clock::now() + std::chrono::seconds(1));
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
// SO_REUSEPORT or a portable shared acceptor, per-reactor admission gate, graceful
// shutdown); this class is the thin HTTP-shaped adapter -- it turns the handler + the
// response options into the per-connection Session (detail::serve_connection) and
// exposes the same surface callers had before the runtime was extracted.
class HttpAsioService {
public:
	using ConnectionFactory = std::function<asio::awaitable<void>(asio::ip::tcp::socket,
		HttpHandler*, reactor::Reactor*, const AsioResponseOptions*)>;
	using TimedConnectionFactory = std::function<asio::awaitable<void>(asio::ip::tcp::socket,
		HttpHandler*, reactor::Reactor*, const AsioResponseOptions*, reactor::SessionContext)>;
	// `reactors` loops, each admitting at most `in_flight_limit` requests at a time (0 =
	// no cap); a request over the cap is refused with a 503 and `Retry-After: 1`.
	HttpAsioService(HttpHandler& handler, std::size_t reactors, std::size_t in_flight_limit,
		ConnectionFactory connection = {})
		: HttpAsioService(handler, reactors, in_flight_limit, TimedConnectionFactory{
			[connection = std::move(connection)](asio::ip::tcp::socket socket, HttpHandler* handler,
				reactor::Reactor* reactor, const AsioResponseOptions* options, reactor::SessionContext) {
				if (connection) { return connection(std::move(socket), handler, reactor, options); }
				return detail::serve_connection(std::move(socket), handler, reactor, options);
			}}, 0) {}
	// Explicit fifth argument keeps existing four-argument brace calls unambiguous.
	HttpAsioService(HttpHandler& handler, std::size_t reactors, std::size_t in_flight_limit,
		TimedConnectionFactory connection, std::size_t maximum_connections)
		: handler_(handler), connection_(std::move(connection)),
		  server_(reactors, static_cast<int>(in_flight_limit), reactor::TimedSession{
			[this](asio::ip::tcp::socket socket, reactor::Reactor& reactor, reactor::SessionContext context) {
				if (connection_) { return connection_(std::move(socket), &handler_, &reactor, &response_, context); }
				return detail::serve_connection(std::move(socket), &handler_, &reactor, &response_);
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

	// Cancel in-flight requests (their responses are still written), force the sockets
	// that are still busy after the grace, stop the loops, join. Also runs from the
	// destructor (server_'s own dtor), so a service going out of scope tears down cleanly.
	void stop() { server_.stop(); }

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
	TimedConnectionFactory connection_;
	reactor::TcpServer server_;
};

}  // namespace http
