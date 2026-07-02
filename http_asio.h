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

// The Asio runtime + connection for Kronuz/http -- the transport layer, rewritten on
// standalone Asio (io_context + C++20 coroutines + thread_pool) in place of the libev
// Worker/BaseClient substrate. Everything above it is unchanged: the HttpHandler seam,
// the radix router, http_message, http-parser (via RequestParser), the codings.
//
// The un-stallable model is native here: a connection is a coroutine, and offloading a
// blocking handler is a single `co_await co_spawn(pool, ...)` -- the work runs on a
// pool thread while the coroutine suspends, the reactor is free, and resumption lands
// back on the connection's own io_context. No pause/resume/pending-input bookkeeping,
// and no cross-thread completion handoff to get wrong: the coroutine is the single
// owner of its request/response, so the ordering is structural.
//
// Scaling is the shared-nothing / thread-per-core model: N io_contexts on N threads,
// each with an acceptor bound to one port via SO_REUSEPORT, plus a bounded thread_pool
// for offload. Response compression is applied on the buffered response (the same
// http_compression helpers as before). Conditional/range transforms slot in the same
// way and are TODO in this scaffold. Request-body intake is dual-mode (the app chooses
// per endpoint via on_request_body): buffered for small bodies, or TRUE INCREMENTAL
// streaming to a sink for large/unbounded ones -- a multi-gigabyte RESTORE/bulk load
// flows through in O(read-buffer) memory, never held whole.

#pragma once

#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/experimental/concurrent_channel.hpp>

#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "http_compression.h"
#include "http_conditional.h"
#include "http_handler.h"
#include "http_message.h"
#include "http_range.h"
#include "http_request_parser.h"

namespace http {

// SO_REUSEPORT as an Asio socket option (Asio ships reuse_address, not reuse_port).
using asio_reuse_port = asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>;

// How the acceptor is bound. Mirrors the libev BindOptions so the API is stable across
// the swap; reuse_port is what lets the N reactors share one port.
struct AsioBindOptions {
	std::string address;  // empty => all interfaces (0.0.0.0)
	bool reuse_port = false;
	bool tcp_nodelay = true;
	int backlog = 1024;
};

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

// The buffered ResponseWriter: the handler sets status/headers and writes the body;
// on serialize() the transforms run and the HTTP/1.1 bytes are framed (Content-Length
// + Connection are the transport's, everything else is the app's).
class AsioResponseWriter : public ResponseWriter {
public:
	AsioResponseWriter(const Request& request, const AsioResponseOptions* options)
		: request_(request), options_(options) {}

	void status(int code) override { status_ = code; }
	void set_header(std::string_view name, std::string_view value) override { headers_.emplace_back(name, value); }
	void write(std::string_view chunk) override { body_.append(chunk); }
	void end() override { ended_ = true; }
	void set_close() override { close_ = true; }

	bool ended() const { return ended_; }
	bool wants_close() const { return close_; }

	// Apply the response transforms and frame the HTTP/1.1 response bytes. Transforms
	// run in order: conditional (may flip to a bodyless 304) -> range (may flip to a
	// 206 slice or 416) -> compression (self-skips 204/206/304 and tiny bodies).
	std::string serialize(bool keep_alive) {
		maybe_conditional();
		if (status_ != 304) { maybe_range(); }
		maybe_compress();
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
		if (status_ != 304) { out += body_; }
		return out;
	}

private:
	const Request& request_;
	const AsioResponseOptions* options_;
	int status_ = 200;
	std::vector<std::pair<std::string, std::string>> headers_;
	std::string body_;
	bool ended_ = false;
	bool close_ = false;

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

// Offload admission for a reactor: a bounded window so a saturated pool sheds as 503
// instead of growing an unbounded backlog (the libev Dispatcher's queue_limit).
struct OffloadGate {
	std::atomic<int> inflight{0};
	int limit;
	explicit OffloadGate(int limit_) : limit(limit_) {}
	bool try_enter() {
		if (inflight.fetch_add(1, std::memory_order_relaxed) >= limit) {
			inflight.fetch_sub(1, std::memory_order_relaxed);
			return false;
		}
		return true;
	}
	void leave() { inflight.fetch_sub(1, std::memory_order_relaxed); }
};

// One reactor: an io_context (the loop, one thread) + its own bounded worker pool
// (shared-nothing) + the offload gate. A pool of size 0 means "always inline".
struct AsioReactor {
	asio::io_context io{1};
	std::unique_ptr<asio::thread_pool> pool;   // null => inline
	OffloadGate gate;
	AsioReactor(std::size_t workers, int queue_limit)
		: pool(workers != 0 ? std::make_unique<asio::thread_pool>(workers) : nullptr),
		  gate(queue_limit) {}
};

namespace detail {

// The Asio-backed BodyReader: a bounded, thread-safe channel of raw body chunks. The
// reactor coroutine pushes chunks and SUSPENDS when the channel is full (flow control /
// back-pressure -> the socket read pauses, the reactor stays free); the handler, on its
// worker thread, pulls them with a blocking read(). An empty chunk is the end-of-body
// marker (real body chunks are never empty); abort() (a transport error) closes the
// channel so a blocked read() returns false and the handler can see aborted().
class ChannelBodyReader : public BodyReader {
public:
	using channel_type = asio::experimental::concurrent_channel<void(asio::error_code, std::string)>;

	ChannelBodyReader(const asio::any_io_executor& ex, std::size_t capacity) : ch_(ex, capacity) {}

	// consumer (handler worker): blocking pull; false at end-of-body or on abort.
	bool read(std::string& chunk) override {
		std::future<std::string> fut = ch_.async_receive(asio::use_future);
		try {
			std::string v = fut.get();
			if (v.empty()) { return false; }   // end-of-body marker
			chunk = std::move(v);
			return true;
		} catch (const std::exception&) {
			return false;                       // channel closed (abort)
		}
	}

	bool aborted() const { return aborted_.load(std::memory_order_acquire); }

	// producer (reactor coroutine): suspends when the channel is full.
	asio::awaitable<void> push(std::string chunk) {
		co_await ch_.async_send(asio::error_code{}, std::move(chunk), asio::use_awaitable);
	}
	asio::awaitable<void> finish() {
		co_await ch_.async_send(asio::error_code{}, std::string{}, asio::use_awaitable);   // end marker
	}
	void abort() {
		aborted_.store(true, std::memory_order_release);
		ch_.close();
	}

private:
	channel_type ch_;
	std::atomic<bool> aborted_{false};
};

// Serve one connection: keep-alive loop of parse -> handler seam -> write. A normal
// request runs handle() inline on the reactor, or (pool + should_offload) on the pool
// with the reactor free -- the un-stallable path. A request the handler marked
// wants_body_stream runs CONCURRENTLY: the handler on the pool pulls body chunks from a
// flow-controlled BodyReader that the reactor feeds -- O(buffer) memory for any size.
inline asio::awaitable<void> serve_connection(asio::ip::tcp::socket socket, HttpHandler* handler,
                                              AsioReactor* reactor, const AsioResponseOptions* options) {
	using asio::use_awaitable;
	using namespace asio::experimental::awaitable_operators;
	try {
		asio::error_code opt_ec;
		socket.set_option(asio::ip::tcp::no_delay(true), opt_ec);
		auto ex = co_await asio::this_coro::executor;
		RequestParser parser;
		// The headers-complete hook: build the app's per-request extension, then choose
		// how to take the body -- concurrent pull streaming (needs a worker), push to an
		// on_request_body sink, or buffer into Request::body.
		parser.set_headers_callback([handler, reactor](Request& req) -> RequestParser::BodyIntake {
			req.extension = handler->create_extension(req);
			RequestParser::BodyIntake intake;
			if (reactor->pool != nullptr && handler->wants_body_stream(req)) {
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
					std::size_t n = co_await socket.async_read_some(asio::buffer(tmp), use_awaitable);
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
				AsioResponseWriter writer(request, options);
				asio::any_io_executor hexec(reactor->pool->get_executor());

				// Feed the body (reactor, flow-controlled) and run the handler (pool)
				// concurrently, then join. feed never throws -- a socket/parse error
				// aborts the reader so the handler's read() returns false and unblocks.
				auto feed = [&]() -> asio::awaitable<void> {
					asio::error_code fec;
					while (!parser.complete()) {
						if (buffered.empty()) {
							std::size_t n = co_await socket.async_read_some(asio::buffer(tmp),
								asio::redirect_error(use_awaitable, fec));
							if (fec || n == 0) { reader->abort(); co_return; }
							buffered.append(tmp, n);
						}
						std::size_t used = parser.feed(buffered.data(), buffered.size());
						buffered.erase(0, used);
						if (parser.errored()) { reader->abort(); co_return; }
						std::string chunk = parser.take_pending_body();
						if (!chunk.empty()) { co_await reader->push(std::move(chunk)); }
					}
					co_await reader->finish();   // end-of-body marker
				};
				auto run = [&]() -> asio::awaitable<void> {
					try { handler->handle(request, writer); }
					catch (...) { if (!writer.ended()) { handler->on_error(std::current_exception(), request, writer); } }
					co_return;
				};
				co_await (feed() && asio::co_spawn(hexec, run(), use_awaitable));

				if (!writer.ended()) {
					writer.status(500);
					writer.write("Internal Server Error\n");
					writer.end();
				}
				std::string out = writer.serialize(keep_alive);
				co_await asio::async_write(socket, asio::buffer(out), use_awaitable);
				if (!keep_alive || writer.wants_close()) { break; }
				parser.reset();
				continue;
			}

			// ---- BUFFERED / PUSH path ----
			while (!parser.complete()) {
				if (buffered.empty()) {
					std::size_t n = co_await socket.async_read_some(asio::buffer(tmp), use_awaitable);
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

			AsioResponseWriter writer(request, options);
			bool offloaded_503 = false;
			try {
				if (reactor->pool && handler->should_offload(request)) {
					if (!reactor->gate.try_enter()) {
						writer.status(503);
						writer.write("Service Unavailable\n");
						writer.end();
						offloaded_503 = true;
					} else {
						// THE UN-STALLABLE OFFLOAD: run handle() on the pool, resume here.
						co_await asio::co_spawn(reactor->pool->get_executor(),
							[handler, &request, &writer]() -> asio::awaitable<void> {
								handler->handle(request, writer);
								co_return;
							}, use_awaitable);
						reactor->gate.leave();
					}
				} else {
					handler->handle(request, writer);   // inline on the reactor
				}
			} catch (...) {
				if (reactor->pool && !offloaded_503) { reactor->gate.leave(); }
				if (!writer.ended()) { handler->on_error(std::current_exception(), request, writer); }
				if (!writer.ended()) {
					writer.status(500);
					writer.write("Internal Server Error\n");
					writer.end();
				}
			}

			std::string out = writer.serialize(keep_alive);
			co_await asio::async_write(socket, asio::buffer(out), use_awaitable);
			if (!keep_alive || writer.wants_close()) { break; }
		}
	} catch (const std::exception&) {
		// client hangup / read-write error -- drop the connection
	}
	asio::error_code ec;
	socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
}

inline asio::awaitable<void> accept_loop(AsioReactor* reactor, HttpHandler* handler,
                                         const AsioResponseOptions* options,
                                         unsigned short port, AsioBindOptions bind) {
	using asio::ip::tcp;
	auto ex = co_await asio::this_coro::executor;
	tcp::acceptor acceptor(ex);
	tcp::endpoint ep = bind.address.empty()
		? tcp::endpoint(tcp::v4(), port)
		: tcp::endpoint(asio::ip::make_address(bind.address), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	if (bind.reuse_port) { acceptor.set_option(asio_reuse_port(true)); }
	acceptor.bind(ep);
	acceptor.listen(bind.backlog);
	for (;;) {
		tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
		asio::co_spawn(ex, serve_connection(std::move(socket), handler, reactor, options), asio::detached);
	}
}

// The no-SO_REUSEPORT path: a single acceptor (on reactor 0's loop) that binds the
// port once and distributes accepted connections round-robin across all reactors. Each
// new connection is accepted directly onto its target reactor's io_context, so only the
// (cheap) accept runs here; the read/handler/write all run sharded on the chosen
// reactor. This is the portable model -- macOS/BSD reject a second same-port bind
// without SO_REUSEPORT, so N independent acceptors are a Linux-only optimization.
inline asio::awaitable<void> shared_accept_loop(std::vector<AsioReactor*> reactors, HttpHandler* handler,
                                                const AsioResponseOptions* options,
                                                unsigned short port, AsioBindOptions bind) {
	using asio::ip::tcp;
	auto ex = co_await asio::this_coro::executor;
	tcp::acceptor acceptor(ex);
	tcp::endpoint ep = bind.address.empty()
		? tcp::endpoint(tcp::v4(), port)
		: tcp::endpoint(asio::ip::make_address(bind.address), port);
	acceptor.open(ep.protocol());
	acceptor.set_option(tcp::acceptor::reuse_address(true));
	acceptor.bind(ep);
	acceptor.listen(bind.backlog);
	std::size_t next = 0;
	for (;;) {
		AsioReactor* r = reactors[next];
		next = (next + 1 == reactors.size()) ? 0 : next + 1;
		tcp::socket socket = co_await acceptor.async_accept(r->io, asio::use_awaitable);
		asio::co_spawn(r->io, serve_connection(std::move(socket), handler, r, options), asio::detached);
	}
}

}  // namespace detail

// The HTTP service: N reactors (io_context + pool each) on N threads, bound to one
// port -- via SO_REUSEPORT where available, else a shared acceptor that fans
// connections out round-robin. Drives the application's HttpHandler.
class HttpAsioService {
public:
	// `reactors` runtimes; each with `workers` offload threads (0 => inline) and an
	// offload admission window of `queue_limit`.
	HttpAsioService(HttpHandler& handler, std::size_t reactors, std::size_t workers, std::size_t queue_limit)
		: handler_(handler), n_reactors_(reactors != 0 ? reactors : 1) {
		for (std::size_t i = 0; i < n_reactors_; ++i) {
			reactors_.push_back(std::make_unique<AsioReactor>(workers, static_cast<int>(queue_limit)));
		}
	}

	~HttpAsioService() { stop(); }

	HttpAsioService(const HttpAsioService&) = delete;
	HttpAsioService& operator=(const HttpAsioService&) = delete;

	void set_bind_options(const AsioBindOptions& options) { bind_ = options; }
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
	void start(unsigned short port) {
		// With SO_REUSEPORT (Linux) each reactor binds its own acceptor and the kernel
		// load-balances; a single reactor needs no sharing. Otherwise (macOS/BSD, N>1)
		// a second same-port bind is rejected, so one shared acceptor on reactor 0
		// distributes connections round-robin across the reactors.
		bool shared = !bind_.reuse_port && reactors_.size() > 1;
		if (!shared) {
			for (auto& r : reactors_) {
				AsioReactor* rp = r.get();
				unsigned short p = port;
				threads_.emplace_back([this, rp, p] {
					asio::co_spawn(rp->io, detail::accept_loop(rp, &handler_, &response_, p, bind_), asio::detached);
					rp->io.run();
				});
			}
			return;
		}
		std::vector<AsioReactor*> raw;
		raw.reserve(reactors_.size());
		for (auto& r : reactors_) { raw.push_back(r.get()); }
		// Idle reactors (all but the acceptor's) have no pending work yet; a work guard
		// keeps run() blocked until stop() instead of returning immediately.
		for (auto& r : reactors_) { guards_.emplace_back(asio::make_work_guard(r->io)); }
		for (auto& r : reactors_) {
			AsioReactor* rp = r.get();
			threads_.emplace_back([rp] { rp->io.run(); });
		}
		asio::co_spawn(raw[0]->io, detail::shared_accept_loop(raw, &handler_, &response_, port, bind_), asio::detached);
	}

	void stop() {
		guards_.clear();
		for (auto& r : reactors_) { r->io.stop(); }
		for (auto& t : threads_) { if (t.joinable()) { t.join(); } }
		threads_.clear();
		for (auto& r : reactors_) { if (r->pool) { r->pool->stop(); r->pool->join(); } }
	}

	std::size_t reactors() const { return n_reactors_; }

private:
	HttpHandler& handler_;
	std::size_t n_reactors_;
	AsioBindOptions bind_{};
	AsioResponseOptions response_{};
	std::vector<std::unique_ptr<AsioReactor>> reactors_;
	std::vector<std::thread> threads_;
	std::vector<asio::executor_work_guard<asio::io_context::executor_type>> guards_;
};

}  // namespace http
