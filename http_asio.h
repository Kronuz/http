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
// way and are TODO in this scaffold. Buffered request bodies only for now (the
// streaming BodySink intake is a follow-up).

#pragma once

#include <asio.hpp>

#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <cstddef>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "http_compression.h"
#include "http_handler.h"
#include "http_message.h"
#include "http_request_parser.h"

namespace http {

// SO_REUSEPORT as an Asio socket option (Asio ships reuse_address, not reuse_port).
using asio_reuse_port = asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>;

// How the acceptor is bound. Mirrors the libev BindOptions so the API is stable across
// the swap; reuse_port is what lets the N reactors share one port.
struct AsioBindOptions {
	bool reuse_port = false;
	bool tcp_nodelay = true;
	int backlog = 1024;
};

// Response transforms applied on the buffered response before it goes on the wire.
// Same shape as the libev ResponseOptions (compression via http_compression; the
// conditional/range transforms slot in identically -- TODO in this scaffold).
struct AsioResponseOptions {
	bool compress = false;
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

	// Apply the response transforms and frame the HTTP/1.1 response bytes.
	std::string serialize(bool keep_alive) {
		maybe_compress();
		std::string out = "HTTP/1.1 ";
		out += std::to_string(status_);
		out += ' ';
		out += reason_phrase(status_);
		out += "\r\n";
		for (const auto& [k, v] : headers_) {
			out += k; out += ": "; out += v; out += "\r\n";
		}
		if (!has_header("Content-Length")) {
			out += "Content-Length: ";
			out += std::to_string(body_.size());
			out += "\r\n";
		}
		out += (keep_alive && !close_) ? "Connection: keep-alive\r\n" : "Connection: close\r\n";
		out += "\r\n";
		out += body_;
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

// Serve one connection: keep-alive loop of parse -> handler seam -> write. The handler
// runs inline on the reactor, or (when a pool is configured and should_offload is true)
// on the pool with the reactor free -- the un-stallable path.
inline asio::awaitable<void> serve_connection(asio::ip::tcp::socket socket, HttpHandler* handler,
                                              AsioReactor* reactor, const AsioResponseOptions* options) {
	using asio::use_awaitable;
	try {
		asio::error_code opt_ec;
		socket.set_option(asio::ip::tcp::no_delay(true), opt_ec);
		RequestParser parser;
		std::string buffered;
		char tmp[8192];

		for (;;) {
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
			request.extension = handler->create_extension(request);

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
	tcp::endpoint ep(tcp::v4(), port);
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

}  // namespace detail

// The HTTP service: N reactors (io_context + pool each) on N threads, all bound to one
// port with SO_REUSEPORT. Drives the application's HttpHandler. The libev HttpService's
// API, on Asio.
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

	// Bind + run all reactors (each on its own thread). Returns once every reactor is
	// listening; the caller keeps its thread and stops with stop().
	void start(unsigned short port) {
		for (auto& r : reactors_) {
			AsioReactor* rp = r.get();
			unsigned short p = port;
			threads_.emplace_back([this, rp, p] {
				asio::co_spawn(rp->io, detail::accept_loop(rp, &handler_, &response_, p, bind_), asio::detached);
				rp->io.run();
			});
		}
	}

	void stop() {
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
};

}  // namespace http
