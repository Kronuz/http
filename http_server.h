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

// The HTTP accept loop and a small service root. These hold the reactor/worker
// plumbing (Kronuz/server) so an application only ever sees its HttpHandler. The
// `io_accept_cb` / `io.start` here are the engine internals that a future Asio
// port replaces wholesale; nothing application-facing touches them.

#pragma once

#include <chrono>
#include <cstddef>
#include <memory>

#include "base_server.h"     // Kronuz/server: MetaBaseServer<ServerImpl>
#include "worker.h"          // Kronuz/server: Worker

#include "http_connection.h"
#include "http_dispatcher.h"
#include "http_handler.h"
#include "http_watchdog.h"

namespace http {

// How the listening socket is bound. A world-class server exposes these as first
// class knobs rather than hard-coding them: `reuse_port` (SO_REUSEPORT) is what lets
// several reactors share one port for scaling and graceful restart, `tcp_nodelay`
// (disable Nagle) trims latency on small responses, and `defer_accept` (Linux
// TCP_DEFER_ACCEPT) wakes the acceptor only once data has arrived.
struct BindOptions {
	bool reuse_port = false;    // SO_REUSEPORT: several reactors may bind the same port
	bool tcp_nodelay = true;    // TCP_NODELAY: disable Nagle for lower small-response latency
	bool defer_accept = false;  // TCP_DEFER_ACCEPT (Linux): accept only when data is ready
	int tries = 1;              // bind retries before giving up

	int to_flags() const {
		int flags = 0;
		if (reuse_port) { flags |= TCP_SO_REUSEPORT; }
		if (tcp_nodelay) { flags |= TCP_TCP_NODELAY; }
		if (defer_accept) { flags |= TCP_TCP_DEFER_ACCEPT; }
		return flags;
	}
};


class HttpServer : public MetaBaseServer<HttpServer> {
	friend Worker;

	HttpHandler& handler_;
	Dispatcher* dispatcher_;   // this reactor's worker pool; null => handlers run inline
	const ResponseOptions* response_;   // response transforms (compress / conditional); null => none
	int tries_;

public:
	HttpServer(const std::shared_ptr<Worker>& parent, ev::loop_ref* loop, unsigned int flags, HttpHandler& handler, Dispatcher* dispatcher = nullptr, const ResponseOptions* response = nullptr, const BindOptions& bind_options = {})
		: MetaBaseServer<HttpServer>(parent, loop, flags, "http", bind_options.to_flags()),
		  handler_(handler),
		  dispatcher_(dispatcher),
		  response_(response),
		  tries_(bind_options.tries) {}

	void listen(unsigned int port) {
		bind(nullptr, port, tries_);
	}

	void start_impl() override {
		Worker::start_impl();
		io.start(sock, ev::READ);
	}

	void io_accept_cb(ev::io& /*watcher*/, int revents) {
		if ((EV_ERROR & revents) != 0) {
			return;
		}
		int client_sock = accept();
		if (client_sock != -1) {
			auto conn = Worker::make_shared<HttpConnection>(share_this<HttpServer>(), ev_loop, ev_flags, handler_, dispatcher_, response_);
			if (!conn->init(client_sock)) {
				conn->detach();
				return;
			}
			conn->start();
		}
	}
};


// The root of the worker tree: owns the event loop, one HttpServer, and -- when
// the application opts into off-reactor dispatch -- this reactor's Dispatcher. An
// app builds one with create(handler), binds a port, and runs.
//
// One HttpService == one reactor (one event loop) == one Dispatcher. That keeps
// the worker pool shared-nothing across cores: to scale, run several HttpService
// instances on several threads (SO_REUSEPORT), each with its own loop + pool,
// rather than one global pool shared by every reactor thread.
class HttpService : public Worker {
	HttpHandler& handler_;
	std::unique_ptr<Dispatcher> dispatcher_;   // null => handlers run inline on the reactor
	std::unique_ptr<StallWatchdog> watchdog_;  // null => no stall monitoring
	ResponseOptions response_;                 // response transforms; off until enable_*()
	BindOptions bind_options_;                 // how the listening socket is bound
	std::shared_ptr<HttpServer> server_;

public:
	HttpService(const std::shared_ptr<Worker>& parent, ev::loop_ref* loop, unsigned int flags, HttpHandler& handler, std::unique_ptr<Dispatcher> dispatcher)
		: Worker(parent, loop, flags),
		  handler_(handler),
		  dispatcher_(std::move(dispatcher)) {}

	// The Worker contract: a Worker subclass must call Worker::deinit() as the last
	// line of its destructor (it stops the base async watchers and sets _deinited,
	// which ~Worker asserts). BaseServer/BaseClient do this for the server and the
	// connections; HttpService is a direct Worker, so it must too.
	~HttpService() noexcept {
		if (watchdog_) { watchdog_->stop(); }   // join the monitor before the loop/members go away
		Worker::deinit();
	}

	// Inline: every handler runs on the reactor thread. Simplest; correct when
	// handlers are cheap and non-blocking.
	static std::shared_ptr<HttpService> create(HttpHandler& handler) {
		return Worker::make_shared<HttpService>(std::shared_ptr<Worker>(), static_cast<ev::loop_ref*>(nullptr), static_cast<unsigned int>(ev::AUTO), handler, std::unique_ptr<Dispatcher>());
	}

	// Off-reactor (the un-stallable model): handlers run on a bounded pool of
	// `workers` threads draining a queue bounded at `queue_limit` tasks, so a slow
	// or blocking handler never stalls the loop and overload sheds as 503. The
	// HttpHandler must be thread-safe (handlers run concurrently across connections).
	static std::shared_ptr<HttpService> create(HttpHandler& handler, std::size_t workers, std::size_t queue_limit) {
		auto dispatcher = std::make_unique<Dispatcher>(workers, queue_limit);
		return Worker::make_shared<HttpService>(std::shared_ptr<Worker>(), static_cast<ev::loop_ref*>(nullptr), static_cast<unsigned int>(ev::AUTO), handler, std::move(dispatcher));
	}

	// Enable the reactor-stall watchdog: a monitor thread flags the loop if it goes
	// `threshold` without ticking. Call before listen()/run(). Optional `on_stall`
	// (default logs to stderr) is invoked from the monitor thread per stall episode.
	void watch_stalls(std::chrono::milliseconds threshold, StallWatchdog::Callback on_stall = {}) {
		watchdog_ = std::make_unique<StallWatchdog>(*ev_loop, threshold, std::move(on_stall));
	}

	// Enable transparent response compression: buffered responses are compressed to
	// the codec the client advertised in Accept-Encoding (zstd or gzip). Call before
	// listen(). The CPU cost lands on the worker for offloaded handlers.
	void enable_compression(CompressionOptions options = {}) {
		response_.compression = options;
		response_.compress = true;
	}

	// Enable conditional requests: buffered GET/HEAD 200s get a weak ETag, and a
	// matching If-None-Match is answered 304 Not Modified (no body). Call before
	// listen().
	void enable_conditional() {
		response_.conditional = true;
	}

	// Enable range requests: a buffered GET 200 advertises Accept-Ranges and serves a
	// single byte Range as 206 Partial Content (416 when unsatisfiable). Call before
	// listen().
	void enable_ranges() {
		response_.ranges = true;
	}

	// Set how the listening socket is bound (SO_REUSEPORT for multi-reactor scaling,
	// TCP_NODELAY, TCP_DEFER_ACCEPT, bind retries). Call before listen(). To run
	// several reactors on one port -- one HttpService per thread, each with its own
	// loop + pool (shared-nothing) -- give every one bind_options.reuse_port = true.
	void set_bind_options(const BindOptions& options) {
		bind_options_ = options;
	}

	void listen(unsigned int port) {
		server_ = Worker::make_shared<HttpServer>(share_this<HttpService>(), ev_loop, ev_flags, handler_, dispatcher_.get(), &response_, bind_options_);
		server_->listen(port);
		server_->start();
		if (watchdog_) { watchdog_->start(); }   // on the loop thread, before the loop runs
	}

	void run() { run_loop(); }
	void stop() {
		if (watchdog_) { watchdog_->stop(); }   // stop monitoring before the loop stops petting
		break_loop();
	}
};

}  // namespace http
