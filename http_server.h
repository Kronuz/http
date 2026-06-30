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

#include <memory>

#include "base_server.h"     // Kronuz/server: MetaBaseServer<ServerImpl>
#include "worker.h"          // Kronuz/server: Worker

#include "http_connection.h"
#include "http_handler.h"

namespace http {

class HttpServer : public MetaBaseServer<HttpServer> {
	friend Worker;

	HttpHandler& handler_;

public:
	HttpServer(const std::shared_ptr<Worker>& parent, ev::loop_ref* loop, unsigned int flags, HttpHandler& handler)
		: MetaBaseServer<HttpServer>(parent, loop, flags, "http", 0),
		  handler_(handler) {}

	void listen(unsigned int port) {
		bind(nullptr, port, 1);
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
			auto conn = Worker::make_shared<HttpConnection>(share_this<HttpServer>(), ev_loop, ev_flags, handler_);
			if (!conn->init(client_sock)) {
				conn->detach();
				return;
			}
			conn->start();
		}
	}
};


// The root of the worker tree: owns the event loop and one HttpServer. An app
// builds one with create(handler), binds a port, and runs.
class HttpService : public Worker {
	HttpHandler& handler_;
	std::shared_ptr<HttpServer> server_;

public:
	HttpService(const std::shared_ptr<Worker>& parent, ev::loop_ref* loop, unsigned int flags, HttpHandler& handler)
		: Worker(parent, loop, flags),
		  handler_(handler) {}

	static std::shared_ptr<HttpService> create(HttpHandler& handler) {
		return Worker::make_shared<HttpService>(std::shared_ptr<Worker>(), static_cast<ev::loop_ref*>(nullptr), static_cast<unsigned int>(ev::AUTO), handler);
	}

	void listen(unsigned int port) {
		server_ = Worker::make_shared<HttpServer>(share_this<HttpService>(), ev_loop, ev_flags, handler_);
		server_->listen(port);
		server_->start();
	}

	void run() { run_loop(); }
	void stop() { break_loop(); }
};

}  // namespace http
