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

// BodyStream: a request body read with co_await on the connection's loop. When a handler
// opts in (HttpHandler::wants_body_stream), the connection feeds the body into a bounded
// channel as it arrives and the handler reads it with read(), so a multi-gigabyte RESTORE or
// bulk load runs in O(capacity * chunk) memory and back-pressure reaches the socket: when the
// channel is full the connection stops reading.
//
//     std::string chunk;
//     while (!(ec = co_await request.body_stream->read(chunk))) { use(chunk); }
//
// Everything runs on the connection's loop; a BodyStream is not thread-safe.

#pragma once

#include <asio.hpp>
#include <asio/experimental/channel.hpp>

#include "task_asio.hh"

#include <cassert>
#include <cstddef>
#include <string>
#include <utility>

namespace http {

class BodyStream {
public:
	BodyStream(task::asio_scheduler& scheduler, std::size_t capacity)
		: scheduler_(scheduler), channel_(scheduler.executor(), capacity != 0 ? capacity : 1) {}
	BodyStream(const BodyStream&) = delete;
	BodyStream& operator=(const BodyStream&) = delete;

	// Reads the next chunk into `chunk`. Returns:
	//   no error                the chunk holds body bytes (never empty);
	//   asio::error::eof        the body ended; `chunk` is empty; the result is sticky;
	//   operation_aborted       `token` was requested; nothing is lost, the next read goes on;
	//   any other error         the body failed (the client closed mid-body, the service is
	//                           stopping, the handler read after it was abandoned).
	// Chunks already buffered are delivered before the end or the error. `token` is the
	// handler's own (a deadline, its source); the transport fails the body on a disconnect
	// or shutdown without one.
	task::task<asio::error_code> read(std::string& chunk, task::cancel_token token = {}) {
		affine();
		auto [error, got] = co_await task::await_asio<void(asio::error_code, std::string)>(
			[this](auto handler) { channel_.async_receive(std::move(handler)); }, std::move(token));
		chunk.clear();
		if (error == asio::experimental::error::channel_closed) {
			co_return error_ ? error_ : asio::error_code(asio::error::eof);
		}
		if (error == asio::experimental::error::channel_cancelled) { co_return asio::error::operation_aborted; }
		if (!error) { chunk = std::move(got); }
		co_return error;
	}

	// ---- the transport's side ----

	// Hands the next chunk to the reader, waiting while the channel is full. false when the
	// reader is gone (finish, fail or abandon was called) or `token` was requested.
	task::task<bool> push(std::string chunk, task::cancel_token token) {
		if (closed_) { co_return false; }
		const asio::error_code error = co_await task::await_asio<void(asio::error_code)>(
			[&](auto handler) { channel_.async_send(asio::error_code{}, std::move(chunk), std::move(handler)); }, std::move(token));
		co_return !error && !closed_;   // a send that a close completed counts as refused
	}
	// The body is complete: reads end with eof after the buffered chunks.
	void finish() { close(); }
	// The body failed: reads end with `error` after the buffered chunks.
	void fail(asio::error_code error) {
		if (!closed_) { error_ = error; }
		close();
	}
	// The handler is done with the body: a push waiting for room returns false.
	void abandon() {
		if (!closed_) { error_ = asio::error::operation_aborted; }
		close();
		channel_.cancel();
	}
	bool closed() const noexcept { return closed_; }

private:
	using Channel = asio::experimental::channel<void(asio::error_code, std::string)>;

	void close() {
		closed_ = true;
		channel_.close();
	}
	// Debug: read on the stream's loop thread, whatever scheduler wraps the task running there.
	void affine() const noexcept { assert(scheduler_.runs_here()); }

	task::asio_scheduler& scheduler_;
	Channel channel_;
	asio::error_code error_;
	bool closed_ = false;
};

}  // namespace http
