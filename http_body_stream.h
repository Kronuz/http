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
// channel as it arrives and the handler reads it with async_read(), so a multi-gigabyte
// RESTORE or bulk load runs in O(capacity * chunk) memory and back-pressure reaches the
// socket: when the channel is full the connection stops reading.
//
// async_read() is an ordinary Asio operation, so it works with any completion token:
//     std::string chunk;
//     auto ec = co_await body.async_read(chunk, asio::redirect_error(asio::use_awaitable, ec));
// or from a task::task through task::await_asio<void(asio::error_code)>. Everything runs
// on the connection's loop; a BodyStream is not thread-safe.

#pragma once

#include <asio.hpp>
#include <asio/experimental/channel.hpp>

#include <cstddef>
#include <string>
#include <utility>

namespace http {

class BodyStream {
public:
	BodyStream(asio::any_io_executor executor, std::size_t capacity)
		: channel_(std::move(executor), capacity != 0 ? capacity : 1) {}
	BodyStream(const BodyStream&) = delete;
	BodyStream& operator=(const BodyStream&) = delete;

	// Reads the next chunk into `chunk`. Completes with:
	//   no error                the chunk holds body bytes (never empty);
	//   asio::error::eof        the body ended; `chunk` is empty; the result is sticky;
	//   any other error         the body failed (the client closed mid-body, the service
	//                           is stopping, the handler read after it was abandoned).
	// Chunks already buffered are delivered before the end or the error. Cancelling the
	// operation completes it with operation_aborted and loses nothing.
	template <typename CompletionToken>
	auto async_read(std::string& chunk, CompletionToken&& token) {
		return asio::async_compose<CompletionToken, void(asio::error_code)>(
			ReadOp{this, &chunk}, token, channel_.get_executor());
	}

	// ---- the transport's side ----

	// Hands the next chunk to the reader, waiting while the channel is full. false when the
	// reader is gone (finish, fail or abandon was called, or the wait was cancelled).
	asio::awaitable<bool> push(std::string chunk) {
		if (closed_) { co_return false; }
		asio::error_code error;
		co_await channel_.async_send(asio::error_code{}, std::move(chunk), asio::redirect_error(asio::use_awaitable, error));
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

	struct ReadOp {
		BodyStream* stream;
		std::string* out;
		bool started = false;
		template <typename Self>
		void operator()(Self& self, asio::error_code error = {}, std::string chunk = {}) {
			if (!started) {
				started = true;
				stream->channel_.async_receive(std::move(self));
				return;
			}
			if (error == asio::experimental::error::channel_closed) {
				out->clear();
				error = stream->error_ ? stream->error_ : asio::error::eof;
			} else if (error) {
				out->clear();
				if (error == asio::experimental::error::channel_cancelled) { error = asio::error::operation_aborted; }
			} else {
				*out = std::move(chunk);
			}
			self.complete(error);
		}
	};

	Channel channel_;
	asio::error_code error_;
	bool closed_ = false;
};

}  // namespace http
