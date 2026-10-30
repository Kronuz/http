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

// The application seam. An HttpHandler turns a request into a response and knows
// nothing about parsing, sockets, or the event loop. The search engine is one
// HttpHandler; the demo is another.
//
// The handler writes its response through a ResponseWriter rather than filling a
// value: one interface serves both the buffered case (most endpoints: build a
// body, send it) and the streaming case (DUMP, large result sets: emit the body
// in chunks, without holding it all in memory). The writer hides the HTTP/1.1
// framing -- it picks Content-Length for a buffered body and chunked transfer
// encoding once a response streams.
//
// handle() is a coroutine awaited on the connection's loop. It may suspend (a timer, a
// task, a body read); while it does, the loop serves other connections. The request and
// the writer stay valid until the coroutine completes, and a request's frame is never
// destroyed early: a handler that sees Request::cancelled() returns, and the transport
// still writes its response. The scoped writer and request must not escape handle(). For
// delayed replies, transfer response authority with defer() and retain only the owned
// capability. A handler calls end() before it completes.
//
// A ready-made HttpHandler that dispatches by method + path -- the application's
// routing table -- lives in http_router.h (the radix-tree Router).

#pragma once

#include <exception>
#include <memory>
#include <string_view>
#include <stdexcept>
#include <string>
#include <utility>

#include <asio/awaitable.hpp>

#include "http_body_stream.h"
#include "http_message.h"
#include "http_deferred.h"
#include "http_owned_stream.h"

namespace http {

// Header admission rejects before consuming a body or invoking handle(). Keep
// the invalid_argument base so existing synchronous callers retain their catch.
enum class RejectionContent { Text, Json };
class RequestRejected : public std::invalid_argument {
	static std::string checked(int status, std::string_view body) {
		if (status < 400 || status > 599 || body.size() > 512) { throw std::invalid_argument("request rejection shape"); }
		return std::string(body);
	}
public:
	// retry_after_seconds > 0 adds a Retry-After header (an overload refusal).
	RequestRejected(int status, std::string_view body, RejectionContent content = RejectionContent::Text,
		unsigned retry_after_seconds = 0)
		: std::invalid_argument(checked(status, body)), status_(status), content_(content), retry_after_(retry_after_seconds) {}
	int status() const noexcept { return status_; }
	unsigned retry_after() const noexcept { return retry_after_; }
	const char* content_type() const noexcept { return content_ == RejectionContent::Json ? "application/json" : "text/plain"; }
private:
	int status_;
	RejectionContent content_;
	unsigned retry_after_;
};

// The output side of the seam. A handler sets the status and headers, then
// streams the body with one or more write() calls, then end(). The buffered
// conveniences (send / send(Response)) cover the common one-shot case.
class ResponseWriter {
public:
	virtual ~ResponseWriter() = default;

	// Set the status code. Valid until the first body byte is written.
	virtual void status(int code) = 0;
	// Add a response header. Valid until the first body byte is written.
	virtual void set_header(std::string_view name, std::string_view value) = 0;
	// Append body bytes. The first write commits the status + headers; a second
	// write (before end()) switches the response to chunked streaming.
	virtual void write(std::string_view chunk) = 0;
	// Finish the response. Required -- it is the completion signal.
	virtual void end() = 0;
	// Close the connection after this response (overrides keep-alive).
	virtual void set_close() = 0;
	// Transfer response authority to an owned capability, when supported.
	// Existing synchronous writers remain source-compatible.
	virtual std::unique_ptr<DeferredReply> defer(DeferredOptions) { return {}; }
	// Optional independently owned finite-body streaming capability.
	virtual OwnedStreamCapability stream(StreamOptions) { return {}; }

	// ---- buffered conveniences (non-virtual) ----
	void content_type(std::string_view ct) { set_header("Content-Type", ct); }

	// Send a complete buffered response in one call.
	void send(int code, std::string_view body, std::string_view ct = "text/plain; charset=utf-8") {
		status(code);
		content_type(ct);
		write(body);
		end();
	}

	// Send a fully-built Response value (the buffered path for handlers that
	// prefer to assemble a value before answering).
	void send(const Response& r) {
		status(r.status);
		content_type(r.content_type);
		for (const auto& [k, v] : r.headers) { set_header(k, v); }
		if (r.close) { set_close(); }
		write(r.body);
		end();
	}
};


// A capability a ResponseWriter can advertise: it retains what it was given (status,
// headers, body) so a middleware can read the response back by reference instead of
// capturing a second copy. A writer that fully buffers its response (e.g. to compute
// Content-Length) implements this at no extra cost; a streaming / non-buffering writer
// does not, and a middleware that needs the bytes falls back to its own capture. The
// returned views are valid until the response is serialized/sent, i.e. for as long as
// the writer is alive after handle() returns.
class BufferedResponse {
public:
	virtual ~BufferedResponse() = default;
	// The status set so far (the writer's default until the handler sets one).
	virtual int response_code() const = 0;
	// Whether the handler began a response at all (set a status or wrote a byte).
	virtual bool response_started() const = 0;
	// True after successful end(); legacy writers retain their started approximation.
	virtual bool response_ended() const { return response_started(); }
	// The response headers, by reference.
	virtual const Headers& response_headers() const = 0;
	// The retained response body, by reference.
	virtual std::string_view response_body() const = 0;
	// The Content-Type value (or the writer's default if the handler set none).
	virtual std::string_view response_content_type() const = 0;
};


// Optional opt-in streaming intake. An application returns a BodySink from
// HttpHandler::on_request_body() to receive the request body incrementally
// instead of having it buffered: the connection calls write() for each chunk as
// it arrives, then end(), then handle() (with Request::body left empty). This is
// for large or unbounded bodies -- an NDJSON bulk load, a RESTORE -- where
// holding the whole body in memory is wrong. The default is to buffer into
// Request::body. The application owns whatever framing (NDJSON line splitting,
// etc.) it layers on the chunks; the library stays format-agnostic.
class BodySink {
public:
	virtual ~BodySink() = default;
	virtual void write(std::string_view chunk) = 0;
	virtual void end() = 0;
};


class HttpHandler {
public:
	virtual ~HttpHandler() = default;

	// Handles one request on the connection's loop. Write the response through `response`
	// and call end() before completing. Suspending (co_await) is the way to wait; never
	// block the thread. A request cancelled by a client hang-up or by shutdown (see
	// Request::on_cancel) is still answered: reads stop at their next checkpoint, writes
	// run to completion, and the response is attempted.
	virtual asio::awaitable<void> handle(Request& request, ResponseWriter& response) = 0;

	// Opt in to streaming the request body: return a sink to receive it
	// incrementally, or nullptr (default) to have it buffered into Request::body.
	// Called once when the headers are complete, before any body byte, so the
	// decision can use the method, path, and headers (e.g. Content-Type). Ignored when
	// wants_body_stream() returns true (that path takes precedence). Sinks run inside the
	// parser on the loop, so keep their work light.
	virtual std::unique_ptr<BodySink> on_request_body(Request& /*request*/) { return nullptr; }

	// Opt in to a streamed body read with co_await (see BodyStream): return true and
	// handle() runs while the body is still arriving, reading it from
	// Request::body_stream with flow control -- O(buffer) memory for any size. Decided at
	// headers-complete (method/path/headers known). Default false: the body is buffered
	// into Request::body (up to the service's body cap, then 413) and handle() runs after
	// it is fully received. Takes precedence over on_request_body(). A handler that
	// returns without reading the whole body gets its response written, then the
	// connection closes.
	virtual bool wants_body_stream(const Request& /*request*/) { return false; }

	// Create this request's typed application extension (see RequestExtension), or
	// nullptr (default) for none. Called once at headers-complete -- before
	// on_request_body() and wants_body_stream() -- so an app can build its per-request
	// state (decoded body plumbing, parsed routes, timings) from the method/path/
	// headers and read it back in handle() via Request::ext<T>(). This is how an app
	// extends the request rather than carrying a parallel request object. It may throw
	// RequestRejected to refuse the request before its body is read.
	virtual std::unique_ptr<RequestExtension> create_extension(const Request& /*request*/) { return nullptr; }

	// Map an exception that escaped handle() to a response. The connection's backstop
	// calls this when handle() threw without having answered; the app rethrows the
	// exception_ptr to inspect the type and writes a status + body through `resp` (its
	// own error-to-status mapping). Anything it leaves unanswered falls back to a
	// generic 500, so an app only handles the exceptions it cares about. This keeps
	// error mapping an application concern while the transport owns the fallback.
	virtual void on_error(std::exception_ptr /*error*/, const Request& /*request*/, ResponseWriter& /*resp*/) {}
};

}  // namespace http
