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
// Forward-compatibility: the writer is also what makes the handler async-ready.
// end() is the completion signal, not "handle() returned" -- so a handler may
// hand the writer to a worker thread (the un-stallable model: heavy work never
// runs on the reactor) or, later, become a coroutine whose body is `co_await`ed,
// and the connection finishes the response when end() is finally called. Today
// handlers run synchronously and call end() inline; nothing here assumes that.
//
// A ready-made HttpHandler that dispatches by method + path -- the application's
// routing table -- lives in http_router.h (the radix-tree Router).

#pragma once

#include <exception>
#include <memory>
#include <string_view>
#include <utility>

#include "http_message.h"

namespace http {

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


// Concurrent streaming intake -- the reusable "true streaming" path. When a handler
// returns true from wants_body_stream(), the framework runs handle() on a worker
// *concurrently* with the body read and hands it a BodyReader to PULL raw body chunks
// from: read() blocks the worker until the next chunk arrives (or the body ends), while
// the reactor feeds chunks with flow control -- it suspends when the reader's bounded
// buffer is full, so a multi-gigabyte body flows through in O(buffer) memory, the
// reactor stays free, and back-pressure reaches the socket. The application parses and
// consumes the chunks in whatever format it owns (NDJSON, MsgPack, a file upload); the
// framework owns the transport, the concurrency, and the back-pressure. This is the
// abstract seam -- the transport provides the concrete (Asio-backed) implementation.
class BodyReader {
public:
	virtual ~BodyReader() = default;
	// Pull the next body chunk into `chunk`. Blocks until one is available; returns
	// false once the body is fully delivered. Call from the handler (on its worker
	// thread), never from the reactor.
	virtual bool read(std::string& chunk) = 0;
	// Unblock a pending read() (make it return false) -- the transport calls this on
	// shutdown so a handler stuck mid-stream doesn't wedge the join. Default no-op.
	virtual void abort() {}
};


class HttpHandler {
public:
	virtual ~HttpHandler() = default;
	virtual void handle(const Request& request, ResponseWriter& response) = 0;

	// Opt in to streaming the request body: return a sink to receive it
	// incrementally, or nullptr (default) to have it buffered into Request::body.
	// Called once when the headers are complete, before any body byte, so the
	// decision can use the method, path, and headers (e.g. Content-Type). Ignored when
	// wants_body_stream() returns true (that path takes precedence).
	virtual std::unique_ptr<BodySink> on_request_body(Request& /*request*/) { return nullptr; }

	// Opt in to CONCURRENT body streaming (the true-streaming path, for a large or
	// unbounded body consumed by heavy work -- indexing a RESTORE). Return true and the
	// framework runs handle() on a worker while it feeds the body, which the handler
	// PULLS from Request::body_reader (see BodyReader) with flow control -- O(buffer)
	// memory for any size. Decided at headers-complete (method/path/headers known).
	// Default false: the body buffers into Request::body (or an on_request_body sink)
	// and handle() runs after it is fully received. Takes precedence over
	// on_request_body(). A streaming handler MUST drain body_reader to completion.
	virtual bool wants_body_stream(const Request& /*request*/) { return false; }

	// Create this request's typed application extension (see RequestExtension), or
	// nullptr (default) for none. Called once at headers-complete -- before
	// on_request_body() and should_offload() -- so an app can build its per-request
	// state (decoded body plumbing, parsed routes, timings) from the method/path/
	// headers and read it back in handle() via Request::ext<T>(). This is how an app
	// extends the request rather than carrying a parallel request object.
	virtual std::unique_ptr<RequestExtension> create_extension(const Request& /*request*/) { return nullptr; }

	// Per-request offload policy (only consulted when a Dispatcher is configured).
	// Return true (default) to run this request on a worker thread -- correct for
	// heavy or potentially-blocking handlers (e.g. a search). Return false to run it
	// inline on the reactor: the cheap fast path, which skips the worker handoff for
	// trivial endpoints (a health check, a cached/metrics GET) so they are served
	// even while every worker is busy. A handler classified cheap MUST be cheap and
	// non-blocking -- it runs on the reactor and a slow one would stall the loop.
	virtual bool should_offload(const Request& /*request*/) const { return true; }

	// Map an exception that escaped handle() to a response. The connection's backstop
	// calls this when handle() threw without having answered; the app rethrows the
	// exception_ptr to inspect the type and writes a status + body through `resp` (its
	// own error-to-status mapping). Anything it leaves unanswered falls back to a
	// generic 500, so an app only handles the exceptions it cares about. This keeps
	// error mapping an application concern while the transport owns the fallback.
	virtual void on_error(std::exception_ptr /*error*/, const Request& /*request*/, ResponseWriter& /*resp*/) {}
};

}  // namespace http
