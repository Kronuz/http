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


class HttpHandler {
public:
	virtual ~HttpHandler() = default;
	virtual void handle(const Request& request, ResponseWriter& response) = 0;
};

}  // namespace http
